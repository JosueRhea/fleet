// Package deploy implements the zero-downtime rollout from docs/architecture.md §5.3.
package deploy

import (
	"fmt"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

// HeartbeatTimeout bounds how long a step can stall on a dead worker before Temporal retries it elsewhere.
const HeartbeatTimeout = 10 * time.Second

const (
	TaskQueue      = "ccloud-deploy"
	RollbackSignal = "rollback"
)

type Input struct {
	ServiceID    string
	DeploymentID string
	Image        string
	Replicas     int
	MaxSurge     int
	Old          []Instance // instances of the deployment being replaced
	ReadyTimeout time.Duration
	DrainTimeout time.Duration
	Bake         time.Duration
	MaxErrorRate float64
}

func (in *Input) applyDefaults() {
	if in.MaxSurge < 1 {
		in.MaxSurge = 1
	}
	if in.ReadyTimeout == 0 {
		in.ReadyTimeout = 5 * time.Minute
	}
	if in.DrainTimeout == 0 {
		in.DrainTimeout = 30 * time.Second
	}
	if in.Bake == 0 {
		in.Bake = 2 * time.Minute
	}
	if in.MaxErrorRate == 0 {
		in.MaxErrorRate = 0.05
	}
}

type Status string

const (
	StatusActive     Status = "active"
	StatusRolledBack Status = "rolled_back"
	// StatusFailed means rollback itself could not finish; traffic was left on whatever was serving.
	StatusFailed Status = "failed"
)

type Result struct {
	Status Status
	Active []Instance
	Reason string
}

var a *Activities

// RollingDeploy replaces Old with Replicas instances of a new deployment, MaxSurge at a time.
// Invariants: an old instance drains only after its replacement is Ready and an edge-acked route
// version contains it; old instances sleep (not stop) until the bake passes so rollback is a wake.
func RollingDeploy(ctx workflow.Context, in Input) (Result, error) {
	in.applyDefaults()
	r := &rollout{
		in:  in,
		ctx: ctx,
		short: workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
			StartToCloseTimeout: time.Minute,
			HeartbeatTimeout:    HeartbeatTimeout,
			RetryPolicy:         &temporal.RetryPolicy{InitialInterval: time.Second, MaximumInterval: 30 * time.Second},
		}),
		rollbackCh: workflow.GetSignalChannel(ctx, RollbackSignal),
		oldLeft:    append([]Instance(nil), in.Old...),
	}

	for len(r.ready) < in.Replicas {
		if reason, ok := r.rollbackRequested(); ok {
			return r.rollBack("rollback requested: " + reason)
		}
		n := min(in.MaxSurge, in.Replicas-len(r.ready))
		var batch []Instance
		req := PlaceRequest{ServiceID: in.ServiceID, DeploymentID: in.DeploymentID, Image: in.Image, Count: n}
		if err := workflow.ExecuteActivity(r.short, a.PlaceInstances, req).Get(ctx, &batch); err != nil {
			return r.rollBack("placement failed: " + err.Error())
		}
		r.created = append(r.created, batch...)
		if err := r.hosts(HostChange{Start: batch}); err != nil {
			return r.rollBack("publishing new instances failed: " + err.Error())
		}
		if err := r.waitReady(batch); err != nil {
			return r.rollBack("new instances never became ready: " + err.Error())
		}
		r.ready = append(r.ready, batch...)
		if err := r.routes(r.serving(), nil); err != nil {
			return r.rollBack("routing new instances failed: " + err.Error())
		}
		k := min(n, len(r.oldLeft))
		if len(r.ready) == in.Replicas {
			k = len(r.oldLeft) // scaling down: retire any extra old instances on the last batch
		}
		if k > 0 {
			retire := append([]Instance(nil), r.oldLeft[:k]...)
			r.oldLeft = r.oldLeft[k:]
			if err := r.retire(retire); err != nil {
				return r.rollBack("retiring old instances failed: " + err.Error())
			}
		}
	}

	if reason, failed := r.bake(); failed {
		return r.rollBack(reason)
	}
	if len(r.retired) > 0 {
		if err := r.hosts(HostChange{Stop: r.retired}); err != nil {
			return Result{}, err
		}
	}
	return Result{Status: StatusActive, Active: r.ready}, nil
}

type rollout struct {
	in         Input
	ctx        workflow.Context
	short      workflow.Context
	rollbackCh workflow.ReceiveChannel
	created    []Instance // new instances started
	ready      []Instance // new instances that passed readiness
	oldLeft    []Instance // old instances still serving
	retired    []Instance // old instances drained and asleep
}

func (r *rollout) serving() []Instance {
	return append(append([]Instance(nil), r.ready...), r.oldLeft...)
}

func (r *rollout) waitCtx(d time.Duration) workflow.Context {
	return workflow.WithActivityOptions(r.ctx, workflow.ActivityOptions{
		StartToCloseTimeout: d + 30*time.Second,
		HeartbeatTimeout:    HeartbeatTimeout,
		RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 3},
	})
}

func (r *rollout) hosts(c HostChange) error {
	c.Image = r.in.Image
	return workflow.ExecuteActivity(r.short, a.PublishHostChange, c).Get(r.ctx, nil)
}

func (r *rollout) waitReady(ins []Instance) error {
	return workflow.ExecuteActivity(r.waitCtx(r.in.ReadyTimeout), a.WaitReady, ins, r.in.ReadyTimeout).Get(r.ctx, nil)
}

// routes publishes the service's backend set and blocks until edges have applied that version.
func (r *rollout) routes(active, draining []Instance) error {
	var version int64
	update := RouteUpdate{ServiceID: r.in.ServiceID, Active: active, Draining: draining}
	if err := workflow.ExecuteActivity(r.short, a.PublishRoutes, update).Get(r.ctx, &version); err != nil {
		return err
	}
	return workflow.ExecuteActivity(r.waitCtx(time.Minute), a.WaitEdgeAck, version).Get(r.ctx, nil)
}

func (r *rollout) retire(old []Instance) error {
	if err := r.routes(r.serving(), old); err != nil {
		return err
	}
	if err := workflow.ExecuteActivity(r.waitCtx(r.in.DrainTimeout), a.WaitDrained, old, r.in.DrainTimeout).Get(r.ctx, nil); err != nil {
		return err
	}
	if err := r.hosts(HostChange{Sleep: old}); err != nil {
		return err
	}
	r.retired = append(r.retired, old...)
	return nil
}

func (r *rollout) rollbackRequested() (string, bool) {
	var reason string
	ok := r.rollbackCh.ReceiveAsync(&reason)
	return reason, ok
}

func (r *rollout) bake() (string, bool) {
	timerCtx, cancel := workflow.WithCancel(r.ctx)
	defer cancel()
	var reason string
	requested := false
	sel := workflow.NewSelector(r.ctx)
	sel.AddFuture(workflow.NewTimer(timerCtx, r.in.Bake), func(workflow.Future) {})
	sel.AddReceive(r.rollbackCh, func(c workflow.ReceiveChannel, _ bool) {
		c.Receive(r.ctx, &reason)
		requested = true
	})
	sel.Select(r.ctx)
	if requested {
		return "rollback requested: " + reason, true
	}
	var h HealthReport
	if err := workflow.ExecuteActivity(r.short, a.CheckHealth, r.in.ServiceID, r.in.DeploymentID).Get(r.ctx, &h); err != nil {
		return "health check failed: " + err.Error(), true
	}
	if h.ErrorRate > r.in.MaxErrorRate {
		return fmt.Sprintf("error rate %.1f%% above %.1f%%", h.ErrorRate*100, r.in.MaxErrorRate*100), true
	}
	return "", false
}

// rollBack wakes retired old instances, routes back to them, then drains and stops the new ones.
// If any step fails it stops touching routes, so traffic stays on whatever is serving.
func (r *rollout) rollBack(reason string) (Result, error) {
	workflow.GetLogger(r.ctx).Warn("rolling back", "reason", reason)
	fail := func(err error) (Result, error) {
		return Result{Status: StatusFailed, Active: r.serving(), Reason: reason + "; rollback incomplete: " + err.Error()}, nil
	}
	if len(r.retired) > 0 {
		if err := r.hosts(HostChange{Wake: r.retired}); err != nil {
			return fail(err)
		}
		if err := r.waitReady(r.retired); err != nil {
			return fail(err)
		}
		r.oldLeft = append(r.oldLeft, r.retired...)
		r.retired = nil
	}
	if len(r.ready) > 0 {
		if err := r.routes(r.oldLeft, r.ready); err != nil {
			return fail(err)
		}
		if err := workflow.ExecuteActivity(r.waitCtx(r.in.DrainTimeout), a.WaitDrained, r.ready, r.in.DrainTimeout).Get(r.ctx, nil); err != nil {
			return fail(err)
		}
	}
	if len(r.created) > 0 {
		if err := r.hosts(HostChange{Stop: r.created}); err != nil {
			return fail(err)
		}
	}
	return Result{Status: StatusRolledBack, Active: r.oldLeft, Reason: reason}, nil
}
