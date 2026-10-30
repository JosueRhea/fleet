// Package deploy implements the zero-downtime rollout from docs/architecture.md §5.3.
package deploy

import (
	"errors"
	"fmt"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

// HeartbeatTimeout bounds how long a step can stall on a dead worker before Temporal retries it elsewhere.
const HeartbeatTimeout = 10 * time.Second

// MaxReplacements caps how many batches a deploy re-places after losing their host mid-boot.
const MaxReplacements = 2

// PlaceAttempts bounds placement retries (1s backoff doubling to 30s: about two minutes).
const PlaceAttempts = 8

// HostLostType is the ApplicationError type WaitReady uses for ErrHostLost.
const HostLostType = "HostLost"

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
	// CanaryPercent > 0 first routes that share of traffic to one new instance for CanaryBake,
	// and rolls back before touching any old instance if its error rate is too high.
	CanaryPercent int
	CanaryBake    time.Duration
	// CanaryMinRequests is how many requests the canary must serve during CanaryBake to be judged;
	// with fewer it fails rather than being promoted untested. Default 20; negative disables.
	CanaryMinRequests int
	Secrets           int // secret set version pinned by this deployment (0 = none)
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
	if in.CanaryPercent > 0 && in.CanaryBake == 0 {
		in.CanaryBake = 30 * time.Second
	}
	if in.CanaryPercent > 0 && in.CanaryMinRequests == 0 {
		in.CanaryMinRequests = 20
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

	if in.CanaryPercent > 0 && len(in.Old) > 0 {
		if res, done := r.canary(); done {
			return res, nil
		}
	}

	replaced := 0
	for len(r.ready) < in.Replicas {
		if reason, ok := r.rollbackRequested(); ok {
			return r.rollBack("rollback requested: " + reason)
		}
		n := min(in.MaxSurge, in.Replicas-len(r.ready))
		var batch []Instance
		req := PlaceRequest{ServiceID: in.ServiceID, DeploymentID: in.DeploymentID, Image: in.Image, Secrets: in.Secrets, Count: n}
		if err := r.place(req, &batch); err != nil {
			return r.rollBack("placement failed: " + err.Error())
		}
		r.created = append(r.created, batch...)
		if err := r.hosts(HostChange{Start: batch}); err != nil {
			return r.rollBack("publishing new instances failed: " + err.Error())
		}
		if err := r.waitReady(batch); err != nil {
			var ae *temporal.ApplicationError
			if errors.As(err, &ae) && ae.Type() == HostLostType && replaced < MaxReplacements {
				replaced++
				workflow.GetLogger(ctx).Warn("host lost while booting; re-placing batch", "err", err)
				if err := r.hosts(HostChange{Stop: batch}); err != nil {
					return r.rollBack("stopping instances on a lost host failed: " + err.Error())
				}
				continue
			}
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

// place gives up after about two minutes (no live host, no capacity) instead of retrying forever,
// so the deploy rolls back with the reason rather than hanging.
func (r *rollout) place(req PlaceRequest, out *[]Instance) error {
	ctx := workflow.WithActivityOptions(r.ctx, workflow.ActivityOptions{
		StartToCloseTimeout: time.Minute,
		HeartbeatTimeout:    HeartbeatTimeout,
		RetryPolicy:         &temporal.RetryPolicy{InitialInterval: time.Second, MaximumInterval: 30 * time.Second, MaximumAttempts: PlaceAttempts},
	})
	return workflow.ExecuteActivity(ctx, a.PlaceInstances, req).Get(r.ctx, out)
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
	return r.publish(RouteUpdate{Active: active, Draining: draining})
}

func (r *rollout) publish(update RouteUpdate) error {
	var version int64
	update.ServiceID = r.in.ServiceID
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
	if err := r.routes(r.serving(), nil); err != nil { // drained: drop them from the edge doc
		return err
	}
	if err := r.hosts(HostChange{Sleep: old}); err != nil {
		return err
	}
	r.retired = append(r.retired, old...)
	return nil
}

// canary boots one new instance, gives it CanaryPercent of traffic for CanaryBake, then either
// promotes it to a full-weight backend (retiring one old instance) or rolls back.
func (r *rollout) canary() (Result, bool) {
	var batch []Instance
	req := PlaceRequest{ServiceID: r.in.ServiceID, DeploymentID: r.in.DeploymentID, Image: r.in.Image, Secrets: r.in.Secrets, Count: 1}
	fail := func(reason string) (Result, bool) {
		res, _ := r.rollBack(reason)
		return res, true
	}
	if err := r.place(req, &batch); err != nil {
		return fail("canary placement failed: " + err.Error())
	}
	r.created = append(r.created, batch...)
	if err := r.hosts(HostChange{Start: batch}); err != nil {
		return fail("publishing canary failed: " + err.Error())
	}
	if err := r.waitReady(batch); err != nil {
		return fail("canary never became ready: " + err.Error())
	}
	r.ready = batch
	if err := r.publish(RouteUpdate{Active: r.oldLeft, Canary: batch, CanaryPercent: r.in.CanaryPercent}); err != nil {
		return fail("routing canary failed: " + err.Error())
	}
	minRequests := 0
	if workflow.GetVersion(r.ctx, "canary-min-requests", workflow.DefaultVersion, 1) == 1 {
		minRequests = r.in.CanaryMinRequests
	}
	if reason, failed := r.watch(r.in.CanaryBake, minRequests); failed {
		return fail("canary: " + reason)
	}
	if err := r.routes(r.serving(), nil); err != nil {
		return fail("promoting canary failed: " + err.Error())
	}
	k := min(1, len(r.oldLeft))
	if len(r.ready) == r.in.Replicas {
		k = len(r.oldLeft)
	}
	retire := append([]Instance(nil), r.oldLeft[:k]...)
	r.oldLeft = r.oldLeft[k:]
	if err := r.retire(retire); err != nil {
		return fail("retiring old instances failed: " + err.Error())
	}
	return Result{}, false
}

func (r *rollout) rollbackRequested() (string, bool) {
	var reason string
	ok := r.rollbackCh.ReceiveAsync(&reason)
	return reason, ok
}

func (r *rollout) bake() (string, bool) { return r.watch(r.in.Bake, 0) }

// watch waits d (or a rollback signal), then checks the new deployment's edge error rate. With
// minRequests > 0, a deployment that served fewer requests than that fails: no traffic is no evidence.
func (r *rollout) watch(d time.Duration, minRequests int) (string, bool) {
	timerCtx, cancel := workflow.WithCancel(r.ctx)
	defer cancel()
	var reason string
	requested := false
	sel := workflow.NewSelector(r.ctx)
	sel.AddFuture(workflow.NewTimer(timerCtx, d), func(workflow.Future) {})
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
	// Errors are evidence even on a few requests (the edge ejects a failing backend quickly, so a
	// bad canary may serve only a handful); too few requests only blocks promotion.
	if h.ErrorRate > r.in.MaxErrorRate {
		return fmt.Sprintf("error rate %.1f%% above %.1f%%", h.ErrorRate*100, r.in.MaxErrorRate*100), true
	}
	if minRequests > 0 && h.Requests < int64(minRequests) {
		return fmt.Sprintf("served %d requests in %s, need %d to judge it", h.Requests, d, minRequests), true
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
		if err := r.routes(r.oldLeft, nil); err != nil {
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
