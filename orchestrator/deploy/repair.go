package deploy

import (
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

type RepairInput struct {
	ServiceID    string
	ReadyTimeout time.Duration
}

type RepairResult struct {
	Replaced []Instance // lost instances that were replaced
	Active   []Instance
	Reason   string // set when the replacements could not be brought up
}

// Repair replaces a service's instances on hosts that stopped reporting. It runs under the same
// workflow ID as deploys ("deploy-<service>"), so it never races a rollout.
// Order: route around the lost instances, start replacements (same deployment and image) on live
// hosts, gate on Ready, route to them, then tombstone the lost ones so a returning host stops them.
func Repair(ctx workflow.Context, in RepairInput) (RepairResult, error) {
	if in.ReadyTimeout == 0 {
		in.ReadyTimeout = 2 * time.Minute
	}
	r := &rollout{
		in:  Input{ServiceID: in.ServiceID, ReadyTimeout: in.ReadyTimeout},
		ctx: ctx,
		short: workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
			StartToCloseTimeout: time.Minute,
			HeartbeatTimeout:    HeartbeatTimeout,
			RetryPolicy:         &temporal.RetryPolicy{InitialInterval: time.Second, MaximumInterval: 30 * time.Second},
		}),
	}
	var rep LostReport
	if err := workflow.ExecuteActivity(r.short, a.FindLost, in.ServiceID).Get(ctx, &rep); err != nil {
		return RepairResult{}, err
	}
	if len(rep.Lost) == 0 {
		return RepairResult{Active: rep.Healthy}, nil
	}
	if len(rep.Healthy) > 0 {
		if err := r.routes(rep.Healthy, nil); err != nil {
			return RepairResult{}, err
		}
	}
	type group struct {
		deployment, image string
		secrets           int
	}
	counts := map[group]int{}
	var order []group
	for _, l := range rep.Lost {
		g := group{l.Deployment, l.Image, l.Secrets}
		if counts[g] == 0 {
			order = append(order, g)
		}
		counts[g]++
	}
	var fresh []Instance
	for _, g := range order {
		var batch []Instance
		req := PlaceRequest{ServiceID: in.ServiceID, DeploymentID: g.deployment, Image: g.image, Secrets: g.secrets, Count: counts[g]}
		if err := r.place(req, &batch); err != nil {
			return RepairResult{Active: rep.Healthy, Reason: "placement failed: " + err.Error()}, nil
		}
		fresh = append(fresh, batch...)
	}
	if err := r.hosts(HostChange{Start: fresh}); err != nil {
		return RepairResult{}, err
	}
	if err := r.waitReady(fresh); err != nil {
		if err := r.hosts(HostChange{Stop: fresh}); err != nil {
			return RepairResult{}, err
		}
		return RepairResult{Active: rep.Healthy, Reason: "replacements never became ready: " + err.Error()}, nil
	}
	active := append(append([]Instance(nil), rep.Healthy...), fresh...)
	if err := r.routes(active, nil); err != nil {
		return RepairResult{}, err
	}
	if err := r.hosts(HostChange{Stop: rep.Lost}); err != nil {
		return RepairResult{}, err
	}
	return RepairResult{Replaced: rep.Lost, Active: active}, nil
}
