package deploy

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"
)

// FakeCluster is an in-memory Cluster that records every call. Used by tests and the demo
// worker until the real publisher/scheduler exist.
type FakeCluster struct {
	Delay          time.Duration
	FailReadyImage string // images containing this never become ready
	ErrorRate      float64
	Logf           func(format string, args ...any)
	LostReport     LostReport
	LoseHostOnce   bool // the first WaitReady reports the batch's host as lost

	mu      sync.Mutex
	events  []string
	next    int
	version int64
	images  map[string]string
}

func (f *FakeCluster) record(format string, args ...any) {
	f.mu.Lock()
	defer f.mu.Unlock()
	e := fmt.Sprintf(format, args...)
	f.events = append(f.events, e)
	if f.Logf != nil {
		f.Logf("%s", e)
	}
}

func (f *FakeCluster) Events() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.events...)
}

func (f *FakeCluster) Place(_ context.Context, req PlaceRequest) ([]Instance, error) {
	f.mu.Lock()
	out := make([]Instance, req.Count)
	for i := range out {
		f.next++
		out[i] = Instance{ID: fmt.Sprintf("ins-%s-%d", req.DeploymentID, f.next), Host: fmt.Sprintf("host-%d", f.next%3), Deployment: req.DeploymentID, Image: req.Image}
	}
	f.mu.Unlock()
	f.record("place %d", req.Count)
	return out, nil
}

func (f *FakeCluster) PublishHosts(_ context.Context, c HostChange) error {
	f.mu.Lock()
	if f.images == nil {
		f.images = map[string]string{}
	}
	for _, i := range c.Start {
		f.images[i.ID] = c.Image
		if i.Image != "" {
			f.images[i.ID] = i.Image
		}
	}
	f.mu.Unlock()
	for _, step := range []struct {
		verb string
		ins  []Instance
	}{{"start", c.Start}, {"wake", c.Wake}, {"sleep", c.Sleep}, {"stop", c.Stop}} {
		if len(step.ins) > 0 {
			f.record("%s %s", step.verb, ids(step.ins))
		}
	}
	return nil
}

func (f *FakeCluster) WaitReady(_ context.Context, ins []Instance, _ time.Duration) error {
	time.Sleep(f.Delay)
	f.mu.Lock()
	if f.LoseHostOnce {
		f.LoseHostOnce = false
		f.mu.Unlock()
		f.record("lost %s", ids(ins))
		return fmt.Errorf("%w: %s stopped reporting", ErrHostLost, ins[0].Host)
	}
	for _, i := range ins {
		if f.FailReadyImage != "" && strings.Contains(f.images[i.ID], f.FailReadyImage) {
			f.mu.Unlock()
			return fmt.Errorf("%w: %s failed its startup probe", ErrNotReady, i.ID)
		}
	}
	f.mu.Unlock()
	f.record("ready %s", ids(ins))
	return nil
}

func (f *FakeCluster) PublishRoutes(_ context.Context, u RouteUpdate) (int64, error) {
	f.mu.Lock()
	f.version++
	v := f.version
	f.mu.Unlock()
	f.record("routes v%d active=%s draining=%s", v, ids(u.Active), ids(u.Draining))
	return v, nil
}

func (f *FakeCluster) WaitEdgeAck(_ context.Context, version int64) error {
	f.record("ack v%d", version)
	return nil
}

func (f *FakeCluster) WaitDrained(_ context.Context, ins []Instance, _ time.Duration) error {
	time.Sleep(f.Delay)
	f.record("drained %s", ids(ins))
	return nil
}

func (f *FakeCluster) Health(context.Context, string, string) (HealthReport, error) {
	return HealthReport{ErrorRate: f.ErrorRate}, nil
}

func (f *FakeCluster) Lost(context.Context, string) (LostReport, error) {
	f.record("find lost")
	return f.LostReport, nil
}

func ids(ins []Instance) string {
	s := make([]string, len(ins))
	for i, x := range ins {
		s[i] = x.ID
	}
	return strings.Join(s, ",")
}
