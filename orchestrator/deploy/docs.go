package deploy

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// ServiceConfig is the per-service config the cell API will own.
type ServiceConfig struct {
	Host       string            `json:"host"` // edge hostname
	Port       int               `json:"port"` // port the app listens on in the VM
	HealthPath string            `json:"health_path,omitempty"`
	MemoryMiB  int               `json:"memory_mib,omitempty"`
	Env        map[string]string `json:"env,omitempty"`
}

// Docs is the Cluster backed by a real hostd agent and edge proxy. It writes their desired-state
// docs (host.json, edge.json) and reads their status reports (host-status.json, edge-status.json)
// in Dir. Dir stands in for the publisher's bucket and status ingest on a single host.
type Docs struct {
	Dir      string
	Host     string
	Services map[string]ServiceConfig
	Poll     time.Duration
	mu       sync.Mutex
}

func NewDocs(dir, host string) (*Docs, error) {
	d := &Docs{Dir: dir, Host: host, Poll: 200 * time.Millisecond}
	b, err := os.ReadFile(filepath.Join(dir, "services.json"))
	if err != nil {
		return nil, fmt.Errorf("service config: %w", err)
	}
	if err := json.Unmarshal(b, &d.Services); err != nil {
		return nil, fmt.Errorf("service config: %w", err)
	}
	return d, nil
}

type health struct {
	Path string `json:"path,omitempty"`
}

type instanceSpec struct {
	ID         string            `json:"id"`
	Service    string            `json:"service"`
	Deployment string            `json:"deployment"`
	Image      string            `json:"image"`
	Port       int               `json:"port"`
	MemoryMiB  int               `json:"memory_mib,omitempty"`
	Env        map[string]string `json:"env,omitempty"`
	Health     health            `json:"health"`
	Desired    string            `json:"desired"`
}

type hostDoc struct {
	Schema     int            `json:"schema"`
	Version    int64          `json:"version"`
	Instances  []instanceSpec `json:"instances"`
	Tombstones []string       `json:"tombstones"`
}

type instanceStatus struct {
	ID         string `json:"id"`
	Deployment string `json:"deployment"`
	State      string `json:"state"`
	Addr       string `json:"addr"`
	Error      string `json:"error"`
}

type hostStatus struct {
	AppliedVersion int64            `json:"applied_version"`
	Instances      []instanceStatus `json:"instances"`
}

type backend struct {
	Instance   string `json:"instance"`
	Deployment string `json:"deployment"`
	Addr       string `json:"addr"`
	Weight     int    `json:"weight"`
	State      string `json:"state"`
}

type route struct {
	Host     string    `json:"host"`
	Service  string    `json:"service"`
	Backends []backend `json:"backends"`
}

type edgeDoc struct {
	Schema     int      `json:"schema"`
	Version    int64    `json:"version"`
	Routes     []route  `json:"routes"`
	Tombstones []string `json:"tombstones"`
}

type backendStats struct {
	Instance   string `json:"instance"`
	Deployment string `json:"deployment"`
	InFlight   int64  `json:"in_flight"`
	Requests   int64  `json:"requests"`
	Errors     int64  `json:"errors"`
}

type edgeStatus struct {
	AppliedVersion int64          `json:"applied_version"`
	Backends       []backendStats `json:"backends"`
}

func (d *Docs) read(name string, v any) error {
	b, err := os.ReadFile(filepath.Join(d.Dir, name))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	return json.Unmarshal(b, v)
}

func (d *Docs) write(name string, v any) error {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	p := filepath.Join(d.Dir, name)
	if err := os.WriteFile(p+".tmp", b, 0o644); err != nil {
		return err
	}
	return os.Rename(p+".tmp", p)
}

// nextVersion is monotonic even if a doc file is lost: consumers ignore versions <= what they applied.
func nextVersion(prev int64) int64 {
	return max(prev+1, time.Now().UnixMicro())
}

func (d *Docs) config(service string) (ServiceConfig, error) {
	c, ok := d.Services[service]
	if !ok {
		return c, fmt.Errorf("unknown service %q", service)
	}
	return c, nil
}

func (d *Docs) Place(_ context.Context, req PlaceRequest) ([]Instance, error) {
	if _, err := d.config(req.ServiceID); err != nil {
		return nil, err
	}
	out := make([]Instance, req.Count)
	for i := range out {
		b := make([]byte, 3)
		_, _ = rand.Read(b)
		out[i] = Instance{
			ID:         fmt.Sprintf("%s-%s", req.DeploymentID, hex.EncodeToString(b)),
			Host:       d.Host,
			Deployment: req.DeploymentID,
			Service:    req.ServiceID,
			Image:      req.Image,
		}
	}
	return out, nil
}

func (d *Docs) PublishHosts(_ context.Context, c HostChange) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	var doc hostDoc
	if err := d.read("host.json", &doc); err != nil {
		return err
	}
	byID := map[string]int{}
	for i, s := range doc.Instances {
		byID[s.ID] = i
	}
	for _, in := range c.Start {
		cfg, err := d.config(in.Service)
		if err != nil {
			return err
		}
		img := in.Image
		if img == "" {
			img = c.Image
		}
		doc.Instances = append(doc.Instances, instanceSpec{
			ID: in.ID, Service: in.Service, Deployment: in.Deployment, Image: img, Port: cfg.Port,
			MemoryMiB: cfg.MemoryMiB, Env: cfg.Env, Health: health{Path: cfg.HealthPath}, Desired: "running",
		})
	}
	set := func(ins []Instance, desired string) {
		for _, in := range ins {
			if i, ok := byID[in.ID]; ok {
				doc.Instances[i].Desired = desired
			}
		}
	}
	set(c.Sleep, "sleeping")
	set(c.Wake, "running")
	if len(c.Stop) > 0 {
		stop := map[string]bool{}
		for _, in := range c.Stop {
			stop[in.ID] = true
			doc.Tombstones = append(doc.Tombstones, in.ID)
		}
		kept := doc.Instances[:0]
		for _, s := range doc.Instances {
			if !stop[s.ID] {
				kept = append(kept, s)
			}
		}
		doc.Instances = kept
	}
	doc.Schema, doc.Version = 1, nextVersion(doc.Version)
	if doc.Instances == nil {
		doc.Instances = []instanceSpec{}
	}
	if doc.Tombstones == nil {
		doc.Tombstones = []string{}
	}
	return d.write("host.json", doc)
}

func (d *Docs) hostStatus() (map[string]instanceStatus, error) {
	var st hostStatus
	if err := d.read("host-status.json", &st); err != nil {
		return nil, err
	}
	m := map[string]instanceStatus{}
	for _, s := range st.Instances {
		m[s.ID] = s
	}
	return m, nil
}

func (d *Docs) poll(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(d.Poll):
		return nil
	}
}

// WaitReady passes once hostd reports every instance Ready (its readiness probe passed).
func (d *Docs) WaitReady(ctx context.Context, ins []Instance, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		st, err := d.hostStatus()
		if err != nil {
			return err
		}
		ready := 0
		for _, in := range ins {
			s := st[in.ID]
			if s.State == "failed" {
				return fmt.Errorf("%w: %s failed: %.300s", ErrNotReady, in.ID, s.Error)
			}
			if s.State == "ready" {
				ready++
			}
		}
		if ready == len(ins) {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("%w: %d/%d ready after %s", ErrNotReady, ready, len(ins), timeout)
		}
		if err := d.poll(ctx); err != nil {
			return err
		}
	}
}

func (d *Docs) PublishRoutes(_ context.Context, u RouteUpdate) (int64, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	cfg, err := d.config(u.ServiceID)
	if err != nil {
		return 0, err
	}
	st, err := d.hostStatus()
	if err != nil {
		return 0, err
	}
	backends := []backend{}
	add := func(ins []Instance, weight int, state string) error {
		for _, in := range ins {
			addr := st[in.ID].Addr
			if addr == "" {
				return fmt.Errorf("no address reported for %s", in.ID)
			}
			backends = append(backends, backend{Instance: in.ID, Deployment: in.Deployment, Addr: addr, Weight: weight, State: state})
		}
		return nil
	}
	if err := add(u.Active, 100, "active"); err != nil {
		return 0, err
	}
	if err := add(u.Draining, 0, "draining"); err != nil {
		return 0, err
	}
	var doc edgeDoc
	if err := d.read("edge.json", &doc); err != nil {
		return 0, err
	}
	r := route{Host: cfg.Host, Service: u.ServiceID, Backends: backends}
	replaced := false
	for i := range doc.Routes {
		if doc.Routes[i].Host == cfg.Host {
			doc.Routes[i], replaced = r, true
		}
	}
	if !replaced {
		doc.Routes = append(doc.Routes, r)
	}
	doc.Schema, doc.Version = 1, nextVersion(doc.Version)
	if doc.Tombstones == nil {
		doc.Tombstones = []string{}
	}
	return doc.Version, d.write("edge.json", doc)
}

func (d *Docs) edgeStatus() (edgeStatus, error) {
	var st edgeStatus
	err := d.read("edge-status.json", &st)
	return st, err
}

// WaitEdgeAck passes once the edge reports it is serving routes version >= version.
func (d *Docs) WaitEdgeAck(ctx context.Context, version int64) error {
	for {
		st, err := d.edgeStatus()
		if err != nil {
			return err
		}
		if st.AppliedVersion >= version {
			return nil
		}
		if err := d.poll(ctx); err != nil {
			return err
		}
	}
}

// WaitDrained waits for the edge's in-flight requests to these instances to reach zero.
// On timeout it returns nil: the drain window is a bound, not a gate.
func (d *Docs) WaitDrained(ctx context.Context, ins []Instance, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	want := map[string]bool{}
	for _, in := range ins {
		want[in.ID] = true
	}
	for {
		st, err := d.edgeStatus()
		if err != nil {
			return err
		}
		var busy int64
		for _, b := range st.Backends {
			if want[b.Instance] {
				busy += b.InFlight
			}
		}
		if busy == 0 || time.Now().After(deadline) {
			return nil
		}
		if err := d.poll(ctx); err != nil {
			return err
		}
	}
}

// Health is the edge-observed error rate (5xx + upstream failures) of the deployment.
func (d *Docs) Health(_ context.Context, _, deploymentID string) (HealthReport, error) {
	st, err := d.edgeStatus()
	if err != nil {
		return HealthReport{}, err
	}
	var req, errs int64
	for _, b := range st.Backends {
		if b.Deployment == deploymentID {
			req += b.Requests
			errs += b.Errors
		}
	}
	if req == 0 {
		return HealthReport{}, nil
	}
	return HealthReport{ErrorRate: float64(errs) / float64(req)}, nil
}

// Current returns the instances the edge routes to for service: what a new deploy replaces.
func (d *Docs) Current(service string) ([]Instance, error) {
	cfg, err := d.config(service)
	if err != nil {
		return nil, err
	}
	var doc edgeDoc
	if err := d.read("edge.json", &doc); err != nil {
		return nil, err
	}
	var out []Instance
	for _, r := range doc.Routes {
		if r.Host != cfg.Host {
			continue
		}
		for _, b := range r.Backends {
			if b.State == "active" {
				out = append(out, Instance{ID: b.Instance, Host: d.Host, Deployment: b.Deployment, Service: service})
			}
		}
	}
	return out, nil
}
