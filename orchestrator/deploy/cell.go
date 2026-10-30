package deploy

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/JosueRhea/fleet/orchestrator/cellstate"
	"go.temporal.io/sdk/activity"
)

// ServiceConfig is the per-service config the cell API will own.
type ServiceConfig struct {
	Host       string            `json:"host"` // edge hostname
	Port       int               `json:"port"` // port the app listens on in the VM
	HealthPath string            `json:"health_path,omitempty"`
	MemoryMiB  int               `json:"memory_mib,omitempty"`
	Env        map[string]string `json:"env,omitempty"`
}

// StaleAfter: a host or edge that has not reported for this long is treated as gone.
const StaleAfter = 10 * time.Second

// Cell is the Cluster for one cell: it is the publisher (signed host and edge docs in the state
// bucket) and reads the status reports hostd agents and edges write to the same bucket.
type Cell struct {
	Store    *cellstate.Store
	Signer   *cellstate.Signer
	Services map[string]ServiceConfig
	Poll     time.Duration
	// SecretsKey opens stored secret sets so they can be re-sealed to the host that runs an instance.
	SecretsKey *cellstate.BoxKey
	mu         sync.Mutex

	statusMu   sync.Mutex
	lastStatus map[string]hostStatus // last report per host that verified
}

// sealSecrets opens secret set version of service with the cell key and seals it to host's
// registered box key (never the one in its unsigned status report). The plaintext only exists in
// this process's memory.
func (c *Cell) sealSecrets(ctx context.Context, reg cellstate.HostRegistry, service string, version int, host string) (string, error) {
	if c.SecretsKey == nil {
		return "", fmt.Errorf("%w: worker has no -secrets-key", ErrSecrets)
	}
	rh, ok := reg.Hosts[host]
	if !ok {
		return "", fmt.Errorf("%w: host %s is not registered", ErrSecrets, host)
	}
	pub, err := cellstate.ParseKey32(rh.BoxPub)
	if err != nil {
		return "", fmt.Errorf("%w: host %s registered box key: %v", ErrSecrets, host, err)
	}
	b, err := c.Store.Get(ctx, cellstate.SecretsKey(service, version))
	if err != nil {
		return "", err
	}
	if b == nil {
		return "", fmt.Errorf("%w: secret set %s v%d does not exist", ErrSecrets, service, version)
	}
	env, err := c.SecretsKey.OpenEnv(string(b))
	if err != nil {
		return "", fmt.Errorf("%w: %v", ErrSecrets, err)
	}
	return cellstate.SealEnv(env, pub)
}

func NewCell(store *cellstate.Store, signer *cellstate.Signer, servicesPath string) (*Cell, error) {
	c := &Cell{Store: store, Signer: signer, Poll: 200 * time.Millisecond}
	b, err := os.ReadFile(servicesPath)
	if err != nil {
		return nil, fmt.Errorf("service config: %w", err)
	}
	if err := json.Unmarshal(b, &c.Services); err != nil {
		return nil, fmt.Errorf("service config: %w", err)
	}
	return c, nil
}

func (c *Cell) config(service string) (ServiceConfig, error) {
	s, ok := c.Services[service]
	if !ok {
		return s, fmt.Errorf("unknown service %q", service)
	}
	if s.MemoryMiB == 0 {
		s.MemoryMiB = 256
	}
	return s, nil
}

// readDoc loads the doc this publisher last wrote; a doc that fails verification is an error,
// never a reason to start from empty.
func (c *Cell) readDoc(ctx context.Context, key string, v any) error {
	b, err := c.Store.Get(ctx, key)
	if err != nil || b == nil {
		return err
	}
	if err := c.Signer.Open(b, v); err != nil {
		return fmt.Errorf("%s: %w", key, err)
	}
	return nil
}

func (c *Cell) writeDoc(ctx context.Context, key string, v any) error {
	b, err := c.Signer.Seal(v)
	if err != nil {
		return err
	}
	return c.Store.Put(ctx, key, b)
}

// nextVersion is monotonic even if a doc is lost: consumers ignore versions <= what they applied.
func nextVersion(prev int64) int64 {
	return max(prev+1, time.Now().UnixMicro())
}

func fresh(reportedAt int64) bool {
	return time.Since(time.Unix(reportedAt, 0)) <= StaleAfter
}

// parseHostStatus decodes the report stored under host id's key. A host registered with a sign key
// must have signed it; one registered without (from before hosts signed) is trusted as written.
func parseHostStatus(id string, b []byte, reg cellstate.HostRegistry) (hostStatus, bool) {
	var st hostStatus
	var env cellstate.Envelope
	signed := json.Unmarshal(b, &env) == nil && len(env.Sig) > 0
	rh := reg.Hosts[id]
	switch {
	case rh.SignPub != "":
		if !signed || cellstate.VerifyEnvelope(rh.SignPub, b, &st) != nil {
			return st, false
		}
	case signed:
		if json.Unmarshal(env.Payload, &st) != nil {
			return st, false
		}
	default:
		if json.Unmarshal(b, &st) != nil {
			return st, false
		}
	}
	// Each host writes its own key; a report under one key that names another host is ignored.
	return st, st.HostID == id
}

// hosts returns each host's latest trusted status report. A report that does not verify (forged,
// or written over by anyone with bucket access) is ignored, and the host's last verified report
// stands, so it goes stale like a host that stopped reporting: never a reason to act at once.
func (c *Cell) hosts(ctx context.Context) (map[string]hostStatus, error) {
	objs, err := c.Store.List(ctx, cellstate.HostStatusPrefix)
	if err != nil {
		return nil, err
	}
	reg, err := c.registry(ctx)
	if err != nil {
		return nil, err
	}
	c.statusMu.Lock()
	defer c.statusMu.Unlock()
	if c.lastStatus == nil {
		c.lastStatus = map[string]hostStatus{}
	}
	for k, b := range objs {
		id, ok := strings.CutSuffix(strings.TrimPrefix(k, cellstate.HostStatusPrefix), ".json")
		if !ok || strings.Contains(id, "/") {
			continue // not a status report (List is recursive)
		}
		if st, ok := parseHostStatus(id, b, reg); ok {
			c.lastStatus[id] = st
		}
	}
	out := make(map[string]hostStatus, len(c.lastStatus))
	for id, st := range c.lastStatus {
		out[id] = st
	}
	return out, nil
}

func (c *Cell) registry(ctx context.Context) (cellstate.HostRegistry, error) {
	return cellstate.ReadRegistry(ctx, c.Store, c.Signer)
}

func (c *Cell) edges(ctx context.Context) ([]edgeStatus, error) {
	objs, err := c.Store.List(ctx, cellstate.EdgeStatusPrefix)
	if err != nil {
		return nil, err
	}
	var out []edgeStatus
	for k, b := range objs {
		var st edgeStatus
		if err := json.Unmarshal(b, &st); err != nil {
			return nil, fmt.Errorf("%s: %w", k, err)
		}
		if fresh(st.ReportedAt) {
			out = append(out, st)
		}
	}
	return out, nil
}

// Place spreads a deployment's instances across live registered hosts (fewest of this deployment
// first), then by most free memory. It fails rather than overcommit. With secrets, a host only
// qualifies if the box key it reports is the registered one, i.e. it can open what is sealed to it.
func (c *Cell) Place(ctx context.Context, req PlaceRequest) ([]Instance, error) {
	cfg, err := c.config(req.ServiceID)
	if err != nil {
		return nil, err
	}
	hs, err := c.hosts(ctx)
	if err != nil {
		return nil, err
	}
	reg, err := c.registry(ctx)
	if err != nil {
		return nil, err
	}
	var cands []*placeCand
	for id, h := range hs {
		rh, registered := reg.Hosts[id]
		if !registered || !fresh(h.ReportedAt) || (req.Secrets > 0 && h.BoxPub != rh.BoxPub) {
			continue
		}
		// Count the deployment's instances already there: rolling deploys place one batch per
		// call, and without this every batch would pick the same (emptiest) host.
		mine := 0
		for _, in := range h.Instances {
			if in.Deployment == req.DeploymentID && in.State != "failed" {
				mine++
			}
		}
		cands = append(cands, &placeCand{id: id, free: h.MemoryTotal - h.MemoryUsed, mine: mine})
	}
	if len(cands) == 0 {
		return nil, fmt.Errorf("no live registered hosts")
	}
	hosts, err := spread(cands, req.Count, cfg.MemoryMiB)
	if err != nil {
		return nil, err
	}
	out := make([]Instance, req.Count)
	for i, host := range hosts {
		b := make([]byte, 3)
		_, _ = rand.Read(b)
		out[i] = Instance{
			ID:         fmt.Sprintf("%s-%s", req.DeploymentID, hex.EncodeToString(b)),
			Host:       host,
			Deployment: req.DeploymentID,
			Service:    req.ServiceID,
			Image:      req.Image,
			Secrets:    req.Secrets,
		}
	}
	return out, nil
}

type placeCand struct {
	id         string
	free, mine int // free MiB; instances of the deployment being placed
}

// spread picks a host for each of n instances needing memMiB: fewest instances of the deployment
// first, then most free memory, then host ID. It fails rather than overcommit.
func spread(cands []*placeCand, n, memMiB int) ([]string, error) {
	out := make([]string, n)
	for i := range out {
		sort.Slice(cands, func(a, b int) bool {
			if cands[a].mine != cands[b].mine {
				return cands[a].mine < cands[b].mine
			}
			if cands[a].free != cands[b].free {
				return cands[a].free > cands[b].free
			}
			return cands[a].id < cands[b].id
		})
		var pick *placeCand
		for _, h := range cands {
			if h.free >= memMiB {
				pick = h
				break
			}
		}
		if pick == nil {
			return nil, fmt.Errorf("no host has %d MiB free for instance %d/%d", memMiB, i+1, n)
		}
		pick.free -= memMiB
		pick.mine++
		out[i] = pick.id
	}
	return out, nil
}

func (c *Cell) PublishHosts(ctx context.Context, ch HostChange) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	type edit struct {
		start               []instanceSpec
		sleep, wake, remove map[string]bool
	}
	edits := map[string]*edit{}
	get := func(host string) *edit {
		if edits[host] == nil {
			edits[host] = &edit{sleep: map[string]bool{}, wake: map[string]bool{}, remove: map[string]bool{}}
		}
		return edits[host]
	}
	var reg cellstate.HostRegistry
	for _, in := range ch.Start {
		if in.Secrets > 0 && reg.Hosts == nil {
			var err error
			if reg, err = c.registry(ctx); err != nil {
				return err
			}
		}
		cfg, err := c.config(in.Service)
		if err != nil {
			return err
		}
		img := in.Image
		if img == "" {
			img = ch.Image
		}
		spec := instanceSpec{
			ID: in.ID, Service: in.Service, Deployment: in.Deployment, Image: img, Port: cfg.Port,
			MemoryMiB: cfg.MemoryMiB, Env: cfg.Env, Health: health{Path: cfg.HealthPath}, Desired: "running",
		}
		if in.Secrets > 0 {
			sealed, err := c.sealSecrets(ctx, reg, in.Service, in.Secrets, in.Host)
			if err != nil {
				return fmt.Errorf("secrets for %s: %w", in.ID, err)
			}
			// An older hostd would ignore sealed_env and boot the app without its secrets.
			spec.SealedEnv, spec.Secrets, spec.Requires = sealed, in.Secrets, []string{"sealed_env"}
		}
		e := get(in.Host)
		e.start = append(e.start, spec)
	}
	for _, in := range ch.Sleep {
		get(in.Host).sleep[in.ID] = true
	}
	for _, in := range ch.Wake {
		get(in.Host).wake[in.ID] = true
	}
	for _, in := range ch.Stop {
		get(in.Host).remove[in.ID] = true
	}
	for host, e := range edits {
		if host == "" {
			return fmt.Errorf("instance without a host in host change")
		}
		key := cellstate.HostDocKey(host)
		var doc hostDoc
		if err := c.readDoc(ctx, key, &doc); err != nil {
			return err
		}
		kept := []instanceSpec{}
		for _, s := range doc.Instances {
			switch {
			case e.remove[s.ID]:
				doc.Tombstones = append(doc.Tombstones, s.ID)
				continue
			case e.sleep[s.ID]:
				s.Desired = "sleeping"
			case e.wake[s.ID]:
				s.Desired = "running"
			}
			kept = append(kept, s)
		}
		doc.Instances = append(kept, e.start...)
		if len(doc.Tombstones) > 500 {
			doc.Tombstones = doc.Tombstones[len(doc.Tombstones)-500:]
		}
		if doc.Tombstones == nil {
			doc.Tombstones = []string{}
		}
		doc.Schema, doc.Version = 1, nextVersion(doc.Version)
		if err := c.writeDoc(ctx, key, doc); err != nil {
			return err
		}
	}
	return nil
}

// poll sleeps one interval. Inside an activity it heartbeats, so a dead worker's wait is retried
// on another worker after HeartbeatTimeout instead of blocking until StartToCloseTimeout.
func (c *Cell) poll(ctx context.Context) error {
	if activity.IsActivity(ctx) {
		activity.RecordHeartbeat(ctx)
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(c.Poll):
		return nil
	}
}

func (c *Cell) instance(hs map[string]hostStatus, in Instance) instanceStatus {
	for _, s := range hs[in.Host].Instances {
		if s.ID == in.ID {
			return s
		}
	}
	return instanceStatus{}
}

// WaitReady passes once each instance's hostd reports it Ready (its readiness probe passed).
func (c *Cell) WaitReady(ctx context.Context, ins []Instance, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		hs, err := c.hosts(ctx)
		if err != nil {
			return err
		}
		ready := 0
		for _, in := range ins {
			s := c.instance(hs, in)
			if s.State == "failed" {
				return fmt.Errorf("%w: %s on %s failed: %.300s", ErrNotReady, in.ID, in.Host, s.Error)
			}
			if h, ok := hs[in.Host]; ok && !fresh(h.ReportedAt) {
				return fmt.Errorf("%w: %s stopped reporting while %s booted", ErrHostLost, in.Host, in.ID)
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
		if err := c.poll(ctx); err != nil {
			return err
		}
	}
}

func (c *Cell) PublishRoutes(ctx context.Context, u RouteUpdate) (int64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	cfg, err := c.config(u.ServiceID)
	if err != nil {
		return 0, err
	}
	hs, err := c.hosts(ctx)
	if err != nil {
		return 0, err
	}
	var doc edgeDoc
	if err := c.readDoc(ctx, cellstate.EdgeDocKey, &doc); err != nil {
		return 0, err
	}
	known := knownAddrs(doc)
	backends := []backend{}
	add := func(ins []Instance, weight int, state string) error {
		for _, in := range ins {
			addr := backendAddr(hs, known, in)
			if addr == "" {
				return fmt.Errorf("%w: %s on %s", ErrNoAddress, in.ID, in.Host)
			}
			backends = append(backends, backend{Instance: in.ID, Host: in.Host, Deployment: in.Deployment, Addr: addr, Weight: weight, State: state})
		}
		return nil
	}
	if err := add(u.Active, 100, "active"); err != nil {
		return 0, err
	}
	if len(u.Canary) > 0 {
		// Active backends weigh 100 each; size the canary weights so they get CanaryPercent in total.
		p := min(max(u.CanaryPercent, 1), 99)
		w := (100*len(u.Active)*p + (100-p)*len(u.Canary) - 1) / ((100 - p) * len(u.Canary))
		if err := add(u.Canary, max(w, 1), "active"); err != nil {
			return 0, err
		}
	}
	if err := add(u.Draining, 0, "draining"); err != nil {
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
	return doc.Version, c.writeDoc(ctx, cellstate.EdgeDocKey, doc)
}

func knownAddrs(doc edgeDoc) map[string]string {
	out := map[string]string{}
	for _, r := range doc.Routes {
		for _, b := range r.Backends {
			out[b.Instance] = b.Addr
		}
	}
	return out
}

// backendAddr is the address the instance's host reports, or else the one the edge doc already
// routes to: a host that stopped reporting (or a status read that came back without it) is not a
// reason to drop or block routes, and the edge ejects an address it can't connect to.
func backendAddr(hs map[string]hostStatus, known map[string]string, in Instance) string {
	for _, s := range hs[in.Host].Instances {
		if s.ID == in.ID && s.Addr != "" {
			return s.Addr
		}
	}
	return known[in.ID]
}

// WaitEdgeAck passes once every live edge reports it is serving routes version >= version.
func (c *Cell) WaitEdgeAck(ctx context.Context, version int64) error {
	for {
		es, err := c.edges(ctx)
		if err != nil {
			return err
		}
		acked := 0
		for _, e := range es {
			if e.AppliedVersion >= version {
				acked++
			}
		}
		if len(es) > 0 && acked == len(es) {
			return nil
		}
		if err := c.poll(ctx); err != nil {
			return err
		}
	}
}

// WaitDrained waits for in-flight requests to these instances to reach zero on every edge.
// On timeout it returns nil: the drain window is a bound, not a gate.
func (c *Cell) WaitDrained(ctx context.Context, ins []Instance, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	want := map[string]bool{}
	for _, in := range ins {
		want[in.ID] = true
	}
	for {
		es, err := c.edges(ctx)
		if err != nil {
			return err
		}
		var busy int64
		for _, e := range es {
			for _, b := range e.Backends {
				if want[b.Instance] {
					busy += b.InFlight
				}
			}
		}
		if busy == 0 || time.Now().After(deadline) {
			return nil
		}
		if err := c.poll(ctx); err != nil {
			return err
		}
	}
}

// Health is the edge-observed error rate (5xx + upstream failures) of the deployment, all edges.
func (c *Cell) Health(ctx context.Context, _, deploymentID string) (HealthReport, error) {
	es, err := c.edges(ctx)
	if err != nil {
		return HealthReport{}, err
	}
	var req, errs int64
	for _, e := range es {
		for _, b := range e.Backends {
			if b.Deployment == deploymentID {
				req += b.Requests
				errs += b.Errors
			}
		}
	}
	if req == 0 {
		return HealthReport{}, nil
	}
	return HealthReport{ErrorRate: float64(errs) / float64(req), Requests: req}, nil
}

// Current returns the instances the edge doc routes to for service: what a new deploy replaces.
func (c *Cell) Current(ctx context.Context, service string) ([]Instance, error) {
	cfg, err := c.config(service)
	if err != nil {
		return nil, err
	}
	var doc edgeDoc
	if err := c.readDoc(ctx, cellstate.EdgeDocKey, &doc); err != nil {
		return nil, err
	}
	var out []Instance
	for _, r := range doc.Routes {
		if r.Host != cfg.Host {
			continue
		}
		for _, b := range r.Backends {
			if b.State == "active" {
				out = append(out, Instance{ID: b.Instance, Host: b.Host, Deployment: b.Deployment, Service: service})
			}
		}
	}
	return out, nil
}

// lost: the instance's host stopped reporting, or reports that the instance failed. A host only
// reports a routed instance as failed when it could not boot it again (e.g. after a hostd restart,
// or because it can no longer open the instance's secrets after its box key changed); instances
// are immutable, so nothing on that host will fix it.
func lost(hs map[string]hostStatus, in Instance) bool {
	h, ok := hs[in.Host]
	if !ok || !fresh(h.ReportedAt) {
		return true
	}
	for _, s := range h.Instances {
		if s.ID == in.ID {
			return s.State == "failed"
		}
	}
	return false
}

// Lost reports which routed instances of service are lost (see lost) and need replacing.
func (c *Cell) Lost(ctx context.Context, service string) (LostReport, error) {
	cur, err := c.Current(ctx, service)
	if err != nil || len(cur) == 0 {
		return LostReport{}, err
	}
	hs, err := c.hosts(ctx)
	if err != nil {
		return LostReport{}, err
	}
	var rep LostReport
	docs := map[string]*hostDoc{}
	for _, in := range cur {
		if !lost(hs, in) {
			rep.Healthy = append(rep.Healthy, in)
			continue
		}
		if docs[in.Host] == nil {
			docs[in.Host] = &hostDoc{}
			if err := c.readDoc(ctx, cellstate.HostDocKey(in.Host), docs[in.Host]); err != nil {
				return LostReport{}, err
			}
		}
		for _, s := range docs[in.Host].Instances {
			if s.ID == in.ID {
				in.Image, in.Secrets = s.Image, s.Secrets
			}
		}
		if in.Image == "" {
			return LostReport{}, fmt.Errorf("lost instance %s is not in host %s's doc", in.ID, in.Host)
		}
		rep.Lost = append(rep.Lost, in)
	}
	return rep, nil
}
