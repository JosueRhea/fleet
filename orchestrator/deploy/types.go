package deploy

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
	SealedEnv  string            `json:"sealed_env,omitempty"`
	Requires   []string          `json:"requires,omitempty"` // hostd fails the instance if it lacks any of these
	Secrets    int               `json:"secrets_version,omitempty"`
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
	HostID         string           `json:"host_id"`
	BoxPub         string           `json:"box_pub"`
	ReportedAt     int64            `json:"reported_at"`
	MemoryTotal    int              `json:"memory_mib_total"`
	MemoryUsed     int              `json:"memory_mib_used"`
	AppliedVersion int64            `json:"applied_version"`
	Instances      []instanceStatus `json:"instances"`
}

type backend struct {
	Instance   string `json:"instance"`
	Host       string `json:"host"`
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
	EdgeID         string         `json:"edge_id"`
	ReportedAt     int64          `json:"reported_at"`
	AppliedVersion int64          `json:"applied_version"`
	Backends       []backendStats `json:"backends"`
}
