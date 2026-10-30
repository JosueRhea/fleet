package deploy

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestBackendAddrFallsBackToRoutedAddress(t *testing.T) {
	hs := map[string]hostStatus{"host-a": {Instances: []instanceStatus{{ID: "i1", Addr: "10.0.0.1:80"}, {ID: "i2"}}}}
	known := knownAddrs(edgeDoc{Routes: []route{{Backends: []backend{{Instance: "i2", Addr: "10.0.0.2:80"}, {Instance: "i3", Addr: "10.0.0.3:80"}}}}})
	require.Equal(t, "10.0.0.1:80", backendAddr(hs, known, Instance{ID: "i1", Host: "host-a"}))
	require.Equal(t, "10.0.0.2:80", backendAddr(hs, known, Instance{ID: "i2", Host: "host-a"}), "reported without an address")
	require.Equal(t, "10.0.0.3:80", backendAddr(hs, known, Instance{ID: "i3", Host: "host-b"}), "host not reporting")
	require.Equal(t, "", backendAddr(hs, known, Instance{ID: "i4", Host: "host-b"}))
}

func TestLostCoversStaleHostsAndFailedInstances(t *testing.T) {
	now := time.Now().Unix()
	hs := map[string]hostStatus{
		"host-a": {ReportedAt: now, Instances: []instanceStatus{{ID: "ok", State: "ready"}, {ID: "bad", State: "failed"}, {ID: "boot", State: "booting"}}},
		"host-b": {ReportedAt: now - 60},
	}
	require.False(t, lost(hs, Instance{ID: "ok", Host: "host-a"}))
	require.False(t, lost(hs, Instance{ID: "boot", Host: "host-a"}))
	require.False(t, lost(hs, Instance{ID: "gone", Host: "host-a"}), "not reported yet is not failed")
	require.True(t, lost(hs, Instance{ID: "bad", Host: "host-a"}))
	require.True(t, lost(hs, Instance{ID: "x", Host: "host-b"}), "stale host")
	require.True(t, lost(hs, Instance{ID: "x", Host: "host-c"}), "never reported")
}
