package deploy

import (
	"testing"

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
