package deploy

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSpreadPutsBatchesOnDifferentHosts(t *testing.T) {
	// Second batch of a MaxSurge=1 rollout: host-a already runs the first replica, and has
	// slightly more free memory. The replica still goes to host-b.
	cands := []*placeCand{{id: "host-a", free: 768, mine: 1}, {id: "host-b", free: 512}}
	hosts, err := spread(cands, 1, 256)
	require.NoError(t, err)
	require.Equal(t, []string{"host-b"}, hosts)
}

func TestSpreadWithinOneCallThenByFreeMemory(t *testing.T) {
	cands := []*placeCand{{id: "host-a", free: 1024}, {id: "host-b", free: 2048}, {id: "host-c", free: 512}}
	hosts, err := spread(cands, 4, 256)
	require.NoError(t, err)
	require.Equal(t, []string{"host-b", "host-a", "host-c", "host-b"}, hosts)
}

func TestSpreadFailsRatherThanOvercommit(t *testing.T) {
	_, err := spread([]*placeCand{{id: "host-a", free: 300}}, 2, 256)
	require.ErrorContains(t, err, "no host has 256 MiB free for instance 2/2")
}
