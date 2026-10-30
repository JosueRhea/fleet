package deploy

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"testing"

	"github.com/JosueRhea/fleet/orchestrator/cellstate"
	"github.com/stretchr/testify/require"
)

func sealStatus(t *testing.T, k ed25519.PrivateKey, st hostStatus) []byte {
	payload, err := json.Marshal(st)
	require.NoError(t, err)
	b, err := json.Marshal(cellstate.Envelope{KeyID: "k", Payload: payload, Sig: ed25519.Sign(k, payload)})
	require.NoError(t, err)
	return b
}

func TestHostStatusMustVerifyAgainstRegisteredKey(t *testing.T) {
	pub, key, _ := ed25519.GenerateKey(rand.Reader)
	_, other, _ := ed25519.GenerateKey(rand.Reader)
	reg := cellstate.HostRegistry{Hosts: map[string]cellstate.RegisteredHost{
		"host-a": {SignPub: hex.EncodeToString(pub)},
		"old":    {}, // registered before hosts signed their reports
	}}
	plain := func(id string) []byte { b, _ := json.Marshal(hostStatus{HostID: id, ReportedAt: 1}); return b }

	st, ok := parseHostStatus("host-a", sealStatus(t, key, hostStatus{HostID: "host-a", ReportedAt: 7}), reg)
	require.True(t, ok)
	require.Equal(t, int64(7), st.ReportedAt)

	_, ok = parseHostStatus("host-a", sealStatus(t, other, hostStatus{HostID: "host-a"}), reg)
	require.False(t, ok, "signed by another key")
	_, ok = parseHostStatus("host-a", plain("host-a"), reg)
	require.False(t, ok, "unsigned, but host-a has a sign key")
	_, ok = parseHostStatus("host-a", []byte("garbage"), reg)
	require.False(t, ok)
	_, ok = parseHostStatus("host-a", sealStatus(t, key, hostStatus{HostID: "host-b"}), reg)
	require.False(t, ok, "names another host")

	_, ok = parseHostStatus("old", plain("old"), reg)
	require.True(t, ok, "no sign key registered yet: unsigned report trusted")
	_, ok = parseHostStatus("old", sealStatus(t, other, hostStatus{HostID: "old"}), reg)
	require.True(t, ok, "signed report from a host without a registered key: payload trusted as written")
}
