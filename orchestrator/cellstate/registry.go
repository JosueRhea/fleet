package cellstate

import (
	"context"
	"fmt"
	"time"
)

// HostRegistryKey is the signed list of hosts the publisher may place instances on, with each
// host's X25519 box key. Operators write it (hostctl). Host status reports are unsigned and
// writable by every agent, so neither placement nor secret sealing trusts what a host says about
// itself: a host that is not registered gets nothing, and secrets are sealed to the registered key.
const HostRegistryKey = "docs/host-registry.json"

type RegisteredHost struct {
	BoxPub string `json:"box_pub"`
	// SignPub verifies the host's status reports. A host registered without one (registered before
	// hosts signed their reports) has its unsigned reports trusted until it is re-registered.
	SignPub string `json:"sign_pub,omitempty"`
}

type HostRegistry struct {
	Schema  int                       `json:"schema"`
	Version int64                     `json:"version"`
	Hosts   map[string]RegisteredHost `json:"hosts"`
}

// ReadRegistry returns an empty registry if none has been written yet. A registry that fails
// verification is an error, never an empty one.
func ReadRegistry(ctx context.Context, s *Store, sg *Signer) (HostRegistry, error) {
	r := HostRegistry{Hosts: map[string]RegisteredHost{}}
	b, err := s.Get(ctx, HostRegistryKey)
	if err != nil || b == nil {
		return r, err
	}
	if err := sg.Open(b, &r); err != nil {
		return r, fmt.Errorf("%s: %w", HostRegistryKey, err)
	}
	if r.Hosts == nil {
		r.Hosts = map[string]RegisteredHost{}
	}
	return r, nil
}

func WriteRegistry(ctx context.Context, s *Store, sg *Signer, r HostRegistry) error {
	r.Schema, r.Version = 1, max(r.Version+1, time.Now().UnixMicro())
	b, err := sg.Seal(r)
	if err != nil {
		return err
	}
	return s.Put(ctx, HostRegistryKey, b)
}
