package cellstate

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"

	"golang.org/x/crypto/nacl/box"
)

// BoxKey is an X25519 keypair for libsodium-compatible sealed boxes. The cell's secrets key
// (held only by the publisher, a stand-in for a KMS key) opens stored secret sets; each hostd
// has its own and the publisher re-seals a secret set to it when it places an instance there.
type BoxKey struct {
	Pub, Priv *[32]byte
}

func GenerateBoxKey(privPath, pubPath string) error {
	pub, priv, err := box.GenerateKey(rand.Reader)
	if err != nil {
		return err
	}
	if err := os.WriteFile(privPath, []byte(hex.EncodeToString(priv[:])+"\n"), 0o600); err != nil {
		return err
	}
	return os.WriteFile(pubPath, []byte(hex.EncodeToString(pub[:])+"\n"), 0o644)
}

func readKey32(path string) (*[32]byte, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return ParseKey32(strings.TrimSpace(string(b)))
}

func ParseKey32(s string) (*[32]byte, error) {
	raw, err := hex.DecodeString(s)
	if err != nil || len(raw) != 32 {
		return nil, fmt.Errorf("want a hex 32-byte key")
	}
	var k [32]byte
	copy(k[:], raw)
	return &k, nil
}

// LoadBoxKey reads a private key file; the public half is derived from it.
func LoadBoxKey(privPath string) (*BoxKey, error) {
	priv, err := readKey32(privPath)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", privPath, err)
	}
	pub, _, err := box.GenerateKey(zeroReader{priv})
	if err != nil {
		return nil, err
	}
	return &BoxKey{Pub: pub, Priv: priv}, nil
}

// zeroReader feeds a fixed private key to box.GenerateKey so it derives the matching public key.
type zeroReader struct{ k *[32]byte }

func (z zeroReader) Read(p []byte) (int, error) { return copy(p, z.k[:]), nil }

func LoadBoxPub(path string) (*[32]byte, error) { return readKey32(path) }

// SealEnv seals an env map to pub; only the holder of the matching private key can open it.
func SealEnv(env map[string]string, pub *[32]byte) (string, error) {
	pt, err := json.Marshal(env)
	if err != nil {
		return "", err
	}
	ct, err := box.SealAnonymous(nil, pt, pub, rand.Reader)
	if err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(ct), nil
}

func (k *BoxKey) OpenEnv(sealed string) (map[string]string, error) {
	ct, err := base64.StdEncoding.DecodeString(sealed)
	if err != nil {
		return nil, err
	}
	pt, ok := box.OpenAnonymous(nil, ct, k.Pub, k.Priv)
	if !ok {
		return nil, errors.New("cannot open secret set (wrong key?)")
	}
	var env map[string]string
	return env, json.Unmarshal(pt, &env)
}

// Secret sets are immutable and versioned: secrets/<service>/<version>.sealed, sealed to the cell secrets key.
func SecretsKey(service string, version int) string {
	return fmt.Sprintf("secrets/%s/%08d.sealed", service, version)
}

// SecretVersions lists the stored secret set versions for service, ascending.
func (s *Store) SecretVersions(ctx context.Context, service string) ([]int, error) {
	objs, err := s.List(ctx, "secrets/"+service+"/")
	if err != nil {
		return nil, err
	}
	var out []int
	for k := range objs {
		base := strings.TrimSuffix(k[strings.LastIndex(k, "/")+1:], ".sealed")
		if v, err := strconv.Atoi(base); err == nil {
			out = append(out, v)
		}
	}
	sort.Ints(out)
	return out, nil
}
