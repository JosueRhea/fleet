// Package cellstate stores the cell's desired-state docs and status reports in an S3-compatible
// bucket. Docs are wrapped in an ed25519-signed envelope that hostd and the edge verify.
package cellstate

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
)

// Envelope is what the publisher writes: Payload is the doc's exact JSON bytes.
type Envelope struct {
	KeyID   string `json:"key_id"`
	Payload []byte `json:"payload"` // base64 in JSON
	Sig     []byte `json:"sig"`
}

type Signer struct {
	key ed25519.PrivateKey
	id  string
}

func KeyID(pub ed25519.PublicKey) string {
	h := sha256.Sum256(pub)
	return hex.EncodeToString(h[:4])
}

// LoadSigner reads a hex-encoded 32-byte ed25519 seed.
func LoadSigner(path string) (*Signer, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	seed, err := hex.DecodeString(strings.TrimSpace(string(b)))
	if err != nil || len(seed) != ed25519.SeedSize {
		return nil, fmt.Errorf("%s: want a hex ed25519 seed", path)
	}
	k := ed25519.NewKeyFromSeed(seed)
	return &Signer{key: k, id: KeyID(k.Public().(ed25519.PublicKey))}, nil
}

// GenerateKey writes seed (private) and public key, both hex.
func GenerateKey(privPath, pubPath string) error {
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return err
	}
	if err := os.WriteFile(privPath, []byte(hex.EncodeToString(priv.Seed())+"\n"), 0o600); err != nil {
		return err
	}
	return os.WriteFile(pubPath, []byte(hex.EncodeToString(pub)+"\n"), 0o644)
}

func (s *Signer) Seal(doc any) ([]byte, error) {
	payload, err := json.Marshal(doc)
	if err != nil {
		return nil, err
	}
	return json.Marshal(Envelope{KeyID: s.id, Payload: payload, Sig: ed25519.Sign(s.key, payload)})
}

// Open verifies an envelope written by this signer and decodes its payload.
func (s *Signer) Open(b []byte, doc any) error {
	var e Envelope
	if err := json.Unmarshal(b, &e); err != nil {
		return err
	}
	if !ed25519.Verify(s.key.Public().(ed25519.PublicKey), e.Payload, e.Sig) {
		return errors.New("bad signature")
	}
	return json.Unmarshal(e.Payload, doc)
}
