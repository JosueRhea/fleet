package cellstate

import (
	"path/filepath"
	"testing"
)

func TestSealedEnvOpensOnlyWithItsKey(t *testing.T) {
	d := t.TempDir()
	for _, n := range []string{"a", "b"} {
		if err := GenerateBoxKey(filepath.Join(d, n+".key"), filepath.Join(d, n+".pub")); err != nil {
			t.Fatal(err)
		}
	}
	a, _ := LoadBoxKey(filepath.Join(d, "a.key"))
	b, _ := LoadBoxKey(filepath.Join(d, "b.key"))
	pub, _ := LoadBoxPub(filepath.Join(d, "a.pub"))
	if *pub != *a.Pub {
		t.Fatal("derived public key differs from keygen's")
	}
	s, err := SealEnv(map[string]string{"K": "v"}, a.Pub)
	if err != nil {
		t.Fatal(err)
	}
	if env, err := a.OpenEnv(s); err != nil || env["K"] != "v" {
		t.Fatalf("open: %v %v", env, err)
	}
	if _, err := b.OpenEnv(s); err == nil {
		t.Fatal("other key opened it")
	}
}
