// Command secretctl manages a service's secret sets in the cell state bucket.
//
//	secretctl keygen -key secrets.key -pub secrets.pub
//	secretctl set -pub secrets.pub -service svc-web DB_PASSWORD=... API_TOKEN=...
//
// set writes a new immutable version sealed to the cell secrets public key, so the CLI never
// needs (or gets) the private key; deploys pin a version, and changing secrets means a new deploy.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/JosueRhea/fleet/orchestrator/cellstate"
)

func main() {
	if len(os.Args) < 2 {
		log.Fatal("usage: secretctl keygen|set ...")
	}
	fs := flag.NewFlagSet(os.Args[1], flag.ExitOnError)
	priv := fs.String("key", "secrets.key", "cell secrets private key (keygen output)")
	pub := fs.String("pub", "secrets.pub", "cell secrets public key")
	service := fs.String("service", "", "service ID")
	_ = fs.Parse(os.Args[2:])
	switch os.Args[1] {
	case "keygen":
		if err := cellstate.GenerateBoxKey(*priv, *pub); err != nil {
			log.Fatal(err)
		}
	case "set":
		if *service == "" {
			log.Fatal("-service is required")
		}
		env := map[string]string{}
		for _, kv := range fs.Args() {
			k, v, ok := strings.Cut(kv, "=")
			if !ok || k == "" {
				log.Fatalf("want KEY=VALUE, got %q", kv)
			}
			env[k] = v
		}
		key, err := cellstate.LoadBoxPub(*pub)
		if err != nil {
			log.Fatal(err)
		}
		sealed, err := cellstate.SealEnv(env, key)
		if err != nil {
			log.Fatal(err)
		}
		ctx := context.Background()
		store, err := cellstate.OpenStore(ctx)
		if err != nil {
			log.Fatal(err)
		}
		// Versions are immutable: claim the next one with a create-only write, and if another
		// secretctl got there first, take the one after it.
		var v int
		for attempt := 0; ; attempt++ {
			vs, err := store.SecretVersions(ctx, *service)
			if err != nil {
				log.Fatal(err)
			}
			v = 1
			if len(vs) > 0 {
				v = vs[len(vs)-1] + 1
			}
			err = store.PutNew(ctx, cellstate.SecretsKey(*service, v), []byte(sealed))
			if err == nil {
				break
			}
			if !errors.Is(err, cellstate.ErrExists) || attempt == 4 {
				log.Fatal(err)
			}
		}
		fmt.Printf("%s secrets v%d (%d keys)\n", *service, v, len(env))
	default:
		log.Fatalf("unknown command %q", os.Args[1])
	}
}
