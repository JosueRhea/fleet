// Command hostctl manages the cell's signed host registry: the hosts the publisher may place
// instances on, and the box key each one's secrets are sealed to.
//
//	hostctl add -key signing.key -id host-a -box-pub $(hostd --state-dir /var/lib/ccloud box-pub)
//	hostctl remove -key signing.key -id host-a
//	hostctl list -key signing.key
//
// Run hostd box-pub on the host itself and carry the key over out of band: a key read from the
// host's status report is exactly what the registry exists not to trust.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"sort"

	"github.com/JosueRhea/fleet/orchestrator/cellstate"
)

func main() {
	if len(os.Args) < 2 {
		log.Fatal("usage: hostctl add|remove|list ...")
	}
	fs := flag.NewFlagSet(os.Args[1], flag.ExitOnError)
	key := fs.String("key", "/etc/ccloud/signing.key", "publisher ed25519 seed (hex)")
	id := fs.String("id", "", "host ID (hostd agent --host-id)")
	boxPub := fs.String("box-pub", "", "the host's X25519 public key (hostd box-pub)")
	_ = fs.Parse(os.Args[2:])
	ctx := context.Background()
	store, signer, err := cellstate.OpenCell(ctx, *key)
	if err != nil {
		log.Fatal(err)
	}
	reg, err := cellstate.ReadRegistry(ctx, store, signer)
	if err != nil {
		log.Fatal(err)
	}
	switch os.Args[1] {
	case "add":
		if *id == "" {
			log.Fatal("-id is required")
		}
		if _, err := cellstate.ParseKey32(*boxPub); err != nil {
			log.Fatalf("-box-pub: %v", err)
		}
		if old, ok := reg.Hosts[*id]; ok && old.BoxPub != *boxPub {
			log.Printf("%s: replacing box key %s", *id, old.BoxPub)
		}
		reg.Hosts[*id] = cellstate.RegisteredHost{BoxPub: *boxPub}
	case "remove":
		if _, ok := reg.Hosts[*id]; !ok {
			log.Fatalf("%s is not registered", *id)
		}
		delete(reg.Hosts, *id)
	case "list":
		ids := make([]string, 0, len(reg.Hosts))
		for h := range reg.Hosts {
			ids = append(ids, h)
		}
		sort.Strings(ids)
		for _, h := range ids {
			fmt.Printf("%s\t%s\n", h, reg.Hosts[h].BoxPub)
		}
		return
	default:
		log.Fatalf("unknown command %q", os.Args[1])
	}
	if err := cellstate.WriteRegistry(ctx, store, signer, reg); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("%d host(s) registered\n", len(reg.Hosts))
}
