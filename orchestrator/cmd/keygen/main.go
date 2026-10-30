// Command keygen creates the publisher's ed25519 signing key (seed) and public key, both hex.
package main

import (
	"flag"
	"log"

	"github.com/JosueRhea/fleet/orchestrator/cellstate"
)

func main() {
	priv := flag.String("key", "signing.key", "where to write the private seed")
	pub := flag.String("pub", "signing.pub", "where to write the public key (give this to hostd/edge --trust)")
	flag.Parse()
	if err := cellstate.GenerateKey(*priv, *pub); err != nil {
		log.Fatal(err)
	}
}
