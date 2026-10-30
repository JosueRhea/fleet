// Command statectl reads and writes raw objects in the cell's state bucket (for operators and tests).
//
//	statectl get docs/edge.json > edge.json
//	statectl put docs/edge.json < edge.json
package main

import (
	"context"
	"io"
	"log"
	"os"

	"github.com/JosueRhea/fleet/orchestrator/cellstate"
)

func main() {
	if len(os.Args) != 3 || (os.Args[1] != "get" && os.Args[1] != "put") {
		log.Fatal("usage: statectl get|put KEY")
	}
	ctx := context.Background()
	st, err := cellstate.OpenStore(ctx)
	if err != nil {
		log.Fatal(err)
	}
	key := os.Args[2]
	if os.Args[1] == "get" {
		b, err := st.Get(ctx, key)
		if err != nil {
			log.Fatal(err)
		}
		if b == nil {
			log.Fatalf("%s: not found", key)
		}
		_, _ = os.Stdout.Write(b)
		return
	}
	b, err := io.ReadAll(os.Stdin)
	if err != nil {
		log.Fatal(err)
	}
	if err := st.Put(ctx, key, b); err != nil {
		log.Fatal(err)
	}
}
