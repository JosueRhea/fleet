// Command statectl reads and writes raw objects in the cell's state bucket (for operators and tests).
//
//	statectl get docs/edge.json > edge.json
//	statectl put docs/edge.json < edge.json
//	statectl list status/
package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"

	"github.com/JosueRhea/fleet/orchestrator/cellstate"
)

func main() {
	if len(os.Args) != 3 || (os.Args[1] != "get" && os.Args[1] != "put" && os.Args[1] != "list") {
		log.Fatal("usage: statectl get|put KEY | list PREFIX")
	}
	ctx := context.Background()
	st, err := cellstate.OpenStore(ctx)
	if err != nil {
		log.Fatal(err)
	}
	key := os.Args[2]
	if os.Args[1] == "list" {
		objs, err := st.List(ctx, key)
		if err != nil {
			log.Fatal(err)
		}
		for k := range objs {
			fmt.Println(k)
		}
		return
	}
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
