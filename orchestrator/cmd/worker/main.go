// Command worker runs the deploy workflow for one cell: it publishes signed host/edge docs to the
// state bucket and gates on the status reports hostd agents and edges write there.
package main

import (
	"context"
	"flag"
	"log"

	"github.com/JosueRhea/fleet/orchestrator/cellstate"
	"github.com/JosueRhea/fleet/orchestrator/deploy"
	"github.com/JosueRhea/fleet/orchestrator/tclient"
	"go.temporal.io/sdk/worker"
)

func main() {
	services := flag.String("services", "/etc/ccloud/services.json", "service config")
	key := flag.String("key", "/etc/ccloud/signing.key", "publisher ed25519 seed (hex)")
	flag.Parse()
	store, signer, err := cellstate.OpenCell(context.Background(), *key)
	if err != nil {
		log.Fatal(err)
	}
	cluster, err := deploy.NewCell(store, signer, *services)
	if err != nil {
		log.Fatal(err)
	}
	c, err := tclient.Dial()
	if err != nil {
		log.Fatalf("dial temporal: %v", err)
	}
	defer c.Close()
	w := worker.New(c, deploy.TaskQueue, worker.Options{})
	w.RegisterWorkflow(deploy.RollingDeploy)
	w.RegisterActivity(&deploy.Activities{Cluster: cluster})
	if err := w.Run(worker.InterruptCh()); err != nil {
		log.Fatal(err)
	}
}
