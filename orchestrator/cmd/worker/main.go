// Command worker runs the deploy workflow for one cell: it publishes signed host/edge docs to the
// state bucket and gates on the status reports hostd agents and edges write there.
package main

import (
	"context"
	"flag"
	"log"
	"time"

	"github.com/JosueRhea/fleet/orchestrator/cellstate"
	"github.com/JosueRhea/fleet/orchestrator/deploy"
	"github.com/JosueRhea/fleet/orchestrator/tclient"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
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
	w.RegisterWorkflow(deploy.Repair)
	w.RegisterActivity(&deploy.Activities{Cluster: cluster})
	go watchHosts(c, cluster)
	if err := w.Run(worker.InterruptCh()); err != nil {
		log.Fatal(err)
	}
}

// watchHosts starts a Repair for any service routed to a host that stopped reporting. Repairs use
// the deploy workflow ID, so a running deploy or repair for the service makes this a no-op.
func watchHosts(c client.Client, cluster *deploy.Cell) {
	ctx := context.Background()
	// After the state bucket was unreachable every host looks stale until it reports again; that's
	// not a host failure, so wait out StaleAfter before trusting staleness again.
	var bucketErr time.Time
	for range time.Tick(3 * time.Second) {
		for svc := range cluster.Services {
			rep, err := cluster.Lost(ctx, svc)
			if err != nil {
				bucketErr = time.Now()
				log.Printf("%s: checking for lost instances: %v", svc, err)
				continue
			}
			if len(rep.Lost) == 0 || time.Since(bucketErr) < deploy.StaleAfter {
				continue
			}
			opts := client.StartWorkflowOptions{ID: "deploy-" + svc, TaskQueue: deploy.TaskQueue, WorkflowExecutionErrorWhenAlreadyStarted: true}
			_, err = c.ExecuteWorkflow(ctx, opts, deploy.Repair, deploy.RepairInput{ServiceID: svc})
			switch {
			case err == nil:
				log.Printf("%s: %d instance(s) on lost hosts, repair started", svc, len(rep.Lost))
			case !temporal.IsWorkflowExecutionAlreadyStartedError(err):
				log.Printf("%s: starting repair: %v", svc, err)
			}
		}
	}
}
