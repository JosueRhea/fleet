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
	secretsKey := flag.String("secrets-key", "", "cell X25519 secrets key (hex); needed to deploy services with secrets")
	flag.Parse()
	store, signer, err := cellstate.OpenCell(context.Background(), *key)
	if err != nil {
		log.Fatal(err)
	}
	cluster, err := deploy.NewCell(store, signer, *services)
	if err != nil {
		log.Fatal(err)
	}
	if *secretsKey != "" {
		if cluster.SecretsKey, err = cellstate.LoadBoxKey(*secretsKey); err != nil {
			log.Fatal(err)
		}
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

// repairBackoff is the least time between two repairs of one service.
const repairBackoff = 30 * time.Second

// watchHosts starts a Repair for any service with lost instances (host stopped reporting, or
// reports them failed). Repairs use
// the deploy workflow ID, so a running deploy or repair for the service makes this a no-op.
func watchHosts(c client.Client, cluster *deploy.Cell) {
	ctx := context.Background()
	// After the state bucket was unreachable every host looks stale until it reports again; that's
	// not a host failure, so wait out StaleAfter before trusting staleness again.
	var bucketErr time.Time
	// A repair whose replacements also fail (an image that can't boot anywhere) would otherwise
	// be restarted every tick, booting and stopping VMs in a loop.
	lastRepair := map[string]time.Time{}
	for range time.Tick(3 * time.Second) {
		for svc := range cluster.Services {
			rep, err := cluster.Lost(ctx, svc)
			if err != nil {
				bucketErr = time.Now()
				log.Printf("%s: checking for lost instances: %v", svc, err)
				continue
			}
			if len(rep.Lost) == 0 || time.Since(bucketErr) < deploy.StaleAfter || time.Since(lastRepair[svc]) < repairBackoff {
				continue
			}
			opts := client.StartWorkflowOptions{ID: "deploy-" + svc, TaskQueue: deploy.TaskQueue, WorkflowExecutionErrorWhenAlreadyStarted: true}
			_, err = c.ExecuteWorkflow(ctx, opts, deploy.Repair, deploy.RepairInput{ServiceID: svc})
			switch {
			case err == nil:
				lastRepair[svc] = time.Now()
				log.Printf("%s: %d lost instance(s), repair started", svc, len(rep.Lost))
			case !temporal.IsWorkflowExecutionAlreadyStartedError(err):
				log.Printf("%s: starting repair: %v", svc, err)
			}
		}
	}
}
