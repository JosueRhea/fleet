// Command deploy starts a rolling deploy and waits for the result.
// The workflow ID is per service, so Temporal refuses a second concurrent rollout of the same service.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/JosueRhea/fleet/orchestrator/deploy"
	"github.com/JosueRhea/fleet/orchestrator/tclient"
	"go.temporal.io/sdk/client"
)

func main() {
	service := flag.String("service", "svc-demo", "service ID")
	deployment := flag.String("deployment", fmt.Sprintf("d%d", time.Now().Unix()), "new deployment ID")
	image := flag.String("image", "", "OCI image to deploy")
	replicas := flag.Int("replicas", 2, "replicas")
	surge := flag.Int("max-surge", 1, "max surge")
	dir := flag.String("dir", "/var/lib/ccloud/cell", "cell state dir (services.json, edge.json)")
	bake := flag.Duration("bake", 10*time.Second, "bake window")
	rollbackAfter := flag.Duration("rollback-after", 0, "send a rollback signal after this long (0 = never)")
	flag.Parse()
	if *image == "" {
		log.Fatal("-image is required")
	}
	cluster, err := deploy.NewDocs(*dir, "")
	if err != nil {
		log.Fatal(err)
	}
	old, err := cluster.Current(*service)
	if err != nil {
		log.Fatalf("current instances: %v", err)
	}

	c, err := tclient.Dial()
	if err != nil {
		log.Fatalf("dial temporal: %v", err)
	}
	defer c.Close()

	in := deploy.Input{ServiceID: *service, DeploymentID: *deployment, Image: *image, Replicas: *replicas, MaxSurge: *surge, Bake: *bake}
	in.Old = old
	log.Printf("replacing %d running instance(s) of %s", len(old), *service)
	ctx := context.Background()
	run, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{ID: "deploy-" + *service, TaskQueue: deploy.TaskQueue}, deploy.RollingDeploy, in)
	if err != nil {
		log.Fatalf("start deploy: %v", err)
	}
	log.Printf("started workflow %s run %s", run.GetID(), run.GetRunID())
	if *rollbackAfter > 0 {
		go func() {
			time.Sleep(*rollbackAfter)
			if err := c.SignalWorkflow(ctx, run.GetID(), run.GetRunID(), deploy.RollbackSignal, "manual rollback from CLI"); err != nil {
				log.Printf("rollback signal: %v", err)
			}
		}()
	}
	var res deploy.Result
	if err := run.Get(ctx, &res); err != nil {
		log.Fatalf("deploy failed: %v", err)
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	_ = enc.Encode(res)
}
