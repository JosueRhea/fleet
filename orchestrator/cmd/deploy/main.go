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

	"github.com/JosueRhea/fleet/orchestrator/cellstate"
	"github.com/JosueRhea/fleet/orchestrator/deploy"
	"github.com/JosueRhea/fleet/orchestrator/tclient"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
)

func main() {
	service := flag.String("service", "svc-demo", "service ID")
	deployment := flag.String("deployment", fmt.Sprintf("d%d", time.Now().Unix()), "new deployment ID")
	image := flag.String("image", "", "OCI image to deploy")
	replicas := flag.Int("replicas", 2, "replicas")
	surge := flag.Int("max-surge", 1, "max surge")
	services := flag.String("services", "/etc/ccloud/services.json", "service config")
	key := flag.String("key", "/etc/ccloud/signing.key", "publisher ed25519 seed (hex)")
	bake := flag.Duration("bake", 10*time.Second, "bake window")
	secrets := flag.Int("secrets", -1, "secret set version to pin (-1 = latest, 0 = none)")
	canary := flag.Int("canary", 0, "route this % of traffic to one new instance first (0 = no canary)")
	canaryBake := flag.Duration("canary-bake", 30*time.Second, "how long the canary takes its share before promotion")
	canaryMin := flag.Int("canary-min-requests", 20, "requests the canary must serve during its bake to be judged (negative = no minimum)")
	rollbackAfter := flag.Duration("rollback-after", 0, "send a rollback signal after this long (0 = never)")
	flag.Parse()
	ctx := context.Background()
	if *image == "" {
		log.Fatal("-image is required")
	}
	store, signer, err := cellstate.OpenCell(ctx, *key)
	if err != nil {
		log.Fatal(err)
	}
	cluster, err := deploy.NewCell(store, signer, *services)
	if err != nil {
		log.Fatal(err)
	}
	old, err := cluster.Current(ctx, *service)
	if err != nil {
		log.Fatalf("current instances: %v", err)
	}

	c, err := tclient.Dial()
	if err != nil {
		log.Fatalf("dial temporal: %v", err)
	}
	defer c.Close()

	in := deploy.Input{ServiceID: *service, DeploymentID: *deployment, Image: *image, Replicas: *replicas, MaxSurge: *surge, Bake: *bake, CanaryPercent: *canary, CanaryBake: *canaryBake, CanaryMinRequests: *canaryMin}
	in.Old = old
	if *secrets < 0 {
		vs, err := store.SecretVersions(ctx, *service)
		if err != nil {
			log.Fatalf("secret versions: %v", err)
		}
		*secrets = 0
		if len(vs) > 0 {
			*secrets = vs[len(vs)-1]
		}
	}
	in.Secrets = *secrets
	log.Printf("secret set version %d", in.Secrets)
	log.Printf("replacing %d running instance(s) of %s", len(old), *service)
	opts := client.StartWorkflowOptions{ID: "deploy-" + *service, TaskQueue: deploy.TaskQueue, WorkflowExecutionErrorWhenAlreadyStarted: true}
	run, err := c.ExecuteWorkflow(ctx, opts, deploy.RollingDeploy, in)
	for i := 0; i < 10 && temporal.IsWorkflowExecutionAlreadyStartedError(err); i++ {
		log.Printf("another deploy or repair of %s is running; waiting for it", *service)
		_ = c.GetWorkflow(ctx, opts.ID, "").Get(ctx, nil)
		if in.Old, err = cluster.Current(ctx, *service); err != nil {
			log.Fatalf("current instances: %v", err)
		}
		run, err = c.ExecuteWorkflow(ctx, opts, deploy.RollingDeploy, in)
	}
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
