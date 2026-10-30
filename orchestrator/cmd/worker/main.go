// Command worker runs the deploy workflow. Activities use FakeCluster until the publisher exists.
package main

import (
	"log"
	"time"

	"github.com/JosueRhea/fleet/orchestrator/deploy"
	"github.com/JosueRhea/fleet/orchestrator/tclient"
	"go.temporal.io/sdk/worker"
)

func main() {
	c, err := tclient.Dial()
	if err != nil {
		log.Fatalf("dial temporal: %v", err)
	}
	defer c.Close()
	w := worker.New(c, deploy.TaskQueue, worker.Options{})
	w.RegisterWorkflow(deploy.RollingDeploy)
	w.RegisterActivity(&deploy.Activities{Cluster: &deploy.FakeCluster{
		Delay:          500 * time.Millisecond,
		FailReadyImage: "bad",
		Logf:           log.Printf,
	}})
	if err := w.Run(worker.InterruptCh()); err != nil {
		log.Fatal(err)
	}
}
