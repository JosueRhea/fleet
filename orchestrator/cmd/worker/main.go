// Command worker runs the deploy workflow against the real hostd agent and edge proxy.
package main

import (
	"flag"
	"log"

	"github.com/JosueRhea/fleet/orchestrator/deploy"
	"github.com/JosueRhea/fleet/orchestrator/tclient"
	"go.temporal.io/sdk/worker"
)

func main() {
	dir := flag.String("dir", "/var/lib/ccloud/cell", "desired-state docs, status reports and services.json")
	host := flag.String("host", "local", "host ID instances are placed on")
	flag.Parse()
	cluster, err := deploy.NewDocs(*dir, *host)
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
