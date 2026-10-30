# orchestrator

Deploy workflows on Temporal. This implements the rolling deploy from [docs/architecture.md §5.3](../docs/architecture.md).

- `deploy/workflow.go`: the `RollingDeploy` workflow. It swaps `MaxSurge` instances at a time, then bakes, with auto-rollback on any failure and a manual `rollback` signal.
  - An old instance drains only after its replacement is Ready **and** an edge-acked route version contains it.
  - Old instances are put to *sleep* (snapshot kept), not stopped, until the bake passes, so rollback is a wake instead of a cold boot.
  - If rollback itself fails, routes are left alone (`StatusFailed`).
- `deploy/activities.go`: the `Cluster` interface (scheduler, publisher, status). The workflow never talks to hosts or edges directly.
- `deploy/fake.go`: an in-memory `Cluster` used by tests and the demo worker until the publisher exists.
- The workflow ID is `deploy-<service>`, so Temporal enforces one rollout per service at a time.

```bash
go test ./...

# Temporal Cloud (or omit the three vars to use `temporal server start-dev`)
export TEMPORAL_ADDRESS=us-west-2.aws.api.temporal.io:7233 TEMPORAL_NAMESPACE=ccloud-dev.nhipi TEMPORAL_API_KEY=...
go run ./cmd/worker
go run ./cmd/deploy -service svc-demo -image app:v2 -bake 5s           # active
go run ./cmd/deploy -service svc-bad  -image app:bad                   # never ready -> rolled back, traffic untouched
go run ./cmd/deploy -service svc-rb   -bake 30s -rollback-after 12s    # manual rollback during bake
```
