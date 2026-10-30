package deploy

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/testsuite"
)

func olds(n int) []Instance {
	out := make([]Instance, n)
	for i := range out {
		out[i] = Instance{ID: "old-" + string(rune('1'+i)), Deployment: "d1"}
	}
	return out
}

func run(t *testing.T, f *FakeCluster, in Input, during func(env *testsuite.TestWorkflowEnvironment)) Result {
	var s testsuite.WorkflowTestSuite
	env := s.NewTestWorkflowEnvironment()
	env.RegisterActivity(&Activities{Cluster: f})
	if during != nil {
		during(env)
	}
	env.ExecuteWorkflow(RollingDeploy, in)
	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	var res Result
	require.NoError(t, env.GetWorkflowResult(&res))
	return res
}

func input(image string) Input {
	return Input{ServiceID: "svc", DeploymentID: "d2", Image: image, Replicas: 2, MaxSurge: 1, Old: olds(2)}
}

func TestRollingDeploySwapsOneAtATime(t *testing.T) {
	f := &FakeCluster{}
	res := run(t, f, input("app:v2"), nil)
	require.Equal(t, StatusActive, res.Status)
	require.Equal(t, "ins-d2-1,ins-d2-2", ids(res.Active))
	require.Equal(t, []string{
		"place 1",
		"start ins-d2-1",
		"ready ins-d2-1",
		"routes v1 active=ins-d2-1,old-1,old-2 draining=",
		"ack v1",
		"routes v2 active=ins-d2-1,old-2 draining=old-1",
		"ack v2",
		"drained old-1",
		"routes v3 active=ins-d2-1,old-2 draining=",
		"ack v3",
		"sleep old-1",
		"place 1",
		"start ins-d2-2",
		"ready ins-d2-2",
		"routes v4 active=ins-d2-1,ins-d2-2,old-2 draining=",
		"ack v4",
		"routes v5 active=ins-d2-1,ins-d2-2 draining=old-2",
		"ack v5",
		"drained old-2",
		"routes v6 active=ins-d2-1,ins-d2-2 draining=",
		"ack v6",
		"sleep old-2",
		"stop old-1,old-2",
	}, f.Events())
}

func TestNotReadyNeverTouchesTraffic(t *testing.T) {
	f := &FakeCluster{FailReadyImage: "bad"}
	res := run(t, f, input("app:bad"), nil)
	require.Equal(t, StatusRolledBack, res.Status)
	require.Contains(t, res.Reason, "never became ready")
	require.Equal(t, "old-1,old-2", ids(res.Active))
	require.Equal(t, []string{"place 1", "start ins-d2-1", "stop ins-d2-1"}, f.Events())
}

func TestRollbackSignalDuringBakeWakesOldInstances(t *testing.T) {
	f := &FakeCluster{}
	res := run(t, f, input("app:v2"), func(env *testsuite.TestWorkflowEnvironment) {
		env.RegisterDelayedCallback(func() { env.SignalWorkflow(RollbackSignal, "user clicked rollback") }, time.Minute)
	})
	require.Equal(t, StatusRolledBack, res.Status)
	require.Equal(t, "old-1,old-2", ids(res.Active))
	ev := f.Events()
	require.Equal(t, []string{
		"wake old-1,old-2",
		"ready old-1,old-2",
		"routes v7 active=old-1,old-2 draining=ins-d2-1,ins-d2-2",
		"ack v7",
		"drained ins-d2-1,ins-d2-2",
		"routes v8 active=old-1,old-2 draining=",
		"ack v8",
		"stop ins-d2-1,ins-d2-2",
	}, ev[len(ev)-8:])
}

func TestHighErrorRateAfterBakeRollsBack(t *testing.T) {
	f := &FakeCluster{ErrorRate: 0.2}
	res := run(t, f, input("app:v2"), nil)
	require.Equal(t, StatusRolledBack, res.Status)
	require.Contains(t, res.Reason, "error rate 20.0%")
	require.NotContains(t, f.Events(), "stop old-1,old-2")
}

func TestFirstDeployHasNoOldInstances(t *testing.T) {
	f := &FakeCluster{}
	in := input("app:v1")
	in.Old = nil
	res := run(t, f, in, nil)
	require.Equal(t, StatusActive, res.Status)
	require.Equal(t, "ins-d2-1,ins-d2-2", ids(res.Active))
}

func TestHostLostWhileBootingReplacesBatch(t *testing.T) {
	f := &FakeCluster{LoseHostOnce: true}
	res := run(t, f, input("app:v2"), nil)
	require.Equal(t, StatusActive, res.Status)
	require.Equal(t, "ins-d2-2,ins-d2-3", ids(res.Active))
	require.Equal(t, []string{"place 1", "start ins-d2-1", "lost ins-d2-1", "stop ins-d2-1", "place 1", "start ins-d2-2", "ready ins-d2-2"}, f.Events()[:7])
}

func TestRepairReplacesInstancesOnLostHosts(t *testing.T) {
	f := &FakeCluster{LostReport: LostReport{
		Healthy: []Instance{{ID: "old-2", Host: "host-a", Deployment: "d1"}},
		Lost:    []Instance{{ID: "old-1", Host: "host-b", Deployment: "d1", Image: "app:v1"}},
	}}
	var s testsuite.WorkflowTestSuite
	env := s.NewTestWorkflowEnvironment()
	env.RegisterActivity(&Activities{Cluster: f})
	env.ExecuteWorkflow(Repair, RepairInput{ServiceID: "svc"})
	require.NoError(t, env.GetWorkflowError())
	var res RepairResult
	require.NoError(t, env.GetWorkflowResult(&res))
	require.Equal(t, "old-2,ins-d1-1", ids(res.Active))
	require.Equal(t, []string{
		"find lost",
		"routes v1 active=old-2 draining=",
		"ack v1",
		"place 1",
		"start ins-d1-1",
		"ready ins-d1-1",
		"routes v2 active=old-2,ins-d1-1 draining=",
		"ack v2",
		"stop old-1",
	}, f.Events())
}

func TestRepairWithBadReplacementLeavesHealthyServing(t *testing.T) {
	f := &FakeCluster{FailReadyImage: "bad", LostReport: LostReport{
		Healthy: []Instance{{ID: "old-2", Host: "host-a", Deployment: "d1"}},
		Lost:    []Instance{{ID: "old-1", Host: "host-b", Deployment: "d1", Image: "app:bad"}},
	}}
	var s testsuite.WorkflowTestSuite
	env := s.NewTestWorkflowEnvironment()
	env.RegisterActivity(&Activities{Cluster: f})
	env.ExecuteWorkflow(Repair, RepairInput{ServiceID: "svc"})
	require.NoError(t, env.GetWorkflowError())
	var res RepairResult
	require.NoError(t, env.GetWorkflowResult(&res))
	require.Contains(t, res.Reason, "never became ready")
	require.Equal(t, "old-2", ids(res.Active))
	require.NotContains(t, f.Events(), "stop old-1")
}
