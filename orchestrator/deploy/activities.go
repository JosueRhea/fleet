package deploy

import (
	"context"
	"errors"
	"time"

	"go.temporal.io/sdk/temporal"
)

type Instance struct {
	ID         string `json:"id"`
	Host       string `json:"host"`
	Deployment string `json:"deployment"`
	Service    string `json:"service,omitempty"`
	Image      string `json:"image,omitempty"`
}

type PlaceRequest struct {
	ServiceID    string
	DeploymentID string
	Image        string
	Count        int
}

// HostChange is a delta the publisher folds into the affected host docs.
type HostChange struct {
	Image string // for Start
	Start []Instance
	Sleep []Instance // stop but keep the memory snapshot, so rollback is a fast wake
	Wake  []Instance
	Stop  []Instance // tombstone
}

// RouteUpdate is the full backend set for one service in the edge doc.
type RouteUpdate struct {
	ServiceID string
	Active    []Instance
	Draining  []Instance
}

type HealthReport struct {
	ErrorRate float64
}

var ErrNotReady = errors.New("instances not ready")

// Cluster is everything the deploy workflow needs from the rest of the cell
// (scheduler, state publisher, status ingest). The workflow never talks to hosts or edges directly.
type Cluster interface {
	Place(ctx context.Context, req PlaceRequest) ([]Instance, error)
	PublishHosts(ctx context.Context, change HostChange) error
	WaitReady(ctx context.Context, ins []Instance, timeout time.Duration) error
	PublishRoutes(ctx context.Context, update RouteUpdate) (version int64, err error)
	WaitEdgeAck(ctx context.Context, version int64) error
	WaitDrained(ctx context.Context, ins []Instance, timeout time.Duration) error
	Health(ctx context.Context, serviceID, deploymentID string) (HealthReport, error)
}

type Activities struct {
	Cluster Cluster
}

func (a *Activities) PlaceInstances(ctx context.Context, req PlaceRequest) ([]Instance, error) {
	return a.Cluster.Place(ctx, req)
}

func (a *Activities) PublishHostChange(ctx context.Context, change HostChange) error {
	return a.Cluster.PublishHosts(ctx, change)
}

func (a *Activities) WaitReady(ctx context.Context, ins []Instance, timeout time.Duration) error {
	err := a.Cluster.WaitReady(ctx, ins, timeout)
	if errors.Is(err, ErrNotReady) {
		return temporal.NewNonRetryableApplicationError(err.Error(), "NotReady", err)
	}
	return err
}

func (a *Activities) PublishRoutes(ctx context.Context, update RouteUpdate) (int64, error) {
	return a.Cluster.PublishRoutes(ctx, update)
}

func (a *Activities) WaitEdgeAck(ctx context.Context, version int64) error {
	return a.Cluster.WaitEdgeAck(ctx, version)
}

func (a *Activities) WaitDrained(ctx context.Context, ins []Instance, timeout time.Duration) error {
	return a.Cluster.WaitDrained(ctx, ins, timeout)
}

func (a *Activities) CheckHealth(ctx context.Context, serviceID, deploymentID string) (HealthReport, error) {
	return a.Cluster.Health(ctx, serviceID, deploymentID)
}
