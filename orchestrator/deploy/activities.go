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
	Secrets    int    `json:"secrets,omitempty"` // secret set version; 0 = none
}

type PlaceRequest struct {
	ServiceID    string
	DeploymentID string
	Image        string
	Secrets      int
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
	// Canary backends together get CanaryPercent of the service's traffic; Active gets the rest.
	Canary        []Instance
	CanaryPercent int
}

type HealthReport struct {
	ErrorRate float64
}

// LostReport splits a service's routed instances by whether their host is still reporting.
type LostReport struct {
	Healthy []Instance
	Lost    []Instance // Image is filled in, so replacements run the same thing
}

var ErrNotReady = errors.New("instances not ready")

// ErrSecrets means a deployment's secret set can't be delivered (missing version, no key); retrying won't help.
var ErrSecrets = errors.New("secrets unavailable")

// ErrHostLost means an instance's host stopped reporting while it was booting; the deploy re-places it.
var ErrHostLost = errors.New("host lost")

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
	Lost(ctx context.Context, serviceID string) (LostReport, error)
}

type Activities struct {
	Cluster Cluster
}

func (a *Activities) PlaceInstances(ctx context.Context, req PlaceRequest) ([]Instance, error) {
	return a.Cluster.Place(ctx, req)
}

func (a *Activities) PublishHostChange(ctx context.Context, change HostChange) error {
	err := a.Cluster.PublishHosts(ctx, change)
	if errors.Is(err, ErrSecrets) {
		return temporal.NewNonRetryableApplicationError(err.Error(), "Secrets", err)
	}
	return err
}

func (a *Activities) WaitReady(ctx context.Context, ins []Instance, timeout time.Duration) error {
	err := a.Cluster.WaitReady(ctx, ins, timeout)
	if errors.Is(err, ErrHostLost) {
		return temporal.NewNonRetryableApplicationError(err.Error(), HostLostType, err)
	}
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

func (a *Activities) FindLost(ctx context.Context, serviceID string) (LostReport, error) {
	return a.Cluster.Lost(ctx, serviceID)
}
