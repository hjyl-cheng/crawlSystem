package proxy

import (
	"context"
	"testing"

	"github.com/alpkeskin/rota/core/internal/models"
	"github.com/alpkeskin/rota/core/internal/proxycontrol"
)

func TestRouteActivationTokenIsMonotonicAndFencesStaleClaims(t *testing.T) {
	handler := NewUpstreamProxyHandler(nil, nil, &models.RotationSettings{}, nil)
	const username = "bullmq-channel-01-g2-token"

	first, err := handler.beginRouteActivation(username, "", "T1")
	if err != nil || first.AlreadyCommitted {
		t.Fatalf("Begin T1 = %+v, err = %v", first, err)
	}
	if handler.proxyUserReady(username) {
		t.Fatal("activating T1 was exposed to managed requests")
	}
	replayed, err := handler.beginRouteActivation(username, "", "T1")
	if err != nil || replayed.AlreadyCommitted {
		t.Fatalf("replayed activating T1 = %+v, err = %v", replayed, err)
	}
	if err := handler.commitRouteActivation(username, "T1"); err != nil {
		t.Fatalf("Commit T1: %v", err)
	}
	if !handler.proxyUserReady(username) {
		t.Fatal("committed T1 remained unavailable")
	}
	replayed, err = handler.beginRouteActivation(username, "", "T1")
	if err != nil || !replayed.AlreadyCommitted {
		t.Fatalf("replayed committed T1 = %+v, err = %v", replayed, err)
	}
	if retired, err := handler.retireProxyUserIfClaim(context.Background(), username, "T1"); err != nil || retired {
		t.Fatalf("conditional retirement of committed T1 = %v, err = %v", retired, err)
	}

	second, err := handler.beginRouteActivation(username, "T1", "T2")
	if err != nil || second.AlreadyCommitted {
		t.Fatalf("T2 takeover = %+v, err = %v", second, err)
	}
	if _, err := handler.beginRouteActivation(username, "", "T1"); err == nil {
		t.Fatal("delayed Begin T1 overwrote T2")
	}
	if err := handler.commitRouteActivation(username, "T1"); err == nil {
		t.Fatal("stale T1 committed after T2 takeover")
	}
	if err := handler.commitRouteActivation(username, "T2"); err != nil {
		t.Fatalf("Commit T2: %v", err)
	}
	if retired, err := handler.retireProxyUserIfClaim(context.Background(), username, "T1"); err != nil || retired {
		t.Fatalf("stale T1 retirement after T2 Commit = %v, err = %v", retired, err)
	}
	if !handler.proxyUserReady(username) {
		t.Fatal("stale T1 retirement disabled committed T2")
	}
}

func TestRouteActivationRegistryRebuildIsAConnectionBarrier(t *testing.T) {
	handler := NewUpstreamProxyHandler(nil, nil, &models.RotationSettings{}, nil)
	const (
		readyUsername   = "bullmq-channel-01-g7-ready"
		pendingUsername = "bullmq-channel-02-g8-pending"
		expiredUsername = "bullmq-channel-03-g4-expired"
	)

	handler.requireRouteActivationRegistry()
	if handler.proxyUserReady(readyUsername) {
		t.Fatal("managed user crossed the startup barrier before registry rebuild")
	}
	if !handler.proxyUserReady("ordinary-proxy-user") {
		t.Fatal("startup barrier blocked an unrelated proxy user")
	}
	if err := handler.rebuildRouteActivationRegistry(context.Background(), []proxycontrol.RouteActivationRegistryEntry{
		{Username: readyUsername, Phase: proxycontrol.RouteActivationCommitted},
		{
			Username: pendingUsername,
			ClaimID:  "T-pending",
			Phase:    proxycontrol.RouteActivationActivating,
			Blocked:  true,
		},
		{Username: expiredUsername},
	}); err != nil {
		t.Fatalf("rebuild activation registry: %v", err)
	}
	if !handler.proxyUserReady(readyUsername) {
		t.Fatal("ready Route was not reconstructed as committed")
	}
	if handler.proxyUserReady(pendingUsername) {
		t.Fatal("pending Route was exposed after registry rebuild")
	}
	if handler.proxyUserReady(expiredUsername) {
		t.Fatal("expired or unleased managed Route was exposed after registry rebuild")
	}
	if handler.proxyUserReady("bullmq-channel-04-g1-unknown") {
		t.Fatal("managed identity absent from the authoritative registry was exposed")
	}

	begin, err := handler.beginRouteActivation(pendingUsername, "", "T-pending")
	if err != nil || begin.AlreadyCommitted {
		t.Fatalf("resume reconstructed T-pending = %+v, err = %v", begin, err)
	}
	if err := handler.commitRouteActivation(pendingUsername, "T-pending"); err != nil {
		t.Fatalf("commit reconstructed T-pending: %v", err)
	}
	if !handler.proxyUserReady(pendingUsername) {
		t.Fatal("committed reconstructed Route remained unavailable")
	}
}
