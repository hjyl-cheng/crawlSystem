package proxycontrol

import "testing"

func TestAppliedHealthVerdictRequestsReconciliation(t *testing.T) {
	manager := New(nil, nil, nil, Options{}, nil)

	manager.NotifyHealthVerdictApplied(785)
	select {
	case <-manager.reconcileRequests:
	default:
		t.Fatal("applied Health Verdict did not request reconciliation")
	}
}
