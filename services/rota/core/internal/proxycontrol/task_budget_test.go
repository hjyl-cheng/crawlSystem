package proxycontrol

import (
	"errors"
	"testing"
)

func TestTaskBudgetPrioritizesBusinessRunExhaustion(t *testing.T) {
	err := taskBudgetError(10, 9, 3, 2)
	if !errors.Is(err, ErrBusinessRunBudget) {
		t.Fatalf("budget error = %v, want business run budget", err)
	}
}

func TestTaskBudgetUsesExecutionLimitWhileRunHasAttempts(t *testing.T) {
	err := taskBudgetError(4, 9, 3, 2)
	if !errors.Is(err, ErrExecutionBudget) {
		t.Fatalf("budget error = %v, want execution budget", err)
	}
}
