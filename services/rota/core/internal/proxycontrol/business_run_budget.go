package proxycontrol

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
)

func (m *Manager) BusinessRunBudget(
	ctx context.Context,
	businessRunID string,
) (BusinessRunBudget, error) {
	if err := m.requireEnabled(); err != nil {
		return BusinessRunBudget{}, err
	}
	businessRunID = strings.TrimSpace(businessRunID)
	if businessRunID == "" || len([]byte(businessRunID)) > 255 {
		return BusinessRunBudget{}, fmt.Errorf("%w: business_run_id", ErrInvalidInput)
	}

	result := BusinessRunBudget{
		OK:            true,
		WorkloadScope: m.options.WorkloadScope,
		BusinessRunID: businessRunID,
	}
	err := m.db.Pool.QueryRow(ctx, `
		SELECT
		  (SELECT COUNT(*)::int
		   FROM proxy_control_tasks task
		   WHERE task.workload_scope=run.workload_scope
		     AND task.business_run_id=run.business_run_id),
		  run.max_network_attempts_per_business_run,
		  COALESCE(latest.job_execution_id,''),
		  COALESCE(latest.task_count,0),
		  1+run.max_route_switches_per_execution,
		  run.budget_exhausted_at
		FROM proxy_control_business_runs run
		LEFT JOIN LATERAL (
		  SELECT task.job_execution_id,COUNT(*)::int AS task_count,
		         MAX(task.attempt_number) AS latest_attempt
		  FROM proxy_control_tasks task
		  WHERE task.workload_scope=run.workload_scope
		    AND task.business_run_id=run.business_run_id
		  GROUP BY task.job_execution_id
		  ORDER BY latest_attempt DESC,task.job_execution_id DESC
		  LIMIT 1
		) latest ON true
		WHERE run.workload_scope=$1 AND run.business_run_id=$2
	`, m.options.WorkloadScope, businessRunID).Scan(
		&result.BusinessTasksUsed,
		&result.BusinessTasksLimit,
		&result.CurrentExecutionID,
		&result.ExecutionTasksUsed,
		&result.ExecutionTasksLimit,
		&result.BudgetExhaustedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return BusinessRunBudget{}, ErrBusinessRunNotFound
	}
	if err != nil {
		return BusinessRunBudget{}, fmt.Errorf("load proxy control Business Run budget: %w", err)
	}
	return result, nil
}
