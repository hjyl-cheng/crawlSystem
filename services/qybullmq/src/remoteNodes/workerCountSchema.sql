-- Explicit upgrade of existing registrations; no task or collection data changes.
-- Apply after workerActivationSchema.sql, in the admin migration transaction.
ALTER TABLE remote_ingestion.node_deployments
  DROP CONSTRAINT IF EXISTS node_deployments_worker_count_check,
  ADD CONSTRAINT node_deployments_worker_count_check CHECK (worker_count >= 0);
ALTER TABLE remote_ingestion.nodes
  DROP CONSTRAINT IF EXISTS nodes_max_leases_check,
  ADD CONSTRAINT nodes_max_leases_check CHECK (max_leases >= 1);
