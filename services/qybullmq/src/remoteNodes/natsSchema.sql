-- Opt-in transport only: no Clock, crawler policy or channel ownership changes.
CREATE TABLE IF NOT EXISTS remote_ingestion.transport_receipts (
  receipt_id TEXT PRIMARY KEY CHECK (receipt_id ~ '^[a-f0-9]{64}$'),
  node_id UUID NOT NULL REFERENCES remote_ingestion.nodes(node_id),
  task_id UUID NOT NULL REFERENCES remote_ingestion.tasks(task_id),
  response JSONB NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX IF NOT EXISTS remote_transport_receipts_created ON remote_ingestion.transport_receipts(created_at);

CREATE OR REPLACE FUNCTION remote_ingestion.notify_transport() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  -- Notifications are hints emitted only after COMMIT. SQL remains authoritative;
  -- reconnect/timeout reads recover missed notifications without another outbox.
  PERFORM pg_notify('qy_remote_transport', 'task:' || NEW.task_id::text);
  IF TG_TABLE_NAME = 'tasks' THEN
   IF NEW.target_node_id IS NOT NULL THEN
    PERFORM pg_notify('qy_remote_transport', 'node:' || NEW.target_node_id::text);
   END IF;
  END IF;
  IF TG_TABLE_NAME = 'network_bindings' THEN
    PERFORM pg_notify('qy_remote_transport', 'binding:' || NEW.binding_id::text);
  END IF;
  IF TG_TABLE_NAME = 'transport_receipts' THEN
    PERFORM pg_notify('qy_remote_transport', 'receipt:' || NEW.receipt_id);
  END IF;
  RETURN NEW;
END $$;
DROP TRIGGER IF EXISTS remote_command_notify ON remote_ingestion.channel_commands;
CREATE TRIGGER remote_command_notify AFTER INSERT OR UPDATE OF state ON remote_ingestion.channel_commands
  FOR EACH ROW EXECUTE FUNCTION remote_ingestion.notify_transport();
DROP TRIGGER IF EXISTS remote_task_notify ON remote_ingestion.tasks;
CREATE TRIGGER remote_task_notify AFTER INSERT OR UPDATE OF state, generation ON remote_ingestion.tasks
  FOR EACH ROW EXECUTE FUNCTION remote_ingestion.notify_transport();
DROP TRIGGER IF EXISTS remote_receipt_notify ON remote_ingestion.transport_receipts;
CREATE TRIGGER remote_receipt_notify AFTER INSERT ON remote_ingestion.transport_receipts
  FOR EACH ROW EXECUTE FUNCTION remote_ingestion.notify_transport();

DROP TRIGGER IF EXISTS remote_binding_notify ON remote_ingestion.network_bindings;
CREATE TRIGGER remote_binding_notify AFTER INSERT OR UPDATE OF state ON remote_ingestion.network_bindings
  FOR EACH ROW EXECUTE FUNCTION remote_ingestion.notify_transport();

-- Admission/config changes wake control loops. Routine heartbeat timestamps do
-- not cause a full fleet scan; revival after expiry does need a notification.
CREATE OR REPLACE FUNCTION remote_ingestion.notify_control() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF TG_TABLE_NAME = 'worker_connections' THEN
    IF TG_OP = 'UPDATE' THEN
      IF (to_jsonb(NEW) - ARRAY['last_seen_at','connected_until']) =
         (to_jsonb(OLD) - ARRAY['last_seen_at','connected_until'])
         AND NOT (COALESCE(OLD.connected_until, '-infinity') <= clock_timestamp()
                  AND NEW.connected_until > clock_timestamp()) THEN RETURN NEW; END IF;
    END IF;
    PERFORM pg_notify('qy_remote_transport','supervisor');
  ELSIF TG_TABLE_NAME = 'local_incremental_workers' THEN
    IF TG_OP = 'UPDATE' THEN
      IF NEW.activation_requested IS NOT DISTINCT FROM OLD.activation_requested THEN RETURN NEW; END IF;
    END IF;
    PERFORM pg_notify('qy_remote_transport','local-intake:' || NEW.worker_id);
  ELSE
    PERFORM pg_notify('qy_remote_transport','credentials');
    PERFORM pg_notify('qy_remote_transport','supervisor');
  END IF;
  RETURN COALESCE(NEW,OLD);
END $$;
DO $$ BEGIN
  IF to_regclass('remote_ingestion.worker_connections') IS NOT NULL THEN
    DROP TRIGGER IF EXISTS remote_worker_control_notify ON remote_ingestion.worker_connections;
    CREATE TRIGGER remote_worker_control_notify AFTER INSERT OR UPDATE OR DELETE ON remote_ingestion.worker_connections
      FOR EACH ROW EXECUTE FUNCTION remote_ingestion.notify_control();
  END IF;
  IF to_regclass('remote_ingestion.node_deployments') IS NOT NULL THEN
    DROP TRIGGER IF EXISTS remote_deployment_notify ON remote_ingestion.node_deployments;
    CREATE TRIGGER remote_deployment_notify AFTER INSERT OR UPDATE OR DELETE ON remote_ingestion.node_deployments
      FOR EACH ROW EXECUTE FUNCTION remote_ingestion.notify_control();
  END IF;
  IF to_regclass('remote_ingestion.local_incremental_workers') IS NOT NULL THEN
    DROP TRIGGER IF EXISTS local_intake_control_notify ON remote_ingestion.local_incremental_workers;
    CREATE TRIGGER local_intake_control_notify AFTER INSERT OR UPDATE OF activation_requested ON remote_ingestion.local_incremental_workers
      FOR EACH ROW EXECUTE FUNCTION remote_ingestion.notify_control();
  END IF;
END $$;
DROP TRIGGER IF EXISTS remote_node_control_notify ON remote_ingestion.nodes;
CREATE TRIGGER remote_node_control_notify AFTER INSERT OR DELETE OR UPDATE OF state, token_hash ON remote_ingestion.nodes
  FOR EACH ROW EXECUTE FUNCTION remote_ingestion.notify_control();
