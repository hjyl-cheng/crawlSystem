package proxycontrol

import "fmt"

// liveLeaseFencePredicate keeps the Slot and Lease history halves of a live
// Lease in one predicate shared by Route control SQL.
func liveLeaseFencePredicate(workloadScopeParameter int) string {
	return fmt.Sprintf(`(
		slot.current_lease_id IS NOT NULL
		AND slot.lease_until > NOW()
		AND EXISTS (
		  SELECT 1
		  FROM proxy_control_leases live_lease
		  WHERE live_lease.workload_scope=$%d
		    AND live_lease.lease_id=slot.current_lease_id
		    AND live_lease.slot_name=slot.slot_name
		    AND live_lease.status='active'
		    AND live_lease.lease_until > NOW()
		)
	)`, workloadScopeParameter)
}

func expectedLiveLeaseFencePredicate(
	leaseIDParameter int,
	workloadScopeParameter int,
) string {
	return fmt.Sprintf(
		"(slot.current_lease_id=$%d AND %s)",
		leaseIDParameter,
		liveLeaseFencePredicate(workloadScopeParameter),
	)
}
