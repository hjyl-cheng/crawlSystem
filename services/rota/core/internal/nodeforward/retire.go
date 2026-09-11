package nodeforward

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"time"
)

// Local stop is always possible, even when the center is unreachable or the
// channel has already finished. It cannot activate/renew/select any route.
type retireRequest struct {
	BootID     string `json:"boot_id"`
	Slot       string `json:"slot"`
	Epoch      int64  `json:"epoch"`
	TaskID     string `json:"task_id"`
	Generation int64  `json:"generation"`
}

func (r *Relay) retireHandler(w http.ResponseWriter, request *http.Request) {
	data, err := io.ReadAll(http.MaxBytesReader(w, request.Body, 4096))
	var expected retireRequest
	if err != nil || strictJSON(data, &expected) != nil || expected.Epoch < 1 || expected.Generation < 1 || expected.TaskID == "" || expected.BootID == "" {
		http.Error(w, "invalid retire request", 400)
		return
	}
	r.mu.Lock()
	s := r.slots[expected.Slot]
	if s == nil {
		r.mu.Unlock()
		http.Error(w, "unknown slot", 409)
		return
	}
	if expected.BootID != r.bootID {
		// Every grant from the previous relay process is invalid here. Do not
		// touch a route belonging to this process while acknowledging that fact.
		r.mu.Unlock()
		writeRetired(w, expected)
		return
	}
	if expected.Epoch > s.epoch && s.active == nil {
		// A grant persisted before apply may never have reached this relay.
		// Tombstone it so a delayed activation cannot revive it after cleanup.
		s.epoch = expected.Epoch
		s.lastRoute = &route{grant: Grant{TaskID: expected.TaskID, Generation: expected.Generation}}
	}
	if s.lastRoute == nil || s.epoch != expected.Epoch ||
		s.lastRoute.grant.TaskID != expected.TaskID || s.lastRoute.grant.Generation != expected.Generation {
		r.mu.Unlock()
		http.Error(w, "stale route", 409)
		return
	}
	current := s.lastRoute
	r.retireLocked(s)
	r.mu.Unlock()
	ctx, cancel := context.WithTimeout(request.Context(), 4*time.Second)
	defer cancel()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		r.mu.Lock()
		pending := current.inFlight
		r.mu.Unlock()
		if pending == 0 {
			break
		}
		select {
		case <-ctx.Done():
			http.Error(w, "route still retiring", 503)
			return
		case <-ticker.C:
		}
	}
	writeRetired(w, expected)
}

func writeRetired(w http.ResponseWriter, expected retireRequest) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(struct {
		retireRequest
		Retired  bool `json:"retired"`
		InFlight int  `json:"in_flight"`
	}{expected, true, 0})
}
