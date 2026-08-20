package models

import (
	"encoding/json"
	"testing"
)

func TestUpdateProxyUserRequestTracksMainPoolPresence(t *testing.T) {
	tests := []struct {
		name      string
		body      string
		wantSet   bool
		wantValue *int
	}{
		{name: "omitted", body: `{}`, wantSet: false},
		{name: "explicit null", body: `{"main_pool_id":null}`, wantSet: true},
		{name: "replacement", body: `{"main_pool_id":42}`, wantSet: true, wantValue: intPointer(42)},
		{name: "zero sentinel", body: `{"main_pool_id":0}`, wantSet: true, wantValue: intPointer(0)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var request UpdateProxyUserRequest
			if err := json.Unmarshal([]byte(tt.body), &request); err != nil {
				t.Fatalf("decode request: %v", err)
			}
			if request.HasMainPoolID() != tt.wantSet {
				t.Fatalf("HasMainPoolID() = %t, want %t", request.HasMainPoolID(), tt.wantSet)
			}
			if tt.wantValue == nil {
				if request.MainPoolID != nil {
					t.Fatalf("MainPoolID = %d, want nil", *request.MainPoolID)
				}
				return
			}
			if request.MainPoolID == nil || *request.MainPoolID != *tt.wantValue {
				t.Fatalf("MainPoolID = %v, want %d", request.MainPoolID, *tt.wantValue)
			}
		})
	}
}

func intPointer(value int) *int {
	return &value
}
