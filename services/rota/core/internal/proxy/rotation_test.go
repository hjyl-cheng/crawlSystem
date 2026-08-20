package proxy

import (
	"context"
	"reflect"
	"testing"

	"github.com/alpkeskin/rota/core/internal/models"
)

func TestLeastConnectionsUsesLiveSelectionCounts(t *testing.T) {
	selector := &LeastConnectionsSelector{
		BaseSelector: &BaseSelector{
			proxies: []*models.Proxy{{ID: 1}, {ID: 2}, {ID: 3}},
		},
		counts: make(map[int]int64),
	}

	var got []int
	for range 6 {
		selected, err := selector.Select(context.Background())
		if err != nil {
			t.Fatalf("select: %v", err)
		}
		got = append(got, selected.ID)
	}
	if want := []int{1, 2, 3, 1, 2, 3}; !reflect.DeepEqual(got, want) {
		t.Fatalf("selection order = %v, want %v", got, want)
	}
}
