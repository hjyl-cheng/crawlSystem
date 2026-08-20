package services

import (
	"context"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/alpkeskin/rota/core/internal/models"
	"github.com/alpkeskin/rota/core/pkg/logger"
)

func TestAlertWebhookTransportErrorRedactsURLSecrets(t *testing.T) {
	server := httptest.NewServer(nil)
	rawURL := server.URL + "/bot-secret-token/send?chat_id=secret-chat"
	server.Close()

	watcher := NewAlertWatcher(nil, logger.New("error"))
	err := watcher.fire(context.Background(), models.PoolAlertRule{
		WebhookURL: rawURL,
	}, models.ProxyPool{})
	if err == nil {
		t.Fatal("expected webhook transport error")
	}
	for _, secret := range []string{"secret-token", "secret-chat"} {
		if strings.Contains(err.Error(), secret) {
			t.Fatalf("webhook error leaked %q: %v", secret, err)
		}
	}
}

func TestAlertWatcherRunStopsWithOwnerContext(t *testing.T) {
	watcher := NewAlertWatcher(nil, logger.New("error"))
	watcher.interval = time.Hour
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		watcher.Run(ctx)
		close(done)
	}()
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("alert watcher did not stop")
	}
}
