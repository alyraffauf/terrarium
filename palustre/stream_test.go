package palustre

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestOversizedStreamMessageRequestsGapRecovery(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		upgrader := websocket.Upgrader{}
		connection, err := upgrader.Upgrade(writer, request, nil)
		if err != nil {
			t.Error(err)
			return
		}
		defer connection.Close()
		// Closing the connection while the client rejects the frame can interrupt this write.
		_ = connection.WriteMessage(websocket.TextMessage, make([]byte, (1<<20)+1))
	}))
	defer server.Close()

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	endpoint := "ws" + strings.TrimPrefix(server.URL, "http")
	err := Stream(ctx, endpoint, 1, func([]byte) error {
		t.Error("oversized message reached the frame handler")
		return nil
	})
	if !errors.Is(err, ErrStreamGap) {
		t.Fatalf("Stream returned %v, want a stream gap", err)
	}
}
