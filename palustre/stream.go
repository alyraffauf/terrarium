package palustre

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/bluesky-social/indigo/xrpc"
	"github.com/gorilla/websocket"
)

var ErrStreamGap = errors.New("Jetstream cursor is outside available history")

// Stream calls handle serially. A handler error closes the socket without acknowledging later events.
func Stream(ctx context.Context, endpoint string, cursor int64, handle func([]byte) error) error {
	parsed, err := url.Parse(endpoint)
	if err != nil {
		return err
	}
	query := parsed.Query()
	query.Set("cursor", strconv.FormatInt(cursor, 10))
	// Identity/account events remain unfiltered across the network.
	query.Set("collections", "app.bsky.graph.follow")
	parsed.RawQuery = query.Encode()
	dialer := websocket.Dialer{HandshakeTimeout: 30 * time.Second, Proxy: http.ProxyFromEnvironment, Subprotocols: []string{"xrpc.v1.json"}}
	connection, response, err := dialer.DialContext(ctx, parsed.String(), nil)
	if response != nil && response.Body != nil {
		if err != nil {
			var rejection xrpc.XRPCError
			if json.NewDecoder(io.LimitReader(response.Body, 1<<20)).Decode(&rejection) == nil && rejection.ErrStr == "CursorTooOld" {
				err = ErrStreamGap
			}
		}
		response.Body.Close()
	}
	if err != nil {
		return err
	}
	defer connection.Close()
	connection.SetReadLimit(1 << 20)
	stop := context.AfterFunc(ctx, func() { connection.Close() })
	defer stop()
	connection.SetPongHandler(func(string) error { return connection.SetReadDeadline(time.Now().Add(90 * time.Second)) })
	pingCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-pingCtx.Done():
				return
			case <-ticker.C:
				if err := connection.WriteControl(websocket.PingMessage, nil, time.Now().Add(10*time.Second)); err != nil {
					connection.Close()
					return
				}
			}
		}
	}()

	for {
		if err := connection.SetReadDeadline(time.Now().Add(90 * time.Second)); err != nil {
			return err
		}
		_, payload, err := connection.ReadMessage()
		if errors.Is(err, websocket.ErrReadLimit) {
			// Reconnecting at the same checkpoint would replay the oversized message.
			return fmt.Errorf("%w: %w", ErrStreamGap, err)
		}
		if err != nil {
			return err
		}
		if err := handle(payload); err != nil {
			return err
		}
	}
}
func Pause(ctx context.Context, duration time.Duration) bool {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
