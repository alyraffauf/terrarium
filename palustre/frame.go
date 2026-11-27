package palustre

import (
	"encoding/json"

	"github.com/alyraffauf/terrarium/palustre/jetstream"
	"github.com/bluesky-social/indigo/xrpc"
)

const StreamPath = "/xrpc/network.bsky.jetstream.subscribeEvents"
const EventPrefix = "network.bsky.jetstream.subscribeEvents#"

type Frame struct {
	Type string `json:"$type"`
	xrpc.XRPCError
	Payload struct {
		// The classifier reads the common event fields and commit metadata.
		// Defer record decoding so an unknown record cannot invalidate the cursor.
		jetstream.SubscribeEvents_Commit
		jetstream.SubscribeEvents_Info
		Type   string          `json:"$type"`
		Record json.RawMessage `json:"record"`
	} `json:"payload"`
}
