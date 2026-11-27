package palustre

import "encoding/json"

const StreamPath = "/xrpc/network.bsky.jetstream.subscribeEvents"
const EventPrefix = "network.bsky.jetstream.subscribeEvents#"

type Frame struct {
	Type    string `json:"$type"`
	Error   string `json:"error"`
	Message string `json:"message"`
	Payload struct {
		Type        string          `json:"$type"`
		DID         string          `json:"did"`
		Seq         int64           `json:"seq"`
		Time        string          `json:"time"`
		WitnessedAt string          `json:"witnessedAt"`
		Collection  string          `json:"collection"`
		Name        string          `json:"name"`
		Rkey        string          `json:"rkey"`
		Operation   string          `json:"operation"`
		Record      json.RawMessage `json:"record"`
	} `json:"payload"`
}
