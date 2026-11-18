package helix

import (
	"context"
	"testing"
)

// Sequence is endpoint-local: resumeCursor only returns stream_seq when
// the stored endpoint matches. Monotonicity therefore applies within an
// endpoint, and a new endpoint's lower sequence must be adopted.

func TestCheckpointSequenceNeverRegressesOnSameEndpoint(t *testing.T) {
	ctx := context.Background()
	db := testStore(t)
	if err := db.ingest(ctx, "", checkpoint{TimeUS: 10_000_000, Seq: 100, Endpoint: "wss://a.example"}, false); err != nil {
		t.Fatal(err)
	}
	if err := db.ingest(ctx, "", checkpoint{TimeUS: 9_000_000, Seq: 90, Endpoint: "wss://a.example"}, false); err != nil {
		t.Fatal(err)
	}
	if cursor, err := db.resumeCursor(ctx, "wss://a.example"); err != nil || cursor != 100 {
		t.Fatalf("resume regressed to %d, %v; want 100", cursor, err)
	}
}

func TestCheckpointAdoptsLowerSequenceFromNewEndpoint(t *testing.T) {
	ctx := context.Background()
	db := testStore(t)
	if err := db.ingest(ctx, "", checkpoint{TimeUS: 10_000_000, Seq: 100, Endpoint: "wss://a.example"}, false); err != nil {
		t.Fatal(err)
	}
	if err := db.ingest(ctx, "", checkpoint{TimeUS: 10_000_001, Seq: 5, Endpoint: "wss://b.example"}, false); err != nil {
		t.Fatal(err)
	}
	if cursor, err := db.resumeCursor(ctx, "wss://b.example"); err != nil || cursor != 5 {
		t.Fatalf("new endpoint resume = %d, %v; want its own sequence 5", cursor, err)
	}
	if cursor, err := db.resumeCursor(ctx, "wss://a.example"); err != nil || cursor != 5_000_001 {
		t.Fatalf("old endpoint resume = %d, %v; want portable cursor 5000001", cursor, err)
	}
	// A zero sequence never replaces an endpoint checkpoint.
	if err := db.ingest(ctx, "", checkpoint{TimeUS: 10_000_002, Seq: 0, Endpoint: "wss://a.example"}, false); err != nil {
		t.Fatal(err)
	}
	if cursor, err := db.resumeCursor(ctx, "wss://b.example"); err != nil || cursor != 5 {
		t.Fatalf("seq=0 ingest replaced checkpoint: %d, %v; want 5", cursor, err)
	}
}

func TestCheckpointInvalidRecoveryNeverRegressesOnSameEndpoint(t *testing.T) {
	ctx := context.Background()
	db := testStore(t)
	if err := db.ingest(ctx, "", checkpoint{TimeUS: 10_000_000, Seq: 100, Endpoint: "wss://a.example"}, false); err != nil {
		t.Fatal(err)
	}
	if err := db.recoverStream(ctx, "wss://a.example", 90, "old invalid frame"); err != nil {
		t.Fatal(err)
	}
	if cursor, err := db.resumeCursor(ctx, "wss://a.example"); err != nil || cursor != 100 {
		t.Fatalf("invalid older frame regressed resume to %d, %v; want 100", cursor, err)
	}
	state, err := db.queries.GetState(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if state.RescanRequested != 1 || state.InvalidFrame != "old invalid frame" {
		t.Fatalf("recovery side effects lost: %+v", state)
	}
}

func TestCheckpointInvalidRecoveryAdoptsLowerSequenceFromNewEndpoint(t *testing.T) {
	ctx := context.Background()
	db := testStore(t)
	if err := db.ingest(ctx, "", checkpoint{TimeUS: 10_000_000, Seq: 100, Endpoint: "wss://a.example"}, false); err != nil {
		t.Fatal(err)
	}
	if err := db.recoverStream(ctx, "wss://b.example", 7, "invalid frame"); err != nil {
		t.Fatal(err)
	}
	if cursor, err := db.resumeCursor(ctx, "wss://b.example"); err != nil || cursor != 7 {
		t.Fatalf("new endpoint resume = %d, %v; want its own sequence 7", cursor, err)
	}
}

func TestCheckpointResetAllowsLowerSequenceFromNewEndpoint(t *testing.T) {
	ctx := context.Background()
	db := testStore(t)
	if err := db.ingest(ctx, "", checkpoint{TimeUS: 10_000_000, Seq: 100, Endpoint: "wss://a.example"}, false); err != nil {
		t.Fatal(err)
	}
	if err := db.resetStream(ctx); err != nil {
		t.Fatal(err)
	}
	if err := db.ingest(ctx, "", checkpoint{TimeUS: 10_000_001, Seq: 5, Endpoint: "wss://b.example"}, false); err != nil {
		t.Fatal(err)
	}
	if cursor, err := db.resumeCursor(ctx, "wss://b.example"); err != nil || cursor != 5 {
		t.Fatalf("post-reset resume = %d, %v; want 5", cursor, err)
	}
}
