package capensis

import (
	"context"
	"path/filepath"
	"testing"
	"time"
)

func jetstreamConfig(form string) Config {
	return Config{Targets: []Target{{PDS: "a.example"}}, Identifier: "id", Password: "pw", Slingshot: "https://sling.example", Jetstream: form, Database: "x.db", RescanInterval: time.Hour}
}

// Regression: explicit and omitted default ports produced different
// endpoint strings, and the stored cursor is keyed by that string.
func TestValidateCanonicalizesJetstreamEndpoint(t *testing.T) {
	want := "wss://jet.example" + streamPath
	for _, form := range []string{"wss://jet.example", "wss://jet.example:443", "WSS://JET.EXAMPLE:443", "wss://jet.example:0443", "https://JET.example:443", "wss://jet.example:443" + streamPath} {
		cfg := jetstreamConfig(form)
		if err := cfg.validate(); err != nil {
			t.Fatalf("jetstream %q: %v", form, err)
		}
		if cfg.Jetstream != want {
			t.Fatalf("jetstream %q normalized to %q, want %q", form, cfg.Jetstream, want)
		}
	}
	for _, invalid := range []string{"wss://jet.example:0", "wss://jet.example:99999"} {
		cfg := jetstreamConfig(invalid)
		if err := cfg.validate(); err == nil {
			t.Fatalf("jetstream %q accepted, normalized to %q", invalid, cfg.Jetstream)
		}
	}
}

// A checkpoint saved under a pre-canonicalization spelling must resume
// under the canonical spelling, and a typed upgrade must rewrite the
// stored endpoint instead of abandoning its sequence.
func TestJetstreamCheckpointRecoveredAcrossRespelling(t *testing.T) {
	ctx := context.Background()
	for _, stored := range []string{"wss://jet.example:443" + streamPath, "wss://JET.example:0443" + streamPath, "wss://jet.example" + streamPath} {
		path := filepath.Join(t.TempDir(), "state.sqlite")
		db, err := openStore(path)
		if err != nil {
			t.Fatal(err)
		}
		if err := db.ingest(ctx, "", checkpoint{TimeUS: 10_000_000, Seq: 42, Endpoint: stored}, false); err != nil {
			t.Fatal(err)
		}
		if _, err := db.conn.Exec(`DELETE FROM goose_db_version WHERE version_id>2`); err != nil {
			t.Fatal(err)
		}
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
		db, err = openStore(path)
		if err != nil {
			t.Fatal(err)
		}
		cursor, err := db.resumeCursor(ctx, "wss://jet.example"+streamPath)
		if err != nil || cursor != 42 {
			t.Fatalf("stored %q: resume = %d, %v; want saved sequence 42", stored, cursor, err)
		}
		state, err := db.queries.GetState(ctx)
		if err != nil || state.StreamEndpoint != "wss://jet.example"+streamPath {
			t.Fatalf("stored %q: upgraded endpoint = %q, want canonical", stored, state.StreamEndpoint)
		}
		if cursor, err := db.resumeCursor(ctx, "wss://other.example"+streamPath); err != nil || cursor != 5_000_000 {
			t.Fatalf("different endpoint must keep timestamp fallback: %d %v", cursor, err)
		}
		db.Close()
	}
	// Runtime recovery even without the upgrade rewriting the row.
	db := testStore(t)
	if err := db.ingest(ctx, "", checkpoint{TimeUS: 10_000_000, Seq: 7, Endpoint: "wss://jet.example:443" + streamPath}, false); err != nil {
		t.Fatal(err)
	}
	if cursor, err := db.resumeCursor(ctx, "wss://jet.example"+streamPath); err != nil || cursor != 7 {
		t.Fatalf("unrewritten stored spelling not recovered: %d %v", cursor, err)
	}
}
