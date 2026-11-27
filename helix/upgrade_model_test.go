package helix

import (
	"database/sql"
	"fmt"
	"io/fs"
	"testing"

	"github.com/alyraffauf/terrarium/helix/storage"
)

func FuzzTypedUpgrade(f *testing.F) {
	f.Add(byte(5), uint16(77))
	f.Fuzz(func(t *testing.T, n byte, seq uint16) {
		path := t.TempDir() + "/legacy"
		legacy, err := sql.Open("sqlite", path)
		if err != nil {
			t.Fatal(err)
		}
		schema, err := fs.ReadFile(storage.Migrations, "00001_initial.sql")
		if err != nil {
			t.Fatal(err)
		}
		_, err = legacy.Exec(string(schema))
		if err != nil {
			t.Fatal(err)
		}
		_, err = legacy.Exec(`CREATE TABLE goose_db_version(id INTEGER PRIMARY KEY,version_id BIGINT NOT NULL,is_applied BOOLEAN NOT NULL,tstamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP); INSERT INTO goose_db_version(version_id,is_applied) VALUES(0,1),(1,1); UPDATE sync_state SET cursor_us=10000000,stream_endpoint='wss://JET.example:0443',stream_seq=?`, seq)
		if err != nil {
			t.Fatal(err)
		}
		for i := range int(n%5) + 1 {
			_, err = legacy.Exec("INSERT INTO candidates VALUES(?,?)", fmt.Sprint(i), fmt.Sprintf("https://PDS%d.example:443", i))
			if err != nil {
				t.Fatal(err)
			}
		}
		if err = legacy.Close(); err != nil {
			t.Fatal(err)
		}
		for range 2 {
			db, err := openStore(path)
			if err != nil {
				t.Fatal(err)
			}
			for i := range int(n%5) + 1 {
				host, err := db.candidatePDS(t.Context(), fmt.Sprint(i))
				if err != nil || host != fmt.Sprintf("https://pds%d.example", i) {
					t.Fatalf("host %d %q %v", i, host, err)
				}
			}
			got, err := db.resumeCursor(t.Context(), "wss://jet.example"+streamPath)
			want := int64(seq)
			if seq == 0 {
				want = 5000000
			}
			if err != nil || got != want {
				t.Fatalf("cursor %d want %d %v", got, want, err)
			}
			if err = db.Close(); err != nil {
				t.Fatal(err)
			}
		}
	})
}
