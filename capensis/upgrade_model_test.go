package capensis

import (
	"database/sql"
	"fmt"
	"slices"
	"testing"

	m "github.com/alyraffauf/terrarium/internal/syncmodel"
)

func FuzzLegacyUpgrade(f *testing.F) {
	f.Add(byte(7), uint16(91))
	f.Fuzz(func(t *testing.T, flags byte, seq uint16) {
		path := t.TempDir() + "/legacy"
		db, err := sql.Open("sqlite", path)
		if err != nil {
			t.Fatal(err)
		}
		host := []string{"https://ONE.example:0443", "https://one.example", "https://one.example:443"}[flags%3]
		_, err = db.Exec(`CREATE TABLE metadata(key TEXT PRIMARY KEY,value TEXT NOT NULL);
 CREATE TABLE members(did TEXT PRIMARY KEY,rkey TEXT NOT NULL);
 CREATE TABLE pending(did TEXT PRIMARY KEY);
 INSERT INTO pending VALUES('pending');
 INSERT INTO metadata VALUES('pds',?),('owner',?),('stream_endpoint','wss://JET.example:443'),('stream_seq',?),('cursor','10000000');`, host, m.Owner, fmt.Sprint(seq))
		if err != nil {
			t.Fatal(err)
		}
		for i := range int(flags % 5) {
			if _, err = db.Exec("INSERT INTO members VALUES(?,?)", fmt.Sprint(i), fmt.Sprint("key", i)); err != nil {
				t.Fatal(err)
			}
		}
		if err = db.Close(); err != nil {
			t.Fatal(err)
		}
		for range 2 {
			current, err := openStore(path)
			if err != nil {
				t.Fatal(err)
			}
			ctx := t.Context()
			uri, err := current.bindList(ctx, Target{PDS: m.Hosts[0]}, m.Owner)
			if err != nil || uri != "at://"+m.Owner+"/"+listCollection+"/"+recordKey(host) {
				t.Fatalf("binding %q %v", uri, err)
			}
			for i := range int(flags % 5) {
				keys, err := current.MemberKeys(ctx, m.Hosts[0], fmt.Sprint(i))
				if err != nil || !slices.Equal(keys, []string{fmt.Sprint("key", i)}) {
					t.Fatalf("member %d keys %v %v", i, keys, err)
				}
			}
			got, err := current.resumeCursor(ctx, "wss://jet.example"+streamPath)
			want := int64(seq)
			if seq == 0 {
				want = 5000000
			}
			if err != nil || got != want {
				t.Fatalf("cursor %d want %d %v", got, want, err)
			}
			work, err := current.next(ctx)
			if err != nil || work.DID != "pending" || work.Attempts != 0 {
				t.Fatalf("legacy job %+v %v", work, err)
			}
			if err = current.Close(); err != nil {
				t.Fatal(err)
			}
		}
	})
}

func TestConvergingBindings(t *testing.T) {
	for _, memberConflict := range []bool{false, true} {
		t.Run(fmt.Sprintf("memberConflict=%t", memberConflict), func(t *testing.T) {
			path := t.TempDir() + "/db"
			db, err := openStore(path)
			if err != nil {
				t.Fatal(err)
			}
			raw := "https://ONE.example:443"
			canonical := m.Hosts[0]
			_, err = db.conn.Exec("DELETE FROM goose_db_version WHERE version_id>2")
			if err != nil {
				t.Fatal(err)
			}
			_, err = db.conn.Exec("INSERT INTO targets VALUES(?,?)", raw, m.List(0))
			if err != nil {
				t.Fatal(err)
			}
			_, err = db.conn.Exec("INSERT INTO list_members VALUES(?,?,?)", raw, m.Subject, "item")
			if err != nil {
				t.Fatal(err)
			}
			if memberConflict {
				_, err = db.conn.Exec("INSERT INTO list_members VALUES(?,?,?)", canonical, m.Owner, "item")
			} else {
				_, err = db.conn.Exec("INSERT INTO targets VALUES(?,?)", canonical, m.List(1))
			}
			if err != nil {
				t.Fatal(err)
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			if reopened, err := openStore(path); err == nil {
				reopened.Close()
				t.Fatal("accepted conflicting legacy records")
			}
			old, err := sql.Open("sqlite", path)
			if err != nil {
				t.Fatal(err)
			}
			defer old.Close()
			var uri, did string
			if err := old.QueryRow("SELECT list_uri FROM targets WHERE pds=?", raw).Scan(&uri); err != nil || uri != m.List(0) {
				t.Fatalf("original binding %q %v", uri, err)
			}
			if err := old.QueryRow("SELECT did FROM list_members WHERE pds=? AND rkey='item'", raw).Scan(&did); err != nil || did != m.Subject {
				t.Fatalf("original member %q %v", did, err)
			}
		})
	}
}
