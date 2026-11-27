package main

import (
	"database/sql"
	"errors"
	"testing"
)

func TestExistingListMigratesAlongsideIndependentCluster(t *testing.T) {
	ctx := t.Context()
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	const owner = "did:plc:zntngpowgd6rorjt3haywj36"
	_, err = db.Exec(schema + `
 INSERT INTO watched VALUES('did:example:a'),('did:example:b');
 INSERT INTO edges VALUES('member','a','did:example:a'),('member','b','did:example:b');
 INSERT INTO published VALUES('member'); INSERT INTO items VALUES('member','original');
 INSERT INTO descriptions VALUES('old description'); INSERT INTO pending VALUES('unfinished');
 INSERT INTO retries VALUES('unfinished',9999999999);
 INSERT INTO scans(did) VALUES('partial'); INSERT INTO pages VALUES('partial',1,'page2');
 INSERT INTO cursor VALUES('stream',42);`)
	if err != nil {
		t.Fatal(err)
	}
	a := service{store: store{db}, owner: owner, targets: map[string]bool{}}
	for range 2 {
		lists, err := a.setupClusters(ctx, map[string]clusterConfig{"second": {Accounts: []string{"did:example:a", "did:example:c"}, Threshold: 1}}, 2)
		if err != nil {
			t.Fatal(err)
		}
		if len(lists) != 2 || lists[0].list != "at://"+owner+"/app.bsky.graph.list/5jwgdmah5yhep" || lists[1].list == lists[0].list {
			t.Fatal("unexpected list binding")
		}
		var did string
		var include bool
		if err = db.QueryRow(nextChange, "default", 2, owner).Scan(&did, &include); !errors.Is(err, sql.ErrNoRows) {
			t.Fatalf("legacy list should be caught up: %v", err)
		}
		if err = db.QueryRow(nextChange, "second", 1, owner).Scan(&did, &include); err != nil || did != "member" || !include {
			t.Fatalf("second list %v %v %v", did, include, err)
		}
		if err = db.QueryRow(nextChange, "second", 2, owner).Scan(&did, &include); !errors.Is(err, sql.ErrNoRows) {
			t.Fatalf("out-of-cluster follow counted: %v", err)
		}
		keys, err := a.MemberKeys(ctx, "default", "member")
		if err != nil || len(keys) != 1 || keys[0] != "original" {
			t.Fatal(keys, err)
		}
	}
	var intact bool
	err = db.QueryRow(`SELECT EXISTS(SELECT 1 FROM pages WHERE cursor='page2') AND EXISTS(SELECT 1 FROM cursor WHERE seq=42) AND EXISTS(SELECT 1 FROM descriptions WHERE cluster='default' AND text='old description') AND EXISTS(SELECT 1 FROM pending WHERE cluster='default' AND did='unfinished')`).Scan(&intact)
	if err != nil || !intact {
		t.Fatalf("lost legacy state: %v", err)
	}
}
