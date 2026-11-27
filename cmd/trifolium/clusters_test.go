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
	_, err = db.Exec(`
 CREATE TABLE watched(target TEXT PRIMARY KEY);
 CREATE TABLE edges(actor TEXT,rkey TEXT,target TEXT,PRIMARY KEY(actor,rkey));
 CREATE TABLE descriptions(text TEXT PRIMARY KEY);
 CREATE TABLE items(did TEXT PRIMARY KEY,rkey TEXT UNIQUE);
 CREATE TABLE published(did TEXT PRIMARY KEY);
 CREATE TABLE pending(did TEXT PRIMARY KEY);
 CREATE TABLE retries(did TEXT PRIMARY KEY,due INTEGER);
 CREATE TABLE scans(did TEXT PRIMARY KEY,version INTEGER DEFAULT 1,due INTEGER DEFAULT 0);
 CREATE TABLE pages(did TEXT PRIMARY KEY,version INTEGER,cursor TEXT);
 CREATE TABLE cursor(endpoint TEXT PRIMARY KEY,seq INTEGER);
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
	if err := (store{db}).initialize(ctx); err != nil {
		t.Fatal(err)
	}
	a := service{store: store{db}, owner: owner, targets: map[string]bool{}}
	configs := map[string]clusterConfig{"second": {Accounts: []string{"did:example:a", "did:example:c"}, Threshold: 1}}
	{
		lists, err := a.setupClusters(ctx, configs, 2)
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
	lists, err := a.setupClusters(ctx, configs, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(lists) != 1 || lists[0].id != "second" {
		t.Fatalf("omitting default must pause it, got %v", lists)
	}
	if a.targets["did:example:b"] {
		t.Fatal("paused cluster still contributes watched targets")
	}
	keys, err := a.MemberKeys(ctx, defaultCluster, "member")
	if err != nil || len(keys) != 1 || keys[0] != "original" {
		t.Fatalf("paused cluster lost its saved membership: %v %v", keys, err)
	}
	var intact bool
	err = db.QueryRow(`SELECT EXISTS(SELECT 1 FROM pages WHERE cursor='page2') AND EXISTS(SELECT 1 FROM cursor WHERE seq=42) AND EXISTS(SELECT 1 FROM descriptions WHERE cluster='default' AND text='old description') AND EXISTS(SELECT 1 FROM pending WHERE cluster='default' AND did='unfinished')`).Scan(&intact)
	if err != nil || !intact {
		t.Fatalf("lost legacy state: %v", err)
	}
}

func TestClusterResumeVerifiesPublishedMembersBeforeRemovingThem(t *testing.T) {
	ctx := t.Context()
	a := openTestService(t)
	const actor = "did:example:actor"
	const first = "did:example:first"
	const second = "did:example:second"
	configs := map[string]clusterConfig{"first": {Accounts: []string{first}, Threshold: 1}, "second": {Accounts: []string{second}, Threshold: 1}}
	setup := func(configs map[string]clusterConfig) {
		t.Helper()
		if _, err := a.setupClusters(ctx, configs, 2); err != nil {
			t.Fatal(err)
		}
	}
	setup(configs)
	if _, err := a.Exec("INSERT INTO published VALUES('second',?)", actor); err != nil {
		t.Fatal(err)
	}
	if _, err := a.Exec("INSERT INTO edges VALUES(?,'known',?)", actor, second); err != nil {
		t.Fatal(err)
	}
	setup(map[string]clusterConfig{"first": configs["first"]})
	setup(configs)
	var did string
	var include bool
	if err := a.QueryRow(nextChange, "second", 1, a.owner).Scan(&did, &include); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("resumed cluster selected an unverified removal: %q %v %v", did, include, err)
	}
	job, err := a.claim(ctx, false)
	if err != nil || job.did != actor {
		t.Fatalf("resumed cluster did not schedule follow verification: %+v %v", job, err)
	}
	if err := a.savePage(ctx, job, "", "", nil); err != nil {
		t.Fatal(err)
	}
	if err := a.QueryRow(nextChange, "second", 1, a.owner).Scan(&did, &include); err != nil || did != actor || include {
		t.Fatalf("resumed cluster did not select the verified removal: %q %v %v", did, include, err)
	}
}

func TestInvalidClusterConfiguration(t *testing.T) {
	for _, tc := range []struct{ raw, accounts string }{
		{"{", ""},
		{`{"default":{"accounts":["a.example"],"threshold":2}}`, "b.example"},
		{`{"garden":{"accounts":[" "],"threshold":2}}`, ""},
		{`{"garden":{"accounts":["a.example"],"threshold":0}}`, ""},
	} {
		if _, err := parseClusters(tc.raw, tc.accounts, 2); err == nil {
			t.Errorf("accepted clusters=%q accounts=%q", tc.raw, tc.accounts)
		}
	}
}
