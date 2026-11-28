package storage

import (
	"testing"

	"github.com/alyraffauf/terrarium/palustre"
	"github.com/pressly/goose/v3"
)

func TestUpgradeWithPendingVerification(t *testing.T) {
	ctx := t.Context()
	db, err := palustre.OpenSQLite(ctx, t.TempDir()+"/store")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	provider, err := goose.NewProvider(goose.DialectSQLite3, db, Migrations, goose.WithGoMigrations(goose.NewGoMigration(1, &goose.GoFunc{RunTx: stageLegacy}, nil), goose.NewGoMigration(3, &goose.GoFunc{RunTx: restoreLegacy}, nil)))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = provider.UpTo(ctx, 4); err != nil {
		t.Fatal(err)
	}
	if _, err = db.Exec("INSERT INTO retries(cluster,did,due) VALUES('default','actor',888); INSERT INTO discovery_schedule(target,backlinks,due) VALUES('target',1,777); INSERT INTO scans(did,version,due) VALUES('actor',7,120); INSERT INTO pages VALUES('actor',7,'cursor'); INSERT INTO hints(did,rkey,target) VALUES('actor','key','target')"); err != nil {
		t.Fatal(err)
	}
	if err = Migrate(ctx, db); err != nil {
		t.Fatal(err)
	}
	var preserved bool
	err = db.QueryRow("SELECT EXISTS(SELECT 1 FROM scans JOIN pages USING(did,version) JOIN hints USING(did) WHERE scans.version=7 AND scans.due=120 AND pages.pds='' AND scans.attempts=0 AND hints.attempts=0) AND EXISTS(SELECT 1 FROM retries WHERE due=888 AND attempts=0) AND EXISTS(SELECT 1 FROM discovery_schedule WHERE due=777 AND retry_seconds=0)").Scan(&preserved)
	if err != nil || !preserved {
		t.Fatal(preserved, err)
	}
}
