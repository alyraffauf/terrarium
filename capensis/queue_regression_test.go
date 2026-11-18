package capensis

import (
	"context"
	"testing"
)

// A rescan enumerates current state while an earlier check for the same
// account may still be running. That signal must survive the running
// check's finish, exactly as a stream event does.
func TestQueueScanDuringInFlightCheckIsNotLost(t *testing.T) {
	db := testStore(t)
	ctx := context.Background()
	if err := db.queueScan(ctx, testDID); err != nil {
		t.Fatal(err)
	}
	work, err := db.next(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.queueScan(ctx, testDID); err != nil {
		t.Fatal(err)
	}
	if err := db.finish(ctx, work); err != nil {
		t.Fatal(err)
	}
	recheck, err := db.next(ctx)
	if err != nil {
		t.Fatalf("scan during in-flight check lost: %v", err)
	}
	if err := db.finish(ctx, recheck); err != nil {
		t.Fatal(err)
	}
}

func TestQueueMembersDuringInFlightCheckIsNotLost(t *testing.T) {
	db := testStore(t)
	ctx := context.Background()
	pds := "https://pds.example"
	if err := db.RememberMember(ctx, pds, testDID, "item1"); err != nil {
		t.Fatal(err)
	}
	if err := db.queueScan(ctx, testDID); err != nil {
		t.Fatal(err)
	}
	work, err := db.next(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.queueMembers(ctx, pds); err != nil {
		t.Fatal(err)
	}
	if err := db.finish(ctx, work); err != nil {
		t.Fatal(err)
	}
	if _, err := db.next(ctx); err != nil {
		t.Fatalf("member rescan during in-flight check lost: %v", err)
	}
}
