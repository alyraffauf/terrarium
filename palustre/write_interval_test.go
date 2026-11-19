package palustre

import (
	"context"
	"testing"
	"testing/synctest"
	"time"
)

func TestWritesWaitEvenAfterIdle(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		api := Session{Config: Config{WriteInterval: 30 * time.Second}}
		time.Sleep(time.Hour)
		start := time.Now()
		for range 3 {
			if err := api.WaitWrite(context.Background()); err != nil {
				t.Fatal(err)
			}
		}
		if elapsed := time.Since(start); elapsed != time.Minute {
			t.Fatalf("three writes after idle took %v, want one minute", elapsed)
		}
	})
}
