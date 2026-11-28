package palustre

import (
	"testing"
	"time"
)

func FuzzRetrySchedules(f *testing.F) {
	f.Add(uint8(0), uint8(0))
	f.Add(uint8(19), uint8(3))
	f.Fuzz(func(t *testing.T, failures, minutes uint8) {
		interval := time.Duration(minutes) * time.Minute
		var scan, stream time.Duration
		member := 2 * time.Second
		verification := time.Minute
		for i := 0; i <= int(failures); i++ {
			scan = ScanRetryDelay(scan, interval)
			stream = StreamRetryDelay(stream, 0)
			if got := MembershipRetryDelay(i); got != member {
				t.Fatal(i, got, member)
			}
			if got := VerificationRetryDelay(i); got != verification {
				t.Fatal(i, got, verification)
			}
			if want := min(verification, max(time.Minute, interval)); scan != want {
				t.Fatal(i, scan, want)
			}
			if want := min(time.Minute, member/2); stream != want {
				t.Fatal(i, stream, want)
			}
			member = min(time.Hour, 2*member)
			verification = min(time.Hour, 2*verification)
		}
		if StreamRetryDelay(stream, time.Minute) != time.Second {
			t.Fatal("healthy stream did not reset retries")
		}
	})
}
