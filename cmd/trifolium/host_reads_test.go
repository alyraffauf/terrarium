package main

import (
	"testing"
	"time"
)

func TestHostReadsExpireOnFinalRelease(t *testing.T) {
	var service service
	first := service.acquireHost("one.example")
	second := service.acquireHost("one.example")
	other := service.acquireHost("two.example")
	if first == nil || second == nil || other == nil || service.acquireHost("one.example") != nil {
		t.Fatal("expected two independent read slots per host")
	}
	first()
	replacement := service.acquireHost("one.example")
	if replacement == nil || service.acquireHost("one.example") != nil {
		t.Fatal("releasing one slot should make exactly one available")
	}
	second()
	replacement()
	if _, retained := service.hostReads["one.example"]; retained || service.hostReads["two.example"] != 1 {
		t.Fatalf("unexpected active readers after release: %v", service.hostReads)
	}
	other()
	if len(service.hostReads) != 0 {
		t.Fatalf("idle hosts retained: %v", service.hostReads)
	}
}

func TestRateLimitAffectsOnlyOneHost(t *testing.T) {
	var a service
	a.coolHost("busy", time.Now().Add(time.Hour))
	a.coolHost("busy", time.Now().Add(-time.Hour))
	if a.acquireHost("busy") != nil {
		t.Fatal("accepted read during Retry-After")
	}
	release := a.acquireHost("available")
	if release == nil {
		t.Fatal("unrelated host blocked")
	}
	release()
}
