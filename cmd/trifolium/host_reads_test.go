package main

import "testing"

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
