package capensis

import (
	"slices"
	"testing"
)

func TestRunLeavesCallerTargetsUnchangedOnValidationFailure(t *testing.T) {
	targets := []Target{{PDS: "FIRST.example"}, {PDS: " "}}
	original := slices.Clone(targets)
	err := Run(t.Context(), Config{Identifier: "owner.example", Password: "test", Targets: targets})
	if err == nil {
		t.Fatal("accepted invalid target")
	}
	if !slices.Equal(targets, original) {
		t.Fatalf("Run changed caller targets from %v to %v", original, targets)
	}
}
