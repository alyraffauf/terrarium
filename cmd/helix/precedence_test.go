package main

import (
	"testing"

	"github.com/alyraffauf/terrarium/internal/clitest"
)

func TestTargetSourcePrecedence(t *testing.T) {
	clitest.CheckTargetPrecedence(t, command, "HELIX", "account")
}

func TestIntervalSourcePrecedence(t *testing.T) {
	clitest.CheckIntervalPrecedence(t, command, "HELIX", "one.example")
}
