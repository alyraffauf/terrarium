package main

import (
	"testing"

	"github.com/alyraffauf/terrarium/internal/clitest"
)

func TestTargetSourcePrecedence(t *testing.T) {
	clitest.CheckTargetPrecedence(t, command, "CAPENSIS", "pds")
}
