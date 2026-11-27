// Package clitest exercises the shared target selection contract of the follower
// and PDS command lines through their actual Cobra commands.
package clitest

import (
	"strings"
	"testing"

	"github.com/spf13/cobra"
)

func CheckTargetPrecedence(t *testing.T, newCommand func() *cobra.Command, prefix, flag string) {
	t.Helper()
	t.Setenv(prefix+"_IDENTIFIER", "")
	t.Setenv(prefix+"_APP_PASSWORD", "")
	t.Setenv(prefix+"_LIST", "")
	// Each input can be absent, name one target, or repeat that target.
	// The selected input is observable before login through duplicate checking.
	for env := 0; env <= 2; env++ {
		for flags := 0; flags <= 2; flags++ {
			for positional := 0; positional <= 2; positional++ {
				t.Setenv(prefix+"_"+strings.ToUpper(flag), strings.TrimSuffix(strings.Repeat("a.example, ", env), ", "))
				args := []string{}
				for range flags {
					args = append(args, "--"+flag, "b.example")
				}
				for range positional {
					args = append(args, "c.example")
				}
				chosen := positional
				if chosen == 0 {
					chosen = flags
				}
				if chosen == 0 {
					chosen = env
				}
				cmd := newCommand()
				cmd.SetArgs(args)
				err := cmd.ExecuteContext(t.Context())
				if err == nil || strings.Contains(err.Error(), "duplicate") != (chosen == 2) ||
					(chosen != 2 && !strings.Contains(err.Error(), "app-password")) {
					t.Fatalf("%s env=%d flag=%d positional=%d: %v", prefix, env, flags, positional, err)
				}
			}
		}
	}
}
