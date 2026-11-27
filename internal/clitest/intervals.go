package clitest

import (
	"strings"
	"testing"

	"github.com/spf13/cobra"
)

// CheckIntervalPrecedence exercises error reporting before the command logs in.
func CheckIntervalPrecedence(t *testing.T, newCommand func() *cobra.Command, prefix, target string) {
	t.Helper()
	t.Setenv(prefix+"_IDENTIFIER", "")
	t.Setenv(prefix+"_APP_PASSWORD", "")
	t.Setenv(prefix+"_WRITE_INTERVAL", "")
	t.Setenv(prefix+"_RESCAN_INTERVAL", "")
	for _, key := range []string{"write-interval", "rescan-interval"} {
		t.Run(key, func(t *testing.T) {
			t.Setenv(prefix+"_"+strings.ToUpper(strings.ReplaceAll(key, "-", "_")), "60sec")
			cmd := newCommand()
			cmd.SetArgs([]string{target})
			if err := cmd.ExecuteContext(t.Context()); err == nil || !strings.Contains(err.Error(), key) || !strings.Contains(err.Error(), "60sec") {
				t.Fatalf("invalid environment duration: %v", err)
			}
			cmd = newCommand()
			cmd.SetArgs([]string{"--" + key + "=60s", target})
			if err := cmd.ExecuteContext(t.Context()); err == nil || !strings.Contains(err.Error(), "app-password") {
				t.Fatalf("valid flag should override environment and reach credential validation: %v", err)
			}
		})
	}
}
