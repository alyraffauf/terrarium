package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/alyraffauf/terrarium/helix"
	"github.com/alyraffauf/terrarium/palustre"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := command().ExecuteContext(ctx); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func command() *cobra.Command {
	config := viper.New()
	config.SetEnvPrefix("HELIX")
	config.SetEnvKeyReplacer(strings.NewReplacer("-", "_"))
	config.AutomaticEnv()
	cmd := &cobra.Command{
		Use: "helix [account ...]", Short: "Maintain a Bluesky moderation list of followers for each target account",
		Long: "Maintain a Bluesky moderation list of followers for each target account.\n\n" +
			"At least one target, an identifier, and an app password are required.\n" +
			"Configuration flags also accept HELIX_ environment variables.\n" +
			"Replace hyphens with underscores: --app-password reads HELIX_APP_PASSWORD.\n" +
			"Flags override environment variables. Positional accounts replace --account and HELIX_ACCOUNT values.",
		Example:      "  HELIX_APP_PASSWORD='your-app-password' helix --identifier you.example.com first.example.com second.example.com",
		SilenceUsage: true, SilenceErrors: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			accounts := stringList(config, "account")
			if len(args) > 0 {
				accounts = args
			}
			targets, err := helix.ParseTargets(accounts, stringList(config, "list"))
			if err != nil {
				return err
			}
			cfg := helix.Config{
				WriteInterval: config.GetDuration("write-interval"),
				Targets:       targets, Identifier: config.GetString("identifier"),
				Password: config.GetString("app-password"), LoginPDS: config.GetString("login-pds"),
				Slingshot: config.GetString("slingshot"), Jetstream: config.GetString("jetstream"),
				AppView: config.GetString("appview"), MetricsAddress: config.GetString("metrics-address"), Database: config.GetString("database"), RescanInterval: config.GetDuration("rescan-interval"),
			}
			return helix.Run(cmd.Context(), cfg)
		},
	}
	flags := cmd.Flags()
	flags.String("metrics-address", "", "Prometheus listen address, such as :9090 (disabled by default)")
	flags.StringSlice("account", nil, "Target account handles or DIDs (repeatable or comma-separated)")
	flags.StringSlice("list", nil, "Bind ACCOUNT=at://DID/app.bsky.graph.list/record-key (repeatable or comma-separated)")
	flags.String("identifier", "", "List owner's handle or DID")
	flags.String("app-password", "", "List owner's app password")
	flags.String("login-pds", "", "List owner's PDS URL (default: resolve with Slingshot)")
	flags.String("appview", "https://public.api.bsky.app", "AppView URL for followers and profiles")
	flags.String("slingshot", "https://slingshot.aly.town", "Slingshot base URL")
	flags.String("jetstream", "wss://jetstream.waow.tech", "Jetstream v2 base URL or subscribeEvents URL")
	flags.String("database", "helix.sqlite", "SQLite state file")
	flags.Duration("write-interval", palustre.DefaultWriteInterval, "Minimum interval between writes across all lists in this process (0 uses the default)")
	flags.Duration("rescan-interval", 6*time.Hour, "Interval between follower backfills")
	if err := config.BindPFlags(flags); err != nil {
		panic(err)
	}
	return cmd
}

func stringList(config *viper.Viper, key string) []string {
	if raw, ok := config.Get(key).(string); ok {
		return strings.FieldsFunc(raw, func(r rune) bool { return r == ',' || r == ' ' || r == '\t' || r == '\n' })
	}
	return config.GetStringSlice(key)
}
