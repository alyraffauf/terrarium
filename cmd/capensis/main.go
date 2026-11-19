package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/alyraffauf/terrarium/capensis"
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
	config.SetEnvPrefix("CAPENSIS")
	config.SetEnvKeyReplacer(strings.NewReplacer("-", "_"))
	config.AutomaticEnv()
	cmd := &cobra.Command{
		Use: "capensis [pds ...]", Short: "Maintain a Bluesky moderation list for each PDS",
		Long: "Maintain a Bluesky moderation list for each PDS.\n\n" +
			"At least one target, an identifier, and an app password are required.\n" +
			"Configuration flags also accept CAPENSIS_ environment variables.\n" +
			"Replace hyphens with underscores: --app-password reads CAPENSIS_APP_PASSWORD.\n" +
			"Flags override environment variables. Positional PDS arguments replace --pds values.",
		Example:      "  CAPENSIS_APP_PASSWORD='your-app-password' capensis --identifier you.example.com first.example.com second.example.com",
		SilenceUsage: true, SilenceErrors: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			pdses := stringList(config, "pds")
			if len(args) > 0 {
				pdses = args
			}
			targets, err := capensis.ParseTargets(pdses, stringList(config, "list"))
			if err != nil {
				return err
			}
			cfg := capensis.Config{
				WriteInterval: config.GetDuration("write-interval"),
				Targets:       targets, Identifier: config.GetString("identifier"),
				Password: config.GetString("app-password"), LoginPDS: config.GetString("login-pds"),
				Slingshot: config.GetString("slingshot"), Jetstream: config.GetString("jetstream"),
				MetricsAddress: config.GetString("metrics-address"), Database: config.GetString("database"), RescanInterval: config.GetDuration("rescan-interval"),
			}
			return capensis.Run(cmd.Context(), cfg)
		},
	}
	flags := cmd.Flags()
	flags.String("metrics-address", "", "Prometheus listen address, such as :9090 (disabled by default)")
	flags.StringSlice("pds", nil, "Target PDS hostnames or URLs (repeatable or comma-separated)")
	flags.StringSlice("list", nil, "Bind PDS=at://DID/app.bsky.graph.list/record-key (repeatable or comma-separated)")
	flags.String("identifier", "", "List owner's handle or DID")
	flags.String("app-password", "", "List owner's app password")
	flags.String("login-pds", "", "List owner's PDS URL (default: resolve with Slingshot)")
	flags.String("slingshot", "https://slingshot.aly.town", "Slingshot base URL")
	flags.String("jetstream", "wss://jetstream.waow.tech", "Jetstream v2 base URL or subscribeEvents URL")
	flags.String("database", "capensis.sqlite", "SQLite state file")
	flags.Duration("write-interval", palustre.DefaultWriteInterval, "Minimum interval between writes across all lists in this process (0 uses the default)")
	flags.Duration("rescan-interval", 6*time.Hour, "Interval between full PDS rescans")
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
