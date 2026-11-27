package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/alyraffauf/terrarium/cordata"
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
	config.SetEnvPrefix("CORDATA")
	config.SetEnvKeyReplacer(strings.NewReplacer("-", "_"))
	config.AutomaticEnv()
	cmd := &cobra.Command{
		Use:           "cordata POST_URL",
		Short:         "Maintain a moderation list of accounts that liked a post",
		Long:          "Maintain a moderation list of everyone who liked a post. Pass a bsky.app post link or an AT URI. Flags override matching CORDATA_ environment variables (replace hyphens with underscores).",
		Example:       "cordata 'at://bsky.app/app.bsky.feed.post/3mzyk34ho4c27'",
		Args:          cobra.ExactArgs(1),
		SilenceUsage:  true,
		SilenceErrors: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			return cordata.Run(cmd.Context(), cordata.Config{
				Config: palustre.Config{
					Identifier:    config.GetString("identifier"),
					Password:      config.GetString("app-password"),
					LoginPDS:      config.GetString("login-pds"),
					Slingshot:     config.GetString("slingshot"),
					WriteInterval: config.GetDuration("write-interval"),
				},
				AppView:        config.GetString("appview"),
				Post:           args[0],
				RescanInterval: config.GetDuration("rescan-interval"),
			})
		},
	}
	flags := cmd.Flags()
	flags.String("identifier", "", "List owner's handle or DID")
	flags.String("app-password", "", "List owner's app password")
	flags.String("login-pds", "", "List owner's PDS (default: resolve automatically)")
	flags.String("slingshot", "https://slingshot.aly.town", "Identity resolver URL")
	flags.String("appview", "https://public.api.bsky.app", "AppView for post likes")
	flags.Duration("rescan-interval", 5*time.Minute, "Time between completed list scans")
	flags.Duration("write-interval", palustre.DefaultWriteInterval, "Minimum interval between writes")
	if err := config.BindPFlags(flags); err != nil {
		panic(err)
	}
	return cmd
}
