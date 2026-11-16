package main

import (
	"github.com/spf13/viper"
	"reflect"
	"testing"
)

func TestAccountListEnvironmentAndFlagPrecedence(t *testing.T) {
	t.Setenv("HELIX_ACCOUNT", "a.example, b.example")
	cfg := viper.New()
	cfg.SetEnvPrefix("HELIX")
	cfg.AutomaticEnv()
	if got := stringList(cfg, "account"); !reflect.DeepEqual(got, []string{"a.example", "b.example"}) {
		t.Fatalf("environment list: %v", got)
	}
	cfg.Set("account", []string{"c.example"})
	if got := stringList(cfg, "account"); !reflect.DeepEqual(got, []string{"c.example"}) {
		t.Fatalf("override: %v", got)
	}
}
