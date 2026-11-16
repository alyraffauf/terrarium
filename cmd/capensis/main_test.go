package main

import (
	"github.com/spf13/viper"
	"reflect"
	"testing"
)

func TestPDSListEnvironmentAndFlagPrecedence(t *testing.T) {
	t.Setenv("CAPENSIS_PDS", "a.example, b.example")
	cfg := viper.New()
	cfg.SetEnvPrefix("CAPENSIS")
	cfg.AutomaticEnv()
	if got := stringList(cfg, "pds"); !reflect.DeepEqual(got, []string{"a.example", "b.example"}) {
		t.Fatalf("environment list: %v", got)
	}
	cfg.Set("pds", []string{"c.example"})
	if got := stringList(cfg, "pds"); !reflect.DeepEqual(got, []string{"c.example"}) {
		t.Fatalf("override: %v", got)
	}
}
