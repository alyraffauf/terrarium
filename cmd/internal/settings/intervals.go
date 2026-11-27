// Package settings reads configuration shared by the Cobra commands.
package settings

import (
	"fmt"
	"time"

	"github.com/spf13/cast"
	"github.com/spf13/viper"
)

type Intervals struct {
	Write  time.Duration
	Rescan time.Duration
}

// ReadIntervals uses Viper's duration conversion while retaining parse errors.
func ReadIntervals(config *viper.Viper) (Intervals, error) {
	write, err := cast.ToDurationE(config.Get("write-interval"))
	if err != nil {
		return Intervals{}, fmt.Errorf("write-interval: %w", err)
	}
	rescan, err := cast.ToDurationE(config.Get("rescan-interval"))
	if err != nil {
		return Intervals{}, fmt.Errorf("rescan-interval: %w", err)
	}
	return Intervals{Write: write, Rescan: rescan}, nil
}
