package tuner

import (
	"context"
	"errors"
	"slices"

	"github.com/21S1298001/Mahiron5/config"
)

type TunerManager struct {
	tuners []*Tuner
}

type TunerManagerConfig struct {
	TunersConfig config.TunersConfig
}

func NewTunerManager(config *TunerManagerConfig) *TunerManager {
	tuners := make([]*Tuner, len(config.TunersConfig))
	for i, tunerConfig := range config.TunersConfig {
		tuners[i] = NewTuner(tunerConfig)
	}

	return &TunerManager{
		tuners: tuners,
	}
}

func (tm *TunerManager) Shutdown(ctx context.Context) error {
	return nil
}

func (tm *TunerManager) GetTuner(name string) *Tuner {
	for _, tuner := range tm.tuners {
		if tuner.Name() == name {
			return tuner
		}
	}
	return nil
}

func (tm *TunerManager) GetTunerByGroup(group string) *Tuner {
	for _, tuner := range tm.tuners {
		if slices.Contains(tuner.Groups(), group) {
			return tuner
		}
	}
	return nil
}

func (tm *TunerManager) NewDeviceByGroup(group string, channel *config.ChannelConfig) (Device, error) {
	tuner := tm.GetTunerByGroup(group)
	if tuner == nil {
		return nil, ErrTunerNotFound
	}
	if tuner.Command() == "" {
		return nil, ErrUnsupportedTuner
	}
	return tuner.NewDevice(channel), nil
}

func (tm *TunerManager) DecoderCommandByGroup(group string) string {
	tuner := tm.GetTunerByGroup(group)
	if tuner == nil {
		return ""
	}
	return tuner.DecoderCommand()
}

func (tm *TunerManager) TunerCount() int {
	return len(tm.tuners)
}

func (tm *TunerManager) TunerCountByGroup(group string) int {
	count := 0
	for _, tuner := range tm.tuners {
		if slices.Contains(tuner.Groups(), group) {
			count++
		}
	}
	return count
}

func (tm *TunerManager) CountTunersByGroup() map[string]int {
	counts := make(map[string]int)
	for _, tuner := range tm.tuners {
		for _, g := range tuner.Groups() {
			counts[g]++
		}
	}
	return counts
}

var (
	ErrTunerNotFound    = errors.New("tuner not found")
	ErrUnsupportedTuner = errors.New("unsupported tuner")
)
