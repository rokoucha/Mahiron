package source

import (
	"context"
	"errors"

	"github.com/21S1298001/mahiron/internal/config"
	"github.com/21S1298001/mahiron/internal/tuner"
)

var ErrChannelNotFound = errors.New("channel not found")

type TunerDevice = tuner.Device

// TunerManager creates tuner devices for local routes.
type TunerManager interface {
	NewDeviceByType(string, *config.ChannelConfig) (TunerDevice, error)
}

// TunerAllocator is an optional TunerManager extension that allocates a
// device together with its decoder command, honoring priorities and waiting.
type TunerAllocator interface {
	AcquireDevice(context.Context, string, *config.ChannelConfig, *config.ChannelConfig, bool) (TunerDevice, string, error)
}

// TunerAvailabilityChecker reports whether a tuner could be acquired without
// reserving it. The result is necessarily only a point-in-time observation.
type TunerAvailabilityChecker interface {
	CheckAvailable(context.Context, string) error
}

// DecoderCommandProvider is an optional TunerManager extension that resolves
// the descrambler command for a channel type.
type DecoderCommandProvider interface {
	DecoderCommandByType(string) string
}
