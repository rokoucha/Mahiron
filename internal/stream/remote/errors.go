package remote

import "errors"

// ErrChannelNotFound is the canonical "channel not found" sentinel for the
// stream packages. It lives here because both this package (mapping upstream
// 404 responses) and the source package (channel config lookups) produce it;
// the root stream package re-exports it for external consumers.
var ErrChannelNotFound = errors.New("channel not found")

var (
	ErrEITObservationUnsupported  = errors.New("EIT observation is not supported by remote sessions")
	ErrLogoObservationUnsupported = errors.New("logo observation is not supported by remote sessions")
	ErrDataBroadcastUnsupported   = errors.New("data broadcast observation is not supported by remote sessions")
)
