package mirakurun

import (
	"github.com/go-faster/jx"

	"github.com/21S1298001/mahiron/internal/model"
	apigen "github.com/21S1298001/mahiron/internal/web/api/gen"
)

// MarshalService encodes a service with the generated encoding and returns
// the bytes. The service shape holds no maps, so the generated encoding is
// already stable across runs.
func MarshalService(svc *apigen.Service) []byte {
	e := &jx.Encoder{}
	svc.Encode(e)
	return e.Bytes()
}

// ServiceState is the Mahiron-managed state the API shows beside a
// broadcast service. web/api fills it from the stored service.
type ServiceState struct {
	// Channel is attached to the service when set.
	Channel          *apigen.Channel
	HasLogoData      bool
	EPGLastAttemptAt *int64
	EPGLastSuccessAt *int64
	EPGLastError     string
}

// ServiceToAPI converts a broadcast service and its state to the
// Mirakurun-compatible API shape. A service without a remote control key is
// written with 0, as before the key became optional.
func ServiceToAPI(svc *model.Service, state ServiceState) apigen.Service {
	var remoteControlKey uint8
	if svc.RemoteControlKey != nil {
		remoteControlKey = *svc.RemoteControlKey
	}
	result := apigen.Service{
		ID:                  apigen.ServiceItemId(svc.Key.MirakurunID()),
		ServiceId:           apigen.ServiceId(svc.Key.ServiceID),
		NetworkId:           apigen.NetworkId(svc.Key.NetworkID),
		TransportStreamId:   apigen.NewOptTransportStreamId(apigen.TransportStreamId(svc.Key.StreamID)),
		Name:                svc.Name,
		Type:                int(svc.Type),
		EitScheduleFlag:     apigen.NewOptBool(svc.EITSchedule),
		EitPresentFollowing: apigen.NewOptBool(svc.EITPresentFollow),
		RemoteControlKeyId:  apigen.NewOptInt(int(remoteControlKey)),
	}
	if state.EPGLastSuccessAt != nil {
		result.EpgReady = apigen.NewOptBool(true)
		result.EpgUpdatedAt = apigen.NewOptUnixtimeMS(apigen.UnixtimeMS(*state.EPGLastSuccessAt))
	} else {
		result.EpgReady = apigen.NewOptBool(false)
	}
	if state.EPGLastAttemptAt != nil {
		result.EpgLastAttemptAt = apigen.NewOptUnixtimeMS(apigen.UnixtimeMS(*state.EPGLastAttemptAt))
	}
	if state.EPGLastError != "" {
		result.EpgLastError = apigen.NewOptString(state.EPGLastError)
	}
	if state.Channel != nil {
		result.Channel = apigen.NewOptChannel(*state.Channel)
	}
	// A simple logo carries characters instead of a logo ID.
	if svc.Logo != nil && !svc.Logo.HasSimpleLogo {
		result.LogoId = apigen.NewOptInt(int(svc.Logo.LogoID))
	}
	result.HasLogoData = apigen.NewOptBool(state.HasLogoData)
	return result
}

// ScanServiceModelFromAPI converts a service received from a remote
// Mirakurun-compatible server to the internal broadcast model. It carries
// the same logo heuristics the remote scan applies: a logo counts only
// when the remote reports both an ID and actual logo data, and the version
// and download ID are synthesized because remotes do not expose the
// broadcast values.
func ScanServiceModelFromAPI(svc *apigen.Service) model.Service {
	out := model.Service{
		Key: model.ServiceKey{
			NetworkID: uint16(svc.NetworkId),
			StreamID:  uint16(svc.TransportStreamId.Value),
			ServiceID: uint16(svc.ServiceId),
		},
		Name: svc.Name,
		Type: uint8(svc.Type),
	}
	if eitScheduleFlag, ok := svc.EitScheduleFlag.Get(); ok {
		out.EITSchedule = eitScheduleFlag
	} else {
		out.EITSchedule = true
	}
	if eitPresentFollowing, ok := svc.EitPresentFollowing.Get(); ok {
		out.EITPresentFollow = eitPresentFollowing
	} else {
		out.EITPresentFollow = true
	}
	if remoteControlKeyID, ok := svc.RemoteControlKeyId.Get(); ok {
		v := uint8(remoteControlKeyID)
		out.RemoteControlKey = &v
	}
	if logoIDValue, ok := svc.LogoId.Get(); ok {
		if hasLogoData, _ := svc.HasLogoData.Get(); int64(logoIDValue) >= 0 && hasLogoData {
			version := uint16(0)
			downloadDataID := uint16(svc.ServiceId)
			out.Logo = &model.LogoRef{
				LogoID:         uint16(logoIDValue),
				Version:        &version,
				DownloadDataID: &downloadDataID,
			}
		}
	}
	return out
}
