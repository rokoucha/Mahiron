package mirakurun

import (
	"github.com/go-faster/jx"

	"github.com/21S1298001/mahiron/internal/config"
	"github.com/21S1298001/mahiron/internal/model"
	"github.com/21S1298001/mahiron/internal/service"
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

// ServiceToAPI converts a service to its Mirakurun-compatible API shape.
// The channel is attached only when includeChannel is set and the channel is
// known; the EPG status and logo presence travel as plain values.
func ServiceToAPI(svc *service.Service, channel *config.ChannelConfig, includeChannel bool) apigen.Service {
	result := apigen.Service{
		ID:                  apigen.ServiceItemId(svc.ItemId()),
		ServiceId:           apigen.ServiceId(svc.ServiceId),
		NetworkId:           apigen.NetworkId(svc.NetworkId),
		TransportStreamId:   apigen.NewOptTransportStreamId(apigen.TransportStreamId(svc.TransportStreamId)),
		Name:                svc.Name,
		Type:                int(svc.Type),
		EitScheduleFlag:     apigen.NewOptBool(svc.EITScheduleFlag),
		EitPresentFollowing: apigen.NewOptBool(svc.EITPresentFollowing),
		RemoteControlKeyId: apigen.NewOptInt(
			int(svc.RemoteControlKeyId),
		),
	}
	applyEPGStatus(&result, &svc.EPG)
	if includeChannel && channel != nil {
		result.Channel = apigen.NewOptChannel(ChannelToAPI(*channel))
	}
	if svc.LogoId != nil {
		result.LogoId = apigen.NewOptInt(int(*svc.LogoId))
	}
	result.HasLogoData = apigen.NewOptBool(svc.HasLogoData)
	return result
}

// ServicesToAPI converts services to their Mirakurun-compatible API shapes.
func ServicesToAPI(services []*service.Service, resolveChannel func(*service.Service) *config.ChannelConfig, includeChannel bool) []apigen.Service {
	result := make([]apigen.Service, len(services))
	for i, svc := range services {
		var channel *config.ChannelConfig
		if includeChannel && resolveChannel != nil {
			channel = resolveChannel(svc)
		}
		result[i] = ServiceToAPI(svc, channel, includeChannel)
	}
	return result
}

func applyEPGStatus(result *apigen.Service, status *service.EPGStatus) {
	if status == nil {
		return
	}
	if status.LastSuccessAt != nil {
		result.EpgReady = apigen.NewOptBool(true)
		result.EpgUpdatedAt = apigen.NewOptUnixtimeMS(apigen.UnixtimeMS(*status.LastSuccessAt))
	} else {
		result.EpgReady = apigen.NewOptBool(false)
	}
	if status.LastAttemptAt != nil {
		result.EpgLastAttemptAt = apigen.NewOptUnixtimeMS(apigen.UnixtimeMS(*status.LastAttemptAt))
	}
	if status.LastError != "" {
		result.EpgLastError = apigen.NewOptString(status.LastError)
	}
}

// ChannelToAPI converts a channel to its Mirakurun-compatible API shape.
func ChannelToAPI(channel config.ChannelConfig) apigen.Channel {
	result := apigen.Channel{
		Type:    channel.Type,
		Channel: channel.Channel,
		Name:    apigen.NewOptString(channel.Name),
		Routes:  channelRoutesToAPI(channel.RoutesOrDefault()),
	}
	if channel.TsmfRelTs != nil {
		result.TsmfRelTs = apigen.NewOptInt(int(*channel.TsmfRelTs))
	}
	return result
}

func channelRoutesToAPI(routes []config.ChannelRouteConfig) []apigen.ChannelRoute {
	result := make([]apigen.ChannelRoute, len(routes))
	for i, route := range routes {
		result[i] = apigen.ChannelRoute{
			ID:      route.Id,
			Type:    route.Type,
			Channel: route.Channel,
		}
		if route.Remote != "" {
			result[i].Remote = apigen.NewOptString(route.Remote)
		}
		if route.Priority != nil {
			result[i].Priority = apigen.NewOptInt(*route.Priority)
		}
		if route.IsDisabled != nil {
			result[i].IsDisabled = apigen.NewOptBool(*route.IsDisabled)
		}
	}
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
