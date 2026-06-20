package api

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strconv"

	"github.com/21S1298001/Mahiron5/internal/stream"
	apigen "github.com/21S1298001/Mahiron5/internal/web/api/gen"
)

func GetServiceStream(ctx context.Context, h *Handler, params apigen.GetServiceStreamParams) (apigen.GetServiceStreamRes, error) {
	service, err := h.serviceManager.GetServiceById(ctx, strconv.FormatInt(params.ID, 10))
	if err != nil {
		return nil, err
	}
	if service == nil {
		return &apigen.GetServiceStreamNotFound{}, nil
	}
	decode := shouldDecode(params.Decode)
	serviceID := service.ServiceId
	networkID := service.NetworkId
	ctx, userID := tunerUserContext(ctx, params.XMirakurunPriority, decode, h.serviceManager.GetChannel(service.ChannelType, service.ChannelId), &networkID, &serviceID)

	session, err := h.streamManager.GetOrCreate(ctx, service.ChannelType, service.ChannelId)
	if err != nil {
		if errors.Is(err, stream.ErrChannelNotFound) {
			return &apigen.GetServiceStreamNotFound{}, nil
		}
		if errors.Is(err, stream.ErrTunerNotFound) || errors.Is(err, stream.ErrUnsupportedTuner) {
			return &apigen.GetServiceStreamServiceUnavailable{}, nil
		}
		return nil, err
	}

	fo, fi := io.Pipe()
	go func() {
		defer fi.Close()
		slog.Info("stream request started", "type", service.ChannelType, "channel", service.ChannelId, "kind", "service", "networkId", networkID, "serviceId", serviceID, "decode", decode, "userId", userID)
		if err := session.ServiceStream(ctx, service.ServiceId, decode, fi); err != nil && !errors.Is(err, io.ErrClosedPipe) {
			slog.Error("failed to stream service", "service", service.Id, "err", err)
		}
		slog.Debug("stream request finished", "type", service.ChannelType, "channel", service.ChannelId, "kind", "service", "networkId", networkID, "serviceId", serviceID, "decode", decode, "userId", userID)
	}()

	return &apigen.GetServiceStreamOKHeaders{
		XMirakurunTunerUserID: apigen.NewOptString(userID),
		Response: apigen.GetServiceStreamOK{
			Data: fo,
		},
	}, nil
}

func ServicesIDStreamHead(ctx context.Context, h *Handler, params apigen.ServicesIDStreamHeadParams) (apigen.ServicesIDStreamHeadRes, error) {
	service, err := h.serviceManager.GetServiceById(ctx, strconv.FormatInt(params.ID, 10))
	if err != nil {
		return nil, err
	}
	if service == nil {
		return &apigen.ServicesIDStreamHeadNotFound{}, nil
	}
	decode := shouldDecode(params.Decode)
	serviceID := service.ServiceId
	networkID := service.NetworkId
	_, userID := tunerUserContext(ctx, params.XMirakurunPriority, decode, h.serviceManager.GetChannel(service.ChannelType, service.ChannelId), &networkID, &serviceID)

	return &apigen.ServicesIDStreamHeadOK{
		XMirakurunTunerUserID: apigen.NewOptString(userID),
	}, nil
}
