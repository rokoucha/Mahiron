package api

import (
	"context"
	"errors"
	"io"
	"log/slog"

	"github.com/21S1298001/Mahiron5/stream"
	apigen "github.com/21S1298001/Mahiron5/web/api/gen"
)

func GetChannelStream(ctx context.Context, h *Handler, params apigen.GetChannelStreamParams) (apigen.GetChannelStreamRes, error) {
	decode := shouldDecode(params.Decode)
	ctx, userID := tunerUserContext(ctx, params.XMirakurunPriority, decode, h.serviceManager.GetChannel(params.Type, params.Channel), nil, nil)
	session, err := h.streamManager.GetOrCreate(ctx, params.Type, params.Channel)
	if err != nil {
		if errors.Is(err, stream.ErrChannelNotFound) {
			return &apigen.GetChannelStreamNotFound{}, nil
		}
		if errors.Is(err, stream.ErrTunerNotFound) || errors.Is(err, stream.ErrUnsupportedTuner) {
			return &apigen.GetChannelStreamServiceUnavailable{}, nil
		}
		return nil, err
	}

	fo, fi := io.Pipe()
	go func() {
		defer fi.Close()
		if err := session.ChannelStream(ctx, decode, fi); err != nil && !errors.Is(err, io.ErrClosedPipe) {
			slog.Error("failed to stream channel", "type", params.Type, "channel", params.Channel, "err", err)
		}
	}()

	return &apigen.GetChannelStreamOKHeaders{
		XMirakurunTunerUserID: apigen.NewOptString(userID),
		Response: apigen.GetChannelStreamOK{
			Data: fo,
		},
	}, nil
}

func GetServiceStreamByChannel(ctx context.Context, h *Handler, params apigen.GetServiceStreamByChannelParams) (apigen.GetServiceStreamByChannelRes, error) {
	decode := shouldDecode(params.Decode)
	serviceID := uint16(params.ID)
	ctx, userID := tunerUserContext(ctx, params.XMirakurunPriority, decode, h.serviceManager.GetChannel(params.Type, params.Channel), nil, &serviceID)
	session, err := h.streamManager.GetOrCreate(ctx, params.Type, params.Channel)
	if err != nil {
		if errors.Is(err, stream.ErrChannelNotFound) {
			return &apigen.GetServiceStreamByChannelNotFound{}, nil
		}
		if errors.Is(err, stream.ErrTunerNotFound) || errors.Is(err, stream.ErrUnsupportedTuner) {
			return &apigen.GetServiceStreamByChannelServiceUnavailable{}, nil
		}
		return nil, err
	}

	fo, fi := io.Pipe()
	go func() {
		defer fi.Close()
		if err := session.ServiceStream(ctx, serviceID, decode, fi); err != nil && !errors.Is(err, io.ErrClosedPipe) {
			slog.Error("failed to stream service by channel", "type", params.Type, "channel", params.Channel, "service", params.ID, "err", err)
		}
	}()

	return &apigen.GetServiceStreamByChannelOKHeaders{
		XMirakurunTunerUserID: apigen.NewOptString(userID),
		Response: apigen.GetServiceStreamByChannelOK{
			Data: fo,
		},
	}, nil
}

func ChannelsTypeChannelStreamHead(ctx context.Context, h *Handler, params apigen.ChannelsTypeChannelStreamHeadParams) (apigen.ChannelsTypeChannelStreamHeadRes, error) {
	channel := h.serviceManager.GetChannel(params.Type, params.Channel)
	if channel == nil {
		return &apigen.ChannelsTypeChannelStreamHeadNotFound{}, nil
	}
	decode := shouldDecode(params.Decode)
	_, userID := tunerUserContext(ctx, params.XMirakurunPriority, decode, channel, nil, nil)
	return &apigen.ChannelsTypeChannelStreamHeadOK{
		XMirakurunTunerUserID: apigen.NewOptString(userID),
	}, nil
}

func ChannelsTypeChannelServicesIDStreamHead(ctx context.Context, h *Handler, params apigen.ChannelsTypeChannelServicesIDStreamHeadParams) (apigen.ChannelsTypeChannelServicesIDStreamHeadRes, error) {
	channel := h.serviceManager.GetChannel(params.Type, params.Channel)
	if channel == nil {
		return &apigen.ChannelsTypeChannelServicesIDStreamHeadNotFound{}, nil
	}
	decode := shouldDecode(params.Decode)
	serviceID := uint16(params.ID)
	_, userID := tunerUserContext(ctx, params.XMirakurunPriority, decode, channel, nil, &serviceID)
	return &apigen.ChannelsTypeChannelServicesIDStreamHeadOK{
		XMirakurunTunerUserID: apigen.NewOptString(userID),
	}, nil
}
