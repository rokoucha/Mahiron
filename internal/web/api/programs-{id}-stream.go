package api

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strconv"

	"github.com/21S1298001/mahiron/internal/stream"
	apigen "github.com/21S1298001/mahiron/internal/web/api/gen"
)

func GetProgramStream(ctx context.Context, h *Handler, params apigen.GetProgramStreamParams) (apigen.GetProgramStreamRes, error) {
	p, ok, err := h.programManager.Get(ctx, params.ID)
	if err != nil {
		return nil, err
	}
	if !ok {
		return &apigen.GetProgramStreamNotFound{}, nil
	}
	serviceItemID := int64(p.NetworkID)*100000 + int64(p.ServiceID)
	service, err := h.serviceManager.GetServiceById(ctx, strconv.FormatInt(serviceItemID, 10))
	if err != nil {
		return nil, err
	}
	if service == nil {
		return &apigen.GetProgramStreamNotFound{}, nil
	}

	decode := shouldDecode(params.Decode)
	networkID := p.NetworkID
	serviceID := p.ServiceID
	ctx, userID := tunerUserContext(ctx, params.XMirakurunPriority, decode, h.serviceManager.GetChannel(service.ChannelType, service.ChannelId), &networkID, &serviceID)

	session, err := h.streamManager.GetOrCreate(ctx, service.ChannelType, service.ChannelId)
	if err != nil {
		if errors.Is(err, stream.ErrChannelNotFound) {
			return &apigen.GetProgramStreamNotFound{}, nil
		}
		if errors.Is(err, stream.ErrTunerNotFound) || errors.Is(err, stream.ErrUnsupportedTuner) || errors.Is(err, stream.ErrTunerUnavailable) {
			return &apigen.GetProgramStreamServiceUnavailable{}, nil
		}
		return nil, err
	}

	fo, fi := io.Pipe()
	go func() {
		defer func() { _ = fi.Close() }()
		slog.Info("stream request started", "type", service.ChannelType, "channel", service.ChannelId, "kind", "program", "networkId", networkID, "serviceId", serviceID, "eventId", p.EventID, "decode", decode, "userId", userID)
		if err := session.ProgramStream(ctx, p, decode, fi); err != nil && !errors.Is(err, io.ErrClosedPipe) && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
			slog.Error("failed to stream program", "program", p.ID, "err", err)
		}
		slog.Debug("stream request finished", "type", service.ChannelType, "channel", service.ChannelId, "kind", "program", "networkId", networkID, "serviceId", serviceID, "eventId", p.EventID, "decode", decode, "userId", userID)
	}()

	return &apigen.GetProgramStreamOKHeaders{
		XMirakurunTunerUserID: apigen.NewOptString(userID),
		Response: apigen.GetProgramStreamOK{
			Data: fo,
		},
	}, nil
}

func ProgramsIDStreamHead(ctx context.Context, h *Handler, params apigen.ProgramsIDStreamHeadParams) (apigen.ProgramsIDStreamHeadRes, error) {
	p, ok, err := h.programManager.Get(ctx, params.ID)
	if err != nil {
		return nil, err
	}
	if !ok {
		return &apigen.ProgramsIDStreamHeadNotFound{}, nil
	}
	serviceItemID := int64(p.NetworkID)*100000 + int64(p.ServiceID)
	service, err := h.serviceManager.GetServiceById(ctx, strconv.FormatInt(serviceItemID, 10))
	if err != nil {
		return nil, err
	}
	decode := shouldDecode(params.Decode)
	networkID := p.NetworkID
	serviceID := p.ServiceID
	var channelType, channelID string
	if service != nil {
		channelType = service.ChannelType
		channelID = service.ChannelId
	}
	_, userID := tunerUserContext(ctx, params.XMirakurunPriority, decode, h.serviceManager.GetChannel(channelType, channelID), &networkID, &serviceID)

	return &apigen.ProgramsIDStreamHeadOK{
		XMirakurunTunerUserID: apigen.NewOptString(userID),
	}, nil
}
