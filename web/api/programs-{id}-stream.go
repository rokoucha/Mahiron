package api

import (
	"context"
	"strconv"

	apigen "github.com/21S1298001/Mahiron5/web/api/gen"
)

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
	if service == nil {
		return &apigen.ProgramsIDStreamHeadNotFound{}, nil
	}
	decode := shouldDecode(params.Decode)
	networkID := p.NetworkID
	serviceID := p.ServiceID
	_, userID := tunerUserContext(ctx, params.XMirakurunPriority, decode, h.serviceManager.GetChannel(service.ChannelType, service.ChannelId), &networkID, &serviceID)

	return &apigen.ProgramsIDStreamHeadOK{
		XMirakurunTunerUserID: apigen.NewOptString(userID),
	}, nil
}
