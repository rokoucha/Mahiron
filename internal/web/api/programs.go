package api

import (
	"context"
	"strconv"

	"github.com/21S1298001/mahiron/internal/mirakurun"
	"github.com/21S1298001/mahiron/internal/program"
	apigen "github.com/21S1298001/mahiron/internal/web/api/gen"
)

func GetPrograms(ctx context.Context, h *Handler, params apigen.GetProgramsParams) (apigen.GetProgramsRes, error) {
	programs, err := h.programManager.List(ctx, programQuery(params))
	if err != nil {
		return nil, err
	}
	res := apigen.GetProgramsOKApplicationJSON(mirakurun.ProgramsToAPI(programs))
	return &res, nil
}

func GetProgram(ctx context.Context, h *Handler, params apigen.GetProgramParams) (apigen.GetProgramRes, error) {
	p, ok, err := h.programManager.Get(ctx, params.ID)
	if err != nil {
		return nil, err
	}
	if !ok {
		return notFound("program not found"), nil
	}
	api := mirakurun.ProgramToAPI(p)
	return &api, nil
}

func GetServicePrograms(ctx context.Context, h *Handler, params apigen.GetServiceProgramsParams) (apigen.GetServiceProgramsRes, error) {
	service, err := h.serviceManager.GetServiceById(ctx, strconv.FormatInt(params.ID, 10))
	if err != nil {
		return nil, err
	}
	if service == nil {
		return notFound("service not found"), nil
	}
	networkID := service.NetworkId
	serviceID := service.ServiceId
	programs, err := h.programManager.List(ctx, program.Query{
		NetworkID: &networkID,
		ServiceID: &serviceID,
	})
	if err != nil {
		return nil, err
	}
	res := apigen.GetServiceProgramsOKApplicationJSON(mirakurun.ProgramsToAPI(programs))
	return &res, nil
}

func programQuery(params apigen.GetProgramsParams) program.Query {
	var query program.Query
	if value, ok := params.NetworkId.Get(); ok {
		v := uint16(value)
		query.NetworkID = &v
	}
	if value, ok := params.ServiceId.Get(); ok {
		v := uint16(value)
		query.ServiceID = &v
	}
	if value, ok := params.EventId.Get(); ok {
		v := uint16(value)
		query.EventID = &v
	}
	if value, ok := params.StartAt.Get(); ok {
		query.StartAt = &value
	}
	if value, ok := params.EndAt.Get(); ok {
		query.EndAt = &value
	}
	return query
}

// Program conversions live in internal/mirakurun.
