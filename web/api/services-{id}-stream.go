package api

import (
	"context"
	"log/slog"
	"strconv"
	"sync"

	"github.com/21S1298001/Mahiron5/filter"
	apigen "github.com/21S1298001/Mahiron5/web/api/gen"
)

func GetServiceStream(ctx context.Context, h *Handler, params apigen.GetServiceStreamParams) (apigen.GetServiceStreamRes, error) {
	tuner := h.tunerManager.GetTuner("test")
	if tuner == nil {
		return &apigen.GetServiceStreamNotFound{}, nil
	}

	filter := filter.NewServiceFilter(ctx, strconv.FormatInt(params.ID, 10))
	fi, fo, err := filter.Pipe()
	if err != nil {
		return nil, err
	}

	go func() {
		wg := sync.WaitGroup{}
		wg.Add(2)
		go func() {
			defer wg.Done()
			tuner.StartStream(ctx, "test", fi)
		}()
		go func() {
			defer wg.Done()
			if err := filter.Filter(); err != nil {
				slog.Error("failed to apply filter", "err", err)
			}
		}()
		wg.Wait()
	}()

	return &apigen.GetServiceStreamOKHeaders{
		XMirakurunTunerUserID: apigen.OptString{},
		Response: apigen.GetServiceStreamOK{
			Data: fo,
		},
	}, nil
}
