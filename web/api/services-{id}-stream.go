package api

import (
	"context"
	"io"

	apigen "github.com/21S1298001/Mahiron5/web/api/gen"
)

func GetServiceStream(ctx context.Context, h *Handler, params apigen.GetServiceStreamParams) (apigen.GetServiceStreamRes, error) {
	pr, pw := io.Pipe()

	tuner := h.tunerManager.GetTuner("test")
	if tuner == nil {
		return &apigen.GetServiceStreamNotFound{}, nil
	}

	go func() {
		defer pw.Close()
		defer pr.Close()

		tuner.StartStream(ctx, "http-test", pw)
	}()

	return &apigen.GetServiceStreamOKHeaders{
		XMirakurunTunerUserID: apigen.OptString{},
		Response: apigen.GetServiceStreamOK{
			Data: pr,
		},
	}, nil
}
