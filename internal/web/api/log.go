package api

import (
	"bytes"
	"context"
	"net/http"

	apigen "github.com/21S1298001/mahiron/internal/web/api/gen"
)

func (h *Handler) GetLog(_ context.Context) (apigen.GetLogRes, error) {
	if h.logStore == nil {
		return &apigen.GetLogDef{StatusCode: http.StatusServiceUnavailable}, nil
	}
	return &apigen.GetLogOK{Data: bytes.NewReader(h.logStore.Snapshot())}, nil
}

func (h *Handler) GetLogStream(ctx context.Context) (apigen.GetLogStreamRes, error) {
	if h.logStore == nil {
		return &apigen.GetLogStreamDef{StatusCode: http.StatusServiceUnavailable}, nil
	}

	reader, unsubscribe := h.logStore.Subscribe()
	go func() {
		<-ctx.Done()
		unsubscribe()
		reader.Close()
	}()

	return &apigen.GetLogStreamOK{Data: reader}, nil
}
