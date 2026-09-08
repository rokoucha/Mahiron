package api

import (
	"fmt"
	"log/slog"
	"net/http"
	"strconv"

	"github.com/21S1298001/mahiron/internal/observability"
	"github.com/21S1298001/mahiron/internal/program"
	apigen "github.com/21S1298001/mahiron/internal/web/api/gen"
	"github.com/go-faster/jx"
)

// programsJSONBuffer is how much encoded JSON accumulates before it is flushed
// to the response.
const programsJSONBuffer = 64 << 10

// WriteProgramsJSON serves GET /api/programs.
//
// The generated server encodes a whole response into a jx.Encoder and only
// then writes it out, so serving this operation through it holds four copies
// of the EPG at once: the database rows, the program.Program values, the
// apigen.Program values and the finished JSON. For the ~65k programs of a full
// EPG that peaks at around 330 MB, well past what the process is given. This
// handler is registered directly on the mux instead and encodes one program at
// a time. The bytes are the same: the same apigen.Program values written by
// the same generated Encode method.
func (h *Handler) WriteProgramsJSON(w http.ResponseWriter, r *http.Request) {
	query, err := programListQuery(r.URL.Query())
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, err.Error())
		return
	}

	ctx, span := observability.StartSpan(r.Context(), observability.SpanAPIGetPrograms)
	defer func() { observability.EndSpan(span, err) }()

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)

	encoder := &jx.Encoder{}
	encoder.Grow(programsJSONBuffer)
	encoder.ResetWriter(w)
	encoder.ArrStart()
	err = h.programManager.ListFunc(ctx, query, func(p *program.Program) error {
		apiProgram(p).Encode(encoder)
		return nil
	})
	if err != nil {
		// The status line and part of the array are already on the wire, so
		// the only thing left is to stop writing and leave the client with a
		// truncated body.
		slog.Error("failed to stream programs", "error", err)
		return
	}
	encoder.ArrEnd()
	if err = encoder.Close(); err != nil {
		slog.Error("failed to write programs response", "error", err)
	}
}

func programListQuery(values map[string][]string) (program.Query, error) {
	var query program.Query
	for _, param := range []struct {
		name string
		set  func(int64)
	}{
		{"networkId", func(v int64) { n := uint16(v); query.NetworkID = &n }},
		{"serviceId", func(v int64) { n := uint16(v); query.ServiceID = &n }},
		{"eventId", func(v int64) { n := uint16(v); query.EventID = &n }},
		{"startAt", func(v int64) { query.StartAt = &v }},
		{"endAt", func(v int64) { query.EndAt = &v }},
	} {
		raw, ok := values[param.name]
		if !ok || len(raw) == 0 {
			continue
		}
		value, err := strconv.ParseInt(raw[0], 10, 64)
		if err != nil {
			return program.Query{}, fmt.Errorf("invalid query parameter: %s", param.name)
		}
		param.set(value)
	}
	return query, nil
}

func writeJSONError(w http.ResponseWriter, status int, reason string) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	encoder := &jx.Encoder{}
	response := apigen.Error{
		Code:   apigen.NewOptInt(status),
		Reason: apigen.NewOptString(reason),
	}
	response.Encode(encoder)
	if _, err := encoder.WriteTo(w); err != nil {
		slog.Error("failed to write error response", "error", err)
	}
}
