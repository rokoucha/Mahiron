package api

import (
	"context"
	"encoding/json"
	"io"
	"net/http"

	"github.com/go-faster/jx"

	"github.com/21S1298001/mahiron/internal/event"
	apigen "github.com/21S1298001/mahiron/internal/web/api/gen"
)

func GetEvents(ctx context.Context, h *Handler) (apigen.GetEventsRes, error) {
	events := apiEvents(h.eventLog())
	res := apigen.GetEventsOKApplicationJSON(events)
	return &res, nil
}

// WriteEventsJSON serves GET /api/events.
//
// The generated server encodes apigen.EventData maps with jx, whose key order
// changes from run to run. Event payloads are already stored deterministically
// encoded (program and service payloads via internal/mirakurun, the rest via
// encoding/json), so this handler embeds the stored bytes as-is in field
// order instead of round-tripping them through the map.
func (h *Handler) WriteEventsJSON(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)

	encoder := &jx.Encoder{}
	encoder.ArrStart()
	for _, e := range h.eventLog() {
		appendEventJSON(encoder, e)
	}
	encoder.ArrEnd()
	if _, err := encoder.WriteTo(w); err != nil {
		return
	}
}

// appendEventJSON writes one event with the stored payload bytes embedded
// as-is, in resource/type/data/time order.
func appendEventJSON(e *jx.Encoder, event event.Event) {
	e.ObjStart()
	{
		e.FieldStart("resource")
		e.Str(event.Resource)
	}
	{
		e.FieldStart("type")
		e.Str(event.Type)
	}
	{
		e.FieldStart("data")
		if len(event.Data) == 0 {
			e.Null()
		} else {
			e.Raw(event.Data)
		}
	}
	{
		e.FieldStart("time")
		e.Int64(event.Time)
	}
	e.ObjEnd()
}

func GetEventsStream(ctx context.Context, h *Handler, params apigen.GetEventsStreamParams, w http.ResponseWriter) error {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	return writeEventsOpenJSONArrayStream(ctx, flushWriter{w: w}, h, params)
}

func apiEvents(events []event.Event) []apigen.Event {
	result := make([]apigen.Event, 0, len(events))
	for _, event := range events {
		apiEvent, err := apiEvent(event)
		if err != nil {
			continue
		}
		result = append(result, apiEvent)
	}
	return result
}

func apiEvent(event event.Event) (apigen.Event, error) {
	data, err := apiEventData(event.Data)
	if err != nil {
		return apigen.Event{}, err
	}
	return apigen.Event{
		Resource: apigen.EventResource(event.Resource),
		Type:     apigen.EventType(event.Type),
		Data:     data,
		Time:     apigen.UnixtimeMS(event.Time),
	}, nil
}

func apiEventData(payload any) (apigen.EventData, error) {
	raw, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	var data apigen.EventData
	if err := json.Unmarshal(raw, &data); err != nil {
		return nil, err
	}
	return data, nil
}

func (h *Handler) eventLog() []event.Event {
	if h.eventHub == nil {
		return nil
	}
	return h.eventHub.Log()
}

func matchesEventStreamParams(event event.Event, params apigen.GetEventsStreamParams) bool {
	if resource, ok := params.Resource.Get(); ok && event.Resource != string(resource) {
		return false
	}
	if typ, ok := params.Type.Get(); ok && event.Type != string(typ) {
		return false
	}
	return true
}

func newEventsStreamReader(ctx context.Context, h *Handler, params apigen.GetEventsStreamParams) io.ReadCloser {
	reader, writer := io.Pipe()
	go func() {
		if err := writeEventsOpenJSONArrayStream(ctx, writer, h, params); err != nil {
			_ = writer.CloseWithError(err)
			return
		}
		_ = writer.Close()
	}()
	return reader
}

func writeEventsOpenJSONArrayStream(ctx context.Context, w io.Writer, h *Handler, params apigen.GetEventsStreamParams) error {
	if _, err := io.WriteString(w, "[\n"); err != nil {
		return err
	}
	if h.eventHub == nil {
		<-ctx.Done()
		return nil
	}
	events, unsubscribe := h.eventHub.Subscribe()
	defer unsubscribe()
	for {
		select {
		case <-ctx.Done():
			return nil
		case event, ok := <-events:
			if !ok {
				return nil
			}
			if !matchesEventStreamParams(event, params) {
				continue
			}
			if err := writeOpenJSONArrayEvent(w, event); err != nil {
				return err
			}
		}
	}
}

func writeEventsOpenJSONArrayEvents(w io.Writer, events []event.Event, params apigen.GetEventsStreamParams) error {
	if _, err := io.WriteString(w, "[\n"); err != nil {
		return err
	}
	for _, event := range events {
		if !matchesEventStreamParams(event, params) {
			continue
		}
		if err := writeOpenJSONArrayEvent(w, event); err != nil {
			return err
		}
	}
	return nil
}

func writeOpenJSONArrayEvent(w io.Writer, event event.Event) error {
	encoder := &jx.Encoder{}
	appendEventJSON(encoder, event)
	if _, err := encoder.WriteTo(w); err != nil {
		return err
	}
	_, err := w.Write([]byte{'\n', ',', '\n'})
	return err
}

type flushWriter struct {
	w http.ResponseWriter
}

func (w flushWriter) Write(p []byte) (int, error) {
	n, err := w.w.Write(p)
	if n > 0 {
		_ = http.NewResponseController(w.w).Flush()
	}
	return n, err
}
