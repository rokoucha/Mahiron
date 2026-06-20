package api

import (
	"context"
	"encoding/json"
	"io"

	"github.com/21S1298001/Mahiron5/internal/event"
	apigen "github.com/21S1298001/Mahiron5/internal/web/api/gen"
)

func GetEvents(ctx context.Context, h *Handler) (apigen.GetEventsRes, error) {
	events := apiEvents(h.eventLog())
	res := apigen.GetEventsOKApplicationJSON(events)
	return &res, nil
}

func GetEventsStream(ctx context.Context, h *Handler, params apigen.GetEventsStreamParams) (apigen.GetEventsStreamRes, error) {
	return &apigen.GetEventsStreamOK{
		Data: newEventsStreamReader(ctx, h, params),
	}, nil
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

func matchesEventStreamParams(event apigen.Event, params apigen.GetEventsStreamParams) bool {
	if resource, ok := params.Resource.Get(); ok && string(event.Resource) != string(resource) {
		return false
	}
	if typ, ok := params.Type.Get(); ok && string(event.Type) != string(typ) {
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
			apiEvent, err := apiEvent(event)
			if err != nil {
				continue
			}
			if !matchesEventStreamParams(apiEvent, params) {
				continue
			}
			if err := writeOpenJSONArrayEvent(w, apiEvent); err != nil {
				return err
			}
		}
	}
}

func writeEventsOpenJSONArrayEvents(w io.Writer, events []event.Event, params apigen.GetEventsStreamParams) error {
	if _, err := io.WriteString(w, "[\n"); err != nil {
		return err
	}
	for _, event := range apiEvents(events) {
		if !matchesEventStreamParams(event, params) {
			continue
		}
		if err := writeOpenJSONArrayEvent(w, event); err != nil {
			return err
		}
	}
	return nil
}

func writeOpenJSONArrayEvent(w io.Writer, event apigen.Event) error {
	data, err := json.Marshal(event)
	if err != nil {
		return err
	}
	_, err = w.Write(append(data, '\n', ',', '\n'))
	return err
}
