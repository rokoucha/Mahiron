package api

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/21S1298001/Mahiron5/internal/event"
	apigen "github.com/21S1298001/Mahiron5/internal/web/api/gen"
)

func TestGetEventsReturnsEventLog(t *testing.T) {
	hub := event.New()
	hub.PublishEvent(event.ResourceProgram, event.TypeCreate, map[string]any{"id": 1, "name": "first"})
	handler := NewHandler(HandlerConfig{EventHub: hub})

	res, err := handler.GetEvents(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	events, ok := res.(*apigen.GetEventsOKApplicationJSON)
	if !ok {
		t.Fatalf("response type = %T, want *GetEventsOKApplicationJSON", res)
	}
	if got, want := len(*events), 1; got != want {
		t.Fatalf("events length = %d, want %d", got, want)
	}

	first := (*events)[0]
	if first.Resource != apigen.EventResourceProgram {
		t.Fatalf("first resource = %q, want program", first.Resource)
	}
	if first.Type != apigen.EventTypeCreate {
		t.Fatalf("first type = %q, want create", first.Type)
	}
	var name string
	if err := json.Unmarshal(first.Data["name"], &name); err != nil {
		t.Fatal(err)
	}
	if got, want := name, "first"; got != want {
		t.Fatalf("first event data name = %q, want %q", got, want)
	}
}

func TestGetEventsReturnsOnlyLast100Events(t *testing.T) {
	hub := event.New()
	for i := 0; i < 101; i++ {
		hub.PublishEvent(event.ResourceProgram, event.TypeUpdate, map[string]any{"id": i})
	}
	handler := NewHandler(HandlerConfig{EventHub: hub})

	res, err := handler.GetEvents(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	events := res.(*apigen.GetEventsOKApplicationJSON)
	if got, want := len(*events), 100; got != want {
		t.Fatalf("events length = %d, want %d", got, want)
	}
	var id int
	if err := json.Unmarshal((*events)[0].Data["id"], &id); err != nil {
		t.Fatal(err)
	}
	if id != 1 {
		t.Fatalf("first retained id = %d, want 1", id)
	}
}

func TestGetEventsStreamReceivesEventsPublishedAfterSubscribe(t *testing.T) {
	hub := event.New()
	hub.PublishEvent(event.ResourceProgram, event.TypeUpdate, map[string]any{"id": 1})
	handler := NewHandler(HandlerConfig{EventHub: hub})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	reader := newEventsStreamReader(ctx, handler, apigen.GetEventsStreamParams{
		Resource: apigen.NewOptGetEventsStreamResource(apigen.GetEventsStreamResourceProgram),
		Type:     apigen.NewOptGetEventsStreamType(apigen.GetEventsStreamTypeUpdate),
	})
	defer reader.Close()

	prefix := make([]byte, 2)
	if _, err := io.ReadFull(reader, prefix); err != nil {
		t.Fatal(err)
	}
	if string(prefix) != "[\n" {
		t.Fatalf("prefix = %q, want open JSON array", prefix)
	}

	time.Sleep(10 * time.Millisecond)
	hub.PublishEvent(event.ResourceProgram, event.TypeUpdate, map[string]any{"id": 2})
	line := readEventLine(t, reader, time.Second)
	var event apigen.Event
	if err := json.Unmarshal([]byte(line), &event); err != nil {
		t.Fatal(err)
	}
	var id int
	if err := json.Unmarshal(event.Data["id"], &id); err != nil {
		t.Fatal(err)
	}
	if id != 2 {
		t.Fatalf("stream event id = %d, want 2", id)
	}
}

func TestGetEventsStreamFiltersSubscribedEvents(t *testing.T) {
	var buf bytes.Buffer
	events := []event.Event{
		mustTestEvent(t, event.ResourceService, event.TypeUpdate, map[string]any{"id": 1}),
		mustTestEvent(t, event.ResourceProgram, event.TypeRemove, map[string]any{"id": 2}),
	}
	if err := writeEventsOpenJSONArrayEvents(&buf, events, apigen.GetEventsStreamParams{
		Type: apigen.NewOptGetEventsStreamType(apigen.GetEventsStreamTypeRemove),
	}); err != nil {
		t.Fatal(err)
	}

	body := buf.String()
	if !strings.HasPrefix(body, "[\n") {
		t.Fatalf("body prefix = %q, want open JSON array", body)
	}
	if got, want := strings.Count(body, "\n,\n"), 1; got != want {
		t.Fatalf("event separator count = %d, want %d\n%s", got, want, body)
	}
}

func TestGetEventsStreamWithoutHubKeepsOpenUntilContextDone(t *testing.T) {
	handler := NewHandler(HandlerConfig{})
	ctx, cancel := context.WithCancel(context.Background())
	reader := newEventsStreamReader(ctx, handler, apigen.GetEventsStreamParams{})
	defer reader.Close()

	prefix := make([]byte, 2)
	if _, err := io.ReadFull(reader, prefix); err != nil {
		t.Fatal(err)
	}
	cancel()
	done := make(chan error, 1)
	go func() {
		_, err := io.ReadAll(reader)
		done <- err
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("stream did not close after context cancellation")
	}
}

func readEventLine(t *testing.T, reader io.Reader, timeout time.Duration) string {
	t.Helper()
	done := make(chan string, 1)
	errCh := make(chan error, 1)
	go func() {
		line, err := readEventLineBlocking(reader)
		if err != nil {
			errCh <- err
			return
		}
		done <- line
	}()
	select {
	case line := <-done:
		return line
	case err := <-errCh:
		t.Fatal(err)
	case <-time.After(timeout):
		t.Fatal("timed out waiting for stream event")
	}
	return ""
}

func readEventLineBlocking(reader io.Reader) (string, error) {
	var buf bytes.Buffer
	tmp := make([]byte, 1)
	for {
		if _, err := reader.Read(tmp); err != nil {
			return "", err
		}
		if tmp[0] == '\n' {
			break
		}
		buf.WriteByte(tmp[0])
	}
	return buf.String(), nil
}

func mustTestEvent(t *testing.T, resource, typ string, data any) event.Event {
	t.Helper()
	raw, err := json.Marshal(data)
	if err != nil {
		t.Fatal(err)
	}
	return event.Event{Resource: resource, Type: typ, Data: raw, Time: 1}
}
