package api

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/21S1298001/mahiron/internal/event"
	apigen "github.com/21S1298001/mahiron/internal/web/api/gen"
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

func TestGetEventsReturnsMirakurunCompatibleData(t *testing.T) {
	hub := event.New()
	hub.PublishServiceEvent(event.TypeUpdate, map[string]any{
		"id":                 int64(100101),
		"serviceId":          uint16(101),
		"networkId":          uint16(1),
		"transportStreamId":  uint16(10),
		"name":               "NHK",
		"type":               1,
		"hasLogoData":        true,
		"remoteControlKeyId": 0,
		"epgReady":           false,
		"channel": map[string]any{
			"type":    "GR",
			"channel": "27",
			"name":    "NHK",
		},
	})
	hub.PublishTunerStatusEvent(event.TypeUpdate, map[string]any{
		"index":       1,
		"name":        "tuner-a",
		"types":       []string{"GR"},
		"command":     "recpt1",
		"pid":         1234,
		"isAvailable": true,
		"isFree":      true,
		"isRemote":    false,
		"users":       []map[string]any{},
	})
	handler := NewHandler(HandlerConfig{EventHub: hub})

	res, err := handler.GetEvents(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	events := res.(*apigen.GetEventsOKApplicationJSON)
	if got, want := len(*events), 2; got != want {
		t.Fatalf("events length = %d, want %d", got, want)
	}

	var serviceTransportStreamID int
	if err := json.Unmarshal((*events)[0].Data["transportStreamId"], &serviceTransportStreamID); err != nil {
		t.Fatal(err)
	}
	var hasLogoData bool
	if err := json.Unmarshal((*events)[0].Data["hasLogoData"], &hasLogoData); err != nil {
		t.Fatal(err)
	}
	if serviceTransportStreamID != 10 || !hasLogoData {
		t.Fatalf("service event data = %#v", (*events)[0].Data)
	}

	var isRemote bool
	if err := json.Unmarshal((*events)[1].Data["isRemote"], &isRemote); err != nil {
		t.Fatal(err)
	}
	if isRemote {
		t.Fatalf("tuner isRemote = true, want false")
	}
}

func TestGetEventsReturnsJobResources(t *testing.T) {
	hub := event.New()
	hub.PublishJobEvent(event.TypeUpdate, map[string]any{
		"key":        "test-job",
		"name":       "Test Job",
		"id":         "job-id",
		"status":     "queued",
		"retryCount": 0,
		"isAborting": false,
		"createdAt":  int64(1000),
		"updatedAt":  int64(1000),
	})
	hub.PublishJobScheduleEvent(event.TypeCreate, map[string]any{
		"key":      "test-schedule",
		"schedule": "5 6 * * *",
		"job": map[string]any{
			"key":  "test-job",
			"name": "Test Job",
		},
	})
	handler := NewHandler(HandlerConfig{EventHub: hub})

	res, err := handler.GetEvents(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	events := res.(*apigen.GetEventsOKApplicationJSON)
	if got, want := len(*events), 2; got != want {
		t.Fatalf("events length = %d, want %d", got, want)
	}
	if (*events)[0].Resource != apigen.EventResourceJob || (*events)[1].Resource != apigen.EventResourceJobSchedule {
		t.Fatalf("event resources = %q, %q", (*events)[0].Resource, (*events)[1].Resource)
	}
	var status string
	if err := json.Unmarshal((*events)[0].Data["status"], &status); err != nil {
		t.Fatal(err)
	}
	if status != "queued" {
		t.Fatalf("job status = %q, want queued", status)
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

func TestGetEventsStreamFlushesInitialBytes(t *testing.T) {
	handler := NewHandler(HandlerConfig{})
	recorder := newFlushRecorder()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- handler.GetEventsStream(ctx, apigen.GetEventsStreamParams{}, recorder)
	}()

	select {
	case flushed := <-recorder.flushes:
		if flushed != "[\n" {
			t.Fatalf("flushed body = %q, want open JSON array", flushed)
		}
	case <-time.After(time.Second):
		t.Fatal("stream response did not flush initial bytes")
	}

	cancel()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("stream response did not finish after context cancellation")
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

func TestGetEventsStreamFiltersJobUpdateEvents(t *testing.T) {
	var buf bytes.Buffer
	events := []event.Event{
		mustTestEvent(t, event.ResourceService, event.TypeUpdate, map[string]any{"id": 1}),
		mustTestEvent(t, event.ResourceJob, event.TypeUpdate, map[string]any{"id": "job-id", "status": "queued"}),
		mustTestEvent(t, event.ResourceJob, event.TypeRemove, map[string]any{"id": "removed-job"}),
	}
	if err := writeEventsOpenJSONArrayEvents(&buf, events, apigen.GetEventsStreamParams{
		Resource: apigen.NewOptGetEventsStreamResource(apigen.GetEventsStreamResourceJob),
		Type:     apigen.NewOptGetEventsStreamType(apigen.GetEventsStreamTypeUpdate),
	}); err != nil {
		t.Fatal(err)
	}

	body := buf.String()
	if got, want := strings.Count(body, "\n,\n"), 1; got != want {
		t.Fatalf("event separator count = %d, want %d\n%s", got, want, body)
	}
	if !strings.Contains(body, `"resource":"job"`) || !strings.Contains(body, `"status":"queued"`) {
		t.Fatalf("filtered stream body = %s", body)
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

type flushRecorder struct {
	header  http.Header
	body    bytes.Buffer
	flushes chan string
}

func newFlushRecorder() *flushRecorder {
	return &flushRecorder{
		header:  make(http.Header),
		flushes: make(chan string, 1),
	}
}

func (r *flushRecorder) Header() http.Header {
	return r.header
}

func (r *flushRecorder) WriteHeader(statusCode int) {}

func (r *flushRecorder) Write(p []byte) (int, error) {
	return r.body.Write(p)
}

func (r *flushRecorder) Flush() {
	select {
	case r.flushes <- r.body.String():
	default:
	}
}
