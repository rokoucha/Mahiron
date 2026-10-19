package epggather

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/21S1298001/mahiron/internal/job/run"
	"github.com/21S1298001/mahiron/internal/model"
	"github.com/21S1298001/mahiron/ts"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func recordCollectionSpans(t *testing.T) *tracetest.SpanRecorder {
	t.Helper()
	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	previous := otel.GetTracerProvider()
	otel.SetTracerProvider(provider)
	t.Cleanup(func() { otel.SetTracerProvider(previous); _ = provider.Shutdown(context.Background()) })
	return recorder
}

func collectionSpan(t *testing.T, recorder *tracetest.SpanRecorder) sdktrace.ReadOnlySpan {
	t.Helper()
	for _, span := range recorder.Ended() {
		if span.Name() == "epg.collect_service_snapshots" {
			return span
		}
	}
	t.Fatal("collection span missing")
	return nil
}

func spanAttribute(span sdktrace.ReadOnlySpan, key string) attribute.Value {
	for _, attr := range span.Attributes() {
		if string(attr.Key) == key {
			return attr.Value
		}
	}
	return attribute.Value{}
}

func telemetryUpdate(key model.ServiceKey, complete bool) model.ScheduleUpdate {
	return model.ScheduleUpdate{Service: key, BasicObserved: true, BasicComplete: true, ExtendedObserved: true, ExtendedComplete: complete,
		Events: func() []model.Event { return []model.Event{{Key: key, EventID: 1, Name: "program"}} },
		Diagnosis: func() string {
			if complete {
				return "complete"
			}
			return "missing table 0x95 section 240"
		},
	}
}

func TestCollectionEndReasons(t *testing.T) {
	oldStable, oldDead := eitsStableStopDuration, eitsDeadStreamTimeout
	eitsStableStopDuration = 15 * time.Millisecond
	eitsDeadStreamTimeout = 20 * time.Millisecond
	t.Cleanup(func() { eitsStableStopDuration = oldStable; eitsDeadStreamTimeout = oldDead })
	key := model.ServiceKey{NetworkID: 11, StreamID: 1, ServiceID: 102}
	receiverErr := errors.New("receiver startup failed")
	for _, reason := range []string{"complete", "deadline", "receive_error", "canceled", "stream_ended", "no_eit"} {
		t.Run(reason, func(t *testing.T) {
			recorder := recordCollectionSpans(t)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			collect := func(ctx context.Context, onSchedule func(model.ScheduleUpdate) error, _ func(model.PresentFollowing) error) error {
				if reason == "receive_error" {
					return receiverErr
				}
				if reason != "no_eit" {
					if err := onSchedule(telemetryUpdate(key, reason != "deadline")); err != nil {
						return err
					}
				}
				if reason == "stream_ended" {
					return nil
				}
				if reason == "canceled" {
					cancel()
				}
				<-ctx.Done()
				return ctx.Err()
			}
			started := time.Now()
			result, err := CollectServiceSnapshots(ctx, &collectProgramStore{}, newRemoteSyncServiceStore(), collect, []model.ServiceKey{key}, 80*time.Millisecond)
			if result.EndReason != reason {
				t.Fatalf("reason=%s, want %s; err=%v", result.EndReason, reason, err)
			}
			span := collectionSpan(t, recorder)
			if got := spanAttribute(span, "epg.end_reason").AsString(); got != reason {
				t.Fatalf("span reason=%s", got)
			}
			if reason == "complete" && (err != nil || time.Since(started) > 250*time.Millisecond) {
				t.Fatalf("stable completion waited for flush ticker: elapsed=%s err=%v", time.Since(started), err)
			}
			if reason == "deadline" {
				if err != nil || len(result.Incomplete) != 1 {
					t.Fatalf("partial result=%+v err=%v", result, err)
				}
				if got := strings.Join(spanAttribute(span, "epg.missing").AsStringSlice(), " "); !strings.Contains(got, "0x95 section 240") {
					t.Fatalf("missing diagnosis=%s", got)
				}
			}
			if reason == "receive_error" && !errors.Is(err, receiverErr) {
				t.Fatalf("lost receive error: %v", err)
			}
			if reason == "canceled" && !errors.Is(err, context.Canceled) {
				t.Fatalf("lost cancellation: %v", err)
			}
		})
	}
}

func TestCompletionTimerWaitsForNewVersion(t *testing.T) {
	old := eitsStableStopDuration
	eitsStableStopDuration = 40 * time.Millisecond
	t.Cleanup(func() { eitsStableStopDuration = old })
	key := model.ServiceKey{NetworkID: 11, StreamID: 1, ServiceID: 102}
	collect := func(ctx context.Context, onSchedule func(model.ScheduleUpdate) error, _ func(model.PresentFollowing) error) error {
		if err := onSchedule(telemetryUpdate(key, true)); err != nil {
			return err
		}
		time.Sleep(10 * time.Millisecond)
		if err := onSchedule(telemetryUpdate(key, false)); err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return errors.New("stopped with incomplete newer version")
		case <-time.After(80 * time.Millisecond):
		}
		if err := onSchedule(telemetryUpdate(key, true)); err != nil {
			return err
		}
		<-ctx.Done()
		return ctx.Err()
	}
	result, err := CollectServiceSnapshots(t.Context(), &collectProgramStore{}, newRemoteSyncServiceStore(), collect, []model.ServiceKey{key}, time.Second)
	if err != nil || result.EndReason != "complete" || len(result.Incomplete) != 0 {
		t.Fatalf("result=%+v err=%v", result, err)
	}
}

func TestOpportunisticIncompleteDoesNotWarnOrDelayTarget(t *testing.T) {
	old := eitsStableStopDuration
	eitsStableStopDuration = 15 * time.Millisecond
	t.Cleanup(func() { eitsStableStopDuration = old })
	recorder := recordCollectionSpans(t)
	var logs bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, nil)))
	t.Cleanup(func() { slog.SetDefault(previous) })
	target := model.ServiceKey{NetworkID: 65534, StreamID: 1, ServiceID: 119}
	other := model.ServiceKey{NetworkID: 65534, StreamID: 2, ServiceID: 121}
	collect := func(ctx context.Context, onSchedule func(model.ScheduleUpdate) error, _ func(model.PresentFollowing) error) error {
		if err := onSchedule(telemetryUpdate(target, true)); err != nil {
			return err
		}
		ticker := time.NewTicker(time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-ticker.C:
				update := telemetryUpdate(other, false)
				update.BasicComplete = false
				if err := onSchedule(update); err != nil {
					return err
				}
			}
		}
	}
	status := newRemoteSyncServiceStore()
	result, err := CollectServiceSnapshots(t.Context(), &collectProgramStore{}, status, collect, []model.ServiceKey{target}, time.Second)
	if err != nil || result.EndReason != "complete" || len(result.Incomplete) != 0 {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	if strings.Contains(logs.String(), `"level":"WARN"`) {
		t.Fatalf("opportunistic warning: %s", logs.String())
	}
	if got := spanAttribute(collectionSpan(t, recorder), "epg.services.opportunistic_incomplete").AsInt64(); got != 1 {
		t.Fatalf("opportunistic incomplete=%d", got)
	}
	if len(status.errors) != 1 {
		t.Fatalf("unexpected status writes: %+v", status.errors)
	}
}

func TestCollectionTraceBoundsResetEvents(t *testing.T) {
	recorder := recordCollectionSpans(t)
	key := model.ServiceKey{NetworkID: 11, StreamID: 1, ServiceID: 102}
	collect := func(ctx context.Context, onSchedule func(model.ScheduleUpdate) error, _ func(model.PresentFollowing) error) error {
		for i := 0; i < 100; i++ {
			u := telemetryUpdate(key, false)
			u.Reception = &model.ScheduleReception{TableID: 0x95, VersionChanged: true, CoverageReset: true, Version: uint8(i % 32)}
			if err := onSchedule(u); err != nil {
				return err
			}
		}
		return nil
	}
	_, _ = CollectServiceSnapshots(t.Context(), &collectProgramStore{}, newRemoteSyncServiceStore(), collect, []model.ServiceKey{key}, time.Second)
	span := collectionSpan(t, recorder)
	if len(span.Events()) > collectionTraceEventLimit {
		t.Fatalf("events=%d", len(span.Events()))
	}
	if spanAttribute(span, "epg.events.omitted").AsInt64() == 0 || spanAttribute(span, "epg.coverage_resets").AsInt64() != 100 {
		t.Fatalf("missing bounded event counters: %v", span.Attributes())
	}
	if len(spanAttribute(span, "epg.missing").AsStringSlice()) != 1 {
		t.Fatal("event limit hid final target diagnosis")
	}
}

type collectionResultRecorder struct{ result run.Result }

func (r *collectionResultRecorder) SetJobResult(result run.Result) { r.result = result }

func TestGatherReportsPartialTargetAfterDeadline(t *testing.T) {
	key := model.ServiceKey{NetworkID: 4, StreamID: 100, ServiceID: 101}
	basic := testEIT(0x50, key, 10)
	extended := testEIT(0x58, key, 10)
	extended.LastTableID = 0x58
	extended.SectionNumber = 64
	extended.LastSectionNumber = 65
	extended.SegmentLastSectionNumber = 65
	streams := staticEPGStreams{session: &collectEITSession{sections: []*ts.EIT{basic, extended}}}
	recorder := &collectionResultRecorder{}
	status := newRemoteSyncServiceStore()
	store := &collectProgramStore{}
	err := gatherNetwork(run.WithReporter(t.Context(), recorder), store, store, status, streams, 4, []Candidate{{Type: "BS", Channel: "BS01_0"}}, []model.ServiceKey{key}, 20*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	result := recorder.result
	if result.Counts["incompleteServices"] != 1 || len(result.Warnings) != 1 || len(result.Items) != 1 {
		t.Fatalf("partial result=%+v", result)
	}
	if result.Items[0].Data["endReason"] != "deadline" || result.Items[0].Data["result"] != "partial" {
		t.Fatalf("candidate=%+v", result.Items[0])
	}
	statusKey := model.ServiceKey{NetworkID: 4, ServiceID: 101}
	if status.errors[statusKey] != "" {
		t.Fatalf("partial save became service failure: %v", status.errors)
	}
}

type failingAfterPartialSession struct{ key model.ServiceKey }

func (s failingAfterPartialSession) CollectSchedule(_ context.Context, onSchedule func(model.ScheduleUpdate) error, _ func(model.PresentFollowing) error) error {
	if err := onSchedule(telemetryUpdate(s.key, false)); err != nil {
		return err
	}
	return errors.New("tuner closed")
}

func TestGatherPreservesPartialSaveAfterReceiveError(t *testing.T) {
	key := model.ServiceKey{NetworkID: 11, StreamID: 1, ServiceID: 102}
	for _, candidates := range [][]Candidate{{{Type: "BS4K", Channel: "45280"}}, {{Type: "BS4K", Channel: "45280"}, {Type: "BS4K", Channel: "fallback"}}} {
		recorder := &collectionResultRecorder{}
		status := newRemoteSyncServiceStore()
		store := &collectProgramStore{}
		streams := staticEPGStreams{session: failingAfterPartialSession{key: key}}
		err := gatherNetwork(run.WithReporter(t.Context(), recorder), store, store, status, streams, 11, candidates, []model.ServiceKey{key}, time.Second)
		if err != nil {
			t.Fatalf("partial save failed with %d candidates: %v", len(candidates), err)
		}
		if len(recorder.result.Warnings) == 0 || recorder.result.Items[0].Data["result"] != "partial" || recorder.result.Items[0].Data["endReason"] != "receive_error" {
			t.Fatalf("lost partial reception warning: %+v", recorder.result)
		}
		if status.errors[model.ServiceKey{NetworkID: 11, ServiceID: 102}] != "" {
			t.Fatalf("partial save changed health: %v", status.errors)
		}
	}
}

func TestSameStreamOpportunisticPartialDoesNotRefreshStatus(t *testing.T) {
	target := model.ServiceKey{NetworkID: 11, StreamID: 1, ServiceID: 102}
	other := model.ServiceKey{NetworkID: 11, StreamID: 1, ServiceID: 103}
	status := newRemoteSyncServiceStore()
	otherStatus := model.ServiceKey{NetworkID: 11, ServiceID: 103}
	status.successes[otherStatus] = 123
	collect := func(_ context.Context, onSchedule func(model.ScheduleUpdate) error, _ func(model.PresentFollowing) error) error {
		if err := onSchedule(telemetryUpdate(target, true)); err != nil {
			return err
		}
		return onSchedule(telemetryUpdate(other, false))
	}
	result, err := CollectServiceSnapshots(t.Context(), &collectProgramStore{}, status, collect, []model.ServiceKey{target}, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if status.successes[otherStatus] != 123 || len(result.Observed) != 1 {
		t.Fatalf("opportunistic partial made fresh: result=%+v status=%v", result, status.successes)
	}
}
