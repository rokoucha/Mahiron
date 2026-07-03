package observability

import (
	"context"
	"testing"

	"github.com/21S1298001/mahiron/internal/config"
	"github.com/21S1298001/mahiron/internal/version"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric/noop"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	tracenoop "go.opentelemetry.io/otel/trace/noop"
)

func TestSetupTracesDisabledUsesNoopProvider(t *testing.T) {
	result := Setup(context.Background(), config.ObservabilityConfig{}, nil)
	if _, ok := result.TracerProvider.(tracenoop.TracerProvider); !ok {
		t.Fatalf("TracerProvider = %T, want noop.TracerProvider", result.TracerProvider)
	}
	if _, ok := result.MeterProvider.(noop.MeterProvider); !ok {
		t.Fatalf("MeterProvider = %T, want noop.MeterProvider", result.MeterProvider)
	}
	if err := result.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown() = %v", err)
	}
}

func TestNewResourceIncludesServiceVersion(t *testing.T) {
	res, err := newResource(config.ObservabilityConfig{ServiceName: "test-service"})
	if err != nil {
		t.Fatal(err)
	}
	attrs := res.Set()
	if got, ok := attrs.Value("service.name"); !ok || got.AsString() != "test-service" {
		t.Fatalf("service.name = %q, %v; want test-service, true", got.AsString(), ok)
	}
	if got, ok := attrs.Value("service.version"); !ok || got.AsString() != version.Current {
		t.Fatalf("service.version = %q, %v; want %q, true", got.AsString(), ok, version.Current)
	}
}

func TestRecordJobRunMetrics(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	initMetrics(provider)

	RecordJobRun(t.Context(), "test-job", "success", 123)
	RecordStreamSessionStart(t.Context(), "GR", "GR", "local", "success")
	RecordStreamSessionDuration(t.Context(), "GR", "GR", "local", 456)
	RecordStreamPacket(t.Context(), "GR", "27", 188)
	RecordStreamPacketError(t.Context(), "GR", "27", "read")
	RecordStreamContinuityCounterError(t.Context(), "GR", "27")
	RecordStreamSubscriberError(t.Context(), "GR", "write")
	RecordStreamSubscriberOverflow(t.Context(), "GR", "packet_overflow")
	RecordTunerAcquire(t.Context(), "GR", "success", false, 12)
	RecordTunerProcessStart(t.Context(), "GR", "27", "success")
	RecordTunerProcessExit(t.Context(), "GR", "27", "success")
	RecordTunerProcessRestartAttempt(t.Context(), "GR", "27")
	RecordRemoteOperation(t.Context(), "remote.check_available", "success", 23)
	RecordRemoteOperation(t.Context(), "remote.check_available", "failure", 34)
	RecordDBOperation(t.Context(), "db.program.upsert_all", 45, nil)
	RecordDBOperation(t.Context(), "db.program.upsert_all", 56, context.Canceled)
	RecordEventPublished(t.Context(), "program", "update")
	RecordEventDropped(t.Context())
	RecordLogsDropped(t.Context(), 1)
	RecordEPGProgramsUpserted(t.Context(), "eits", "success", 2)
	RecordEPGProgramsDeleted(t.Context(), "cleanup", "success", 3)
	RecordEPGServiceUpdateError(t.Context(), "remote", "attempt")

	var data metricdata.ResourceMetrics
	if err := reader.Collect(t.Context(), &data); err != nil {
		t.Fatal(err)
	}
	if got := int64Sum(data, MetricJobRuns); got != 1 {
		t.Fatalf("%s = %d, want 1", MetricJobRuns, got)
	}
	if got := int64HistogramCount(data, MetricJobDuration); got != 1 {
		t.Fatalf("%s count = %d, want 1", MetricJobDuration, got)
	}
	if got := int64Sum(data, MetricStreamSessionStarts); got != 1 {
		t.Fatalf("%s = %d, want 1", MetricStreamSessionStarts, got)
	}
	if got := int64HistogramCount(data, MetricStreamSessionDuration); got != 1 {
		t.Fatalf("%s count = %d, want 1", MetricStreamSessionDuration, got)
	}
	if got := int64Sum(data, MetricStreamPackets); got != 1 {
		t.Fatalf("%s = %d, want 1", MetricStreamPackets, got)
	}
	if got := int64Sum(data, MetricStreamBytes); got != 188 {
		t.Fatalf("%s = %d, want 188", MetricStreamBytes, got)
	}
	if got := int64Sum(data, MetricStreamPacketErrors); got != 1 {
		t.Fatalf("%s = %d, want 1", MetricStreamPacketErrors, got)
	}
	if got := int64Sum(data, MetricStreamContinuityErrors); got != 1 {
		t.Fatalf("%s = %d, want 1", MetricStreamContinuityErrors, got)
	}
	if got := int64Sum(data, MetricStreamSubscriberErrors); got != 2 {
		t.Fatalf("%s = %d, want 2", MetricStreamSubscriberErrors, got)
	}
	if got := int64Sum(data, MetricStreamSubscriberOverflow); got != 1 {
		t.Fatalf("%s = %d, want 1", MetricStreamSubscriberOverflow, got)
	}
	if got := int64Sum(data, MetricTunerAcquireRequests); got != 1 {
		t.Fatalf("%s = %d, want 1", MetricTunerAcquireRequests, got)
	}
	if got := int64HistogramCount(data, MetricTunerAcquireDuration); got != 1 {
		t.Fatalf("%s count = %d, want 1", MetricTunerAcquireDuration, got)
	}
	if got := int64Sum(data, MetricTunerProcessStarts); got != 1 {
		t.Fatalf("%s = %d, want 1", MetricTunerProcessStarts, got)
	}
	if got := int64Sum(data, MetricTunerProcessExits); got != 1 {
		t.Fatalf("%s = %d, want 1", MetricTunerProcessExits, got)
	}
	if got := int64Sum(data, MetricTunerProcessRestarts); got != 1 {
		t.Fatalf("%s = %d, want 1", MetricTunerProcessRestarts, got)
	}
	if got := int64Sum(data, MetricRemoteRequests); got != 2 {
		t.Fatalf("%s = %d, want 2", MetricRemoteRequests, got)
	}
	if got := int64HistogramCount(data, MetricRemoteDuration); got != 2 {
		t.Fatalf("%s count = %d, want 2", MetricRemoteDuration, got)
	}
	if got := int64Sum(data, MetricRemoteErrors); got != 1 {
		t.Fatalf("%s = %d, want 1", MetricRemoteErrors, got)
	}
	if got := int64HistogramCount(data, MetricDBOperationDuration); got != 2 {
		t.Fatalf("%s count = %d, want 2", MetricDBOperationDuration, got)
	}
	if got := int64Sum(data, MetricDBOperationErrors); got != 1 {
		t.Fatalf("%s = %d, want 1", MetricDBOperationErrors, got)
	}
	if got := int64Sum(data, MetricEventsPublished); got != 1 {
		t.Fatalf("%s = %d, want 1", MetricEventsPublished, got)
	}
	if got := int64Sum(data, MetricEventsDropped); got != 1 {
		t.Fatalf("%s = %d, want 1", MetricEventsDropped, got)
	}
	if got := int64Sum(data, MetricLogsDropped); got != 1 {
		t.Fatalf("%s = %d, want 1", MetricLogsDropped, got)
	}
	if got := int64Sum(data, MetricEPGProgramsUpserted); got != 2 {
		t.Fatalf("%s = %d, want 2", MetricEPGProgramsUpserted, got)
	}
	if got := int64Sum(data, MetricEPGProgramsDeleted); got != 3 {
		t.Fatalf("%s = %d, want 3", MetricEPGProgramsDeleted, got)
	}
	if got := int64Sum(data, MetricEPGServiceUpdateErrors); got != 1 {
		t.Fatalf("%s = %d, want 1", MetricEPGServiceUpdateErrors, got)
	}
	if got := int64SumWithAttrs(data, MetricEPGProgramsUpserted, AttrSource.String("eits"), AttrResult.String("success")); got != 2 {
		t.Fatalf("%s eits/success = %d, want 2", MetricEPGProgramsUpserted, got)
	}
	if got := int64SumWithAttrs(data, MetricEPGProgramsDeleted, AttrSource.String("cleanup"), AttrResult.String("success")); got != 3 {
		t.Fatalf("%s cleanup/success = %d, want 3", MetricEPGProgramsDeleted, got)
	}
	if got := int64SumWithAttrs(data, MetricEPGServiceUpdateErrors, AttrSource.String("remote"), AttrResult.String("attempt")); got != 1 {
		t.Fatalf("%s remote/attempt = %d, want 1", MetricEPGServiceUpdateErrors, got)
	}
}

func TestFilteringTracerProviderSkipsExcludedSpans(t *testing.T) {
	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	filtered := NewFilteringTracerProvider(provider, []string{"GetChannelStream"})
	tracer := filtered.Tracer("test")

	_, statusSpan := tracer.Start(context.Background(), "GetStatus")
	statusSpan.End()
	_, streamSpan := tracer.Start(context.Background(), "GetChannelStream")
	streamSpan.End()

	spans := recorder.Ended()
	if len(spans) != 1 {
		t.Fatalf("ended spans = %d, want 1", len(spans))
	}
	if spans[0].Name() != "GetStatus" {
		t.Fatalf("span name = %q, want GetStatus", spans[0].Name())
	}
}

func int64Sum(data metricdata.ResourceMetrics, name string) int64 {
	for _, scope := range data.ScopeMetrics {
		for _, item := range scope.Metrics {
			if item.Name != name {
				continue
			}
			sum, ok := item.Data.(metricdata.Sum[int64])
			if !ok {
				return 0
			}
			var total int64
			for _, point := range sum.DataPoints {
				total += point.Value
			}
			return total
		}
	}
	return 0
}

func int64SumWithAttrs(data metricdata.ResourceMetrics, name string, attrs ...attribute.KeyValue) int64 {
	for _, scope := range data.ScopeMetrics {
		for _, item := range scope.Metrics {
			if item.Name != name {
				continue
			}
			sum, ok := item.Data.(metricdata.Sum[int64])
			if !ok {
				return 0
			}
			var total int64
			for _, point := range sum.DataPoints {
				if pointHasAttrs(point.Attributes, attrs...) {
					total += point.Value
				}
			}
			return total
		}
	}
	return 0
}

func pointHasAttrs(set attribute.Set, attrs ...attribute.KeyValue) bool {
	for _, attr := range attrs {
		got, ok := set.Value(attr.Key)
		if !ok || got.String() != attr.Value.String() {
			return false
		}
	}
	return true
}

func int64HistogramCount(data metricdata.ResourceMetrics, name string) uint64 {
	for _, scope := range data.ScopeMetrics {
		for _, item := range scope.Metrics {
			if item.Name != name {
				continue
			}
			histogram, ok := item.Data.(metricdata.Histogram[int64])
			if !ok {
				return 0
			}
			var total uint64
			for _, point := range histogram.DataPoints {
				total += point.Count
			}
			return total
		}
	}
	return 0
}
