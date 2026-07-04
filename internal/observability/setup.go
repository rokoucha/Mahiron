package observability

import (
	"context"
	"errors"
	"log/slog"
	"os"

	"github.com/21S1298001/mahiron/internal/config"
	"github.com/21S1298001/mahiron/internal/version"
	"go.opentelemetry.io/contrib/bridges/otelslog"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploghttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	otelmetric "go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
	"go.opentelemetry.io/otel/sdk/log"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.41.0"
	"go.opentelemetry.io/otel/trace"
	tracenoop "go.opentelemetry.io/otel/trace/noop"
)

const instrumentationName = "github.com/21S1298001/mahiron"

type SetupResult struct {
	LogStore       *LogStore
	MeterProvider  otelmetric.MeterProvider
	TracerProvider trace.TracerProvider
	Shutdown       func(context.Context) error
}

func Setup(ctx context.Context, cfg config.ObservabilityConfig, level slog.Leveler) SetupResult {
	store := NewLogStore(defaultLogCapacity)
	stderr := slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level})
	apiLogs := slog.NewTextHandler(store, &slog.HandlerOptions{Level: level})

	var shutdowns []func(context.Context) error
	handlers := []slog.Handler{stderr, apiLogs}

	if cfg.Endpoint != "" && cfg.Logs.Enabled {
		handler, cleanup, err := newOTelLogHandler(ctx, cfg)
		if err != nil {
			slog.New(stderr).Warn("failed to initialize OTLP log exporter", "err", err)
		} else {
			handlers = append(handlers, handler)
			shutdowns = append(shutdowns, cleanup)
		}
	}

	slog.SetDefault(slog.New(newFanoutHandler(handlers...)))

	tracerProvider := trace.TracerProvider(tracenoop.NewTracerProvider())
	if cfg.Endpoint != "" && cfg.Traces.Enabled {
		provider, cleanup, err := newOTelTracerProvider(ctx, cfg)
		if err != nil {
			slog.Warn("failed to initialize OTLP trace exporter", "err", err)
		} else {
			tracerProvider = provider
			shutdowns = append(shutdowns, cleanup)
		}
	}
	otel.SetTracerProvider(tracerProvider)

	meterProvider := otelmetric.MeterProvider(noop.NewMeterProvider())
	if cfg.Endpoint != "" && cfg.Metrics.Enabled {
		provider, cleanup, err := newOTelMeterProvider(ctx, cfg)
		if err != nil {
			slog.Warn("failed to initialize OTLP metric exporter", "err", err)
		} else {
			meterProvider = provider
			shutdowns = append(shutdowns, cleanup)
		}
	}
	otel.SetMeterProvider(meterProvider)
	initMetrics(meterProvider)

	return SetupResult{
		LogStore:       store,
		MeterProvider:  meterProvider,
		TracerProvider: tracerProvider,
		Shutdown:       shutdownAll(shutdowns...),
	}
}

func newOTelLogHandler(ctx context.Context, cfg config.ObservabilityConfig) (slog.Handler, func(context.Context) error, error) {
	options := []otlploghttp.Option{otlploghttp.WithEndpointURL(cfg.Endpoint)}
	if len(cfg.Headers) > 0 {
		options = append(options, otlploghttp.WithHeaders(cfg.Headers))
	}

	exporter, err := otlploghttp.New(ctx, options...)
	if err != nil {
		return nil, nil, err
	}

	res, err := newResource(cfg)
	if err != nil {
		return nil, nil, err
	}

	provider := log.NewLoggerProvider(
		log.WithResource(res),
		log.WithProcessor(log.NewBatchProcessor(exporter)),
	)
	handler := otelslog.NewHandler(instrumentationName, otelslog.WithLoggerProvider(provider))
	return handler, provider.Shutdown, nil
}

func newOTelTracerProvider(ctx context.Context, cfg config.ObservabilityConfig) (trace.TracerProvider, func(context.Context) error, error) {
	options := []otlptracehttp.Option{otlptracehttp.WithEndpointURL(cfg.Endpoint)}
	if len(cfg.Headers) > 0 {
		options = append(options, otlptracehttp.WithHeaders(cfg.Headers))
	}

	exporter, err := otlptracehttp.New(ctx, options...)
	if err != nil {
		return nil, nil, err
	}
	res, err := newResource(cfg)
	if err != nil {
		return nil, nil, err
	}
	provider := sdktrace.NewTracerProvider(
		sdktrace.WithResource(res),
		sdktrace.WithBatcher(exporter),
	)
	return provider, provider.Shutdown, nil
}

func newOTelMeterProvider(ctx context.Context, cfg config.ObservabilityConfig) (otelmetric.MeterProvider, func(context.Context) error, error) {
	options := []otlpmetrichttp.Option{otlpmetrichttp.WithEndpointURL(cfg.Endpoint)}
	if len(cfg.Headers) > 0 {
		options = append(options, otlpmetrichttp.WithHeaders(cfg.Headers))
	}

	exporter, err := otlpmetrichttp.New(ctx, options...)
	if err != nil {
		return nil, nil, err
	}
	res, err := newResource(cfg)
	if err != nil {
		return nil, nil, err
	}
	provider := sdkmetric.NewMeterProvider(
		sdkmetric.WithResource(res),
		sdkmetric.WithReader(sdkmetric.NewPeriodicReader(exporter)),
	)
	return provider, provider.Shutdown, nil
}

func newResource(cfg config.ObservabilityConfig) (*resource.Resource, error) {
	return resource.Merge(
		resource.Default(),
		resource.NewSchemaless(
			semconv.ServiceName(cfg.ServiceName),
			semconv.ServiceVersion(version.Current),
			attribute.String("telemetry.sdk.language", "go"),
		),
	)
}

func shutdownAll(shutdowns ...func(context.Context) error) func(context.Context) error {
	return func(ctx context.Context) error {
		var result error
		for _, shutdown := range shutdowns {
			if shutdown == nil {
				continue
			}
			result = errors.Join(result, shutdown(ctx))
		}
		return result
	}
}

// Meter returns the mahiron instrumentation-scoped meter for provider, so
// callers outside this package (e.g. internal/app) can create instruments
// without duplicating the instrumentation name.
func Meter(provider otelmetric.MeterProvider) otelmetric.Meter {
	return provider.Meter(instrumentationName)
}

// NewInt64ObservableGauge creates an Int64ObservableGauge, logging and
// returning a nil instrument on failure rather than propagating the error.
func NewInt64ObservableGauge(meter otelmetric.Meter, name string, opts ...otelmetric.Int64ObservableGaugeOption) otelmetric.Int64ObservableGauge {
	instrument, err := meter.Int64ObservableGauge(name, opts...)
	if err != nil {
		slog.Warn("failed to create metric instrument", "metric", name, "err", err)
	}
	return instrument
}
