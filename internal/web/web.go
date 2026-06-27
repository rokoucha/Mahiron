package web

import (
	"net/http"

	"github.com/21S1298001/mahiron/internal/event"
	"github.com/21S1298001/mahiron/internal/observability"
	"github.com/21S1298001/mahiron/internal/web/api"
	apigen "github.com/21S1298001/mahiron/internal/web/api/gen"
	"github.com/21S1298001/mahiron/internal/web/ui"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

type WebConfig struct {
	ServiceManager api.ServiceManager
	ProgramManager api.ProgramManager
	StreamManager  api.StreamManager
	TunerManager   api.TunerManager
	JobManager     api.JobManager
	LogStore       api.LogStore
	EventHub       *event.Hub
	EpgStaleAfter  int64
	MeterProvider  metric.MeterProvider
	TracerProvider trace.TracerProvider
}

func NewWeb(config WebConfig) (http.Handler, error) {
	mux := http.NewServeMux()
	api, err := apigen.NewServer(api.NewHandler(api.HandlerConfig{
		ServiceManager: config.ServiceManager,
		ProgramManager: config.ProgramManager,
		StreamManager:  config.StreamManager,
		TunerManager:   config.TunerManager,
		JobManager:     config.JobManager,
		LogStore:       config.LogStore,
		EventHub:       config.EventHub,
		EpgStaleAfter:  config.EpgStaleAfter,
	}),
		apigen.WithMeterProvider(config.MeterProvider),
		apigen.WithTracerProvider(observability.NewFilteringTracerProvider(config.TracerProvider, observability.StreamOperationNames)),
	)
	if err != nil {
		return nil, err
	}

	mux.Handle("/api/", http.StripPrefix("/api", api))
	mux.Handle("/", ui.NewHandler())

	return mux, nil
}
