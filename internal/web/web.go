package web

import (
	"net/http"
	"net/http/pprof"
	"runtime"
	"time"

	"github.com/21S1298001/mahiron/internal/event"
	"github.com/21S1298001/mahiron/internal/observability"
	"github.com/21S1298001/mahiron/internal/version"
	"github.com/21S1298001/mahiron/internal/web/api"
	apigen "github.com/21S1298001/mahiron/internal/web/api/gen"
	"github.com/21S1298001/mahiron/internal/web/ui"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

// serverHeader はレスポンスに付与する Server ヘッダの値。
// 一部のクライアントはこのヘッダの `名前/バージョン` 表記で
// Mirakurun互換サーバーかどうかを判定するため、常に付与する必要がある。
const serverHeader = "Mahiron/" + version.Current

type WebConfig struct {
	ServiceManager        api.ServiceManager
	ProgramManager        api.ProgramManager
	StreamManager         api.StreamManager
	TunerManager          api.TunerManager
	JobManager            api.JobManager
	LogStore              api.LogStore
	EventHub              *event.Hub
	EpgStaleAfter         int64
	DataBroadcastDisabled bool
	MeterProvider         metric.MeterProvider
	TracerProvider        trace.TracerProvider
	// Pprof serves the net/http/pprof handlers under /debug/pprof.
	Pprof bool
}

func NewWeb(config WebConfig) (http.Handler, error) {
	mux := http.NewServeMux()
	apiHandler := api.NewHandler(api.HandlerConfig{
		ServiceManager:        config.ServiceManager,
		ProgramManager:        config.ProgramManager,
		StreamManager:         config.StreamManager,
		TunerManager:          config.TunerManager,
		JobManager:            config.JobManager,
		LogStore:              config.LogStore,
		EventHub:              config.EventHub,
		EpgStaleAfter:         config.EpgStaleAfter,
		DataBroadcastDisabled: config.DataBroadcastDisabled,
	})
	api, err := apigen.NewServer(apiHandler, apiHandler,
		apigen.WithMeterProvider(config.MeterProvider),
		apigen.WithTracerProvider(observability.NewFilteringTracerProvider(config.TracerProvider, observability.StreamOperationNames)),
	)
	if err != nil {
		return nil, err
	}

	mux.Handle("/api/", http.StripPrefix("/api", api))
	if config.Pprof {
		registerPprof(mux)
	}
	mux.Handle("/", ui.NewHandler())

	return withServerHeader(mux), nil
}

// registerPprof mounts the net/http/pprof handlers. They are registered
// explicitly rather than through the package's default mux, which this server
// never serves.
//
// The block and mutex profiles sample nothing until their rates are set, so
// enabling the endpoints without setting them would serve two empty profiles to
// someone who turned profiling on precisely to read them. The rates chosen cost
// little: one sample per millisecond spent blocked, and one in a hundred
// contended mutex events.
func registerPprof(mux *http.ServeMux) {
	runtime.SetBlockProfileRate(int(time.Millisecond))
	runtime.SetMutexProfileFraction(100)

	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
}

func withServerHeader(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Server", serverHeader)
		next.ServeHTTP(w, r)
	})
}
