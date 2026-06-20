package web

import (
	"net/http"

	"github.com/21S1298001/Mahiron5/internal/web/api"
	apigen "github.com/21S1298001/Mahiron5/internal/web/api/gen"
)

type WebConfig struct {
	ServiceManager api.ServiceManager
	ProgramManager api.ProgramManager
	StreamManager  api.StreamManager
	TunerManager   api.TunerManager
	JobManager     api.JobManager
	EpgStaleAfter  int64
}

func NewWeb(config WebConfig) (http.Handler, error) {
	mux := http.NewServeMux()
	api, err := apigen.NewServer(api.NewHandler(api.HandlerConfig{
		ServiceManager: config.ServiceManager,
		ProgramManager: config.ProgramManager,
		StreamManager:  config.StreamManager,
		TunerManager:   config.TunerManager,
		JobManager:     config.JobManager,
		EpgStaleAfter:  config.EpgStaleAfter,
	}))
	if err != nil {
		return nil, err
	}

	mux.Handle("/api/", http.StripPrefix("/api", api))

	return mux, nil
}
