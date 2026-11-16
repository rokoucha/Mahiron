package api

import (
	"context"

	"github.com/21S1298001/mahiron/internal/version"
	apigen "github.com/21S1298001/mahiron/internal/web/api/gen"
)

const currentVersion = version.Current

func (h *Handler) CheckVersion(ctx context.Context) (apigen.CheckVersionRes, error) {
	return &apigen.Version{
		Current: apigen.NewOptString(currentVersion),
		Latest:  apigen.NewOptString(""),
		Server:  apigen.NewOptString(version.Server),
	}, nil
}
