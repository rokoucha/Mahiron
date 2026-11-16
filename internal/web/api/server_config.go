package api

import (
	"context"

	apigen "github.com/21S1298001/mahiron/internal/web/api/gen"
)

// GetServerConfig returns an empty object. Mahiron does not expose its server
// configuration through the API.
func (h *Handler) GetServerConfig(context.Context) (apigen.GetServerConfigRes, error) {
	return &apigen.ConfigServer{}, nil
}
