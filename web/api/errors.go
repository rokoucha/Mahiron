package api

import (
	"net/http"

	apigen "github.com/21S1298001/Mahiron5/web/api/gen"
)

func notImplemented(reason string) *apigen.ErrorStatusCode {
	return &apigen.ErrorStatusCode{
		StatusCode: http.StatusNotImplemented,
		Response: apigen.Error{
			Code:   apigen.NewOptInt(http.StatusNotImplemented),
			Reason: apigen.NewOptString(reason),
		},
	}
}
