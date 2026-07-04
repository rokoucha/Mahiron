package api

import (
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	apigen "github.com/21S1298001/mahiron/internal/web/api/gen"
)

func TestOpenAPIDoesNotExposeContainerHostileOperations(t *testing.T) {
	data, err := os.ReadFile("api.yml")
	if err != nil {
		t.Fatal(err)
	}
	spec := string(data)
	for _, operationID := range []string{
		"getChannelsConfig",
		"updateChannelsConfig",
		"getServerConfig",
		"updateServerConfig",
		"getTunersConfig",
		"updateTunersConfig",
		"channelScan",
		"getChannelScanStatus",
		"stopChannelScan",
		"updateVersion",
		"restart",
	} {
		if strings.Contains(spec, "operationId: "+operationID) {
			t.Fatalf("api.yml exposes excluded operationId %q", operationID)
		}
	}
}

func TestXMirakurunPriorityHeaderAcceptsNegativeOne(t *testing.T) {
	handler, _ := testStreamHeadHandler(t)
	server, err := apigen.NewServer(handler, handler)
	if err != nil {
		t.Fatal(err)
	}

	req := httptest.NewRequest(http.MethodHead, "/channels/GR/27/stream", nil)
	req.Header.Set("X-Mirakurun-Priority", "-1")
	rec := httptest.NewRecorder()
	server.ServeHTTP(rec, req)

	if rec.Code == http.StatusBadRequest {
		t.Fatalf("status = %d, want X-Mirakurun-Priority: -1 to pass validation, body = %s", rec.Code, rec.Body.String())
	}
}
