package api

import (
	"context"
	"encoding/json"
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

func TestOpenAPIExposesReadOnlyServerConfig(t *testing.T) {
	data, err := os.ReadFile("api.yml")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), "operationId: getServerConfig") {
		t.Fatal("api.yml does not expose getServerConfig")
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

// The official mirakurun npm client spreads the path-level parameters without a
// nil guard (`[...p.parameters, ...(p.get.parameters || [])]`), so every path
// item must carry a parameters array even when it is empty.
func TestOpenAPIPathItemsAlwaysDeclareParameters(t *testing.T) {
	res, err := GetApiDocumentation(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	docs, ok := res.(*apigen.GetApiDocumentationOK)
	if !ok {
		t.Fatalf("response type = %T, want *GetApiDocumentationOK", res)
	}
	raw, ok := (*docs)["paths"]
	if !ok {
		t.Fatal("docs has no paths")
	}

	var paths map[string]map[string]json.RawMessage
	if err := json.Unmarshal(raw, &paths); err != nil {
		t.Fatal(err)
	}
	if len(paths) == 0 {
		t.Fatal("docs paths is empty")
	}
	for path, item := range paths {
		params, ok := item["parameters"]
		if !ok {
			t.Errorf("path %q has no path-level parameters", path)
			continue
		}
		var decoded []any
		if err := json.Unmarshal(params, &decoded); err != nil {
			t.Errorf("path %q parameters = %s: %v", path, params, err)
		}
	}
}
