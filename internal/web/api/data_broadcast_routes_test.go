package api

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/21S1298001/mahiron/internal/bml"
	apigen "github.com/21S1298001/mahiron/internal/web/api/gen"
)

// TestBMLRoutesServeUnderBMLPath pins the ISDB-S3 phase 3-3 path move:
// BML endpoints live under /data-broadcast/bml/..., and the old
// /data-broadcast/... paths no longer route.
func TestBMLRoutesServeUnderBMLPath(t *testing.T) {
	handler := testProgramHandler(t)
	handler.streamManager = fakeDataBroadcastStreamManager{}
	handler.bmlSnapshotStore = stubBMLSnapshotStore{
		service: bml.PersistedService{ServiceID: 101, PMTSection: testBMLPMTSection(101, 0x0200, 0x40), StoredAt: 1700000000},
		found:   true,
	}
	server, err := apigen.NewServer(handler, handler)
	if err != nil {
		t.Fatal(err)
	}

	rec := httptest.NewRecorder()
	server.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/services/100101/data-broadcast/bml/state", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("new state path status = %d, body = %q", rec.Code, rec.Body.String())
	}
	if body := rec.Body.String(); !strings.Contains(body, `"origin":"cache"`) {
		t.Fatalf("new state path body = %s, want cache snapshot", body)
	}

	rec = httptest.NewRecorder()
	server.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/services/100101/data-broadcast/state", nil))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("old state path status = %d, want %d", rec.Code, http.StatusNotFound)
	}

	rec = httptest.NewRecorder()
	server.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/services/100101/data-broadcast/events", nil))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("old events path status = %d, want %d", rec.Code, http.StatusNotFound)
	}
}
