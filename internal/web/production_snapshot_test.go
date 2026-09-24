package web

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/21S1298001/mahiron/internal/config"
	"github.com/21S1298001/mahiron/internal/db"
	"github.com/21S1298001/mahiron/internal/event"
	"github.com/21S1298001/mahiron/internal/job"
	"github.com/21S1298001/mahiron/internal/mirakurun"
	"github.com/21S1298001/mahiron/internal/observability"
	"github.com/21S1298001/mahiron/internal/program"
	"github.com/21S1298001/mahiron/internal/service"
	"github.com/21S1298001/mahiron/internal/tuner"
)

// snapshotEnv names a directory holding a copy of a production database
// (mahiron.db) and its channels.yml. The database cannot be committed, so the
// test only runs when the variable is set.
const snapshotEnv = "MAHIRON_TEST_SNAPSHOT"

// snapshotEventPrograms bounds the programs republished to record /events
// payloads: the program manager publishes one event every 10 ms.
const snapshotEventPrograms = 400

// TestProductionSnapshot records the responses a production database yields
// under <dir>/current and compares them with <dir>/baseline, which the first
// run creates. It guards refactorings that must not change the API output;
// intended changes are reviewed with diff -ru baseline current and accepted by
// replacing the baseline.
func TestProductionSnapshot(t *testing.T) {
	dir := os.Getenv(snapshotEnv)
	if dir == "" {
		t.Skipf("%s is not set", snapshotEnv)
	}
	// XMLTV formats times in the local zone.
	local := time.Local
	time.Local = time.FixedZone("JST", 9*60*60)
	t.Cleanup(func() { time.Local = local })

	channels, err := config.LoadAndParseChannelsConfig(filepath.Join(dir, "channels.yml"))
	if err != nil {
		t.Fatal(err)
	}
	database := openSnapshotDB(t, filepath.Join(dir, "mahiron.db"))
	hub := event.NewWithCapacity(100000)
	services := service.NewManager(service.NewSQLiteStore(database), channels, mirakurun.NewEventPublisher(hub))
	programs := program.NewManager(program.NewSQLiteStore(database))
	handler := newSnapshotHandler(t, services, programs, hub)

	outputs := map[string][]byte{}
	for name, path := range map[string]string{
		"services.json":     "/api/services",
		"channels.json":     "/api/channels",
		"programs.json":     "/api/programs",
		"iptv-playlist.m3u": "/api/iptv/playlist",
		"iptv-xmltv.xml":    "/api/iptv/xmltv",
		"iptv-lineup.json":  "/api/iptv/lineup.json",
	} {
		outputs[name] = snapshotGet(t, handler, path)
	}
	if err := services.SeedEventLog(t.Context()); err != nil {
		t.Fatal(err)
	}
	outputs["events-services.json"] = snapshotGet(t, handler, "/api/events")
	outputs["events-programs.json"] = programEvents(t, programs)
	for name, raw := range outputs {
		outputs[name] = normalizeSnapshot(t, name, raw)
	}

	current := filepath.Join(dir, "current")
	writeSnapshot(t, current, outputs)
	baseline := filepath.Join(dir, "baseline")
	if _, err := os.Stat(baseline); os.IsNotExist(err) {
		writeSnapshot(t, baseline, outputs)
		t.Logf("created baseline in %s", baseline)
		return
	}
	for name, got := range outputs {
		want, err := os.ReadFile(filepath.Join(baseline, name))
		if err != nil {
			t.Errorf("%s: %v", name, err)
			continue
		}
		if !bytes.Equal(got, want) {
			t.Errorf("%s differs from the baseline: diff -u %s %s", name, filepath.Join(baseline, name), filepath.Join(current, name))
		}
	}
}

// openSnapshotDB opens a copy so that migrations never touch the recorded
// database.
func openSnapshotDB(t *testing.T, path string) *db.DB {
	t.Helper()
	src, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = src.Close() }()
	copyPath := filepath.Join(t.TempDir(), "mahiron.db")
	dst, err := os.Create(copyPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.Copy(dst, src); err != nil {
		t.Fatal(err)
	}
	if err := dst.Close(); err != nil {
		t.Fatal(err)
	}
	database, err := db.Open(copyPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	if err := db.Migrate(t.Context(), database); err != nil {
		t.Fatal(err)
	}
	return database
}

func newSnapshotHandler(t *testing.T, services *service.Manager, programs *program.Manager, hub *event.Hub) http.Handler {
	t.Helper()
	jobs, err := job.NewManager(job.Config{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = jobs.Shutdown(context.Background()) })
	handler, err := NewWeb(WebConfig{
		ServiceManager: services,
		ProgramManager: programs,
		StreamManager:  testStreamManager{},
		TunerManager:   tuner.NewManager(&tuner.ManagerConfig{}),
		JobManager:     jobs,
		LogStore:       observability.NewLogStore(16),
		EventHub:       hub,
		EpgStaleAfter:  7_200_000,
	})
	if err != nil {
		t.Fatal(err)
	}
	return handler
}

func snapshotGet(t *testing.T, handler http.Handler, path string) []byte {
	t.Helper()
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("GET %s = %d: %s", path, rec.Code, rec.Body)
	}
	return rec.Body.Bytes()
}

// programEvents republishes a spread of the stored programs through a program
// manager on an empty database, which publishes a create event for each, and
// returns the event payloads.
func programEvents(t *testing.T, source *program.Manager) []byte {
	t.Helper()
	all, err := source.List(t.Context(), program.Query{})
	if err != nil {
		t.Fatal(err)
	}
	step := max(1, len(all)/snapshotEventPrograms)
	var sample []*program.Program
	for i := 0; i < len(all); i += step {
		sample = append(sample, all[i])
	}

	database, err := db.OpenInMemory()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	hub := event.NewWithCapacity(len(sample))
	manager := program.NewManager(program.NewSQLiteStore(database), mirakurun.NewEventPublisher(hub))
	if err := manager.UpsertPrograms(t.Context(), sample); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Minute)
	for len(hub.Log()) < len(sample) {
		if time.Now().After(deadline) {
			t.Fatalf("got %d program events, want %d", len(hub.Log()), len(sample))
		}
		time.Sleep(100 * time.Millisecond)
	}
	raw, err := json.Marshal(hub.Log())
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func writeSnapshot(t *testing.T, dir string, outputs map[string][]byte) {
	t.Helper()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	for name, raw := range outputs {
		if err := os.WriteFile(filepath.Join(dir, name), raw, 0o644); err != nil {
			t.Fatal(err)
		}
	}
}

// normalizeSnapshot re-encodes JSON with indentation, so that differences
// show up line by line, and drops the wall-clock publication time of events.
// Object key order is preserved, not sorted: the outputs are
// deterministically ordered (extended items sorted by key, event payloads in
// field order), so the snapshot also pins the order. Decoding through the
// token stream keeps the document order that decoding into any would lose.
func normalizeSnapshot(t *testing.T, name string, raw []byte) []byte {
	t.Helper()
	if filepath.Ext(name) != ".json" {
		return raw
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var buf bytes.Buffer
	if err := writeCanonicalJSON(&buf, decoder, 0, strings.HasPrefix(name, "events-")); err != nil {
		t.Fatalf("%s: %v", name, err)
	}
	if decoder.More() {
		t.Fatalf("%s: trailing data after JSON document", name)
	}
	return append(buf.Bytes(), '\n')
}

// writeCanonicalJSON copies one JSON value from decoder to buf with two-space
// indentation, preserving object key order. At depth 1 of an events file
// (the event objects), the "time" entry is dropped.
func writeCanonicalJSON(buf *bytes.Buffer, decoder *json.Decoder, depth int, dropTime bool) error {
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	if delim, ok := token.(json.Delim); ok {
		switch delim {
		case '{':
			return writeCanonicalObject(buf, decoder, depth, dropTime && depth == 1)
		case '[':
			return writeCanonicalArray(buf, decoder, depth, dropTime)
		}
	}
	return writeCanonicalScalar(buf, token)
}

func writeCanonicalObject(buf *bytes.Buffer, decoder *json.Decoder, depth int, dropTime bool) error {
	if !decoder.More() {
		if _, err := decoder.Token(); err != nil {
			return err
		}
		buf.WriteString("{}")
		return nil
	}
	buf.WriteString("{\n")
	first := true
	for decoder.More() {
		keyToken, err := decoder.Token()
		if err != nil {
			return err
		}
		key, ok := keyToken.(string)
		if !ok {
			return io.ErrUnexpectedEOF
		}
		if dropTime && key == "time" {
			if err := skipCanonicalValue(decoder); err != nil {
				return err
			}
			continue
		}
		if !first {
			buf.WriteString(",\n")
		}
		first = false
		writeCanonicalIndent(buf, depth+1)
		keyJSON, err := json.Marshal(key)
		if err != nil {
			return err
		}
		buf.Write(keyJSON)
		buf.WriteString(": ")
		if err := writeCanonicalJSON(buf, decoder, depth+1, false); err != nil {
			return err
		}
	}
	if _, err := decoder.Token(); err != nil {
		return err
	}
	buf.WriteString("\n")
	writeCanonicalIndent(buf, depth)
	buf.WriteString("}")
	return nil
}

func writeCanonicalArray(buf *bytes.Buffer, decoder *json.Decoder, depth int, dropTime bool) error {
	if !decoder.More() {
		if _, err := decoder.Token(); err != nil {
			return err
		}
		buf.WriteString("[]")
		return nil
	}
	buf.WriteString("[\n")
	first := true
	for decoder.More() {
		if !first {
			buf.WriteString(",\n")
		}
		first = false
		writeCanonicalIndent(buf, depth+1)
		if err := writeCanonicalJSON(buf, decoder, depth+1, dropTime); err != nil {
			return err
		}
	}
	if _, err := decoder.Token(); err != nil {
		return err
	}
	buf.WriteString("\n")
	writeCanonicalIndent(buf, depth)
	buf.WriteString("]")
	return nil
}

func writeCanonicalScalar(buf *bytes.Buffer, token any) error {
	switch value := token.(type) {
	case nil:
		buf.WriteString("null")
	case bool:
		if value {
			buf.WriteString("true")
		} else {
			buf.WriteString("false")
		}
	case string:
		keyJSON, err := json.Marshal(value)
		if err != nil {
			return err
		}
		buf.Write(keyJSON)
	case json.Number:
		buf.WriteString(value.String())
	default:
		return io.ErrUnexpectedEOF
	}
	return nil
}

// skipCanonicalValue consumes one JSON value without writing it.
func skipCanonicalValue(decoder *json.Decoder) error {
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	if delim, ok := token.(json.Delim); ok {
		switch delim {
		case '{':
			for decoder.More() {
				if _, err := decoder.Token(); err != nil {
					return err
				}
				if err := skipCanonicalValue(decoder); err != nil {
					return err
				}
			}
		case '[':
			for decoder.More() {
				if err := skipCanonicalValue(decoder); err != nil {
					return err
				}
			}
		}
		if _, err := decoder.Token(); err != nil {
			return err
		}
	}
	return nil
}

func writeCanonicalIndent(buf *bytes.Buffer, depth int) {
	for i := 0; i < depth; i++ {
		buf.WriteString("  ")
	}
}

func TestNormalizeSnapshotPreservesKeyOrder(t *testing.T) {
	raw := `{"b":2,"a":{"d":[3,{"f":false,"e":null}],"c":"x"},"empty":{},"list":[]}`
	got := string(normalizeSnapshot(t, "programs.json", []byte(raw)))
	want := `{
  "b": 2,
  "a": {
    "d": [
      3,
      {
        "f": false,
        "e": null
      }
    ],
    "c": "x"
  },
  "empty": {},
  "list": []
}
`
	if got != want {
		t.Fatalf("normalized = %q, want %q", got, want)
	}

	events := `[{"resource":"program","type":"create","data":{"id":1},"time":123}]`
	got = string(normalizeSnapshot(t, "events-programs.json", []byte(events)))
	if strings.Contains(got, `"time"`) {
		t.Fatalf("time not dropped: %q", got)
	}
	if !strings.Contains(got, `"data": {`) {
		t.Fatalf("order not preserved: %q", got)
	}

	if got := normalizeSnapshot(t, "iptv-playlist.m3u", []byte("#EXTM3U\n")); string(got) != "#EXTM3U\n" {
		t.Fatalf("non-JSON changed: %q", got)
	}
}
