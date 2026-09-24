package web

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/21S1298001/mahiron/internal/db"
	"github.com/21S1298001/mahiron/internal/event"
	"github.com/21S1298001/mahiron/internal/job"
	"github.com/21S1298001/mahiron/internal/observability"
	"github.com/21S1298001/mahiron/internal/program"
	"github.com/21S1298001/mahiron/internal/service"
	"github.com/21S1298001/mahiron/internal/tuner"
)

// fullEPGPrograms is the size of a real EPG: 65363 programs of roughly 1.2 kB
// of JSON each, as measured on a running instance in 2026-09.
const fullEPGPrograms = 65363

func buildFullEPG(n int) []*program.Program {
	const kanji = "羽柴軍の圧倒的な兵力の前に勝家は敗走市の待つ北庄城に籠城する小一郎は降伏を促すべきと言うが秀吉は勝家が降伏するなどありえないと一刀両断利家に先鋒を命じる"
	programs := make([]*program.Program, 0, n)
	for i := range n {
		networkID := uint16(32736 + i%20)
		serviceID := uint16(1024 + i%151)
		eventID := uint16(i % 65535)
		mainText := kanji + fmt.Sprint(i)
		componentTag := 16
		isMain := true
		samplingRate := 48000
		programs = append(programs, &program.Program{
			ID:          program.ProgramID(networkID, serviceID, eventID) + int64(i),
			EventID:     eventID,
			ServiceID:   serviceID,
			NetworkID:   networkID,
			StartAt:     1788609060000 + int64(i)*120000,
			Duration:    1800000,
			IsFree:      true,
			Name:        "大河ドラマ「豊臣兄弟！」２分ダイジェスト " + fmt.Sprint(i),
			Description: mainText[:120],
			Genres:      []program.Genre{{Lv1: 3, Lv2: 0, Un1: 15, Un2: 15}, {Lv1: 3, Lv2: 2, Un1: 15, Un2: 15}},
			Video:       &program.Video{StreamContent: 1, ComponentType: 179},
			Audios: []program.Audio{{
				ComponentType: 3,
				ComponentTag:  &componentTag,
				IsMain:        &isMain,
				SamplingRate:  &samplingRate,
				Langs:         []string{"jpn"},
			}},
			Extended: map[string]string{
				"番組内容":  mainText,
				"原作・脚本": "　【作】八津弘幸",
			},
			RelatedItems: []program.RelatedItem{},
			Series:       &program.Series{ID: i % 1000, Repeat: 0, Pattern: 3, Episode: i % 50, LastEpisode: 50, Name: "豊臣兄弟！"},
		})
	}
	return programs
}

func newFullEPGHandler(t *testing.T) http.Handler {
	t.Helper()
	dir := t.TempDir()
	database, err := db.Open(filepath.Join(dir, "mahiron.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	if err := db.Migrate(t.Context(), database); err != nil {
		t.Fatal(err)
	}

	store := program.NewSQLiteStore(database)
	programs := buildFullEPG(fullEPGPrograms)
	for chunk := 0; chunk < len(programs); chunk += 2000 {
		end := min(chunk+2000, len(programs))
		if err := store.UpsertAll(t.Context(), programs[chunk:end]); err != nil {
			t.Fatal(err)
		}
	}
	programs = nil

	hub := event.New()
	jobs, err := job.NewManager(job.Config{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = jobs.Shutdown(context.Background()) })

	handler, err := NewWeb(WebConfig{
		ServiceManager: service.NewManager(service.NewSQLiteStore(database), nil, hub),
		ProgramManager: program.NewManager(store, hub),
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

// TestFullEPGResponsesStayOffHeap guards the two endpoints that return the
// whole EPG. Building either response in memory first — the rows, the
// program.Program values, the response values and the finished document all at
// once — peaked at 328 MB for the JSON and 242 MB for the XMLTV, against a
// process that is given 640 MB in production. Both now encode one program at a
// time.
func TestFullEPGResponsesStayOffHeap(t *testing.T) {
	if testing.Short() {
		t.Skip("builds a database of every program in a full EPG")
	}
	handler := newFullEPGHandler(t)
	for _, path := range []string{"/api/programs", "/api/iptv/xmltv"} {
		t.Run(path, func(t *testing.T) {
			written, peak := measurePeakHeap(t, handler, path)
			if written < 10<<20 {
				t.Fatalf("response was %d bytes, too small to be the whole EPG", written)
			}
			if peak > peakHeapBudget {
				t.Fatalf("peak heap %.1fMB exceeds the %.1fMB budget: the response is being buffered",
					float64(peak)/1e6, float64(peakHeapBudget)/1e6)
			}
		})
	}
}

// peakHeapBudget is far above what streaming costs (~7 MB) and far below what
// buffering costs (240-330 MB), so it fails on a regression to buffering
// without tracking every allocation the encoders make.
const peakHeapBudget = 64 << 20

// newContentEPGHandler holds one fully described program, one program without
// any optional information, and one program whose service is unknown, so the
// streaming responses can be checked for the same key omission, extended items
// and fallbacks the contract tests pin at the conversion level.
func newContentEPGHandler(t *testing.T) http.Handler {
	t.Helper()
	dir := t.TempDir()
	database, err := db.Open(filepath.Join(dir, "mahiron.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	if err := db.Migrate(t.Context(), database); err != nil {
		t.Fatal(err)
	}

	serviceStore := service.NewSQLiteStore(database)
	if err := serviceStore.ReplaceChannelServices(t.Context(), "GR", "27", []*service.Service{
		{Id: "0000100101", ServiceId: 101, NetworkId: 1, TransportStreamId: 10,
			Name: "NHK", Type: 1, RemoteControlKeyId: 1, ChannelType: "GR", ChannelId: "27"},
	}); err != nil {
		t.Fatal(err)
	}
	store := program.NewSQLiteStore(database)
	full := &program.Program{
		ID: program.ProgramID(1, 101, 7), EventID: 7, ServiceID: 101, NetworkID: 1,
		StartAt: 1788609060000, Duration: 1800000, IsFree: true,
		Name: "大河ドラマ", Description: "解説文",
		Genres: []program.Genre{{Lv1: 3, Lv2: 2, Un1: 15, Un2: 15}},
		Video:  &program.Video{StreamContent: 0x5, ComponentType: 0xB3},
		Extended: map[string]string{
			"番組内容": "本文",
			"出演者":  "Foo",
		},
		Series: &program.Series{ID: 5, Pattern: 1, Episode: 1, LastEpisode: 12, Name: "series"},
	}
	minimal := &program.Program{
		ID: program.ProgramID(1, 101, 8), EventID: 8, ServiceID: 101, NetworkID: 1,
		StartAt: 1788609060000, Duration: 1800000,
	}
	orphan := &program.Program{
		ID: program.ProgramID(9, 109, 1), EventID: 1, ServiceID: 109, NetworkID: 9,
		StartAt: 1788609060000, Duration: 1800000, IsFree: true,
	}
	if err := store.UpsertAll(t.Context(), []*program.Program{full, minimal, orphan}); err != nil {
		t.Fatal(err)
	}

	hub := event.New()
	jobs, err := job.NewManager(job.Config{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = jobs.Shutdown(context.Background()) })

	handler, err := NewWeb(WebConfig{
		ServiceManager: service.NewManager(serviceStore, nil, hub),
		ProgramManager: program.NewManager(store, hub),
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

// TestStreamingResponsesPreserveProgramDetails checks the content carried by
// the two whole-EPG streaming endpoints. The heap test above only measures
// their size; this pins that genres, video type/resolution, extended items and
// series survive the streaming encodings, and that programs without optional
// information omit those keys and fall back in XMLTV.
func TestStreamingResponsesPreserveProgramDetails(t *testing.T) {
	handler := newContentEPGHandler(t)

	t.Run("programs", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/programs", nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want 200", rec.Code)
		}
		var items []map[string]json.RawMessage
		if err := json.Unmarshal(rec.Body.Bytes(), &items); err != nil {
			t.Fatal(err)
		}
		if len(items) != 3 {
			t.Fatalf("programs length = %d, want 3", len(items))
		}
		byID := map[int64]map[string]json.RawMessage{}
		for _, item := range items {
			var id int64
			if err := json.Unmarshal(item["id"], &id); err != nil {
				t.Fatal(err)
			}
			byID[id] = item
		}
		full := byID[program.ProgramID(1, 101, 7)]
		for _, key := range []string{"name", "description", "genres", "video", "extended", "series"} {
			if _, ok := full[key]; !ok {
				t.Errorf("full program key %q absent, want present", key)
			}
		}
		var video struct {
			Type       string `json:"type"`
			Resolution string `json:"resolution"`
		}
		if err := json.Unmarshal(full["video"], &video); err != nil {
			t.Fatal(err)
		}
		if video.Type != "h.264" || video.Resolution != "1080i" {
			t.Errorf("video = %+v, want h.264/1080i", video)
		}
		var extended map[string]string
		if err := json.Unmarshal(full["extended"], &extended); err != nil {
			t.Fatal(err)
		}
		if len(extended) != 2 || extended["出演者"] != "Foo" {
			t.Errorf("extended = %#v, want both items", extended)
		}
		minimal := byID[program.ProgramID(1, 101, 8)]
		for _, key := range []string{"name", "description", "genres", "video", "extended", "series"} {
			if value, ok := minimal[key]; ok {
				t.Errorf("minimal program key %q = %s, want absent", key, value)
			}
		}
	})

	t.Run("xmltv", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/iptv/xmltv", nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want 200", rec.Code)
		}
		body := rec.Body.String()
		for _, want := range []string{
			`<title>大河ドラマ</title>`,
			`<desc>解説文</desc>`,
			`<category>3/2</category>`,
			`<title>NHK</title>`,
			`<title>No Title</title>`,
		} {
			if !strings.Contains(body, want) {
				t.Errorf("xmltv missing %q:\n%s", want, body)
			}
		}
		if got := strings.Count(body, "<category>"); got != 1 {
			t.Errorf("category count = %d, want 1", got)
		}
		if got := strings.Count(body, "<desc>"); got != 1 {
			t.Errorf("desc count = %d, want 1", got)
		}
	})
}

func measurePeakHeap(t *testing.T, handler http.Handler, path string) (written int64, peak uint64) {
	t.Helper()

	runtime.GC()
	var base runtime.MemStats
	runtime.ReadMemStats(&base)

	var sampled atomic.Uint64
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		var m runtime.MemStats
		for {
			select {
			case <-stop:
				return
			default:
			}
			runtime.ReadMemStats(&m)
			for {
				current := sampled.Load()
				if m.HeapAlloc <= current || sampled.CompareAndSwap(current, m.HeapAlloc) {
					break
				}
			}
			time.Sleep(time.Millisecond)
		}
	}()

	start := time.Now()
	req := httptest.NewRequest(http.MethodGet, path, nil)
	rec := &discardResponseWriter{header: http.Header{}}
	handler.ServeHTTP(rec, req)
	elapsed := time.Since(start)

	close(stop)
	<-done

	var after runtime.MemStats
	runtime.ReadMemStats(&after)

	t.Logf("status=%d bytes=%.1fMB elapsed=%s", rec.code, float64(rec.written)/1e6, elapsed)
	t.Logf("heapAlloc baseline=%.1fMB peak=%.1fMB, allocated during request=%.1fMB",
		float64(base.HeapAlloc)/1e6, float64(sampled.Load())/1e6,
		float64(after.TotalAlloc-base.TotalAlloc)/1e6)

	if rec.code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.code)
	}
	return rec.written, sampled.Load()
}

// discardResponseWriter counts bytes without retaining the body, so the
// measurement reflects what the server holds rather than what a test recorder
// accumulates.
type discardResponseWriter struct {
	header  http.Header
	code    int
	written int64
}

func (w *discardResponseWriter) Header() http.Header { return w.header }

func (w *discardResponseWriter) Write(p []byte) (int, error) {
	if w.code == 0 {
		w.code = http.StatusOK
	}
	w.written += int64(len(p))
	return len(p), nil
}

func (w *discardResponseWriter) WriteHeader(code int) {
	if w.code == 0 {
		w.code = code
	}
}
