package tlv

import (
	"bytes"
	"context"
	"image/png"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/21S1298001/mahiron/internal/model"
	"github.com/21S1298001/mahiron/internal/stream/source"
	"github.com/21S1298001/mahiron/mmt"
)

// recordingEnv lists TLV recordings (.mmts), separated by the OS path list
// separator, as in the mmt package. Recordings cannot be committed, so the
// test is skipped without it.
const recordingEnv = "MAHIRON_TEST_MMTS"

// fileSource replays a recording as fast as it can be read.
type fileSource struct {
	path string
	done chan struct{}
	err  error
}

func (s *fileSource) Start(ctx context.Context, dst io.Writer) error {
	f, err := os.Open(s.path)
	if err != nil {
		return err
	}
	s.done = make(chan struct{})
	go func() {
		defer close(s.done)
		defer func() { _ = f.Close() }()
		_, s.err = io.Copy(dst, f)
	}()
	return nil
}

func (s *fileSource) Stop(context.Context) error { return nil }

func (s *fileSource) Done() <-chan struct{} { return s.done }

func (s *fileSource) Err() error { return nil }

func (s *fileSource) WithUser(ctx context.Context, run func(context.Context) error) error {
	return run(ctx)
}

func recordingSession(path string) *Session {
	return testSession(source.NewBroadcast(&fileSource{path: path}, nil), nil)
}

func TestSessionOnRecordings(t *testing.T) {
	paths := filepath.SplitList(os.Getenv(recordingEnv))
	if len(paths) == 0 {
		t.Skipf("%s is not set", recordingEnv)
	}
	for _, path := range paths {
		t.Run(filepath.Base(path), func(t *testing.T) {
			ctx := context.Background()

			// The recordings lost no packet, though one skips an
			// application asset's packet_sequence_number now and then.
			if f, err := os.Open(path); err == nil {
				reader := mmt.NewTLVReader(f)
				var monitor sequenceMonitor
				drops := 0
				for {
					p, err := reader.Next()
					if err != nil {
						break
					}
					if monitor.Observe(p) != nil {
						drops++
					}
				}
				_ = f.Close()
				if drops != 0 {
					t.Errorf("drops = %d, want none", drops)
				}
			}

			services, err := recordingSession(path).ScanServices(ctx)
			if err != nil {
				t.Fatalf("ScanServices: %v", err)
			}
			if len(services) != 1 {
				t.Fatalf("services = %+v, want the stream's one service", services)
			}
			service := services[0]
			t.Logf("service %+v remote key %v logo %+v", service.Key, derefKey(service.RemoteControlKey), service.Logo)
			if service.Key.NetworkID != 0x000B || service.Name == "" || service.Type != 0x01 || !service.EITSchedule {
				t.Fatalf("service = %+v, want a named advanced BS TV service with EIT schedule", service)
			}
			if service.RemoteControlKey == nil {
				t.Error("service has no remote control key from TLV-NIT")
			}
			if service.Logo == nil || service.Logo.Version == nil || service.Logo.DownloadDataID == nil {
				t.Errorf("logo = %+v, want a resolved CDT reference", service.Logo)
			}

			var mu sync.Mutex
			updates := map[model.ServiceKey]model.ScheduleUpdate{}
			var pf []model.PresentFollowing
			err = recordingSession(path).CollectSchedule(ctx, func(update model.ScheduleUpdate) error {
				mu.Lock()
				defer mu.Unlock()
				updates[update.Service] = update
				return nil
			}, func(p model.PresentFollowing) error {
				mu.Lock()
				defer mu.Unlock()
				pf = append(pf, p)
				return nil
			})
			if err != nil {
				t.Fatalf("CollectSchedule: %v", err)
			}
			update, ok := updates[service.Key]
			if !ok || !update.BasicObserved {
				t.Fatalf("schedule of %+v not observed: %v", service.Key, updates)
			}
			events := update.Events()
			t.Logf("%d scheduled events, complete basic=%v extended=%v: %s", len(events), update.BasicComplete, update.ExtendedComplete, update.Diagnosis())
			if len(events) == 0 {
				t.Fatal("no scheduled events")
			}
			checkEvent(t, events[0])
			extended := 0
			for _, event := range events {
				if len(event.Extended) > 0 {
					extended++
				}
			}
			if extended == 0 {
				t.Error("no event has extended items")
			}
			if len(pf) == 0 || pf[len(pf)-1].Present == nil {
				t.Fatal("no present event from MH-EIT[p/f]")
			}
			checkEvent(t, *pf[len(pf)-1].Present)

			var logos []model.Logo
			if err := recordingSession(path).ObserveLogos(ctx, func(logo model.Logo) error {
				logos = append(logos, logo)
				return nil
			}); err != nil {
				t.Fatalf("ObserveLogos: %v", err)
			}
			if len(logos) == 0 {
				t.Fatal("no logo joined from MH-CDT")
			}
			for _, logo := range logos {
				t.Logf("logo network %#04x id %d version %d download %d type %d: %d bytes", logo.NetworkID, logo.LogoID, logo.Version, logo.DownloadDataID, logo.LogoType, len(logo.Data))
				if _, err := png.Decode(bytes.NewReader(logo.Data)); err != nil {
					t.Fatalf("logo type %d: %v", logo.LogoType, err)
				}
				if logo.NetworkID != service.Key.NetworkID || logo.LogoID != service.Logo.LogoID || logo.Version != *service.Logo.Version || logo.DownloadDataID != *service.Logo.DownloadDataID {
					t.Errorf("logo %+v does not match the service's reference %+v", logo, service.Logo)
				}
			}

			// Replaying faster than real time overflows a slow subscriber
			// like a live broadcast would, so only check that the service
			// is found and delivered; extraction is covered by unit tests.
			var serviceBytes countingWriter
			if err := recordingSession(path).ServiceStream(ctx, service.Key.ServiceID, false, &serviceBytes); err != nil {
				t.Fatalf("ServiceStream: %v", err)
			}
			if serviceBytes.n == 0 {
				t.Error("service stream is empty")
			}

			// The present event of MH-EIT[p/f] opens the program stream.
			var programBytes countingWriter
			if err := recordingSession(path).ProgramStream(ctx, *pf[len(pf)-1].Present, false, &programBytes); err != nil {
				t.Fatalf("ProgramStream: %v", err)
			}
			if programBytes.n == 0 {
				t.Error("program stream is empty")
			}
		})
	}
}

func checkEvent(t *testing.T, event model.Event) {
	t.Helper()
	t.Logf("event %d %q start %v duration %v videos %+v audios %d genres %v", event.EventID, event.Name, deref(event.StartAt), deref(event.DurationMS), event.Videos, len(event.Audios), event.Genres)
	if event.Name == "" || event.StartAt == nil || event.DurationMS == nil {
		t.Errorf("event = %+v, want a name, a start time and a duration", event)
	}
	if len(event.Videos) == 0 || event.Videos[0].Codec != model.VideoCodecH265 || event.Videos[0].Resolution == model.VideoResolutionUnknown {
		t.Errorf("videos = %+v, want HEVC with a resolution", event.Videos)
	}
	if len(event.Audios) == 0 || event.Audios[0].Codec != model.AudioCodecAAC || event.Audios[0].SamplingHz == 0 {
		t.Errorf("audios = %+v, want AAC with a sampling rate", event.Audios)
	}
}

func deref[T any](v *T) any {
	if v == nil {
		return nil
	}
	return *v
}

func derefKey(v *uint8) any { return deref(v) }

type countingWriter struct{ n int }

func (w *countingWriter) Write(p []byte) (int, error) {
	w.n += len(p)
	return len(p), nil
}
