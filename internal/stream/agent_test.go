package stream

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/21S1298001/mahiron/internal/agent"
	"github.com/21S1298001/mahiron/internal/config"
	"github.com/21S1298001/mahiron/internal/stream/source"
	"github.com/21S1298001/mahiron/internal/tuner"
)

type agentTestDecoder struct {
	mu      sync.Mutex
	input   bytes.Buffer
	command string
}

func (d *agentTestDecoder) Descramble(_ context.Context, r io.Reader, w io.Writer) error {
	data, err := io.ReadAll(r)
	d.mu.Lock()
	d.input.Write(data)
	d.mu.Unlock()
	if err != nil {
		return err
	}
	_, err = w.Write(data)
	return err
}

func TestAgentInputUsesLocalTSAndTLVDecoders(t *testing.T) {
	for _, transport := range []string{"ts", "tlv"} {
		t.Run(transport, func(t *testing.T) {
			token := "stream-agent-test"
			hub := agent.NewHub(map[string]string{"living": fmt.Sprintf("%x", sha256.Sum256([]byte(token)))})
			server := httptest.NewServer(hub)
			ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
			exited := make(chan error, 1)
			go func() { exited <- agent.Run(ctx, server.URL, token) }()
			defer func() {
				cancel()
				hub.Close()
				server.Close()
				select {
				case err := <-exited:
					if err != nil {
						t.Error(err)
					}
				case <-time.After(5 * time.Second):
					t.Error("agent stuck")
				}
			}()
			for {
				changed := hub.Changed()
				if hub.Available("living", "remote") {
					break
				}
				select {
				case <-changed:
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
			}
			var data []byte
			if transport == "ts" {
				p := make([]byte, 188)
				p[0] = 0x47
				p[2] = 0x20
				p[3] = 0x10
				data = bytes.Repeat(p, 256)
			} else {
				data = bytes.Repeat([]byte{0x7f, 0xff, 0, 1, 0xaa}, 256)
			}
			path := filepath.Join(t.TempDir(), "input")
			if err := os.WriteFile(path, data, 0600); err != nil {
				t.Fatal(err)
			}
			tuners := tuner.NewManager(&tuner.ManagerConfig{Agents: hub, TunersConfig: config.TunersConfig{{Name: "remote", Types: []string{"TEST"}, Agent: &config.AgentTunerConfig{Name: "living", Command: "cat '" + path + "'"}, Decoder: "b25-local", B61Decoder: "b61-local"}}})
			decoder := new(agentTestDecoder)
			manager := NewManager(ManagerConfig{Channels: config.ChannelsConfig{{Name: "test", Type: "TEST", Channel: "1", Transport: transport}}, TunerManager: tuners, DescramblerFactory: func(command string) source.Descrambler { decoder.command = command; return decoder }})
			defer func() { _ = manager.Shutdown(context.Background()) }()
			session, err := manager.GetOrCreate(ctx, "TEST", "1")
			if err != nil {
				t.Fatal(err)
			}
			var dst bytes.Buffer
			if err = session.ChannelStream(ctx, true, &dst); err != nil {
				t.Fatal(err)
			}
			want := "b25-local"
			if transport == "tlv" {
				want = "b61-local"
			}
			if decoder.command != want {
				t.Fatalf("decoder %q, want %q", decoder.command, want)
			}
			decoder.mu.Lock()
			got := bytes.Clone(decoder.input.Bytes())
			decoder.mu.Unlock()
			if !bytes.Equal(got, data) {
				t.Fatalf("local decoder got %d bytes, want %d", len(got), len(data))
			}
			if !bytes.Equal(dst.Bytes(), data) {
				t.Fatalf("output got %d bytes, want %d", dst.Len(), len(data))
			}
		})
	}
}
