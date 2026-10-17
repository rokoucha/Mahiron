package agent

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func startTestAgent(t *testing.T) (*Hub, string, string) {
	t.Helper()
	return startTestAgentWithReconnectGate(t, nil)
}

func startTestAgentWithReconnectGate(t *testing.T, reconnect <-chan struct{}) (*Hub, string, string) {
	t.Helper()
	token := "test-token-long-enough"
	hash := fmt.Sprintf("%x", sha256.Sum256([]byte(token)))
	hub := NewHub(map[string]string{"living": hash})
	var connections atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/agent/v1/control" && connections.Add(1) > 1 && reconnect != nil {
			select {
			case <-reconnect:
			case <-r.Context().Done():
				return
			}
		}
		hub.ServeHTTP(w, r)
	}))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- Run(ctx, server.URL, token) }()
	t.Cleanup(func() {
		cancel()
		hub.Close()
		server.Close()
		select {
		case err := <-done:
			if err != nil {
				t.Error(err)
			}
		case <-time.After(8 * time.Second):
			t.Error("agent did not stop")
		}
	})
	until := time.NewTimer(5 * time.Second)
	defer until.Stop()
	for {
		changed := hub.Changed()
		if hub.Available("living", "catv") {
			break
		}
		select {
		case <-changed:
		case err := <-done:
			t.Fatalf("agent failed: %v", err)
		case <-until.C:
			t.Fatal("agent did not connect")
		}
	}
	return hub, server.URL, token
}

type capture struct {
	mu       sync.Mutex
	buf      bytes.Buffer
	received chan struct{}
	once     sync.Once
}

func (c *capture) Write(p []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	n, e := c.buf.Write(p)
	c.once.Do(func() { close(c.received) })
	return n, e
}
func (c *capture) Bytes() []byte { c.mu.Lock(); defer c.mu.Unlock(); return bytes.Clone(c.buf.Bytes()) }

func tsData() []byte {
	p := make([]byte, 188)
	p[0] = 0x47
	p[2] = 0x20
	p[3] = 0x10
	return bytes.Repeat(p, 128)
}
func tsmfData() ([]byte, []byte) {
	h := make([]byte, 188)
	h[0] = 0x47
	h[2] = 0x2f
	h[3] = 0x10
	h[4] = 0xfa
	h[5] = 0x86
	h[6] = 1
	for i := 73; i < 99; i++ {
		h[i] = 0x11
	}
	crc := uint32(0xffffffff)
	for _, b := range h[4:184] {
		crc ^= uint32(b) << 24
		for range 8 {
			if crc&0x80000000 != 0 {
				crc = crc<<1 ^ 0x04c11db7
			} else {
				crc <<= 1
			}
		}
	}
	binary.BigEndian.PutUint32(h[184:], crc)
	packets := tsData()[:188*52]
	frame := append(bytes.Clone(h), packets...)
	return bytes.Repeat(frame, 4), bytes.Repeat(packets, 4)
}

func TestAgentReceivesTSMFAndTLV(t *testing.T) {
	mux, wantTSMF := tsmfData()
	for _, tc := range []struct {
		name, transport string
		input, want     []byte
		rel             uint8
	}{
		{"ts", "ts", tsData(), tsData(), 0},
		{"tsmf", "ts", mux, wantTSMF, 1},
		{"tlv", "tlv", bytes.Repeat([]byte{0x7f, 0xff, 0, 0}, 512), bytes.Repeat([]byte{0x7f, 0xff, 0, 0}, 512), 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			hub, _, _ := startTestAgent(t)
			path := filepath.Join(t.TempDir(), "input")
			if err := os.WriteFile(path, tc.input, 0600); err != nil {
				t.Fatal(err)
			}
			d := hub.NewDevice("living", Request{Tuner: "catv", Command: "cat '" + path + "'", Transport: tc.transport, RelativeTS: tc.rel, TimeoutMS: 1000})
			out := &capture{received: make(chan struct{})}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := d.Start(ctx, out); err != nil {
				t.Fatal(err)
			}
			select {
			case <-out.received:
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			// The full initial fixture must arrive before we stop the process.
			until := time.NewTimer(time.Second)
			defer until.Stop()
			for len(out.Bytes()) < len(tc.want) {
				select {
				case <-until.C:
					t.Fatalf("short data: %d", len(out.Bytes()))
				default:
					time.Sleep(time.Millisecond)
				}
			}
			if got := out.Bytes(); !bytes.Equal(got, tc.want) {
				t.Fatalf("received %d bytes, want %d", len(got), len(tc.want))
			}
			if err := d.Stop(ctx); err != nil {
				t.Fatal(err)
			}
			if d.Err() != nil {
				t.Fatalf("normal stop reported a stream failure: %v", d.Err())
			}
			if !hub.Available("living", "catv") {
				t.Fatal("cleanly stopped tuner unavailable")
			}
		})
	}
}

func TestAgentNoRelativeTSOutputFailsAndReleases(t *testing.T) {
	hub, _, _ := startTestAgent(t)
	data, _ := tsmfData()
	path := filepath.Join(t.TempDir(), "input")
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	d := hub.NewDevice("living", Request{Tuner: "catv", Command: "cat '" + path + "'; sleep 60", Transport: "ts", RelativeTS: 3, TimeoutMS: 80})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := d.Start(ctx, io.Discard); err == nil {
		t.Fatal("missing relative TS succeeded")
	}
	if !hub.Available("living", "catv") {
		t.Fatal("failed startup was not cleaned")
	}
}

func TestAgentStartCancellation(t *testing.T) {
	hub, _, _ := startTestAgent(t)
	d := hub.NewDevice("living", Request{Tuner: "catv", Command: "sleep 60", Transport: "ts", TimeoutMS: 10000})
	ctx, cancel := context.WithCancel(context.Background())
	finished := make(chan error, 1)
	go func() { finished <- d.Start(ctx, io.Discard) }()
	cancel()
	select {
	case err := <-finished:
		if err == nil {
			t.Fatal("canceled start succeeded")
		}
	case <-time.After(6 * time.Second):
		t.Fatal("start cancellation hung")
	}
}

func TestAgentRejectsAuthenticationAndStaleSession(t *testing.T) {
	hub, url, token := startTestAgent(t)
	for _, tc := range []struct {
		token, id string
		status    int
	}{
		{"wrong", "missing", 401}, {token, "missing", 404},
	} {
		req, _ := http.NewRequest(http.MethodPost, url+"/agent/v1/sessions/"+tc.id+"/stream", strings.NewReader("bad"))
		req.Header.Set("Authorization", "Bearer "+tc.token)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != tc.status {
			t.Fatalf("HTTP %d, want %d", resp.StatusCode, tc.status)
		}
	}
	d := hub.NewDevice("living", Request{Tuner: "catv", Command: "sleep 60", Transport: "ts"})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = d.Start(ctx, io.Discard) }()
	// Lock protects the session identity while Start registers it.
	var id, generation string
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		d.mu.Lock()
		id = d.id
		if d.conn != nil {
			generation = d.conn.generation
		}
		d.mu.Unlock()
		if id != "" {
			break
		}
		time.Sleep(time.Millisecond)
	}
	req, _ := http.NewRequest(http.MethodPost, url+"/agent/v1/sessions/"+id+"/stream", strings.NewReader("bad"))
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("X-Mahiron-Agent-Generation", generation+"stale")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 404 {
		t.Fatalf("stale generation accepted: %d", resp.StatusCode)
	}
	cancel()
}

func TestAgentDisconnectQuarantinesActiveTuner(t *testing.T) {
	for _, decoder := range []string{"", "cat"} {
		t.Run("decoder="+decoder, func(t *testing.T) {
			reconnect, resume := context.WithCancel(t.Context())
			defer resume()
			hub, _, _ := startTestAgentWithReconnectGate(t, reconnect.Done())
			path := filepath.Join(t.TempDir(), "input")
			if err := os.WriteFile(path, tsData(), 0600); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			d := hub.NewDevice("living", Request{Tuner: "catv", Decoder: decoder, Command: "cat '" + path + "'; sleep 60", Transport: "ts", TimeoutMS: 1000})
			if err := d.Start(ctx, io.Discard); err != nil {
				t.Fatal(err)
			}
			hub.mu.Lock()
			conn := hub.connections["living"]
			hub.mu.Unlock()
			_ = conn.conn.Close()
			// Upload completion can precede control disconnect handling.
			for {
				changed := hub.Changed()
				hub.mu.Lock()
				disconnected := hub.connections["living"] != conn
				hub.mu.Unlock()
				if disconnected {
					break
				}
				select {
				case <-changed:
				case <-ctx.Done():
					t.Fatal("control disconnect was not handled")
				}
			}
			select {
			case <-d.Done():
			case <-ctx.Done():
				t.Fatal("disconnect did not stop input")
			}
			if hub.Available("living", "catv") {
				t.Fatal("disconnected tuner available")
			}
			hub.mu.Lock()
			q := hub.quarantined[tunerKey{"living", "catv"}] != nil
			hub.mu.Unlock()
			if !q {
				t.Fatal("unconfirmed cleanup was not quarantined")
			}
			resume()
			for {
				changed := hub.Changed()
				if hub.Available("living", "catv") {
					break
				}
				select {
				case <-changed:
				case <-ctx.Done():
					t.Fatal("clean reconnect did not recover tuner")
				}
			}
			hub.mu.Lock()
			current := hub.connections["living"]
			hub.mu.Unlock()
			if current == conn || current.instance != conn.instance {
				t.Fatal("recovery did not use same process instance")
			}
		})
	}
}

func TestStopDeadlineAfterAcknowledgementDoesNotQuarantine(t *testing.T) {
	for range 100 {
		hub := NewHub(nil)
		d := hub.NewDevice("living", Request{Tuner: "catv"})
		d.conn = &connection{}
		d.id = "session"
		d.stopOnce.Do(func() {})
		hub.sessions[d.id] = d
		d.complete("")
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_ = d.Stop(ctx)
		if hub.quarantined[tunerKey{"living", "catv"}] != nil {
			t.Fatal("acknowledged cleanup was quarantined")
		}
	}
}

func TestAgentStoppingLiveStreamIsNotFailure(t *testing.T) {
	for _, decoder := range []string{"", "cat"} {
		t.Run("decoder="+decoder, func(t *testing.T) {
			hub, _, _ := startTestAgent(t)
			path := filepath.Join(t.TempDir(), "input")
			if err := os.WriteFile(path, tsData(), 0600); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			device := hub.NewDevice("living", Request{Tuner: "catv", Decoder: decoder, Command: "cat '" + path + "'; sleep 60", Transport: "ts", TimeoutMS: 1000})
			if err := device.Start(ctx, io.Discard); err != nil {
				t.Fatal(err)
			}
			if err := device.Stop(ctx); err != nil {
				t.Fatal(err)
			}
			if err := device.Err(); err != nil {
				t.Fatalf("normal stop marked stream failed: %v", err)
			}
		})
	}
}

func TestAgentDecoderReceivesSeparatedTS(t *testing.T) {
	mux, want := tsmfData()
	dir := t.TempDir()
	input, capture := filepath.Join(dir, "mux"), filepath.Join(dir, "decoder-input")
	if err := os.WriteFile(input, mux, 0600); err != nil {
		t.Fatal(err)
	}
	r, first, err := openReceiver(context.Background(), Request{Command: "cat '" + input + "'", Decoder: "tee '" + capture + "'", Transport: "ts", RelativeTS: 1, TimeoutMS: 1000})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := r.stop(); err != nil {
			t.Error(err)
		}
	})
	rest, err := io.ReadAll(r.reader)
	if err != nil {
		t.Fatal(err)
	}
	got := append(first, rest...)
	if !bytes.Equal(got, want) {
		t.Fatalf("output %d bytes, want %d", len(got), len(want))
	}
	fed, err := os.ReadFile(capture)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(fed, want) {
		t.Fatal("decoder received unseparated input")
	}
}

func TestAgentDecoderStartupFailureAndCancellation(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "input")
	if err := os.WriteFile(path, tsData(), 0600); err != nil {
		t.Fatal(err)
	}
	for _, command := range []string{"exit 1", "sleep 60"} {
		t.Run(command, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()
			start := time.Now()
			r, _, err := openReceiver(ctx, Request{Command: "cat '" + path + "'; sleep 60", Decoder: command, Transport: "ts", TimeoutMS: 1000})
			if r != nil {
				_ = r.stop()
				t.Fatal("failed decoder started")
			}
			if err == nil || errors.Is(err, errCleanup) {
				t.Fatalf("unexpected error: %v", err)
			}
			if time.Since(start) > 3*time.Second {
				t.Fatal("decoder cleanup stalled")
			}
		})
	}
}

func TestAgentDecoderFailureAfterOutput(t *testing.T) {
	path := filepath.Join(t.TempDir(), "input")
	if err := os.WriteFile(path, tsData(), 0600); err != nil {
		t.Fatal(err)
	}
	r, first, err := openReceiver(context.Background(), Request{Command: "cat '" + path + "'", Decoder: "cat; exit 1", Transport: "ts", TimeoutMS: 1000})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := r.stop(); err != nil {
			t.Error(err)
		}
	})
	rest, err := io.ReadAll(r.reader)
	if err == nil {
		t.Fatal("decoder failure treated as clean EOF")
	}
	if !bytes.Equal(append(first, rest...), tsData()) {
		t.Fatal("decoder output truncated")
	}
}

type failingDecoderSource struct {
	data *bytes.Reader
	err  error
}

func (s failingDecoderSource) Read(p []byte) (int, error) {
	n, err := s.data.Read(p)
	if err == io.EOF {
		err = s.err
	}
	return n, err
}
func (s failingDecoderSource) Close() error { return nil }

func TestAgentDecoderPreservesSourceError(t *testing.T) {
	sourceErr := errors.New("receiver read failed")
	src := failingDecoderSource{bytes.NewReader(tsData()), sourceErr}
	r := &receiver{source: src}
	reader, err := r.descramble(src, "cat")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := r.stop(); err != nil {
			t.Error(err)
		}
	})
	got, err := io.ReadAll(reader)
	if !errors.Is(err, sourceErr) {
		t.Fatalf("source failure lost: %v", err)
	}
	if !bytes.Equal(got, tsData()) {
		t.Fatal("output truncated")
	}
}

func TestAgentDecoderStopsLiveProcesses(t *testing.T) {
	path := filepath.Join(t.TempDir(), "input")
	if err := os.WriteFile(path, tsData(), 0600); err != nil {
		t.Fatal(err)
	}
	r, first, err := openReceiver(context.Background(), Request{Command: "cat '" + path + "'; exec sleep 60", Decoder: "exec cat", Transport: "ts", TimeoutMS: 1000})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := r.stop(); err != nil {
			t.Error(err)
		}
	})
	if len(first) == 0 {
		t.Fatal("no stream output")
	}
	start := time.Now()
	if err := r.stop(); err != nil {
		t.Fatal(err)
	}
	if time.Since(start) > 3*time.Second {
		t.Fatal("stop exceeded deadline")
	}
	for _, wait := range []func() error{r.process.Wait, r.decoder.Wait} {
		done := make(chan error, 1)
		go func() { done <- wait() }()
		select {
		case err := <-done:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(time.Second):
			t.Fatal("process was not reaped")
		}
	}
}

func TestAgentDecoderReportsExitStatus(t *testing.T) {
	_, _, err := openReceiver(context.Background(), Request{Command: "sleep 60", Decoder: "exit 42", Transport: "ts", TimeoutMS: 1000})
	if err == nil || !strings.Contains(err.Error(), "exit status 42") {
		t.Fatalf("exit status lost: %v", err)
	}
}

func TestAgentDecoderPrimesTSMFInput(t *testing.T) {
	if os.Getenv("MAHIRON_TEST_FRAMING_DECODER") == "source" {
		data, err := os.ReadFile(os.Args[len(os.Args)-1])
		if err != nil {
			os.Exit(5)
		}
		for len(data) > 0 {
			if _, err := os.Stdout.Write(data[:188]); err != nil {
				os.Exit(6)
			}
			data = data[188:]
			time.Sleep(time.Millisecond)
		}
		os.Exit(0)
	}
	if os.Getenv("MAHIRON_TEST_FRAMING_DECODER") == "1" {
		buf := make([]byte, 32<<10)
		n, _ := os.Stdin.Read(buf)
		if n < 5*188 {
			os.Exit(2)
		}
		if _, err := os.Stdout.Write(buf[:n]); err != nil {
			os.Exit(3)
		}
		if _, err := io.Copy(os.Stdout, os.Stdin); err != nil {
			os.Exit(4)
		}
		os.Exit(0)
	}
	mux, want := tsmfData()
	path := filepath.Join(t.TempDir(), "mux")
	if err := os.WriteFile(path, mux, 0600); err != nil {
		t.Fatal(err)
	}
	decoder := fmt.Sprintf("MAHIRON_TEST_FRAMING_DECODER=1 '%s' -test.run '^TestAgentDecoderPrimesTSMFInput$'", os.Args[0])
	source := fmt.Sprintf("MAHIRON_TEST_FRAMING_DECODER=source '%s' -test.run '^TestAgentDecoderPrimesTSMFInput$' -- '%s'", os.Args[0], path)
	r, first, err := openReceiver(t.Context(), Request{Command: source, Decoder: decoder, Transport: "ts", RelativeTS: 1, TimeoutMS: 5000})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := r.stop(); err != nil {
			t.Error(err)
		}
	})
	rest, err := io.ReadAll(r.reader)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(append(first, rest...), want) {
		t.Fatal("decoder output differs from separated TS")
	}
}
