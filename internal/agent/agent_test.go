package agent

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func startTestAgent(t *testing.T) (*Hub, string, string) {
	t.Helper()
	token := "test-token-long-enough"
	hash := fmt.Sprintf("%x", sha256.Sum256([]byte(token)))
	hub := NewHub(map[string]string{"living": hash})
	server := httptest.NewServer(hub)
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
	hub, _, _ := startTestAgent(t)
	path := filepath.Join(t.TempDir(), "input")
	if err := os.WriteFile(path, tsData(), 0600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	d := hub.NewDevice("living", Request{Tuner: "catv", Command: "cat '" + path + "'; sleep 60", Transport: "ts", TimeoutMS: 1000})
	if err := d.Start(ctx, io.Discard); err != nil {
		t.Fatal(err)
	}
	hub.mu.Lock()
	conn := hub.connections["living"]
	hub.mu.Unlock()
	_ = conn.conn.Close()
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
	hub, _, _ := startTestAgent(t)
	path := filepath.Join(t.TempDir(), "input")
	if err := os.WriteFile(path, tsData(), 0600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	device := hub.NewDevice("living", Request{Tuner: "catv", Command: "cat '" + path + "'; sleep 60", Transport: "ts", TimeoutMS: 1000})
	if err := device.Start(ctx, io.Discard); err != nil {
		t.Fatal(err)
	}
	if err := device.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	if err := device.Err(); err != nil {
		t.Fatalf("normal stop marked stream failed: %v", err)
	}
}
