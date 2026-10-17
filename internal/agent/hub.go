package agent

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/21S1298001/mahiron/mmt"
	"github.com/21S1298001/mahiron/packet"
	"github.com/21S1298001/mahiron/ts"
	"golang.org/x/net/websocket"
)

var ErrUnavailable = errors.New("agent unavailable")

type connection struct {
	*control
	generation string
	instance   string
}

type tunerKey struct{ agent, tuner string }

type Hub struct {
	mu          sync.Mutex
	credentials map[string]string
	connections map[string]*connection
	sessions    map[string]*Device
	quarantined map[tunerKey]*connection
	closed      bool
	changed     chan struct{}
}

func NewHub(credentials map[string]string) *Hub {
	return &Hub{credentials: credentials, connections: make(map[string]*connection), sessions: make(map[string]*Device), quarantined: make(map[tunerKey]*connection), changed: make(chan struct{})}
}

func (h *Hub) Available(id, tuner string) bool {
	if h == nil {
		return false
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	return !h.closed && h.connections[id] != nil && h.connections[id].control != nil && h.quarantined[tunerKey{id, tuner}] == nil
}

func (h *Hub) Changed() <-chan struct{} {
	if h == nil {
		return nil
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.changed
}
func (h *Hub) notifyLocked() { close(h.changed); h.changed = make(chan struct{}) }

func (h *Hub) authenticate(r *http.Request) string {
	token, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
	if !ok || token == "" {
		return ""
	}
	sum := sha256.Sum256([]byte(token))
	for id, hash := range h.credentials {
		expected, err := hex.DecodeString(hash)
		if err == nil && subtle.ConstantTimeCompare(sum[:], expected) == 1 {
			return id
		}
	}
	return ""
}

func (h *Hub) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	id := h.authenticate(r)
	if id == "" {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	if r.URL.Path == "/agent/v1/control" && r.Method == http.MethodGet {
		h.serveControl(id, w, r)
		return
	}
	if r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/agent/v1/sessions/") && strings.HasSuffix(r.URL.Path, "/stream") {
		session := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/agent/v1/sessions/"), "/stream")
		h.serveStream(id, session, w, r)
		return
	}
	http.NotFound(w, r)
}

func (h *Hub) serveControl(id string, w http.ResponseWriter, r *http.Request) {
	c := &connection{generation: rand.Text()}
	h.mu.Lock()
	if h.closed || h.connections[id] != nil {
		h.mu.Unlock()
		http.Error(w, "agent already connected or shutting down", http.StatusConflict)
		return
	}
	h.connections[id] = c
	h.mu.Unlock()
	defer h.disconnect(id, c)
	websocket.Server{Handshake: func(_ *websocket.Config, _ *http.Request) error { return nil }, Handler: func(ws *websocket.Conn) {
		ws.MaxPayloadBytes = 64 << 10
		ctl := &control{conn: ws}
		var hello message
		if ctl.receive(&hello) != nil || hello.Type != "hello" || hello.Version != Version || hello.Instance == "" {
			return
		}
		if ctl.send(message{Type: "welcome", Version: Version, Generation: c.generation}) != nil {
			return
		}
		h.mu.Lock()
		if h.closed {
			h.mu.Unlock()
			return
		}
		c.control = ctl
		c.instance = hello.Instance
		for key, previous := range h.quarantined {
			if key.agent == id && previous.instance == hello.Instance && previous.generation == hello.RecoveredGeneration {
				delete(h.quarantined, key)
			}
		}
		h.notifyLocked()
		h.mu.Unlock()
		slog.Info("agent connected", "agent", id, "generation", c.generation)

		done := make(chan struct{})
		defer close(done)
		go func() {
			tick := time.NewTicker(heartbeatInterval)
			defer tick.Stop()
			for {
				select {
				case <-done:
					return
				case <-tick.C:
					if ctl.send(message{Type: "heartbeat"}) != nil {
						_ = ws.Close()
						return
					}
				}
			}
		}()
		for {
			var m message
			if ctl.receive(&m) != nil {
				return
			}
			switch m.Type {
			case "heartbeat":
			case "failed", "stopped":
				h.mu.Lock()
				d := h.sessions[m.Session]
				h.mu.Unlock()
				if d != nil && d.conn == c && d.agent == id {
					if m.Type == "failed" {
						d.signalReady(errors.New(m.Error))
					} else {
						d.complete(m.Error)
					}
				}
			default:
				return
			}
		}
	}}.ServeHTTP(w, r)
}

func (h *Hub) disconnect(id string, c *connection) {
	h.mu.Lock()
	if h.connections[id] == c {
		delete(h.connections, id)
	}
	var devices []*Device
	for _, d := range h.sessions {
		if d.conn == c {
			h.quarantined[tunerKey{id, d.request.Tuner}] = c
			devices = append(devices, d)
			delete(h.sessions, d.id)
		}
	}
	h.notifyLocked()
	h.mu.Unlock()
	slog.Info("agent disconnected", "agent", id, "sessions", len(devices))
	for _, d := range devices {
		d.abort(ErrUnavailable)
	}
}

func (h *Hub) Close() {
	h.mu.Lock()
	h.closed = true
	var conns []*control
	for _, c := range h.connections {
		if c.control != nil {
			conns = append(conns, c.control)
		}
	}
	h.mu.Unlock()
	for _, c := range conns {
		_ = c.conn.Close()
	}
}

func (h *Hub) serveStream(id, session string, w http.ResponseWriter, r *http.Request) {
	h.mu.Lock()
	d := h.sessions[session]
	if d == nil || d.agent != id || h.connections[id] != d.conn || r.Header.Get("X-Mahiron-Agent-Generation") != d.conn.generation {
		h.mu.Unlock()
		http.Error(w, "unknown session", http.StatusNotFound)
		return
	}
	d.mu.Lock()
	if d.uploaded || d.finished {
		d.mu.Unlock()
		h.mu.Unlock()
		http.Error(w, "session already used", http.StatusConflict)
		return
	}
	d.uploaded = true
	upload := &uploadBody{body: r.Body, controller: http.NewResponseController(w)}
	d.body = upload
	d.mu.Unlock()
	h.mu.Unlock()
	reader := upload
	input := packet.NewCheckReader(reader, ts.Detector, mmt.Detector)
	if d.request.Transport == "tlv" {
		input = packet.NewCheckReader(reader, mmt.Detector, ts.Detector)
	}
	buf := make([]byte, 32<<10)
	n, err := input.Read(buf)
	if err == nil && n > 0 {
		d.signalReady(nil)
		if _, err = d.dst.Write(buf[:n]); err == nil {
			_, err = io.Copy(d.dst, input)
		}
	}
	if n == 0 && err == nil {
		err = io.ErrNoProgress
	}
	d.finish(err)
	if err != nil {
		d.signalReady(err)
		http.Error(w, "stream failed", http.StatusBadGateway)
	} else {
		w.WriteHeader(http.StatusNoContent)
	}
}

type uploadBody struct {
	body       io.ReadCloser
	controller *http.ResponseController
	mu         sync.Mutex
	closed     bool
}

func (b *uploadBody) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.closed = true
	return b.controller.SetReadDeadline(time.Now())
}
func (b *uploadBody) Read(p []byte) (int, error) {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return 0, context.Canceled
	}
	_ = b.controller.SetReadDeadline(time.Now().Add(streamIdleTimeout))
	b.mu.Unlock()
	n, e := b.body.Read(p)
	b.mu.Lock()
	if !b.closed {
		_ = b.controller.SetReadDeadline(time.Time{})
	}
	b.mu.Unlock()
	return n, e
}

// Device adapts one agent session to the tuner's existing lifecycle.
type Device struct {
	hub        *Hub
	agent      string
	request    Request
	conn       *connection
	id         string
	dst        io.Writer
	mu         sync.Mutex
	body       io.Closer
	uploaded   bool
	finished   bool
	err        error
	ready      chan error
	readyOnce  sync.Once
	done       chan struct{}
	cleaned    chan struct{}
	cleanOnce  sync.Once
	stopOnce   sync.Once
	cleanupErr error
	commandMu  sync.Mutex
}

func (h *Hub) NewDevice(id string, r Request) *Device {
	return &Device{hub: h, agent: id, request: r, ready: make(chan error, 1), done: make(chan struct{}), cleaned: make(chan struct{})}
}
func (d *Device) Done() <-chan struct{} { return d.done }
func (d *Device) Err() error            { d.mu.Lock(); defer d.mu.Unlock(); return d.err }
func (d *Device) signalReady(e error)   { d.readyOnce.Do(func() { d.ready <- e }) }
func (d *Device) finish(e error) {
	d.mu.Lock()
	if !d.finished {
		d.finished = true
		d.err = e
		close(d.done)
	}
	body := d.body
	d.mu.Unlock()
	if body != nil {
		_ = body.Close()
	}
}
func (d *Device) abort(e error) {
	d.signalReady(e)
	d.finish(e)
	d.cleanOnce.Do(func() { close(d.cleaned) })
}
func (d *Device) complete(reason string) {
	var err error
	if reason != "" {
		err = fmt.Errorf("agent cleanup failed: %s", reason)
	}
	d.hub.mu.Lock()
	if err != nil {
		d.hub.quarantined[tunerKey{d.agent, d.request.Tuner}] = d.conn
	} else {
		if d.hub.quarantined[tunerKey{d.agent, d.request.Tuner}] == d.conn {
			delete(d.hub.quarantined, tunerKey{d.agent, d.request.Tuner})
		}
	}
	d.hub.notifyLocked()
	delete(d.hub.sessions, d.id)
	d.hub.mu.Unlock()
	d.signalReady(errors.New("agent stopped before stream became ready"))
	d.finish(err)
	d.mu.Lock()
	d.cleanupErr = err
	d.mu.Unlock()
	d.cleanOnce.Do(func() { close(d.cleaned) })
}
func (d *Device) Start(ctx context.Context, dst io.Writer) error {
	d.commandMu.Lock()
	unlock := true
	defer func() {
		if unlock {
			d.commandMu.Unlock()
		}
	}()
	d.hub.mu.Lock()
	c := d.hub.connections[d.agent]
	if d.hub.closed || c == nil || c.control == nil || d.hub.quarantined[tunerKey{d.agent, d.request.Tuner}] != nil {
		d.hub.mu.Unlock()
		d.abort(ErrUnavailable)
		return ErrUnavailable
	}
	d.mu.Lock()
	if d.finished {
		d.mu.Unlock()
		d.hub.mu.Unlock()
		return ErrUnavailable
	}
	d.conn = c
	d.id = rand.Text()
	d.dst = dst
	d.hub.sessions[d.id] = d
	d.mu.Unlock()
	d.hub.mu.Unlock()
	stopCancel := context.AfterFunc(ctx, func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = d.Stop(stopCtx)
	})
	go func() { <-d.done; stopCancel() }()
	if err := c.send(message{Type: "start", Session: d.id, Request: &d.request}); err != nil {
		_ = c.conn.Close()
		d.abort(err)
		return err
	}
	d.commandMu.Unlock()
	unlock = false
	timer := time.NewTimer(d.request.startTimeout())
	defer timer.Stop()
	var startErr error
	select {
	case err := <-d.ready:
		if err == nil {
			return nil
		}
		startErr = err
	case <-ctx.Done():
		startErr = ctx.Err()
	case <-timer.C:
		startErr = errors.New("agent startup timed out")
	}
	d.finish(startErr)
	stopCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return errors.Join(startErr, d.Stop(stopCtx))
}
func (d *Device) Stop(ctx context.Context) error {
	d.commandMu.Lock()
	d.mu.Lock()
	c := d.conn
	id := d.id
	d.mu.Unlock()
	if c == nil {
		d.commandMu.Unlock()
		d.abort(context.Canceled)
		return nil
	}
	d.stopOnce.Do(func() {
		d.signalReady(context.Canceled)
		d.finish(nil)
		if err := c.send(message{Type: "stop", Session: id}); err != nil {
			_ = c.conn.Close()
		}
	})
	d.commandMu.Unlock()
	select {
	case <-d.cleaned:
		d.mu.Lock()
		err := d.cleanupErr
		d.mu.Unlock()
		return err
	case <-ctx.Done():
		d.hub.mu.Lock()
		if d.hub.sessions[d.id] == d {
			d.hub.quarantined[tunerKey{d.agent, d.request.Tuner}] = d.conn
		}
		d.hub.mu.Unlock()
		return ctx.Err()
	}
}
