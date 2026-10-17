// Package agent transports tuner input without depending on the application runtime.
package agent

import (
	"sync"
	"time"

	"golang.org/x/net/websocket"
)

const (
	Version           = 1
	heartbeatInterval = 5 * time.Second
	heartbeatTimeout  = 15 * time.Second
	streamIdleTimeout = 15 * time.Second
	sendTimeout       = 30 * time.Second
)

type Request struct {
	Tuner         string `json:"tuner"`
	Command       string `json:"command"`
	DVBDevicePath string `json:"dvbDevicePath,omitempty"`
	Transport     string `json:"transport"`
	RelativeTS    uint8  `json:"relativeTS,omitempty"`
	RetryMax      int    `json:"retryMax,omitempty"`
	TimeoutMS     int    `json:"timeoutMs"`
	RetryDelayMS  int    `json:"retryDelayMs,omitempty"`
}

type message struct {
	Type                string   `json:"type"`
	Version             int      `json:"version,omitempty"`
	Generation          string   `json:"generation,omitempty"`
	Instance            string   `json:"instance,omitempty"`
	RecoveredGeneration string   `json:"recoveredGeneration,omitempty"`
	Session             string   `json:"session,omitempty"`
	Request             *Request `json:"request,omitempty"`
	Error               string   `json:"error,omitempty"`
}

type control struct {
	conn *websocket.Conn
	mu   sync.Mutex
}

func (c *control) send(m message) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	_ = c.conn.SetWriteDeadline(time.Now().Add(heartbeatTimeout))
	return websocket.JSON.Send(c.conn, m)
}

func (c *control) receive(m *message) error {
	_ = c.conn.SetReadDeadline(time.Now().Add(heartbeatTimeout))
	return websocket.JSON.Receive(c.conn, m)
}

func (r Request) timeout() time.Duration {
	if r.TimeoutMS <= 0 {
		return 2 * time.Second
	}
	return time.Duration(r.TimeoutMS) * time.Millisecond
}
func (r Request) delay() time.Duration {
	if r.RetryDelayMS <= 0 {
		return 500 * time.Millisecond
	}
	return time.Duration(r.RetryDelayMS) * time.Millisecond
}
func (r Request) startTimeout() time.Duration {
	return time.Duration(r.RetryMax+1)*r.timeout() + time.Duration(r.RetryMax)*r.delay() + 5*time.Second
}
