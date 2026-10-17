package agent

import (
	"bytes"
	"context"
	cryptorand "crypto/rand"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"golang.org/x/net/websocket"
)

// Run connects outward and retries only after all sessions of the old connection stop.
func Run(ctx context.Context, server, token string) error {
	u, err := url.Parse(server)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return errors.New("MAHIRON_URL must be an http(s) URL without credentials or query")
	}
	if token == "" {
		return errors.New("agent token is required")
	}
	state := &clientState{instance: cryptorand.Text()}
	backoff := time.Second
	for ctx.Err() == nil {
		err = connect(ctx, u, token, state)
		if errors.Is(err, errCleanup) {
			return err
		}
		if ctx.Err() != nil {
			break
		}
		slog.Warn("agent connection ended; reconnecting", "err", err)
		timer := time.NewTimer(backoff + time.Duration(rand.Int64N(int64(backoff/2))))
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil
		case <-timer.C:
		}
		backoff = min(backoff*2, 30*time.Second)
	}
	return nil
}

var errCleanup = errors.New("receiver cleanup failed; refusing to reconnect")

type worker struct {
	cancel context.CancelFunc
	tuner  string
}

type clientState struct {
	instance            string
	recoveredGeneration string
}

func connect(ctx context.Context, base *url.URL, token string, state *clientState) (resultErr error) {
	u := *base
	u.Path = strings.TrimRight(u.Path, "/") + "/agent/v1/control"
	u.Scheme = "ws"
	if base.Scheme == "https" {
		u.Scheme = "wss"
	}
	cfg, err := websocket.NewConfig(u.String(), base.String())
	if err != nil {
		return err
	}
	cfg.Header.Set("Authorization", "Bearer "+token)
	cfg.Dialer = &net.Dialer{Timeout: 10 * time.Second}
	dialCtx, cancelDial := context.WithTimeout(ctx, 10*time.Second)
	conn, err := cfg.DialContext(dialCtx)
	cancelDial()
	if err != nil {
		return errors.New("control connection failed")
	}
	defer func() { _ = conn.Close() }()
	conn.MaxPayloadBytes = 64 << 10
	ctl := &control{conn: conn}
	if err = ctl.send(message{Type: "hello", Version: Version, Instance: state.instance, RecoveredGeneration: state.recoveredGeneration}); err != nil {
		return err
	}
	var welcome message
	if err = ctl.receive(&welcome); err != nil {
		return err
	}
	if welcome.Type != "welcome" || welcome.Version != Version || welcome.Generation == "" {
		return errors.New("incompatible agent protocol")
	}
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	stopClose := context.AfterFunc(runCtx, func() { _ = conn.Close() })
	defer stopClose()
	var mu sync.Mutex
	workers := map[string]*worker{}
	var wg sync.WaitGroup
	var cleanupFailed bool
	defer func() {
		cancel()
		wg.Wait()
		mu.Lock()
		defer mu.Unlock()
		if cleanupFailed {
			resultErr = errCleanup
		} else {
			state.recoveredGeneration = welcome.Generation
		}
	}()
	go func() {
		tick := time.NewTicker(heartbeatInterval)
		defer tick.Stop()
		for {
			select {
			case <-runCtx.Done():
				return
			case <-tick.C:
				if ctl.send(message{Type: "heartbeat"}) != nil {
					cancel()
					return
				}
			}
		}
	}()
	for {
		var m message
		if err = ctl.receive(&m); err != nil {
			break
		}
		switch m.Type {
		case "heartbeat":
		case "start":
			if m.Session == "" || m.Request == nil || !validRequest(*m.Request) {
				return errors.New("invalid start request")
			}
			mu.Lock()
			busy := workers[m.Session] != nil
			for _, w := range workers {
				if w.tuner == m.Request.Tuner {
					busy = true
				}
			}
			if busy {
				mu.Unlock()
				_ = ctl.send(message{Type: "failed", Session: m.Session, Error: "tuner busy"})
				continue
			}
			workerCtx, workerCancel := context.WithCancel(runCtx)
			w := &worker{cancel: workerCancel, tuner: m.Request.Tuner}
			workers[m.Session] = w
			mu.Unlock()
			wg.Add(1)
			go func(m message, w *worker) {
				defer wg.Done()
				defer w.cancel()
				e := receiveAndUpload(workerCtx, base, token, welcome.Generation, m.Session, *m.Request)
				mu.Lock()
				if errors.Is(e, errCleanup) {
					cleanupFailed = true
				}
				delete(workers, m.Session)
				mu.Unlock()
				reason := ""
				if errors.Is(e, errCleanup) {
					reason = e.Error()
				}
				if e != nil && workerCtx.Err() == nil {
					_ = ctl.send(message{Type: "failed", Session: m.Session, Error: e.Error()})
				}
				_ = ctl.send(message{Type: "stopped", Session: m.Session, Error: reason})
			}(m, w)
		case "stop":
			mu.Lock()
			w := workers[m.Session]
			mu.Unlock()
			if w != nil {
				w.cancel()
			} else {
				_ = ctl.send(message{Type: "stopped", Session: m.Session})
			}
		default:
			return errors.New("unexpected control message")
		}
	}
	cancel()
	wg.Wait()
	mu.Lock()
	failed := cleanupFailed
	mu.Unlock()
	if failed {
		return errCleanup
	}
	return err
}

func validRequest(r Request) bool {
	return r.Tuner != "" && r.Command != "" && (r.Transport == "ts" || r.Transport == "tlv") && r.RelativeTS <= 15 && (r.Transport != "tlv" || r.RelativeTS == 0) && r.RetryMax >= 0 && r.RetryMax <= 100 && r.TimeoutMS >= 0 && r.TimeoutMS <= 3600000 && r.RetryDelayMS >= 0 && r.RetryDelayMS <= 3600000
}

func receiveAndUpload(ctx context.Context, base *url.URL, token, generation, id string, r Request) (resultErr error) {
	var receiver *receiver
	var first []byte
	var err error
	for attempt := 0; attempt <= r.RetryMax; attempt++ {
		receiver, first, err = openReceiver(ctx, r)
		if err == nil {
			break
		}
		if errors.Is(err, errCleanup) || ctx.Err() != nil {
			return err
		}
		if attempt < r.RetryMax {
			timer := time.NewTimer(r.delay())
			select {
			case <-ctx.Done():
				timer.Stop()
				return ctx.Err()
			case <-timer.C:
			}
		}
	}
	if err != nil {
		return fmt.Errorf("receiver startup failed: %w", err)
	}
	defer func() {
		if e := receiver.stop(); e != nil {
			resultErr = errors.Join(resultErr, e)
		}
	}()
	uploadCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	progress := time.AfterFunc(sendTimeout, cancel)
	defer progress.Stop()
	reader := io.MultiReader(bytes.NewReader(first), receiver.reader)
	body := &progressReader{reader: reader, timer: progress}
	u := *base
	u.Path = strings.TrimRight(u.Path, "/") + "/agent/v1/sessions/" + url.PathEscape(id) + "/stream"
	req, err := http.NewRequestWithContext(uploadCtx, http.MethodPost, u.String(), io.NopCloser(body))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("X-Mahiron-Agent-Generation", generation)
	req.Header.Set("Content-Type", "application/octet-stream")
	stopRead := context.AfterFunc(uploadCtx, func() { _ = receiver.source.Close() })
	defer stopRead()
	transport := &http.Transport{DialContext: (&net.Dialer{Timeout: 10 * time.Second}).DialContext, DisableKeepAlives: true}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, CheckRedirect: func(_ *http.Request, _ []*http.Request) error { return http.ErrUseLastResponse }}
	resp, requestErr := client.Do(req)
	if resp != nil {
		responseBody, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusNoContent {
			detail := strings.TrimSpace(strings.ReplaceAll(string(responseBody), token, "[redacted]"))
			requestErr = fmt.Errorf("stream rejected: HTTP %d: %s", resp.StatusCode, detail)
		}
	}
	cleanupErr := receiver.stop()
	if cleanupErr != nil {
		return cleanupErr
	}
	return requestErr
}

type progressReader struct {
	reader io.Reader
	timer  *time.Timer
}

func (r *progressReader) Read(p []byte) (int, error) {
	n, e := r.reader.Read(p)
	if n > 0 {
		r.timer.Reset(sendTimeout)
	}
	return n, e
}
