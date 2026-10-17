package agent

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/21S1298001/mahiron/internal/util"
	"github.com/21S1298001/mahiron/mmt"
	"github.com/21S1298001/mahiron/packet"
	"github.com/21S1298001/mahiron/ts"
)

type receiver struct {
	process *util.Process
	source  io.ReadCloser
	reader  io.Reader
	once    sync.Once
	stopErr error
}

func (r *receiver) stop() error {
	r.once.Do(func() {
		if r.source != nil {
			_ = r.source.Close()
		}
		done := make(chan struct{})
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			if err := r.process.Stop(ctx); err != nil {
				// A process may exit between the signal and wait; only reaping proves exit.
				_ = r.process.Wait()
			}
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			r.stopErr = errCleanup
		}

	})
	return r.stopErr
}

func openReceiver(ctx context.Context, req Request) (*receiver, []byte, error) {
	p := util.NewProcess(req.Command)
	p.Stderr(os.Stderr)
	r := &receiver{process: p}
	var err error
	if req.DVBDevicePath == "" {
		r.source, err = p.StdoutPipe()
		if err != nil {
			return nil, nil, err
		}
	}
	if err = p.Start(); err != nil {
		if r.source != nil {
			_ = r.source.Close()
		}
		return nil, nil, errors.New("receiver spawn failed")
	}
	if req.DVBDevicePath != "" {
		r.source, err = os.Open(req.DVBDevicePath)
		if err != nil {
			cleanup := r.stop()
			return nil, nil, errors.Join(errors.New("DVB open failed"), cleanup)
		}
	}
	var reader io.Reader = r.source
	if req.RelativeTS != 0 {
		reader = ts.NewTSMFReader(reader, req.RelativeTS)
	}
	if req.Transport == "tlv" {
		reader = packet.NewCheckReader(reader, mmt.Detector, ts.Detector)
	} else {
		reader = packet.NewCheckReader(reader, ts.Detector, mmt.Detector)
	}
	r.reader = reader
	type result struct {
		data []byte
		err  error
	}
	out := make(chan result, 1)
	go func() {
		buf := make([]byte, 32<<10)
		n, e := reader.Read(buf)
		if n == 0 && e == nil {
			e = io.ErrNoProgress
		}
		out <- result{buf[:n], e}
	}()
	timer := time.NewTimer(req.timeout())
	defer timer.Stop()
	select {
	case got := <-out:
		if len(got.data) > 0 {
			return r, got.data, nil
		}
		err = fmt.Errorf("no valid %s output", req.Transport)
	case <-ctx.Done():
		err = ctx.Err()
	case <-timer.C:
		err = errors.New("receiver startup timed out")
	}
	return nil, nil, errors.Join(err, r.stop())
}
