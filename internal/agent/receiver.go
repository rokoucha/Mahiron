package agent

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"sync"
	"time"

	"github.com/21S1298001/mahiron/internal/util"
	"github.com/21S1298001/mahiron/mmt"
	"github.com/21S1298001/mahiron/packet"
	"github.com/21S1298001/mahiron/ts"
)

type receiver struct {
	process         *util.Process
	decoder         *util.Process
	decoderInput    io.WriteCloser
	decoderOutput   io.ReadCloser
	decoderCopyDone chan struct{}
	decoderCopyErr  error
	source          io.ReadCloser
	reader          io.Reader
	once            sync.Once
	stopErr         error
}

func (r *receiver) stop() error {
	r.once.Do(func() {
		if r.source != nil {
			_ = r.source.Close()
		}
		if r.decoderInput != nil {
			_ = r.decoderInput.Close()
		}
		if r.decoderOutput != nil {
			_ = r.decoderOutput.Close()
		}
		done := make(chan struct{})
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			var wg sync.WaitGroup
			for _, process := range []*util.Process{r.process, r.decoder} {
				if process == nil {
					continue
				}
				wg.Add(1)
				go func() {
					defer wg.Done()
					if err := process.Stop(ctx); err != nil {
						// Only reaping proves exit.
						_ = process.Wait()
					}
				}()
			}
			wg.Wait()
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
	if req.Decoder != "" {
		reader, err = r.descramble(reader, req.Decoder)
		if err != nil {
			return nil, nil, errors.Join(err, r.stop())
		}
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
		err = errors.Join(fmt.Errorf("no valid %s output", req.Transport), got.err)
	case <-ctx.Done():
		err = ctx.Err()
	case <-timer.C:
		err = errors.New("receiver startup timed out")
	}
	return nil, nil, errors.Join(err, r.stop())
}

func (r *receiver) descramble(src io.Reader, command string) (io.Reader, error) {
	d := util.NewProcess(command)
	d.Stderr(os.Stderr)
	var err error
	r.decoderInput, err = d.StdinPipe()
	if err != nil {
		return nil, errors.New("decoder stdin failed")
	}
	r.decoderOutput, err = d.StdoutPipe()
	if err != nil {
		return nil, errors.New("decoder stdout failed")
	}
	if err := d.Start(); err != nil {
		return nil, errors.New("decoder spawn failed")
	}
	r.decoder = d
	r.decoderCopyDone = make(chan struct{})
	go func() {
		// TSMF separation can yield single packets; decoders need an initial framing batch.
		input := bufio.NewReaderSize(src, ts.PacketSize*128)
		_, _ = input.Peek(ts.PacketSize * 128)
		_, err := io.Copy(r.decoderInput, input)
		_ = r.decoderInput.Close()
		r.decoderCopyErr = err
		close(r.decoderCopyDone)
	}()
	return decoderReader{receiver: r}, nil
}

type decoderReader struct{ receiver *receiver }

func (d decoderReader) Read(p []byte) (int, error) {
	r := d.receiver
	n, err := r.decoderOutput.Read(p)
	if err == io.EOF {
		_ = r.source.Close()
		waitErr := r.decoder.Wait()
		<-r.decoderCopyDone
		copyErr := r.decoderCopyErr
		if waitErr != nil {
			var exitErr *exec.ExitError
			if errors.As(waitErr, &exitErr) {
				waitErr = fmt.Errorf("decoder failed (exit status %d)", exitErr.ExitCode())
			} else {
				waitErr = errors.New("decoder failed")
			}
		}
		if err := errors.Join(waitErr, copyErr); err != nil {
			return n, err
		}
	}
	return n, err
}
