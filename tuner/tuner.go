package tuner

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"time"

	"github.com/21S1298001/Mahiron5/util"
	"github.com/21S1298001/Mahiron5/util/dynamicmultiwriter"
)

type Tuner struct {
	name      string
	process   *util.Process
	streaming bool
	writer    *dynamicmultiwriter.DynamicMultiWriter
}

func NewTuner(name string) *Tuner {
	return &Tuner{
		name:   name,
		writer: dynamicmultiwriter.New([]io.Writer{}),
	}
}

func (t *Tuner) StartStream(ctx context.Context, name string, writer io.Writer) {
	slog.Info("tuner attach stream", "name", t.name, "stream", name)

	t.writer.Attach(writer)
	defer t.writer.Detach(writer)

	if !t.streaming {
		slog.Info("request to start stream", "name", t.name, "stream", name)
		go func() {
			if err := t.spawn(); err != nil && !errors.Is(err, context.Canceled) {
				slog.Error("failed to spawn stream", "name", t.name, "stream", name, "err", err)
			}
		}()
	}

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			slog.Info("tuner detach stream", "name", t.name, "stream", name)
			return
		case <-ticker.C:
			if t.streaming {
				break
			}
			slog.Info("tuner stream closed", "name", t.name, "stream", name)
			return
		}
	}
}

func (t *Tuner) Shutdown(ctx context.Context) {
	t.writer.Close()
}

func (t *Tuner) spawn() error {
	t.streaming = true

	if t.process != nil && t.process.Pid() > 0 {
		slog.Warn("tuner process already running", "name", t.name, "pid", t.process.Pid())
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := t.process.Stop(ctx); err != nil {
			slog.Error("failed to stop process", "name", t.name, "err", err)
		}
	}

	process, err := util.NewProcess([]string{"curl", "-s", "http://v6.haruka.dns.ggrel.net:40772/api/services/3273601024/stream"})
	if err != nil {
		return err
	}

	t.process = process

	err = process.Start()
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	defer func() {
		t.writer.Close()
		if err := t.process.Stop(ctx); err != nil {
			slog.Error("failed to stop process", "name", t.name, "err", err)
		}
		slog.Info("tuner process stopped", "name", t.name, "pid", t.process.Pid())
		t.process = nil
	}()

	slog.Info("tuner stream started", "name", t.name)
	_, err = io.Copy(t.writer, t.process.Stdout())
	if err == nil {
		slog.Info("tuner stream ended", "name", t.name)
		t.streaming = false
		return nil
	}

	if errors.Is(err, io.ErrClosedPipe) {
		slog.Info("tuner stream closed", "name", t.name)
		t.streaming = false
		return nil
	}

	slog.Error("tuner stream closed unexpectedly", "name", t.name, "err", err)
	t.streaming = false
	return err
}
