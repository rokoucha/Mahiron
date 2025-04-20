package tuner

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"time"

	"github.com/21S1298001/Mahiron5/config"
	"github.com/21S1298001/Mahiron5/util"
	"github.com/21S1298001/Mahiron5/util/dynamicmultiwriter"
)

type Tuner struct {
	command   string
	config    *config.TunerConfig
	process   *util.Process
	streaming bool
	writer    *dynamicmultiwriter.DynamicMultiWriter
}

func NewTuner(config *config.TunerConfig) *Tuner {
	return &Tuner{
		config: config,
		writer: dynamicmultiwriter.New([]io.Writer{}),
	}
}

func (t *Tuner) Name() string {
	return t.config.Name
}

func (t *Tuner) Command() string {
	return t.command
}

func (t *Tuner) createCommand() (string, error) {
	return t.config.Command, nil
}

func (t *Tuner) StartStream(ctx context.Context, name string, writer io.Writer) {
	slog.Info("tuner attach stream", "name", t.Name(), "stream", name)

	t.writer.Attach(writer)
	defer t.writer.Detach(writer)

	if !t.streaming {
		slog.Info("request to start stream", "name", t.Name(), "stream", name)
		go func() {
			if err := t.spawn(); err != nil && !errors.Is(err, context.Canceled) {
				slog.Error("failed to spawn stream", "name", t.Name(), "stream", name, "err", err)
			}
		}()
	}

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			slog.Info("tuner detach stream", "name", t.Name(), "stream", name)
			return
		case <-ticker.C:
			if t.streaming {
				break
			}
			slog.Info("tuner stream closed", "name", t.Name(), "stream", name)
			return
		}
	}
}

func (t *Tuner) Shutdown(ctx context.Context) error {
	t.writer.Close()
	return nil
}

func (t *Tuner) spawn() error {
	t.streaming = true

	if t.process != nil && t.process.Pid() > 0 {
		slog.Warn("tuner process already running", "name", t.Name(), "pid", t.process.Pid())
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := t.process.Stop(ctx); err != nil {
			slog.Error("failed to stop process", "name", t.Name(), "err", err)
		}
	}

	command, err := t.createCommand()
	if err != nil {
		return err
	}
	t.command = command

	args, err := util.ParseCommandLine(command)
	if err != nil {
		return err
	}

	t.process, err = util.NewProcess(util.ProcessConfig{
		Args: args,
	})
	if err != nil {
		return err
	}

	or, err := t.process.StdoutPipe()
	if err != nil {
		return err
	}

	err = t.process.Start()
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	defer func() {
		t.writer.Close()
		if err := t.process.Stop(ctx); err != nil {
			slog.Error("failed to stop process", "name", t.Name(), "err", err)
		}
		slog.Info("tuner process stopped", "name", t.Name(), "pid", t.process.Pid())
		t.process = nil
	}()

	slog.Info("tuner stream started", "name", t.Name())
	_, err = io.Copy(t.writer, or)
	if err == nil {
		slog.Info("tuner stream ended", "name", t.Name())
		t.streaming = false
		return nil
	}

	if errors.Is(err, io.ErrClosedPipe) {
		slog.Info("tuner stream closed", "name", t.Name())
		t.streaming = false
		return nil
	}

	slog.Error("tuner stream closed unexpectedly", "name", t.Name(), "err", err)
	t.streaming = false
	return err
}
