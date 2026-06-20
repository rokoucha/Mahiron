package stream

import (
	"bytes"
	"context"
	"io"
	"testing"
	"time"
)

func TestStreamTaskRunnerCancelsSourceWhenTaskCompletes(t *testing.T) {
	source := &fakeTaskSource{canceled: make(chan struct{})}
	runner := NewStreamTaskRunner(source)

	var out bytes.Buffer
	err := runner.Run(context.Background(), &out, func(_ context.Context, src io.Reader, dst io.Writer) error {
		buf := make([]byte, 2)
		if _, err := io.ReadFull(src, buf); err != nil {
			return err
		}
		_, err := dst.Write([]byte("task:" + string(buf)))
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	if got, want := out.String(), "task:ts"; got != want {
		t.Fatalf("task output = %q, want %q", got, want)
	}
	select {
	case <-source.canceled:
	case <-time.After(time.Second):
		t.Fatal("source subscription was not canceled after task completed")
	}
}

type fakeTaskSource struct {
	canceled chan struct{}
}

func (s *fakeTaskSource) Subscribe(ctx context.Context, dst io.Writer) error {
	if _, err := dst.Write([]byte("ts")); err != nil {
		return err
	}
	<-ctx.Done()
	close(s.canceled)
	return nil
}

func (s *fakeTaskSource) Err() error {
	return nil
}
