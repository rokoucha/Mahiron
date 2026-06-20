package stream

import (
	"bytes"
	"context"
	"io"
	"sync"
	"testing"
	"time"
)

func TestFlowRegistrySharesSourceWithoutTunerDependency(t *testing.T) {
	var (
		mu     sync.Mutex
		starts int
	)
	source := func(_ context.Context, dst io.Writer) error {
		mu.Lock()
		starts++
		mu.Unlock()
		for range 10 {
			if _, err := dst.Write([]byte("ts")); err != nil {
				return err
			}
			time.Sleep(time.Millisecond)
		}
		return nil
	}
	registry := NewFlowRegistry(source, func(PipelineKey) []Processor { return nil }, nil)

	key := PipelineKey{ChannelType: "GR", ChannelID: "27", Kind: PipelineChannelStream}
	var first bytes.Buffer
	var second bytes.Buffer
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		if err := registry.Attach(context.Background(), key, &first); err != nil {
			t.Errorf("first flow: %v", err)
		}
	}()
	go func() {
		defer wg.Done()
		if err := registry.Attach(context.Background(), key, &second); err != nil {
			t.Errorf("second flow: %v", err)
		}
	}()
	wg.Wait()

	mu.Lock()
	gotStarts := starts
	mu.Unlock()
	if gotStarts != 1 {
		t.Fatalf("source starts = %d, want 1", gotStarts)
	}
	if first.String() == "" || second.String() == "" {
		t.Fatalf("both flow consumers should receive data: first=%q second=%q", first.String(), second.String())
	}
}
