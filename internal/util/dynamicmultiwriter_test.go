package util

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"
)

type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func waitUntil(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("condition was not satisfied before timeout")
}

func TestDynamicMultiWriter_Attach(t *testing.T) {
	// Create initial buffers
	buf1 := &bytes.Buffer{}
	buf2 := &bytes.Buffer{}

	// Create a DynamicMultiWriter with one writer
	d := NewDynamicMultiWriter(buf1)

	// Check initial count
	if got := d.Count(); got != 1 {
		t.Errorf("Initial Count() = %v, want %v", got, 1)
	}

	// Attach second writer
	d.Attach(buf2)

	// Check that writer count increased
	if got := d.Count(); got != 2 {
		t.Errorf("Count after Attach() = %v, want %v", got, 2)
	}
}

func TestDynamicMultiWriter_Detach(t *testing.T) {
	// Create initial buffers
	buf1 := &bytes.Buffer{}
	buf2 := &bytes.Buffer{}

	// Create a DynamicMultiWriter with two writers
	d := NewDynamicMultiWriter(buf1, buf2)

	// Check initial count
	if got := d.Count(); got != 2 {
		t.Errorf("Initial Count() = %v, want %v", got, 2)
	}

	// Detach second writer
	d.Detach(buf2)

	// Check that writer count decreased
	if got := d.Count(); got != 1 {
		t.Errorf("Count after Detach() = %v, want %v", got, 1)
	}
}

func TestDynamicMultiWriter_Count(t *testing.T) {
	tests := []struct {
		name    string
		writers []io.Writer
		want    int
	}{
		{
			name:    "empty writers",
			writers: []io.Writer{},
			want:    0,
		},
		{
			name:    "single writer",
			writers: []io.Writer{&bytes.Buffer{}},
			want:    1,
		},
		{
			name:    "multiple writers",
			writers: []io.Writer{&bytes.Buffer{}, &bytes.Buffer{}, &bytes.Buffer{}},
			want:    3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := NewDynamicMultiWriter(tt.writers...)
			if got := d.Count(); got != tt.want {
				t.Errorf("Count() = %v, want %v", got, tt.want)
			}
		})
	}
}

// mockClosableWriter is a mock writer that implements io.WriteCloser
type mockClosableWriter struct {
	closed bool
	buf    bytes.Buffer
}

func (m *mockClosableWriter) Write(p []byte) (n int, err error) {
	return m.buf.Write(p)
}

func (m *mockClosableWriter) Close() error {
	m.closed = true
	return nil
}

// mockShortWriter is a mock writer that always does a short write
type mockShortWriter struct{}

func (m *mockShortWriter) Write(p []byte) (n int, err error) {
	if len(p) > 0 {
		return len(p) - 1, nil
	}
	return 0, nil
}

func TestDynamicMultiWriter_Close(t *testing.T) {
	// Create mock writers
	mock1 := &mockClosableWriter{}
	mock2 := &mockClosableWriter{}
	normalWriter := &bytes.Buffer{} // This one doesn't implement Close

	// Create a DynamicMultiWriter with both types of writers
	d := NewDynamicMultiWriter(mock1, mock2, normalWriter)

	// Verify initial count
	if got := d.Count(); got != 3 {
		t.Errorf("Initial Count() = %v, want %v", got, 3)
	}

	// Close all writers
	d.Close()

	// Verify that closable writers were actually closed
	if !mock1.closed {
		t.Error("mock1 was not closed")
	}
	if !mock2.closed {
		t.Error("mock2 was not closed")
	}

	// Verify that writers slice is nil
	if got := d.Count(); got != 0 {
		t.Errorf("Count after Close() = %v, want %v", got, 0)
	}
}

// mockErrorWriter is a mock writer that always returns an error
type mockErrorWriter struct {
	err error
}

func (m *mockErrorWriter) Write(p []byte) (n int, err error) {
	return 0, m.err
}

type discardWriter struct{}

func (d *discardWriter) Write(p []byte) (n int, err error) {
	return len(p), nil
}

func TestDynamicMultiWriter_Write(t *testing.T) {
	t.Run("successful write to multiple writers", func(t *testing.T) {
		buf1 := &syncBuffer{}
		buf2 := &syncBuffer{}
		d := NewDynamicMultiWriter(buf1, buf2)
		defer d.Close()

		data := []byte("test data")
		n, err := d.Write(data)

		if err != nil {
			t.Errorf("Write() error = %v, want nil", err)
		}
		if n != len(data) {
			t.Errorf("Write() n = %v, want %v", n, len(data))
		}
		waitUntil(t, func() bool {
			return buf1.String() == string(data) && buf2.String() == string(data)
		})
		if buf1.String() != string(data) {
			t.Errorf("buf1 content = %v, want %v", buf1.String(), string(data))
		}
		if buf2.String() != string(data) {
			t.Errorf("buf2 content = %v, want %v", buf2.String(), string(data))
		}
	})

	t.Run("write with error", func(t *testing.T) {
		buf := &syncBuffer{}
		errWriter := &mockErrorWriter{err: errors.New("write error")}
		d := NewDynamicMultiWriter(buf, errWriter)
		defer d.Close()

		n, err := d.Write([]byte("test"))
		if err != nil {
			t.Errorf("Write() error = %v, want nil", err)
		}
		if n != len("test") {
			t.Errorf("Write() n = %v, want %v", n, len("test"))
		}
		waitUntil(t, func() bool { return d.Count() == 1 })
	})

	t.Run("write with closed pipe", func(t *testing.T) {
		buf := &syncBuffer{}
		closedWriter := &mockErrorWriter{err: io.ErrClosedPipe}
		d := NewDynamicMultiWriter(buf, closedWriter)
		defer d.Close()

		data := []byte("test")
		n, err := d.Write(data)
		if err != nil {
			t.Errorf("Write() error = %v, want nil", err)
		}
		if n != len(data) {
			t.Errorf("Write() n = %v, want %v", n, len(data))
		}
		waitUntil(t, func() bool { return d.Count() == 1 })
		if d.Count() != 1 {
			t.Errorf("Count after closed pipe = %v, want 1", d.Count())
		}
	})

	t.Run("write with all writers closed", func(t *testing.T) {
		closedWriter1 := &mockErrorWriter{err: io.ErrClosedPipe}
		closedWriter2 := &mockErrorWriter{err: io.ErrClosedPipe}
		d := NewDynamicMultiWriter(closedWriter1, closedWriter2)
		defer d.Close()

		n, err := d.Write([]byte("test"))
		if err != nil {
			t.Errorf("Write() error = %v, want nil", err)
		}
		if n != len("test") {
			t.Errorf("Write() n = %v, want %v", n, len("test"))
		}
		waitUntil(t, func() bool { return d.Count() == 0 })
		if got := d.Count(); got != 0 {
			t.Errorf("Count after all closed pipes = %v, want 0", got)
		}
	})

	t.Run("write with short write", func(t *testing.T) {
		buf := &syncBuffer{}
		shortWriter := &mockShortWriter{}
		d := NewDynamicMultiWriter(buf, shortWriter)
		defer d.Close()

		n, err := d.Write([]byte("test"))
		if err != nil {
			t.Errorf("Write() error = %v, want nil", err)
		}
		if n != len("test") {
			t.Errorf("Write() n = %v, want %v", n, len("test"))
		}
		waitUntil(t, func() bool { return d.Count() == 1 })
	})

	t.Run("write with no writers", func(t *testing.T) {
		d := NewDynamicMultiWriter()

		_, err := d.Write([]byte("test"))
		if !errors.Is(err, io.ErrClosedPipe) {
			t.Errorf("Write() error = %v, want %v", err, io.ErrClosedPipe)
		}
	})
}

type blockingWriter struct {
	release chan struct{}
	started chan struct{}
	once    sync.Once
}

func newBlockingWriter() *blockingWriter {
	return &blockingWriter{
		release: make(chan struct{}),
		started: make(chan struct{}),
	}
}

func (w *blockingWriter) Write(p []byte) (int, error) {
	w.once.Do(func() { close(w.started) })
	<-w.release
	return len(p), nil
}

func TestDynamicMultiWriter_WriteDoesNotWaitForSlowWriter(t *testing.T) {
	fast := &syncBuffer{}
	slow := newBlockingWriter()
	d := NewDynamicMultiWriter(fast, slow)
	defer d.Close()
	defer close(slow.release)

	if _, err := d.Write([]byte("first")); err != nil {
		t.Fatal(err)
	}
	select {
	case <-slow.started:
	case <-time.After(time.Second):
		t.Fatal("slow writer did not start")
	}
	waitUntil(t, func() bool { return fast.String() == "first" })

	done := make(chan error, 1)
	go func() {
		_, err := d.Write([]byte("second"))
		done <- err
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Write blocked on slow writer")
	}
	waitUntil(t, func() bool { return fast.String() == "firstsecond" })
}

type blockingRecorder struct {
	mu      sync.Mutex
	release chan struct{}
	started chan struct{}
	once    sync.Once
	chunks  []string
}

func newBlockingRecorder() *blockingRecorder {
	return &blockingRecorder{
		release: make(chan struct{}),
		started: make(chan struct{}),
	}
}

func (w *blockingRecorder) Write(p []byte) (int, error) {
	block := false
	w.once.Do(func() {
		block = true
		close(w.started)
	})
	if block {
		<-w.release
	}
	w.mu.Lock()
	w.chunks = append(w.chunks, string(p))
	w.mu.Unlock()
	return len(p), nil
}

func (w *blockingRecorder) snapshot() []string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]string(nil), w.chunks...)
}

func TestDynamicMultiWriter_DropsOldChunksForSlowWriter(t *testing.T) {
	slow := newBlockingRecorder()
	d := NewDynamicMultiWriter(slow)
	defer d.Close()

	if _, err := d.Write([]byte("chunk-000")); err != nil {
		t.Fatal(err)
	}
	select {
	case <-slow.started:
	case <-time.After(time.Second):
		t.Fatal("slow writer did not start")
	}

	for i := 1; i < dynamicMultiWriterBufferSize+50; i++ {
		if _, err := fmt.Fprintf(d, "chunk-%03d", i); err != nil {
			t.Fatal(err)
		}
	}

	close(slow.release)
	waitUntil(t, func() bool {
		for _, chunk := range slow.snapshot() {
			if chunk == fmt.Sprintf("chunk-%03d", dynamicMultiWriterBufferSize+49) {
				return true
			}
		}
		return false
	})

	chunks := slow.snapshot()
	if len(chunks) >= dynamicMultiWriterBufferSize+50 {
		t.Fatalf("slow writer received %d chunks, want fewer than %d", len(chunks), dynamicMultiWriterBufferSize+50)
	}
}

func TestDynamicMultiWriter_ConcurrentAccess(t *testing.T) {
	d := NewDynamicMultiWriter(&discardWriter{})
	data := bytes.Repeat([]byte{0x47}, 188)

	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 1000 {
				if _, err := d.Write(data); err != nil && !errors.Is(err, io.ErrClosedPipe) {
					t.Errorf("Write() error = %v, want nil or ErrClosedPipe", err)
				}
			}
		}()
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		for range 1000 {
			w := &discardWriter{}
			d.Attach(w)
			d.Detach(w)
		}
	}()

	wg.Wait()
}

func BenchmarkDynamicMultiWriter_Write(b *testing.B) {
	sizes := []int{188, 1316, 8192}
	writerCounts := []int{1, 2, 4}

	for _, size := range sizes {
		size := size
		for _, writerCount := range writerCounts {
			writerCount := writerCount
			b.Run(fmt.Sprintf("%dB_%dWriters", size, writerCount), func(b *testing.B) {
				writers := make([]io.Writer, writerCount)
				for i := range writers {
					writers[i] = io.Discard
				}
				d := NewDynamicMultiWriter(writers...)
				data := bytes.Repeat([]byte{0x47}, size)

				b.SetBytes(int64(size))
				b.ReportAllocs()
				b.ResetTimer()

				for range b.N {
					if _, err := d.Write(data); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}
