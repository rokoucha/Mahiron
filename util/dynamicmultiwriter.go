package util

import (
	"errors"
	"io"
	"slices"
	"sync"
)

type DynamicMultiWriter struct {
	mutex   sync.RWMutex
	writers []io.Writer
}

func NewDynamicMultiWriter(writers ...io.Writer) *DynamicMultiWriter {
	return &DynamicMultiWriter{
		writers: writers,
	}
}

func (d *DynamicMultiWriter) Attach(writer io.Writer) {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	d.writers = append(d.writers, writer)
}

func (d *DynamicMultiWriter) Detach(writer io.Writer) {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	for i, w := range d.writers {
		if w == writer {
			d.writers = slices.Delete(d.writers, i, i+1)
			break
		}
	}
}

func (d *DynamicMultiWriter) Count() int {
	d.mutex.RLock()
	defer d.mutex.RUnlock()

	return len(d.writers)
}

func (d *DynamicMultiWriter) Close() {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	for _, w := range d.writers {
		if c, ok := w.(io.Closer); ok {
			c.Close()
		}
	}
	d.writers = []io.Writer{}
}

func (d *DynamicMultiWriter) Write(p []byte) (n int, err error) {
	d.mutex.RLock()
	if len(d.writers) == 0 {
		d.mutex.RUnlock()
		return 0, io.ErrClosedPipe
	}

	var closed []io.Writer
	var result error
	for _, w := range d.writers {
		written, err := w.Write(p)
		if errors.Is(err, io.ErrClosedPipe) {
			closed = append(closed, w)
			continue
		}
		if err != nil {
			result = errors.Join(result, err)
			continue
		}
		if written != len(p) {
			result = errors.Join(result, io.ErrShortWrite)
		}
	}
	remaining := len(d.writers) - len(closed)
	d.mutex.RUnlock()

	for _, w := range closed {
		d.Detach(w)
	}

	if result != nil {
		return 0, result
	}
	if remaining == 0 {
		return 0, io.ErrClosedPipe
	}
	return len(p), nil
}
