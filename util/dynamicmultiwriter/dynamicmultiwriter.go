package dynamicmultiwriter

import (
	"errors"
	"io"
)

type DynamicMultiWriter struct {
	writers map[string]io.Writer
}

func (t *DynamicMultiWriter) Add(name string, w io.Writer) {
	t.writers[name] = w
}

func (t *DynamicMultiWriter) Close(name string) {
	for n, w := range t.writers {
		if n == name {
			if c, ok := w.(io.Closer); ok {
				c.Close()
			}
			return
		}
	}
}

func (t *DynamicMultiWriter) CloseAll() {
	for _, w := range t.writers {
		if c, ok := w.(io.Closer); ok {
			c.Close()
		}
	}
}

func (t *DynamicMultiWriter) Write(p []byte) (n int, err error) {
	for name, w := range t.writers {
		n, err = w.Write(p)
		if errors.Is(err, io.ErrClosedPipe) {
			delete(t.writers, name)
			continue
		}
		if err != nil {
			return
		}
		if n != len(p) {
			err = io.ErrShortWrite
			return
		}
	}

	return len(p), nil
}

func New(writers map[string]io.Writer) *DynamicMultiWriter {
	return &DynamicMultiWriter{
		writers,
	}
}
