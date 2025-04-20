package filter

import (
	"io"
)

type Filter interface {
	Pipe() (io.Writer, io.Reader, error)
	Filter() error
}
