package stream

import (
	"context"
	"io"
)

type Processor interface {
	Run(context.Context, io.Reader, io.Writer) error
}

type errorProcessor struct {
	err error
}

func (p errorProcessor) Run(context.Context, io.Reader, io.Writer) error {
	return p.err
}

type descramblerProcessor struct {
	descrambler Descrambler
}

func (p descramblerProcessor) Run(ctx context.Context, src io.Reader, dst io.Writer) error {
	return p.descrambler.Descramble(ctx, src, dst)
}

type serviceFilterProcessor struct {
	filter    ServiceFilter
	serviceID uint16
}

func (p serviceFilterProcessor) Run(ctx context.Context, src io.Reader, dst io.Writer) error {
	return p.filter.FilterService(ctx, p.serviceID, src, dst)
}
