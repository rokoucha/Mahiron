package local

import (
	"context"
	"io"

	"github.com/21S1298001/mahiron/internal/program"
)

func (s *Session) programStream(ctx context.Context, p *program.Program, decode bool, dst io.Writer) error {
	d, err := s.streamDemuxer(decode)
	if err != nil {
		return err
	}
	return s.broadcast.WithUser(ctx, func(ctx context.Context) error { return s.rawDemuxer.SubscribeProgram(ctx, d, p, dst) })
}
