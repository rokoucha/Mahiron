package channel

import (
	"context"
	"io"

	"github.com/21S1298001/mahiron/internal/model"
)

func (s *Session) programStream(ctx context.Context, event model.Event, decode bool, dst io.Writer) error {
	d, err := s.streamDemuxer(decode)
	if err != nil {
		return err
	}
	return s.input.WithUser(ctx, func(ctx context.Context) error { return s.rawDemuxer.SubscribeProgram(ctx, d, event, dst) })
}
