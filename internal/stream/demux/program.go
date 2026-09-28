package demux

import (
	"context"
	"io"

	"github.com/21S1298001/mahiron/internal/model"
	"github.com/21S1298001/mahiron/internal/stream/fanout"
	"github.com/21S1298001/mahiron/internal/stream/programstream"
	"github.com/21S1298001/mahiron/ts"
)

// SubscribeProgram filters a service stream to the requested event, using EIT
// present/following sections observed on the receiver demuxer.
func (e *Demuxer) SubscribeProgram(ctx context.Context, stream *Demuxer, event model.Event, dst io.Writer) error {
	observe := func(ctx context.Context, present func(uint16), attached chan<- struct{}) error {
		return e.ObserveSectionsPassive(ctx, func(section ts.Section) bool { return ts.IsEITPF(section.TableID()) }, func(section ts.Section) error {
			eit, err := ts.ParseEIT(section)
			if err == nil && eit.TableID == ts.TableIDEITPF0 && eit.SectionNumber == 0 && eit.ServiceID == event.Key.ServiceID && eit.OriginalNetworkID == event.Key.NetworkID && len(eit.Events) > 0 {
				present(eit.Events[0].EventID)
			}
			return nil
		}, attached)
	}
	return programstream.Run(ctx, event, dst, observe, func(ctx context.Context, w io.Writer) error {
		return stream.SubscribeService(ctx, event.Key.ServiceID, w)
	}, func(r io.Reader) fanout.PacketReader {
		return &packetReader{reader: ts.NewPacketReader(r), buf: make([]byte, ts.PacketSize)}
	})
}
