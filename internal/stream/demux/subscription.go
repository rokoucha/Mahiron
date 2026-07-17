package demux

import (
	"context"
	"io"
	"slices"
	"strconv"

	"github.com/21S1298001/mahiron/internal/observability"
	"github.com/21S1298001/mahiron/internal/tuner"
	"github.com/21S1298001/mahiron/ts"
)

type packetSubscription struct {
	ctx        context.Context
	continuity *continuityMonitor
	done       chan struct{}
	err        error
	finished   bool
	queue      chan ts.Packet
	service    *ts.ServiceDemux
	serviceID  *uint16
	stats      tuner.StreamInfo
	statsKey   string
	writerDone chan struct{}
}

type sectionSubscription struct {
	accept     func(ts.Section) bool
	done       chan struct{}
	err        error
	finished   bool
	observe    func(ts.Section) error
	queue      chan ts.Section
	writerDone chan struct{}
}

const packetWriteBatchBytes = 64 * ts.PacketSize

func (e *Demuxer) writePackets(id uint64, sub *packetSubscription, dst io.Writer) {
	defer close(sub.writerDone)
	buf := make([]byte, 0, packetWriteBatchBytes)
	for packet := range sub.queue {
		buf = append(buf[:0], packet...)
		sub.stats.Packet++
		if drop := sub.continuity.observe(packet); drop != nil {
			sub.stats.Drop++
			logStreamDrop(e.channelType, e.channelID, sub.statsKey, *drop)
		}
	drain:
		for len(buf)+ts.PacketSize <= packetWriteBatchBytes {
			select {
			case p, ok := <-sub.queue:
				if !ok {
					break drain
				}
				buf = append(buf, p...)
				sub.stats.Packet++
				if drop := sub.continuity.observe(p); drop != nil {
					sub.stats.Drop++
					logStreamDrop(e.channelType, e.channelID, sub.statsKey, *drop)
				}
			default:
				break drain
			}
		}
		n, err := dst.Write(buf)
		if err == nil && n != len(buf) {
			err = io.ErrShortWrite
		}
		if err != nil {
			observability.RecordStreamSubscriberError(context.Background(), e.channelType, "write")
			e.finishPacket(id, err)
			return
		}
		tuner.ReportStreamInfo(sub.ctx, sub.statsKey, sub.stats)
	}
}

func (e *Demuxer) streamInfoKey(serviceID *uint16) string {
	key := e.channelType
	if e.channelID != "" {
		if key != "" {
			key += "/"
		}
		key += e.channelID
	}
	if serviceID != nil {
		key += ":" + strconv.Itoa(int(*serviceID))
	}
	if key == "" {
		key = "stream"
	}
	return key
}

func (e *Demuxer) writeSections(id uint64, sub *sectionSubscription) {
	defer close(sub.writerDone)
	for section := range sub.queue {
		if err := sub.observe(section); err != nil {
			observability.RecordStreamSubscriberError(context.Background(), e.channelType, "observe")
			e.finishSection(id, err)
			return
		}
	}
}

func (e *Demuxer) finishPacket(id uint64, err error) {
	e.mu.Lock()
	e.finishPacketLocked(id, err)
	e.mu.Unlock()
}

func (e *Demuxer) finishPacketLocked(id uint64, err error) {
	sub := e.packets[id]
	if sub == nil || sub.finished {
		return
	}
	sub.finished = true
	sub.err = err
	delete(e.packets, id)
	e.packetSubs = slices.DeleteFunc(e.packetSubs, func(entry packetSubscriptionEntry) bool {
		return entry.id == id
	})
	close(sub.queue)
	close(sub.done)
	e.cancelIfEmptyLocked()
}

func (e *Demuxer) finishSection(id uint64, err error) {
	e.mu.Lock()
	e.finishSectionLocked(id, err)
	e.mu.Unlock()
}

func (e *Demuxer) finishSectionLocked(id uint64, err error) {
	sub := e.sections[id]
	if sub == nil || sub.finished {
		return
	}
	sub.finished = true
	sub.err = err
	delete(e.sections, id)
	e.sectionSubs = slices.DeleteFunc(e.sectionSubs, func(entry sectionSubscriptionEntry) bool {
		return entry.id == id
	})
	close(sub.queue)
	close(sub.done)
	e.cancelIfEmptyLocked()
}

func (e *Demuxer) cancelIfEmptyLocked() {
	if len(e.packets) != 0 || len(e.sections) != 0 {
		return
	}
	e.stopped = true
	if e.cancel != nil {
		e.cancel()
	} else {
		go e.close(nil)
	}
}

func (e *Demuxer) close(err error) {
	e.stopOnce.Do(func() {
		e.mu.Lock()
		e.err = err
		e.stopped = true
		for len(e.packetSubs) > 0 {
			e.finishPacketLocked(e.packetSubs[0].id, err)
		}
		for len(e.sectionSubs) > 0 {
			e.finishSectionLocked(e.sectionSubs[0].id, err)
		}
		onEmpty := e.onEmpty
		e.mu.Unlock()
		close(e.done)
		if onEmpty != nil {
			onEmpty()
		}
	})
}
