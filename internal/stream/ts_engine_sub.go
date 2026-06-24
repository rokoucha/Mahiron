package stream

import (
	"context"
	"io"
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

func (e *packetEngine) writePackets(id uint64, sub *packetSubscription, dst io.Writer) {
	defer close(sub.writerDone)
	for packet := range sub.queue {
		n, err := dst.Write(packet)
		if err == nil && n != len(packet) {
			err = io.ErrShortWrite
		}
		if err != nil {
			observability.RecordStreamSubscriberError(context.Background(), e.channelType, "write")
			e.finishPacket(id, err)
			return
		}
		sub.stats.Packet++
		if sub.continuity.observe(packet) {
			sub.stats.Drop++
		}
		tuner.ReportStreamInfo(sub.ctx, sub.statsKey, sub.stats)
	}
}

func (e *packetEngine) streamInfoKey(serviceID *uint16) string {
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

func (e *packetEngine) writeSections(id uint64, sub *sectionSubscription) {
	defer close(sub.writerDone)
	for section := range sub.queue {
		if err := sub.observe(section); err != nil {
			observability.RecordStreamSubscriberError(context.Background(), e.channelType, "observe")
			e.finishSection(id, err)
			return
		}
	}
}

func (e *packetEngine) finishPacket(id uint64, err error) {
	e.mu.Lock()
	e.finishPacketLocked(id, err)
	e.mu.Unlock()
}

func (e *packetEngine) finishPacketLocked(id uint64, err error) {
	sub := e.packets[id]
	if sub == nil || sub.finished {
		return
	}
	sub.finished = true
	sub.err = err
	delete(e.packets, id)
	close(sub.queue)
	close(sub.done)
	e.cancelIfEmptyLocked()
}

func (e *packetEngine) finishSection(id uint64, err error) {
	e.mu.Lock()
	e.finishSectionLocked(id, err)
	e.mu.Unlock()
}

func (e *packetEngine) finishSectionLocked(id uint64, err error) {
	sub := e.sections[id]
	if sub == nil || sub.finished {
		return
	}
	sub.finished = true
	sub.err = err
	delete(e.sections, id)
	close(sub.queue)
	close(sub.done)
	e.cancelIfEmptyLocked()
}

func (e *packetEngine) cancelIfEmptyLocked() {
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

func (e *packetEngine) close(err error) {
	e.stopOnce.Do(func() {
		e.mu.Lock()
		e.err = err
		e.stopped = true
		for id := range e.packets {
			e.finishPacketLocked(id, err)
		}
		for id := range e.sections {
			e.finishSectionLocked(id, err)
		}
		onEmpty := e.onEmpty
		e.mu.Unlock()
		close(e.done)
		if onEmpty != nil {
			onEmpty()
		}
	})
}
