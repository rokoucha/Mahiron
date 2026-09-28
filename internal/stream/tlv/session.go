// Package tlv is the ISDB-S3 (MMT/TLV) channel session. It fans the TLV
// stream out through fanout.Engine like the TS session does, and converts
// signaling to the internal model: services (TLV-NIT/MH-SDT), events
// (MH-EIT), logos (MH-CDT). Video and audio pass through untouched.
package tlv

import (
	"context"
	"errors"
	"io"
	"sync"
	"time"

	"github.com/21S1298001/mahiron/internal/isdb"
	"github.com/21S1298001/mahiron/internal/model"
	"github.com/21S1298001/mahiron/internal/stream/fanout"
	"github.com/21S1298001/mahiron/internal/stream/programstream"
	"github.com/21S1298001/mahiron/internal/stream/schedule"
	"github.com/21S1298001/mahiron/internal/stream/source"
	"github.com/21S1298001/mahiron/internal/tuner"
	"github.com/21S1298001/mahiron/internal/util"
	"github.com/21S1298001/mahiron/mmt"
)

// ErrNotTLVStream is returned when the bytes delivered for a TLV channel do
// not start with the TLV sync byte, e.g. a remote that converts to TS.
var ErrNotTLVStream = errors.New("tlv: stream is not TLV")

var (
	ErrSessionStopped = errors.New("tlv session stopped")
	errScanComplete   = errors.New("service scan complete")
)

// EventUpdater persists the present and following events observed on the
// stream.
type EventUpdater interface {
	UpsertEvents(context.Context, []model.Event) error
}

// LogoUpdater persists the logos observed on the stream.
type LogoUpdater interface {
	UpsertLogoImage(context.Context, model.Logo) error
}

// Session serves one TLV channel. The decoded engine reads the raw engine
// through the ACAS descrambler, so one b61Decoder process serves every
// decode=1 subscriber.
type Session struct {
	input        source.ChannelInput
	handle       source.InputHandle
	channel      string
	typ          string
	descrambler  source.Descrambler
	eventUpdater EventUpdater
	logoUpdater  LogoUpdater

	mu            sync.Mutex
	stopped       bool
	raw           *fanout.Engine[signal]
	decoded       *fanout.Engine[signal]
	updateCancel  context.CancelFunc
	updateDone    chan struct{}
	updates       chan signal
	logos         *logoAssembler
	fingerprintMu sync.Mutex
	fingerprints  map[sectionKey]uint32

	codecMu sync.Mutex
	// codecs maps a service's component tags to the codecs of its MPT
	// assets. MH-EIT does not carry the video codec.
	codecs map[uint16]map[uint16]model.VideoCodec
}

// Config builds a TLV session. Either Handle or Broadcast must be set;
// Descrambler overrides the handle's, for tests.
type Config struct {
	Channel      string
	Type         string
	Handle       source.InputHandle
	Broadcast    *source.Broadcast
	Descrambler  source.Descrambler
	EventUpdater EventUpdater
	LogoUpdater  LogoUpdater
	OnStop       func()
}

// updateQueueSize bounds the sections waiting for the p/f and logo updater.
const updateQueueSize = 64

func NewSession(config Config) *Session {
	input := source.ChannelInput(nil)
	descrambler := config.Descrambler
	if config.Handle != nil {
		input = config.Handle.Input()
		if descrambler == nil {
			descrambler = config.Handle.Descrambler()
		}
	} else if config.Broadcast != nil {
		input = localBroadcastInput{config.Broadcast}
	}
	s := &Session{
		input:        input,
		handle:       config.Handle,
		channel:      config.Channel,
		typ:          config.Type,
		descrambler:  descrambler,
		eventUpdater: config.EventUpdater,
		logoUpdater:  config.LogoUpdater,
		updates:      make(chan signal, updateQueueSize),
		logos:        newLogoAssembler(),
		codecs:       map[uint16]map[uint16]model.VideoCodec{},
	}
	s.startUpdatesLocked()
	s.raw = s.newRawEngine(func() {
		s.stopUpdates()
		if config.OnStop != nil {
			config.OnStop()
		}
	})
	s.decoded = s.newDecodedEngine()
	return s
}

type localBroadcastInput struct{ *source.Broadcast }

func (i localBroadcastInput) Subscribe(ctx context.Context, _ source.StreamVariant, dst io.Writer) error {
	return i.SubscribeRaw(ctx, dst)
}

func (s *Session) newRawEngine(onEmpty func()) *fanout.Engine[signal] {
	return fanout.New[signal](newTransport(), func(ctx context.Context, dst io.Writer) error {
		return s.input.Subscribe(ctx, source.StreamRaw, dst)
	}, onEmpty).WithSections(s.observeSignal).WithMetricLabels(s.typ, s.channel)
}

func (s *Session) newDecodedEngine() *fanout.Engine[signal] {
	return fanout.New[signal](newTransport(), s.subscribeDecoded, nil).WithMetricLabels(s.typ, s.channel)
}

// Type reports the public channel type this session was created for.
func (s *Session) Type() string { return s.typ }

// Channel reports the public channel ID this session was created for.
func (s *Session) Channel() string { return s.channel }

// ChannelStream delivers the whole TLV stream, raw or ACAS-descrambled.
func (s *Session) ChannelStream(ctx context.Context, decode bool, dst io.Writer) error {
	engine, err := s.streamEngine(decode)
	if err != nil {
		return err
	}
	return s.input.WithUser(ctx, func(ctx context.Context) error {
		return engine.SubscribeChannel(ctx, dst)
	})
}

// ServiceStream delivers one service. A TLV stream with a single service
// passes through; with several, the other services' assets are dropped.
func (s *Session) ServiceStream(ctx context.Context, serviceID uint16, decode bool, dst io.Writer) error {
	engine, err := s.streamEngine(decode)
	if err != nil {
		return err
	}
	return s.input.WithUser(ctx, func(ctx context.Context) error {
		return engine.SubscribeService(ctx, serviceID, dst)
	})
}

// ProgramStream delivers the service while event is on air, following
// MH-EIT[p/f] read from the same engine, so decode=1 needs no second source.
func (s *Session) ProgramStream(ctx context.Context, event model.Event, decode bool, dst io.Writer) error {
	engine, err := s.streamEngine(decode)
	if err != nil {
		return err
	}
	observe := func(ctx context.Context, present func(uint16), attached chan<- struct{}) error {
		return engine.ObserveSectionsPassive(ctx, func(sig signal) bool {
			return !sig.TLVSI && len(sig.Section) > 0 && mmt.IsMHEITPF(sig.Section.TableID())
		}, func(sig signal) error {
			eit, err := mmt.ParseMHEIT(sig.Section)
			if err == nil && eit.SectionNumber == 0 && eit.ServiceID == event.Key.ServiceID && eit.OriginalNetworkID == event.Key.NetworkID && len(eit.Events) > 0 {
				present(eit.Events[0].EventID)
			}
			return nil
		}, attached)
	}
	return s.input.WithUser(ctx, func(ctx context.Context) error {
		return programstream.Run(ctx, event, dst, observe, func(ctx context.Context, w io.Writer) error {
			return engine.SubscribeService(ctx, event.Key.ServiceID, w)
		}, func(r io.Reader) fanout.PacketReader { return &packetReader{reader: mmt.NewTLVReader(r)} })
	})
}

// ScanServices reads the channel's services from MH-SDT[actual] and their
// remote control keys from TLV-NIT[actual].
func (s *Session) ScanServices(ctx context.Context) ([]model.Service, error) {
	scan := newServiceScan()
	err := s.observe(ctx, func(sig signal) bool {
		if len(sig.Section) == 0 {
			return false
		}
		if sig.TLVSI {
			return sig.Section.TableID() == mmt.TableIDTLVNITActual
		}
		return sig.Section.TableID() == mmt.TableIDMHSDTActual
	}, func(sig signal) error {
		scan.Observe(sig)
		if scan.Complete() {
			return errScanComplete
		}
		return nil
	})
	if errors.Is(err, errScanComplete) {
		return scan.Services(), nil
	}
	return scan.Services(), err
}

// CollectSchedule reports MH-EIT schedule and present/following progress
// until ctx ends, using MH-TOT as the broadcast clock.
func (s *Session) CollectSchedule(ctx context.Context, onSchedule func(model.ScheduleUpdate) error, onPresentFollowing func(model.PresentFollowing) error) error {
	collector := schedule.NewCollector(isdb.ScheduleMMT)
	if onSchedule != nil {
		next := onSchedule
		onSchedule = func(update model.ScheduleUpdate) error {
			events := update.Events
			update.Events = func() []model.Event { return s.fillCodecs(events()) }
			return next(update)
		}
	}
	if onPresentFollowing != nil {
		next := onPresentFollowing
		onPresentFollowing = func(pf model.PresentFollowing) error {
			pf.Present = s.fillEventCodecs(pf.Present)
			pf.Following = s.fillEventCodecs(pf.Following)
			return next(pf)
		}
	}
	var clock time.Time
	return s.observe(ctx, func(sig signal) bool {
		if sig.TLVSI || len(sig.Section) == 0 {
			return false
		}
		id := sig.Section.TableID()
		return mmt.IsMHEITPF(id) || mmt.IsMHEITSchedule(id) || id == mmt.TableIDMHTOT
	}, func(sig signal) error {
		if sig.Section.TableID() == mmt.TableIDMHTOT {
			if tot, err := mmt.ParseMHTOT(sig.Section); err == nil {
				clock = tot.JSTTime
			}
			return nil
		}
		eit, err := mmt.ParseMHEIT(sig.Section)
		if err != nil {
			return nil
		}
		now := clock
		if now.IsZero() {
			now = time.Now()
		}
		return collector.Observe(scheduleSection(eit), now, onSchedule, onPresentFollowing)
	})
}

// ObserveLogos reports the logos joined from MH-CDT, 2K logos completed with
// the common fixed palette.
func (s *Session) ObserveLogos(ctx context.Context, observe func(model.Logo) error) error {
	logos := newLogoAssembler()
	return s.observe(ctx, logos.accepts, func(sig signal) error {
		for _, logo := range logos.Observe(sig) {
			if err := observe(logo); err != nil {
				return err
			}
		}
		return nil
	})
}

// observe passes the raw stream's accepted signaling to fn.
func (s *Session) observe(ctx context.Context, accept func(signal) bool, fn func(signal) error) error {
	engine, err := s.streamEngine(false)
	if err != nil {
		return err
	}
	return s.input.WithUser(ctx, func(ctx context.Context) error {
		return engine.ObserveSections(ctx, accept, fn)
	})
}

// streamEngine returns the engine serving raw or decoded bytes; without a
// b61Decoder, decode=1 falls back to raw. A stopped engine is replaced, for
// a remote input that restarts its stream on demand.
func (s *Session) streamEngine(decode bool) (*fanout.Engine[signal], error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stopped || s.input == nil {
		return nil, ErrSessionStopped
	}
	_, nativeDecode := s.input.(interface{ SupportsDecodedInput() bool })
	if nativeDecode && s.raw.Stopped() {
		s.startUpdatesLocked()
		s.raw = s.newRawEngine(nil)
	}
	if decode && (s.descrambler != nil || nativeDecode) {
		if s.decoded.Stopped() {
			s.decoded = s.newDecodedEngine()
		}
		return s.decoded, nil
	}
	return s.raw, nil
}

func (s *Session) subscribeDecoded(ctx context.Context, dst io.Writer) error {
	if s.descrambler == nil {
		return s.input.Subscribe(ctx, source.StreamDecoded, dst)
	}
	s.mu.Lock()
	raw := s.raw
	s.mu.Unlock()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	r, w := io.Pipe()
	rawDone := make(chan error, 1)
	go func() {
		rawDone <- raw.SubscribeChannel(tuner.WithoutStreamInfoReporter(ctx), w)
		_ = w.Close()
	}()
	err := s.descrambler.Descramble(ctx, r, dst)
	_ = r.Close()
	cancel()
	rawErr := <-rawDone
	if err == nil || util.IsExpectedStreamCloseError(err) || errors.Is(err, context.Canceled) {
		err = nil
	}
	if rawErr == nil || util.IsExpectedStreamCloseError(rawErr) || errors.Is(rawErr, context.Canceled) {
		rawErr = nil
	}
	return errors.Join(err, rawErr)
}

// Alive reports whether the session can still accept new subscribers, so
// the manager evicts one whose raw engine stopped and acquires a fresh one.
func (s *Session) Alive() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, restartable := s.input.(interface{ SupportsDecodedInput() bool }); restartable {
		return !s.stopped
	}
	return !s.stopped && !s.raw.Stopped()
}

func (s *Session) Stop(ctx context.Context) error {
	s.mu.Lock()
	if s.stopped {
		s.mu.Unlock()
		return nil
	}
	s.stopped = true
	handle := s.handle
	raw, decoded := s.raw, s.decoded
	s.mu.Unlock()

	decoded.Stop()
	raw.Stop()
	s.stopUpdates()
	if handle != nil {
		return handle.Release(ctx)
	}
	return nil
}

// fillCodecs sets the video codec of events from the MPT of their service,
// copying video lists that change since they are shared with the collector.
func (s *Session) fillCodecs(events []model.Event) []model.Event {
	s.codecMu.Lock()
	defer s.codecMu.Unlock()
	for i := range events {
		events[i].Videos = s.videosWithCodecsLocked(events[i].Key.ServiceID, events[i].Videos)
	}
	return events
}

func (s *Session) fillEventCodecs(event *model.Event) *model.Event {
	if event == nil {
		return nil
	}
	filled := *event
	s.codecMu.Lock()
	filled.Videos = s.videosWithCodecsLocked(filled.Key.ServiceID, filled.Videos)
	s.codecMu.Unlock()
	return &filled
}

func (s *Session) videosWithCodecsLocked(serviceID uint16, videos []model.VideoComponent) []model.VideoComponent {
	codecs := s.codecs[serviceID]
	var out []model.VideoComponent
	for i, video := range videos {
		codec, ok := codecs[video.Tag]
		if !ok || video.Codec != model.VideoCodecUnknown {
			continue
		}
		if out == nil {
			out = append([]model.VideoComponent(nil), videos...)
		}
		out[i].Codec = codec
	}
	if out == nil {
		return videos
	}
	return out
}

// observeCodecs records the video codecs of an MPT's assets by component
// tag, which the MH-stream identifier descriptor links to MH-EIT.
func (s *Session) observeCodecs(table mmt.Table) {
	mpt, err := mmt.ParseMPT(table)
	if err != nil {
		return
	}
	codecs := map[uint16]model.VideoCodec{}
	for _, asset := range mpt.Assets {
		codec, ok := videoCodecForAssetType(asset.AssetType)
		if !ok {
			continue
		}
		for _, d := range asset.Descriptors {
			if d.Tag != mmt.DescriptorTagMHStreamIdentifier {
				continue
			}
			if tag, err := mmt.ParseMHStreamIdentifierDescriptor(d); err == nil {
				codecs[tag] = codec
			}
		}
	}
	s.codecMu.Lock()
	s.codecs[mpt.ServiceID()] = codecs
	s.codecMu.Unlock()
}
