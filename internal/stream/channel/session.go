package channel

import (
	"context"
	"errors"
	"io"
	"sync"
	"time"

	"github.com/21S1298001/mahiron/internal/program"
	"github.com/21S1298001/mahiron/internal/stream/databroadcast"
	"github.com/21S1298001/mahiron/internal/stream/demux"
	"github.com/21S1298001/mahiron/internal/stream/source"
	"github.com/21S1298001/mahiron/internal/tuner"
	"github.com/21S1298001/mahiron/internal/util"
	"github.com/21S1298001/mahiron/ts"
)

type Session struct {
	input                      source.ChannelInput
	handle                     source.InputHandle
	channel                    string
	descrambler                source.Descrambler
	mu                         sync.Mutex
	stopped                    bool
	typ                        string
	rawDemuxer                 *demux.Demuxer
	decodedDemuxer             *demux.Demuxer
	eitUpdater                 EITSectionUpdater
	logoUpdater                LogoUpdater
	logoCarousel               *ts.DSMCCLogoCarousel
	dataBroadcast              *databroadcast.DataBroadcastHub
	sectionCancel              context.CancelFunc
	sectionDone                chan struct{}
	sectionQueue               chan ts.Section
	carouselQueue              chan ts.Section
	dataBroadcastQueue         chan ts.PIDSection
	dataBroadcastPriorityQueue chan ts.PIDSection
	dataBroadcastDone          chan struct{}
	dataBroadcastWG            sync.WaitGroup
	sectionUpdateMu            sync.Mutex
	eitPFFingerprints          map[eitPFSectionKey]uint32
}

// ChannelSession is the shared TS streaming, demux, and data-broadcast
// implementation used by both local and remote sessions.
type ChannelSession = Session

type Config struct {
	Channel     string
	Broadcast   *source.Broadcast
	Handle      source.InputHandle
	Descrambler source.Descrambler
	EITUpdater  EITSectionUpdater
	LogoUpdater LogoUpdater
	OnStop      func()
	Type        string
	ModuleCache *databroadcast.ModuleCache
}

func NewSession(config Config) *Session {
	input := source.ChannelInput(nil)
	if config.Handle != nil {
		input = config.Handle.Input()
		config.Descrambler = config.Handle.Descrambler()
	} else if config.Broadcast != nil {
		input = localBroadcastInput{config.Broadcast}
	}
	session := &Session{
		input:         input,
		handle:        config.Handle,
		channel:       config.Channel,
		descrambler:   config.Descrambler,
		typ:           config.Type,
		eitUpdater:    config.EITUpdater,
		logoUpdater:   config.LogoUpdater,
		logoCarousel:  ts.NewDSMCCLogoCarousel(),
		dataBroadcast: databroadcast.NewDataBroadcastHub().WithMetricLabels(config.Type, config.Channel).WithModuleCache(config.ModuleCache),
	}
	session.sectionQueue = make(chan ts.Section, sectionQueueSize)
	session.carouselQueue = make(chan ts.Section, carouselQueueSize)
	session.dataBroadcastQueue = make(chan ts.PIDSection, dataBroadcastQueueSize)
	session.dataBroadcastPriorityQueue = make(chan ts.PIDSection, dataBroadcastPriorityQueueSize)
	session.startUpdateWorkersLocked()
	session.rawDemuxer = demux.New(func(ctx context.Context, dst io.Writer) error { return input.Subscribe(ctx, source.StreamRaw, dst) }, func() {
		session.stopSectionUpdates()
		if config.OnStop != nil {
			config.OnStop()
		}
	}, session.observeSection).WithPIDSections(session.observePIDSection).WithPackets(session.dataBroadcast.ObservePacket).WithMetricLabels(config.Type, config.Channel)
	session.decodedDemuxer = demux.New(session.subscribeDecodedMux, nil).WithMetricLabels(config.Type, config.Channel)
	return session
}

func NewChannelSession(config Config) *ChannelSession { return NewSession(config) }

type localBroadcastInput struct{ *source.Broadcast }

func (i localBroadcastInput) Subscribe(ctx context.Context, _ source.StreamVariant, dst io.Writer) error {
	return i.SubscribeRaw(ctx, dst)
}

// Type reports the public channel type this session was created for.
func (s *Session) Type() string {
	return s.typ
}

// Channel reports the public channel ID this session was created for.
func (s *Session) Channel() string {
	return s.channel
}

func (s *Session) ChannelStream(ctx context.Context, decode bool, dst io.Writer) error {
	return s.attachDemuxer(ctx, decode, 0, false, dst)
}

func (s *Session) ServiceStream(ctx context.Context, serviceID uint16, decode bool, dst io.Writer) error {
	return s.attachDemuxer(ctx, decode, serviceID, true, dst)
}

func (s *Session) ProgramStream(ctx context.Context, p *program.Program, decode bool, dst io.Writer) error {
	return s.programStream(ctx, p, decode, dst)
}

func (s *Session) ScanServices(ctx context.Context) ([]ts.ServiceInfo, error) {
	scan := ts.NewServiceScan()
	err := s.input.WithUser(ctx, func(ctx context.Context) error {
		return s.rawDemuxer.ObserveSections(ctx, func(section ts.Section) bool {
			switch section.TableID() {
			case ts.TableIDPAT, ts.TableIDSDT0, ts.TableIDNIT0:
				return true
			default:
				return false
			}
		}, func(section ts.Section) error {
			scan.Observe(section)
			if scan.Complete() {
				return errScanComplete
			}
			return nil
		})
	})
	if errors.Is(err, errScanComplete) {
		return scan.Services(), nil
	}
	return scan.Services(), err
}

func (s *Session) CollectEIT(ctx context.Context, observe func(*ts.EIT) error) error {
	return s.CollectEITWithClock(ctx, func(eit *ts.EIT, _ time.Time) error {
		return observe(eit)
	})
}

func (s *Session) CollectEITWithClock(ctx context.Context, observe func(*ts.EIT, time.Time) error) error {
	return s.input.WithUser(ctx, func(ctx context.Context) error { return s.observeEIT(ctx, observe) })
}

func (s *Session) ObserveLogos(ctx context.Context, observe func(*ts.LogoImage) error) error {
	return s.input.WithUser(ctx, func(ctx context.Context) error {
		return s.rawDemuxer.ObserveSections(ctx, func(section ts.Section) bool {
			return section.TableID() == ts.TableIDCDT
		}, func(section ts.Section) error {
			cdt, err := ts.ParseCDT(section)
			if err != nil {
				return nil
			}
			image, err := ts.ParseCDTLogoImage(cdt)
			if err != nil {
				return nil
			}
			return observe(image)
		})
	})
}

func (s *Session) ObserveDataBroadcast(ctx context.Context, serviceID uint16, decode bool, observe func(databroadcast.DataBroadcastEvent) error) error {
	if s.dataBroadcast == nil {
		return waitContext(ctx)
	}
	return s.input.WithUser(ctx, func(ctx context.Context) error {
		snapshot, events, unsubscribe := s.dataBroadcast.Subscribe(ctx, serviceID)
		defer unsubscribe()
		if err := observe(databroadcast.DataBroadcastEvent{Type: "snapshot", Snapshot: snapshot}); err != nil {
			return err
		}
		observeCtx, cancel := context.WithCancel(ctx)
		defer cancel()
		done := make(chan error, 1)
		go func() {
			done <- s.rawDemuxer.ObserveSections(observeCtx, acceptDataBroadcastSection, func(ts.Section) error {
				return nil
			})
		}()
		for {
			select {
			case <-ctx.Done():
				cancel()
				<-done
				return nil
			case err := <-done:
				s.dataBroadcastWG.Wait()
				// PID-section observers may have queued the final carousel event
				// immediately before the finite/disconnected input completed. Drain
				// those events so a completed module notification is not lost.
				for {
					select {
					case event, ok := <-events:
						if !ok {
							return err
						}
						if observeErr := observe(event); observeErr != nil {
							return observeErr
						}
					default:
						return err
					}
				}
			case event, ok := <-events:
				if !ok {
					cancel()
					<-done
					return nil
				}
				if err := observe(event); err != nil {
					cancel()
					<-done
					return err
				}
			}
		}
	})
}

func (s *Session) DataBroadcastModule(serviceID uint16, componentTag byte, moduleID uint16) (databroadcast.DataBroadcastModule, bool) {
	if s.dataBroadcast == nil {
		return databroadcast.DataBroadcastModule{}, false
	}
	return s.dataBroadcast.Module(serviceID, componentTag, moduleID)
}

func (s *Session) Stop(ctx context.Context) error {
	s.mu.Lock()
	if s.stopped {
		s.mu.Unlock()
		return nil
	}
	s.stopped = true
	handle := s.handle
	rawDemuxer := s.rawDemuxer
	decodedDemuxer := s.decodedDemuxer
	sectionCancel := s.sectionCancel
	s.mu.Unlock()

	if decodedDemuxer != nil {
		decodedDemuxer.Stop()
	}
	if rawDemuxer != nil {
		rawDemuxer.Stop()
	}
	if sectionCancel != nil {
		s.stopSectionUpdates()
	}

	var result error
	if handle != nil {
		result = errors.Join(result, handle.Release(ctx))
	}
	return result
}

func (s *Session) stopSectionUpdates() {
	s.mu.Lock()
	cancel := s.sectionCancel
	done := s.sectionDone
	dataBroadcastDone := s.dataBroadcastDone
	s.sectionCancel = nil
	s.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	if done != nil {
		<-done
	}
	if dataBroadcastDone != nil {
		<-dataBroadcastDone
	}
}

func (s *Session) startUpdateWorkersLocked() {
	if s.sectionCancel != nil {
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	s.sectionCancel = cancel
	s.sectionDone = make(chan struct{})
	s.dataBroadcastDone = make(chan struct{})
	go s.runSectionUpdates(ctx)
	go s.runDataBroadcastUpdates(ctx)
}

var (
	errScanComplete   = errors.New("service scan complete")
	ErrSessionStopped = errors.New("channel session stopped")
)

// Alive reports whether the session can still accept new subscribers. It is
// used by the stream manager to detect a session that has finished shutting
// down but has not yet been evicted from the session registry.
func (s *Session) Alive() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, restartable := s.input.(interface{ SupportsDecodedInput() bool }); restartable {
		return !s.stopped
	}
	return !s.stopped && !s.rawDemuxer.Stopped()
}

func (s *Session) attachDemuxer(ctx context.Context, decode bool, serviceID uint16, service bool, dst io.Writer) error {
	demuxer, err := s.streamDemuxer(decode)
	if err != nil {
		return err
	}
	return s.input.WithUser(ctx, func(ctx context.Context) error {
		if service {
			return demuxer.SubscribeService(ctx, serviceID, dst)
		}
		return demuxer.SubscribeChannel(ctx, dst)
	})
}

func (s *Session) streamDemuxer(decode bool) (*demux.Demuxer, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stopped {
		return nil, ErrSessionStopped
	}
	demuxer := s.rawDemuxer
	_, nativeDecode := s.input.(interface{ SupportsDecodedInput() bool })
	if nativeDecode && demuxer.Stopped() {
		s.startUpdateWorkersLocked()
		demuxer = demux.New(func(ctx context.Context, dst io.Writer) error {
			return s.input.Subscribe(ctx, source.StreamRaw, dst)
		}, nil, s.observeSection).WithPIDSections(s.observePIDSection).WithPackets(s.dataBroadcast.ObservePacket).WithMetricLabels(s.typ, s.channel)
		s.rawDemuxer = demuxer
	}
	if decode && (s.descrambler != nil || nativeDecode) {
		if s.decodedDemuxer.Stopped() {
			s.decodedDemuxer = demux.New(s.subscribeDecodedMux, nil).WithMetricLabels(s.typ, s.channel)
		}
		demuxer = s.decodedDemuxer
	}
	return demuxer, nil
}

func (s *Session) subscribeDecodedMux(ctx context.Context, dst io.Writer) error {
	if s.descrambler == nil {
		return s.input.Subscribe(ctx, source.StreamDecoded, dst)
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	r, w := io.Pipe()
	rawDone := make(chan error, 1)
	go func() {
		rawDone <- s.rawDemuxer.SubscribeChannel(tuner.WithoutStreamInfoReporter(ctx), w)
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

func acceptDataBroadcastSection(section ts.Section) bool {
	switch section.TableID() {
	case ts.TableIDPMT, ts.TableIDDSMCCDII, ts.TableIDDSMCCDDB, ts.TableIDDSMCCStream, ts.TableIDBIT, ts.TableIDTOT:
		return true
	default:
		return ts.IsEITPF(section.TableID())
	}
}

func waitContext(ctx context.Context) error {
	<-ctx.Done()
	if err := ctx.Err(); err != nil && err != context.Canceled {
		return err
	}
	return nil
}

func (s *Session) observeEIT(ctx context.Context, observe func(*ts.EIT, time.Time) error) error {
	var latestClock time.Time
	return s.rawDemuxer.ObserveSections(ctx, func(section ts.Section) bool {
		return ts.IsEITS(section.TableID()) || ts.IsEITPF(section.TableID()) || section.TableID() == ts.TableIDTOT
	}, func(section ts.Section) error {
		if section.TableID() == ts.TableIDTOT {
			if tot, err := ts.ParseTOT(section); err == nil {
				latestClock = tot.JSTTime
			}
			return nil
		}
		eit, err := ts.ParseEIT(section)
		if err != nil {
			return nil
		}
		return observe(eit, latestClock)
	})
}
