package stream

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"time"

	"github.com/21S1298001/mahiron/internal/epg"
	"github.com/21S1298001/mahiron/internal/program"
	"github.com/21S1298001/mahiron/internal/tuner"
	"github.com/21S1298001/mahiron/internal/util"
	"github.com/21S1298001/mahiron/ts"
)

type ChannelSession struct {
	broadcast     *Broadcast
	channel       string
	descrambler   Descrambler
	mu            sync.Mutex
	stopped       bool
	typ           string
	rawEngine     *packetEngine
	decodedEngine *packetEngine
	eitUpdater    EITSectionUpdater
	logoUpdater   LogoUpdater
	sectionCancel context.CancelFunc
	sectionQueue  chan ts.Section
}

type ChannelSessionConfig struct {
	Channel     string
	Broadcast   *Broadcast
	Descrambler Descrambler
	EITUpdater  EITSectionUpdater
	LogoUpdater LogoUpdater
	OnStop      func()
	Type        string
}

func NewChannelSession(config ChannelSessionConfig) *ChannelSession {
	session := &ChannelSession{
		broadcast:   config.Broadcast,
		channel:     config.Channel,
		descrambler: config.Descrambler,
		typ:         config.Type,
		eitUpdater:  config.EITUpdater,
		logoUpdater: config.LogoUpdater,
	}
	sectionCtx, sectionCancel := context.WithCancel(context.Background())
	session.sectionCancel = sectionCancel
	session.sectionQueue = make(chan ts.Section, sectionSubscriberBuffer)
	go session.runSectionUpdates(sectionCtx)
	session.rawEngine = newPacketEngine(config.Broadcast.SubscribeRaw, config.OnStop, session.observeSection).withMetricLabels(config.Type, config.Channel)
	session.decodedEngine = newPacketEngine(session.subscribeDecodedMux, nil).withMetricLabels(config.Type, config.Channel)
	return session
}

func (s *ChannelSession) RawStream(ctx context.Context, dst io.Writer) error {
	return s.ChannelStream(ctx, false, dst)
}

func (s *ChannelSession) ChannelStream(ctx context.Context, decode bool, dst io.Writer) error {
	return s.attachEngine(ctx, decode, 0, false, dst)
}

func (s *ChannelSession) ServiceStream(ctx context.Context, serviceID uint16, decode bool, dst io.Writer) error {
	return s.attachEngine(ctx, decode, serviceID, true, dst)
}

func (s *ChannelSession) ProgramStream(ctx context.Context, p *program.Program, decode bool, dst io.Writer) error {
	return s.programStream(ctx, p, decode, dst)
}

func (s *ChannelSession) ScanServices(ctx context.Context) ([]ts.ServiceInfo, error) {
	scan := ts.NewServiceScan()
	err := s.broadcast.WithUser(ctx, func(ctx context.Context) error {
		return s.rawEngine.ObserveSections(ctx, func(section ts.Section) bool {
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

func (s *ChannelSession) CollectEIT(ctx context.Context, observe func(*ts.EIT) error) error {
	return s.broadcast.WithUser(ctx, func(ctx context.Context) error { return s.observeEIT(ctx, observe) })
}

func (s *ChannelSession) ObserveLogos(ctx context.Context, observe func(*ts.LogoImage) error) error {
	return s.broadcast.WithUser(ctx, func(ctx context.Context) error {
		return s.rawEngine.ObserveSections(ctx, func(section ts.Section) bool {
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

func (s *ChannelSession) Stop(ctx context.Context) error {
	s.mu.Lock()
	if s.stopped {
		s.mu.Unlock()
		return nil
	}
	s.stopped = true
	broadcast := s.broadcast
	rawEngine := s.rawEngine
	decodedEngine := s.decodedEngine
	sectionCancel := s.sectionCancel
	s.mu.Unlock()

	if decodedEngine != nil {
		decodedEngine.Stop()
	}
	if rawEngine != nil {
		rawEngine.Stop()
	}
	if sectionCancel != nil {
		sectionCancel()
	}

	var result error
	if broadcast != nil {
		result = errors.Join(result, broadcast.Stop(ctx))
	}
	return result
}

var errScanComplete = errors.New("service scan complete")

func (s *ChannelSession) attachEngine(ctx context.Context, decode bool, serviceID uint16, service bool, dst io.Writer) error {
	s.mu.Lock()
	stopped := s.stopped
	s.mu.Unlock()
	if stopped {
		return errors.New("channel session stopped")
	}
	engine := s.rawEngine
	if decode && s.descrambler != nil {
		engine = s.decodedEngine
	}
	return s.broadcast.WithUser(ctx, func(ctx context.Context) error {
		if service {
			return engine.SubscribeService(ctx, serviceID, dst)
		}
		return engine.SubscribeChannel(ctx, dst)
	})
}

func (s *ChannelSession) subscribeDecodedMux(ctx context.Context, dst io.Writer) error {
	if s.descrambler == nil {
		return s.rawEngine.SubscribeChannel(ctx, dst)
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	r, w := io.Pipe()
	rawDone := make(chan error, 1)
	go func() {
		rawDone <- s.rawEngine.SubscribeChannel(tuner.WithoutStreamInfoReporter(ctx), w)
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

func (s *ChannelSession) programStream(ctx context.Context, p *program.Program, decode bool, dst io.Writer) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	gate := newProgramEventGate(p.NetworkID, p.ServiceID, p.EventID, programEventTimeout(p.StartAt, p.Duration), cancel)
	observerAttached := make(chan struct{})
	observeDone := make(chan error, 1)
	go func() {
		observeDone <- s.rawEngine.observeSectionsPassive(ctx, func(section ts.Section) bool {
			return ts.IsEITPF(section.TableID())
		}, func(section ts.Section) error {
			eit, err := ts.ParseEIT(section)
			if err == nil {
				gate.observe(epg.EITSectionFromTS(eit))
			}
			return nil
		}, observerAttached)
	}()
	select {
	case <-observerAttached:
	case err := <-observeDone:
		return expectedNil(err)
	case <-ctx.Done():
		return expectedNil(ctx.Err())
	}

	r, w := io.Pipe()
	sourceDone := make(chan error, 1)
	go func() {
		sourceDone <- s.attachEngine(ctx, decode, p.ServiceID, true, w)
		_ = w.Close()
	}()
	err := runProgramGate(r, dst, gate)
	_ = r.Close()
	cancel()
	sourceErr := <-sourceDone
	observeErr := <-observeDone
	return errors.Join(expectedNil(err), expectedNil(sourceErr), expectedNil(observeErr))
}

func runProgramGate(src io.Reader, dst io.Writer, gate *programEventGate) error {
	packet := make([]byte, ts.PacketSize)
	var result error
	for {
		_, err := io.ReadFull(src, packet)
		if err != nil {
			result = expectedNil(err)
			break
		}
		if gate.isReady() {
			n, err := dst.Write(packet)
			if err == nil && n != len(packet) {
				err = io.ErrShortWrite
			}
			if err != nil {
				result = err
				break
			}
		}
	}
	return result
}

func (s *ChannelSession) observeEIT(ctx context.Context, observe func(*ts.EIT) error) error {
	return s.rawEngine.ObserveSections(ctx, func(section ts.Section) bool {
		return ts.IsEITS(section.TableID()) || ts.IsEITPF(section.TableID())
	}, func(section ts.Section) error {
		eit, err := ts.ParseEIT(section)
		if err != nil {
			return nil
		}
		return observe(eit)
	})
}

func (s *ChannelSession) observeSection(section ts.Section) {
	select {
	case s.sectionQueue <- section:
	default:
		slog.Warn("TS section updater overflow", "type", s.typ, "channel", s.channel)
	}
}

func (s *ChannelSession) runSectionUpdates(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case section := <-s.sectionQueue:
			s.updateSection(ctx, section)
		}
	}
}

func (s *ChannelSession) updateSection(ctx context.Context, section ts.Section) {
	if ts.IsEITPF(section.TableID()) && s.eitUpdater != nil {
		if eit, err := ts.ParseEIT(section); err == nil {
			if err := s.eitUpdater.UpsertEIT(ctx, eit); err != nil {
				slog.Error("failed to update EITPF", "type", s.typ, "channel", s.channel, "err", err)
			}
		}
	}
	if section.TableID() == ts.TableIDCDT && s.logoUpdater != nil {
		if cdt, err := ts.ParseCDT(section); err == nil {
			if image, err := ts.ParseCDTLogoImage(cdt); err == nil {
				if err := s.logoUpdater.UpsertLogoImage(ctx, image); err != nil {
					slog.Error("failed to update logo", "type", s.typ, "channel", s.channel, "err", err)
				}
			}
		}
	}
}

func programEventTimeout(startAt int64, duration int) time.Duration {
	timeout := time.Until(time.UnixMilli(startAt + int64(duration)))
	if duration == 1 {
		timeout += programEventMissingFallback
	}
	if timeout < 0 {
		return programEventMissingFallback
	}
	return timeout
}
