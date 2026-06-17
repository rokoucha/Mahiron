package stream

import (
	"bufio"
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"time"

	"github.com/21S1298001/Mahiron5/config"
	"github.com/21S1298001/Mahiron5/util"
)

type ChannelSession struct {
	channel       string
	channelConfig *config.ChannelConfig
	ctx           context.Context
	cancel        context.CancelFunc
	descrambler   Descrambler
	device        TunerDevice
	done          <-chan struct{}
	eitCollector  EITCollector
	eitPFStarted  bool
	eitUpdater    EITSectionUpdater
	filter        ServiceFilter
	hub           *util.DynamicMultiWriter
	mu            sync.Mutex
	onStop        func()
	pipelines     map[PipelineKey]*streamPipeline
	refs          int
	scanner       ServiceScanner
	started       bool
	stopped       bool
	typ           string
}

type ChannelSessionConfig struct {
	Channel       string
	ChannelConfig *config.ChannelConfig
	Descrambler   Descrambler
	Device        TunerDevice
	EITCollector  EITCollector
	EITUpdater    EITSectionUpdater
	Filter        ServiceFilter
	OnStop        func()
	Scanner       ServiceScanner
	Type          string
}

func NewChannelSession(config ChannelSessionConfig) *ChannelSession {
	return &ChannelSession{
		channel:       config.Channel,
		channelConfig: config.ChannelConfig,
		descrambler:   config.Descrambler,
		device:        config.Device,
		eitCollector:  config.EITCollector,
		eitUpdater:    config.EITUpdater,
		filter:        config.Filter,
		hub:           util.NewDynamicMultiWriter(),
		onStop:        config.OnStop,
		pipelines:     map[PipelineKey]*streamPipeline{},
		scanner:       config.Scanner,
		typ:           config.Type,
	}
}

func (s *ChannelSession) RawStream(ctx context.Context, dst io.Writer) error {
	return s.ChannelStream(ctx, false, dst)
}

func (s *ChannelSession) ChannelStream(ctx context.Context, decode bool, dst io.Writer) error {
	key := PipelineKey{
		ChannelType: s.typ,
		ChannelID:   s.channel,
		Kind:        PipelineChannelStream,
		Decode:      decode,
	}
	return s.attachPipeline(ctx, key, dst)
}

func (s *ChannelSession) ServiceStream(ctx context.Context, serviceID uint16, decode bool, dst io.Writer) error {
	key := PipelineKey{
		ChannelType: s.typ,
		ChannelID:   s.channel,
		Kind:        PipelineServiceStream,
		ServiceID:   serviceID,
		Decode:      decode,
	}
	return s.attachPipeline(ctx, key, dst)
}

func (s *ChannelSession) ScanServices(ctx context.Context, dst io.Writer) error {
	r, w := io.Pipe()
	scannerCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	waitCh := make(chan error, 1)
	go func() {
		waitCh <- s.scanner.ScanServices(scannerCtx, r, dst)
	}()

	if err := s.attachRaw(w); err != nil {
		_ = r.Close()
		_ = w.Close()
		cancel()
		<-waitCh
		return err
	}
	defer s.detachRaw(w)

	select {
	case err := <-waitCh:
		_ = w.Close()
		cancel()
		return err
	case <-ctx.Done():
		_ = w.Close()
		cancel()
		return <-waitCh
	case <-s.done:
		_ = w.Close()
		cancel()
		scanErr := <-waitCh
		if err := s.device.Err(); err != nil && !util.IsExpectedStreamCloseError(err) {
			return err
		}
		return scanErr
	}
}

func (s *ChannelSession) CollectEITS(ctx context.Context, dst io.Writer) error {
	if s.eitCollector == nil {
		return errors.New("EIT collector not configured")
	}
	return s.collectEIT(ctx, dst, s.eitCollector.CollectEITS)
}

func (s *ChannelSession) CollectEITPF(ctx context.Context, dst io.Writer) error {
	if s.eitCollector == nil {
		return errors.New("EIT collector not configured")
	}
	return s.collectEIT(ctx, dst, s.eitCollector.CollectEITPF)
}

func (s *ChannelSession) Stop(ctx context.Context) error {
	s.mu.Lock()
	if s.stopped {
		s.mu.Unlock()
		return nil
	}
	s.stopped = true
	cancel := s.cancel
	device := s.device
	pipelines := make([]*streamPipeline, 0, len(s.pipelines))
	for _, pipeline := range s.pipelines {
		pipelines = append(pipelines, pipeline)
	}
	s.pipelines = map[PipelineKey]*streamPipeline{}
	s.hub.Close()
	s.mu.Unlock()

	for _, pipeline := range pipelines {
		pipeline.Stop()
	}
	if cancel != nil {
		cancel()
	}

	var result error
	if device != nil {
		result = errors.Join(result, device.Stop(ctx))
	}

	if s.onStop != nil {
		s.onStop()
	}
	return result
}

func (s *ChannelSession) attachPipeline(ctx context.Context, key PipelineKey, dst io.Writer) error {
	pipeline, err := s.getOrCreatePipeline(key)
	if err != nil {
		return err
	}
	return pipeline.Attach(ctx, dst)
}

func (s *ChannelSession) getOrCreatePipeline(key PipelineKey) (*streamPipeline, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.stopped {
		return nil, errors.New("channel session stopped")
	}
	if pipeline := s.pipelines[key]; pipeline != nil {
		return pipeline, nil
	}

	pipeline := newStreamPipeline(
		key,
		s.pipelineProcessors(key),
		s.subscribeRaw,
		s.attachRaw,
		s.detachRaw,
		s.waitRaw,
		func() {
			s.removePipeline(key)
		},
	)
	s.pipelines[key] = pipeline
	return pipeline, nil
}

func (s *ChannelSession) pipelineProcessors(key PipelineKey) []Processor {
	processors := []Processor{}
	if key.Decode && s.descrambler != nil {
		processors = append(processors, descramblerProcessor{descrambler: s.descrambler})
	}
	if key.Kind == PipelineServiceStream {
		processors = append(processors, serviceFilterProcessor{
			filter:    s.filter,
			serviceID: key.ServiceID,
		})
	}
	return processors
}

func (s *ChannelSession) removePipeline(key PipelineKey) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.pipelines, key)
}

func (s *ChannelSession) subscribeRaw(ctx context.Context, dst io.Writer) error {
	if err := s.attachRaw(dst); err != nil {
		return err
	}
	defer s.detachRaw(dst)

	return s.waitRaw(ctx)
}

func (s *ChannelSession) waitRaw(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return nil
	case <-s.done:
		err := s.device.Err()
		if util.IsExpectedStreamCloseError(err) {
			return nil
		}
		return err
	}
}

func (s *ChannelSession) attachRaw(dst io.Writer) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.stopped {
		return errors.New("channel session stopped")
	}
	s.refs++
	s.hub.Attach(dst)
	if err := s.startLocked(); err != nil {
		s.refs--
		s.hub.Detach(dst)
		return err
	}
	return nil
}

func (s *ChannelSession) detachRaw(dst io.Writer) {
	s.mu.Lock()
	if s.refs > 0 {
		s.refs--
	}
	s.hub.Detach(dst)
	refs := s.refs
	s.mu.Unlock()

	if refs == 0 {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := s.Stop(ctx); err != nil {
			if util.IsExpectedStreamCloseError(err) {
				return
			}
			slog.Error("failed to stop channel session", "type", s.typ, "channel", s.channel, "err", err)
		}
	}
}

func (s *ChannelSession) startLocked() error {
	if s.started {
		return nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	s.ctx = ctx
	s.cancel = cancel
	if err := s.device.Start(ctx, s.hub); err != nil {
		cancel()
		return err
	}
	s.done = s.device.Done()

	s.started = true
	s.startEITPFLocked()
	return nil
}

func (s *ChannelSession) collectEIT(ctx context.Context, dst io.Writer, collect func(context.Context, io.Reader, io.Writer) error) error {
	r, w := io.Pipe()
	collectorCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	waitCh := make(chan error, 1)
	go func() {
		waitCh <- collect(collectorCtx, r, dst)
	}()

	if err := s.attachRaw(w); err != nil {
		_ = r.Close()
		_ = w.Close()
		cancel()
		<-waitCh
		return err
	}
	defer s.detachRaw(w)

	select {
	case err := <-waitCh:
		_ = w.Close()
		cancel()
		return err
	case <-ctx.Done():
		_ = w.Close()
		cancel()
		return <-waitCh
	case <-s.done:
		_ = w.Close()
		cancel()
		collectErr := <-waitCh
		if err := s.device.Err(); err != nil && !util.IsExpectedStreamCloseError(err) {
			return err
		}
		return collectErr
	}
}

func (s *ChannelSession) startEITPFLocked() {
	if s.eitPFStarted || s.eitCollector == nil || s.eitUpdater == nil {
		return
	}
	s.eitPFStarted = true

	r, w := io.Pipe()
	s.hub.Attach(w)
	ctx := s.ctx
	go func() {
		slog.Debug("starting EITPF piggyback collection", "type", s.typ, "channel", s.channel)
		defer s.hub.Detach(w)
		defer r.Close()
		defer w.Close()
		defer slog.Debug("finished EITPF piggyback collection", "type", s.typ, "channel", s.channel)

		pr, pw := io.Pipe()
		done := make(chan error, 1)
		go func() {
			done <- s.eitCollector.CollectEITPF(ctx, r, pw)
			_ = pw.Close()
		}()

		scanner := bufio.NewScanner(pr)
		scanner.Buffer(make([]byte, 64*1024), 4*1024*1024)
		for scanner.Scan() {
			if err := s.eitUpdater.UpsertEITSectionJSON(scanner.Bytes()); err != nil {
				slog.Error("failed to update EITPF", "type", s.typ, "channel", s.channel, "err", err)
			}
		}
		_ = pr.Close()
		if err := <-done; err != nil && ctx.Err() == nil && !util.IsExpectedStreamCloseError(err) {
			slog.Error("failed to collect EITPF", "type", s.typ, "channel", s.channel, "err", err)
		}
	}()
}
