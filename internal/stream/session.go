package stream

import (
	"context"
	"errors"
	"io"
	"sync"
	"time"

	"github.com/21S1298001/Mahiron5/internal/config"
	"github.com/21S1298001/Mahiron5/internal/program"
)

type ChannelSession struct {
	broadcast    *Broadcast
	channel      string
	descrambler  Descrambler
	eitCollector EITCollector
	filter       ServiceFilter
	flows        *FlowRegistry
	mu           sync.Mutex
	scanner      ServiceScanner
	stopped      bool
	typ          string
}

type ChannelSessionConfig struct {
	Channel       string
	ChannelConfig *config.ChannelConfig
	Broadcast     *Broadcast
	Descrambler   Descrambler
	EITCollector  EITCollector
	EITUpdater    EITSectionUpdater
	Filter        ServiceFilter
	OnStop        func()
	Scanner       ServiceScanner
	Type          string
}

func NewChannelSession(config ChannelSessionConfig) *ChannelSession {
	session := &ChannelSession{
		broadcast:    config.Broadcast,
		channel:      config.Channel,
		descrambler:  config.Descrambler,
		eitCollector: config.EITCollector,
		filter:       config.Filter,
		scanner:      config.Scanner,
		typ:          config.Type,
	}
	session.flows = NewFlowRegistry(session.broadcast.SubscribeRaw, session.pipelineProcessors, config.OnStop)
	return session
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

func (s *ChannelSession) ProgramStream(ctx context.Context, p *program.Program, decode bool, dst io.Writer) error {
	key := PipelineKey{
		ChannelType:  s.typ,
		ChannelID:    s.channel,
		Kind:         PipelineProgramStream,
		NetworkID:    p.NetworkID,
		ServiceID:    p.ServiceID,
		EventID:      p.EventID,
		EventTimeout: programEventTimeout(p.StartAt, p.Duration),
		Decode:       decode,
	}
	return s.attachPipeline(ctx, key, dst)
}

func (s *ChannelSession) ScanServices(ctx context.Context, dst io.Writer) error {
	if s.scanner == nil {
		return ErrServiceScannerNotConfigured
	}
	return NewStreamTaskRunner(s.broadcast).Run(ctx, dst, s.scanner.ScanServices)
}

func (s *ChannelSession) CollectEITS(ctx context.Context, dst io.Writer) error {
	if s.eitCollector == nil {
		return ErrEITCollectorNotConfigured
	}
	return NewStreamTaskRunner(s.broadcast).Run(ctx, dst, s.eitCollector.CollectEITS)
}

func (s *ChannelSession) CollectEITPF(ctx context.Context, dst io.Writer) error {
	if s.eitCollector == nil {
		return ErrEITCollectorNotConfigured
	}
	return NewStreamTaskRunner(s.broadcast).Run(ctx, dst, s.eitCollector.CollectEITPF)
}

func (s *ChannelSession) Stop(ctx context.Context) error {
	s.mu.Lock()
	if s.stopped {
		s.mu.Unlock()
		return nil
	}
	s.stopped = true
	flows := s.flows
	broadcast := s.broadcast
	s.mu.Unlock()

	if flows != nil {
		flows.Stop()
	}

	var result error
	if broadcast != nil {
		result = errors.Join(result, broadcast.Stop(ctx))
	}
	return result
}

func (s *ChannelSession) attachPipeline(ctx context.Context, key PipelineKey, dst io.Writer) error {
	s.mu.Lock()
	stopped := s.stopped
	flows := s.flows
	s.mu.Unlock()
	if stopped {
		return errors.New("channel session stopped")
	}
	return s.broadcast.WithUser(ctx, func() error {
		return flows.Attach(ctx, key, dst)
	})
}

func (s *ChannelSession) pipelineProcessors(key PipelineKey) []Processor {
	processors := []Processor{}
	if key.Decode && s.descrambler != nil {
		processors = append(processors, descramblerProcessor{descrambler: s.descrambler})
	}
	if key.Kind == PipelineServiceStream || key.Kind == PipelineProgramStream {
		if s.filter == nil {
			processors = append(processors, errorProcessor{err: ErrServiceFilterNotConfigured})
			return processors
		}
		processors = append(processors, serviceFilterProcessor{
			filter:    s.filter,
			serviceID: key.ServiceID,
		})
	}
	if key.Kind == PipelineProgramStream {
		processors = append(processors, programEventGateProcessor{
			collector:      s.eitCollector,
			eventID:        key.EventID,
			initialTimeout: key.EventTimeout,
			networkID:      key.NetworkID,
			serviceID:      key.ServiceID,
		})
	}
	return processors
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
