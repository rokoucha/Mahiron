package stream

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
)

type FlowRegistry struct {
	buildProcessors func(PipelineKey) []Processor
	mu              sync.Mutex
	onEmpty         func()
	pipelines       map[PipelineKey]*streamPipeline
	source          sourceSubscriber
	stopped         bool
}

func NewFlowRegistry(source sourceSubscriber, buildProcessors func(PipelineKey) []Processor, onEmpty func()) *FlowRegistry {
	return &FlowRegistry{
		buildProcessors: buildProcessors,
		onEmpty:         onEmpty,
		pipelines:       map[PipelineKey]*streamPipeline{},
		source:          source,
	}
}

func (r *FlowRegistry) Attach(ctx context.Context, key PipelineKey, dst io.Writer) error {
	pipeline, err := r.getOrCreate(key)
	if err != nil {
		return err
	}
	return pipeline.Attach(ctx, dst)
}

func (r *FlowRegistry) Stop() {
	r.mu.Lock()
	if r.stopped {
		r.mu.Unlock()
		return
	}
	r.stopped = true
	pipelines := make([]*streamPipeline, 0, len(r.pipelines))
	for _, pipeline := range r.pipelines {
		pipelines = append(pipelines, pipeline)
	}
	r.pipelines = map[PipelineKey]*streamPipeline{}
	r.mu.Unlock()

	for _, pipeline := range pipelines {
		pipeline.Stop()
	}
}

func (r *FlowRegistry) getOrCreate(key PipelineKey) (*streamPipeline, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.stopped {
		return nil, errors.New("flow registry stopped")
	}
	if pipeline := r.pipelines[key]; pipeline != nil {
		slog.Debug("reusing stream pipeline", "type", key.ChannelType, "channel", key.ChannelID, "kind", key.Kind, "serviceId", key.ServiceID, "decode", key.Decode)
		return pipeline, nil
	}

	pipeline := newStreamPipeline(key, r.buildProcessors(key), r.source, func() {
		r.remove(key)
	})
	r.pipelines[key] = pipeline
	slog.Debug("created stream pipeline", "type", key.ChannelType, "channel", key.ChannelID, "kind", key.Kind, "serviceId", key.ServiceID, "decode", key.Decode)
	return pipeline, nil
}

func (r *FlowRegistry) remove(key PipelineKey) {
	r.mu.Lock()
	delete(r.pipelines, key)
	empty := len(r.pipelines) == 0
	onEmpty := r.onEmpty
	r.mu.Unlock()

	if empty && onEmpty != nil {
		onEmpty()
	}
	slog.Debug("removed stream pipeline", "type", key.ChannelType, "channel", key.ChannelID, "kind", key.Kind, "serviceId", key.ServiceID, "decode", key.Decode)
}
