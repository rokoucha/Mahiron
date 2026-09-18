package observability

import (
	"context"

	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

type FilteringTracerProvider struct {
	noop.TracerProvider
	delegate trace.TracerProvider
	excluded map[string]struct{}
}

func NewFilteringTracerProvider(delegate trace.TracerProvider, excluded []string) trace.TracerProvider {
	if delegate == nil {
		delegate = noop.NewTracerProvider()
	}
	excludedSet := make(map[string]struct{}, len(excluded))
	for _, name := range excluded {
		excludedSet[name] = struct{}{}
	}
	return FilteringTracerProvider{delegate: delegate, excluded: excludedSet}
}

func (p FilteringTracerProvider) Tracer(name string, opts ...trace.TracerOption) trace.Tracer {
	return filteringTracer{
		delegate: p.delegate.Tracer(name, opts...),
		excluded: p.excluded,
		noop:     noop.NewTracerProvider().Tracer(name, opts...),
	}
}

type filteringTracer struct {
	noop.Tracer
	delegate trace.Tracer
	excluded map[string]struct{}
	noop     trace.Tracer
}

type tracingSuppressedKey struct{}

func tracingSuppressed(ctx context.Context) bool {
	return ctx.Value(tracingSuppressedKey{}) == true
}

func (t filteringTracer) Start(ctx context.Context, spanName string, opts ...trace.SpanStartOption) (context.Context, trace.Span) {
	if _, excluded := t.excluded[spanName]; excluded || tracingSuppressed(ctx) {
		// Keep suppression in the context so internal spans cannot become new roots.
		ctx = context.WithValue(ctx, tracingSuppressedKey{}, true)
		ctx = trace.ContextWithSpanContext(ctx, trace.SpanContext{})
		return t.noop.Start(ctx, spanName, opts...)
	}
	return t.delegate.Start(ctx, spanName, opts...)
}
