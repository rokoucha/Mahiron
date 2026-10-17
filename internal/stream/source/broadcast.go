package source

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"time"

	"github.com/21S1298001/mahiron/internal/observability"
	"github.com/21S1298001/mahiron/internal/util"
)

var ErrBroadcastStopped = errors.New("broadcast stopped")

type Broadcast struct {
	cancel      context.CancelFunc
	done        <-chan struct{}
	err         error
	hub         *util.DynamicMultiWriter
	mu          sync.Mutex
	onStops     []func()
	refs        int
	source      LiveSource
	starting    chan struct{}
	startErr    error
	stopped     bool
	channelID   string
	channelType string
}

func NewBroadcast(source LiveSource, onStop func()) *Broadcast {
	broadcast := &Broadcast{
		hub:    util.NewDynamicMultiWriter(),
		source: source,
	}
	if labeled, ok := source.(interface{ StreamMetricLabels() (string, string) }); ok {
		broadcast.channelType, broadcast.channelID = labeled.StreamMetricLabels()
		observability.RegisterStreamFanoutMetrics(context.Background(), broadcast.channelType, broadcast.channelID)
	}
	if onStop != nil {
		broadcast.onStops = append(broadcast.onStops, onStop)
	}
	return broadcast
}

func (b *Broadcast) AddOnStop(onStop func()) bool {
	if onStop == nil {
		return true
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.stopped {
		return false
	}
	b.onStops = append(b.onStops, onStop)
	return true
}

func (b *Broadcast) Subscribe(ctx context.Context, dst io.Writer) error {
	return b.source.WithUser(ctx, func(ctx context.Context) error {
		return b.SubscribeRaw(ctx, dst)
	})
}

func (b *Broadcast) SubscribeRaw(ctx context.Context, dst io.Writer) error {
	if err := b.attach(ctx, dst); err != nil {
		return err
	}
	defer b.detach(dst)
	return b.wait(ctx)
}

func (b *Broadcast) WithUser(ctx context.Context, run func(context.Context) error) error {
	return b.source.WithUser(ctx, run)
}

func (b *Broadcast) Stop(ctx context.Context) error {
	b.mu.Lock()
	if b.stopped {
		b.mu.Unlock()
		return nil
	}
	b.stopped = true
	cancel := b.cancel
	starting := b.starting
	onStops := append([]func(){}, b.onStops...)
	b.onStops = nil
	b.hub.Close()
	b.mu.Unlock()

	if cancel != nil {
		cancel()
	}

	var result error
	if starting != nil {
		select {
		case <-starting:
		case <-ctx.Done():
			result = ctx.Err()
			go func() {
				<-starting
				stopCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				_ = b.source.Stop(stopCtx)
			}()
		}
	}
	if result == nil && b.source != nil {
		result = b.source.Stop(ctx)
	}
	for _, onStop := range onStops {
		onStop()
	}
	slog.Debug("broadcast stopped", "err", result)
	return result
}

func (b *Broadcast) Err() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.err
}

// SubscriberCount reports the number of writers currently attached to the
// shared source.
func (b *Broadcast) SubscriberCount() int {
	return b.hub.Count()
}

// Stopped reports whether the broadcast has been stopped and can no longer
// accept subscribers.
func (b *Broadcast) Stopped() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.stopped
}

func (b *Broadcast) attach(ctx context.Context, dst io.Writer) error {
	b.mu.Lock()
	if b.stopped {
		b.mu.Unlock()
		return ErrBroadcastStopped
	}
	b.refs++
	refs := b.refs
	b.hub.AttachWithOptions(dst, util.DynamicMultiWriterSubscriberOptions{
		Lossless: true,
		OnDrop: func(bytes int) {
			observability.RecordStreamFanoutDrop(context.Background(), b.channelType, b.channelID, bytes)
		},
		OnQueueDepth: func(delta int64) {
			observability.RecordStreamFanoutQueueDepth(context.Background(), b.channelType, b.channelID, delta)
		},
	})
	if b.starting == nil {
		b.starting = make(chan struct{})
		sharedCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
		b.cancel = cancel
		go b.start(sharedCtx)
	}
	ready := b.starting
	b.mu.Unlock()
	select {
	case <-ctx.Done():
		b.detach(dst)
		return ctx.Err()
	case <-ready:
	}
	b.mu.Lock()
	err := b.startErr
	if b.stopped && err == nil {
		err = ErrBroadcastStopped
	}
	b.mu.Unlock()
	if err != nil {
		b.detach(dst)
		return err
	}
	slog.Debug("broadcast subscriber attached", "refs", refs)
	return nil
}

func (b *Broadcast) detach(dst io.Writer) {
	b.mu.Lock()
	if b.refs > 0 {
		b.refs--
	}
	b.hub.Detach(dst)
	refs := b.refs
	b.mu.Unlock()

	if refs == 0 {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := b.Stop(ctx); err != nil {
			if util.IsExpectedStreamCloseError(err) {
				return
			}
			slog.Error("failed to stop broadcast", "err", err)
		}
	}
	slog.Debug("broadcast subscriber detached", "refs", refs)
}

func (b *Broadcast) start(ctx context.Context) {
	err := ctx.Err()
	if err == nil {
		err = b.source.Start(ctx, b.hub)
	}
	b.mu.Lock()
	b.startErr = err
	if err == nil {
		b.done = b.source.Done()
	}
	close(b.starting)
	b.mu.Unlock()
}

func (b *Broadcast) wait(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return nil
	case <-b.done:
		err := b.source.Err()
		if util.IsExpectedStreamCloseError(err) {
			err = nil
		}
		b.mu.Lock()
		b.err = err
		b.mu.Unlock()
		return err
	}
}
