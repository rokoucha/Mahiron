package stream

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"time"

	"github.com/21S1298001/Mahiron5/internal/util"
)

type BroadcastHook func(context.Context, *Broadcast)

type Broadcast struct {
	cancel  context.CancelFunc
	done    <-chan struct{}
	err     error
	hooks   []BroadcastHook
	hub     *util.DynamicMultiWriter
	mu      sync.Mutex
	onStop  func()
	refs    int
	source  LiveSource
	started bool
	stopped bool
}

func NewBroadcast(source LiveSource, hooks []BroadcastHook, onStop func()) *Broadcast {
	return &Broadcast{
		hooks:  hooks,
		hub:    util.NewDynamicMultiWriter(),
		onStop: onStop,
		source: source,
	}
}

func (b *Broadcast) Subscribe(ctx context.Context, dst io.Writer) error {
	return b.source.WithUser(ctx, func() error {
		return b.SubscribeRaw(ctx, dst)
	})
}

func (b *Broadcast) SubscribeRaw(ctx context.Context, dst io.Writer) error {
	if err := b.attach(dst); err != nil {
		return err
	}
	defer b.detach(dst)
	return b.wait(ctx)
}

func (b *Broadcast) WithUser(ctx context.Context, run func() error) error {
	return b.source.WithUser(ctx, run)
}

func (b *Broadcast) Tap(ctx context.Context, dst io.Writer) error {
	b.mu.Lock()
	if b.stopped {
		b.mu.Unlock()
		return errors.New("broadcast stopped")
	}
	if !b.started {
		b.mu.Unlock()
		return errors.New("broadcast not started")
	}
	b.hub.Attach(dst)
	b.mu.Unlock()
	defer b.hub.Detach(dst)

	return b.wait(ctx)
}

func (b *Broadcast) Stop(ctx context.Context) error {
	b.mu.Lock()
	if b.stopped {
		b.mu.Unlock()
		return nil
	}
	b.stopped = true
	cancel := b.cancel
	onStop := b.onStop
	b.hub.Close()
	b.mu.Unlock()

	if cancel != nil {
		cancel()
	}

	var result error
	if b.source != nil {
		result = errors.Join(result, b.source.Stop(ctx))
	}
	if onStop != nil {
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

func (b *Broadcast) attach(dst io.Writer) error {
	b.mu.Lock()
	if b.stopped {
		b.mu.Unlock()
		return errors.New("broadcast stopped")
	}
	b.refs++
	refs := b.refs
	b.hub.Attach(dst)
	if err := b.startLocked(); err != nil {
		b.refs--
		b.hub.Detach(dst)
		b.mu.Unlock()
		stopCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = b.Stop(stopCtx)
		return err
	}
	b.mu.Unlock()
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

func (b *Broadcast) startLocked() error {
	if b.started {
		return nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	b.cancel = cancel
	if err := b.source.Start(ctx, b.hub); err != nil {
		cancel()
		return err
	}
	b.done = b.source.Done()
	b.started = true
	slog.Info("broadcast started")
	for _, hook := range b.hooks {
		hook(ctx, b)
	}
	return nil
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
