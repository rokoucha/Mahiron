package stream

import (
	"context"
	"encoding/json"
	"log/slog"
	"strconv"
	"sync"

	"github.com/21S1298001/mahiron/internal/config"
)

type routeSourceKey struct {
	remote      string
	typ         string
	channel     string
	serviceID   string
	tsmfRelTS   string
	commandVars string
}

type sharedRouteSource struct {
	broadcast      *Broadcast
	decoderCommand string
}

func newRouteSourceKey(route config.ChannelRouteConfig) routeSourceKey {
	commandVars, _ := json.Marshal(route.CommandVars)
	key := routeSourceKey{
		remote:      route.Remote,
		typ:         route.Type,
		channel:     route.Channel,
		commandVars: string(commandVars),
	}
	if route.ServiceId != nil {
		key.serviceID = strconv.FormatUint(uint64(*route.ServiceId), 10)
	}
	if route.TsmfRelTs != nil {
		key.tsmfRelTS = strconv.FormatUint(uint64(*route.TsmfRelTs), 10)
	}
	return key
}

func (p *SourcePool) beginRouteSourceCreate(ctx context.Context, key routeSourceKey) (*sharedRouteSource, func(), error) {
	for {
		p.mu.Lock()
		if shared := p.routeSources[key]; shared != nil {
			p.mu.Unlock()
			return shared, nil, nil
		}
		if creating := p.routeSourceCreates[key]; creating != nil {
			p.mu.Unlock()
			select {
			case <-ctx.Done():
				return nil, nil, ctx.Err()
			case <-creating:
				continue
			}
		}
		creating := make(chan struct{})
		p.routeSourceCreates[key] = creating
		p.mu.Unlock()
		var once sync.Once
		finish := func() {
			once.Do(func() {
				p.mu.Lock()
				if p.routeSourceCreates[key] == creating {
					delete(p.routeSourceCreates, key)
					close(creating)
				}
				p.mu.Unlock()
			})
		}
		return nil, finish, nil
	}
}

func (p *SourcePool) commitRouteSource(key routeSourceKey, source LiveSource, decoderCommand string) *Broadcast {
	p.mu.Lock()
	if shared := p.routeSources[key]; shared != nil {
		p.mu.Unlock()
		return shared.broadcast
	}
	broadcast := NewBroadcast(source, func() { p.removeRouteSource(key) })
	p.routeSources[key] = &sharedRouteSource{broadcast: broadcast, decoderCommand: decoderCommand}
	p.mu.Unlock()
	return broadcast
}

func (p *SourcePool) removeRouteSource(key routeSourceKey) {
	p.mu.Lock()
	delete(p.routeSources, key)
	p.mu.Unlock()
	slog.Debug("stream route source removed", "type", key.typ, "channel", key.channel, "remote", key.remote)
}
