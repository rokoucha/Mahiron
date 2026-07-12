package stream

import (
	"context"

	channelstream "github.com/21S1298001/mahiron/internal/stream/channel"
	"github.com/21S1298001/mahiron/internal/stream/remote"
)

// Both session wrappers must satisfy the public Session interface.
var (
	_ Session = (*channelstream.ChannelSession)(nil)
	_ Session = (*remote.Session)(nil)
)

// createSession acquires an input and builds the shared channel session. A
// remote handle only adds API-backed operations around that implementation.
func (m *StreamManager) createSession(ctx context.Context, key sessionKey, channelType, channel string, wait bool) (Session, string, string, error) {
	handle, err := m.sources.Acquire(ctx, channelType, channel, wait)
	if err != nil {
		return nil, "", "", err
	}
	metadata := handle.Metadata()
	if metadata.Remote != "" {
		client := m.remotes[metadata.Remote]
		return remote.NewSession(remote.SessionConfig{Client: client, Handle: handle}), handle.RouteType(), handle.SourceLabel(), nil
	}

	session := channelstream.NewChannelSession(channelstream.Config{
		Channel:     channel,
		Handle:      handle,
		EITUpdater:  m.eitUpdater,
		LogoUpdater: m.logoUpdater,
		OnStop:      func() { m.remove(key) },
		Type:        channelType,
	})
	return session, handle.RouteType(), handle.SourceLabel(), nil
}
