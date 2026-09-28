package stream

import (
	"context"

	channelstream "github.com/21S1298001/mahiron/internal/stream/channel"
	"github.com/21S1298001/mahiron/internal/stream/remote"
	"github.com/21S1298001/mahiron/internal/stream/tlv"
)

// All session types must satisfy the public Session interface.
var (
	_ Session = (*channelstream.ChannelSession)(nil)
	_ Session = (*remote.Session)(nil)
	_ Session = (*tlv.Session)(nil)
)

// createSession acquires an input and builds the channel session. TLV
// channels use the TLV session; a remote TS handle only adds API-backed
// operations around the shared channel implementation.
func (m *Manager) createSession(ctx context.Context, key sessionKey, channelType, channel string, wait bool) (Session, string, string, error) {
	handle, err := m.sources.Acquire(ctx, channelType, channel, wait)
	if err != nil {
		return nil, "", "", err
	}
	metadata := handle.Metadata()
	if m.sources.IsTLVChannel(channelType, channel) {
		session := tlv.NewSession(tlv.Config{
			Channel:      channel,
			Type:         channelType,
			Handle:       handle,
			EventUpdater: m.eitUpdater,
			LogoUpdater:  m.logoUpdater,
			OnStop:       func() { m.remove(key) },
		})
		return session, handle.RouteType(), handle.SourceLabel(), nil
	}
	if metadata.Remote != "" {
		client := m.remotes[metadata.Remote]
		return remote.NewSession(remote.SessionConfig{Client: client, Handle: handle, ModuleStore: m.dataBroadcastStore}), handle.RouteType(), handle.SourceLabel(), nil
	}

	session := channelstream.NewChannelSession(channelstream.Config{
		Channel:       channel,
		Handle:        handle,
		EITUpdater:    m.eitUpdater,
		LogoUpdater:   m.logoUpdater,
		OnStop:        func() { m.remove(key) },
		Type:          channelType,
		ModuleStore:   m.dataBroadcastStore,
		SnapshotStore: m.snapshotStore,
	})
	return session, handle.RouteType(), handle.SourceLabel(), nil
}
