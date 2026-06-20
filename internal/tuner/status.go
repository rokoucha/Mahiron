package tuner

import (
	"context"
	"log/slog"
	"sort"

	"github.com/21S1298001/Mahiron5/internal/config"
)

type User struct {
	ID             string
	Priority       int
	Agent          string
	URL            string
	DisableDecoder bool
	StreamSetting  StreamSetting
}

type StreamSetting struct {
	Channel   *config.ChannelConfig
	NetworkID *uint16
	ServiceID *uint16
	EventID   *uint16
	NoProvide *bool
	ParseNIT  *bool
	ParseSDT  *bool
	ParseEIT  *bool
}

type Status struct {
	Index              int
	Name               string
	Types              []string
	Command            string
	PID                int
	Users              []User
	IsAvailable        bool
	IsFree             bool
	IsUsing            bool
	IsFault            bool
	CurrentChannelType string
	CurrentChannel     string
	TunedChannelType   string
	TunedChannel       string
}

type userContextKey struct{}

func WithUser(ctx context.Context, user User) context.Context {
	return context.WithValue(ctx, userContextKey{}, user)
}

func UserFromContext(ctx context.Context) (User, bool) {
	user, ok := ctx.Value(userContextKey{}).(User)
	return user, ok
}

type trackedUser struct {
	user User
	refs int
}

type tunerRuntime struct {
	inUse     bool
	running   bool
	stopped   bool
	fault     bool
	device    Device
	requested *config.ChannelConfig
	tuned     *config.ChannelConfig
	users     map[string]*trackedUser
}

func (tm *TunerManager) Statuses() []Status {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	result := make([]Status, len(tm.tuners))
	for i := range tm.tuners {
		result[i] = tm.statusLocked(i)
	}
	return result
}

func (tm *TunerManager) Status(index int) (Status, bool) {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	if index < 0 || index >= len(tm.tuners) {
		return Status{}, false
	}
	return tm.statusLocked(index), true
}

func (tm *TunerManager) statusLocked(index int) Status {
	item := tm.tuners[index]
	runtime := tm.runtime[item]
	available := !item.IsDisabled() && item.Usable() && !runtime.fault && !runtime.stopped
	status := Status{
		Index: index, Name: item.Name(), Types: append([]string(nil), item.Groups()...),
		IsAvailable: available, IsFault: runtime.fault,
	}
	if process, ok := runtime.device.(ProcessStatus); ok {
		info := process.ProcessStatus()
		status.Command = info.Command
		status.PID = info.PID
	}
	if runtime.requested != nil {
		status.CurrentChannelType = runtime.requested.Type
		status.CurrentChannel = runtime.requested.Channel
	}
	if runtime.tuned != nil {
		status.TunedChannelType = runtime.tuned.Type
		status.TunedChannel = runtime.tuned.Channel
	}
	ids := make([]string, 0, len(runtime.users))
	for id := range runtime.users {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		status.Users = append(status.Users, runtime.users[id].user)
	}
	status.IsFree = available && !runtime.inUse && len(status.Users) == 0
	status.IsUsing = available && runtime.running && len(status.Users) != 0
	return status
}

func (tm *TunerManager) addUser(item *Tuner, user User) {
	if user.ID == "" {
		return
	}
	tm.mu.Lock()
	defer tm.mu.Unlock()
	runtime := tm.runtime[item]
	if tracked := runtime.users[user.ID]; tracked != nil {
		tracked.refs++
		tracked.user = user
		slog.Debug("tuner user reference added", "name", item.Name(), "userId", user.ID, "refs", tracked.refs)
		return
	}
	runtime.users[user.ID] = &trackedUser{user: user, refs: 1}
	slog.Debug("tuner user added", "name", item.Name(), "userId", user.ID, "agent", user.Agent, "url", user.URL, "priority", user.Priority, "disableDecoder", user.DisableDecoder)
}

func (tm *TunerManager) removeUser(item *Tuner, id string) {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	runtime := tm.runtime[item]
	tracked := runtime.users[id]
	if tracked == nil {
		return
	}
	tracked.refs--
	if tracked.refs == 0 {
		delete(runtime.users, id)
		slog.Debug("tuner user removed", "name", item.Name(), "userId", id)
		return
	}
	slog.Debug("tuner user reference removed", "name", item.Name(), "userId", id, "refs", tracked.refs)
}
