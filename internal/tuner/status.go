package tuner

import (
	"context"
	"log/slog"
	"sort"
	"time"

	"github.com/21S1298001/mahiron/internal/config"
)

type User struct {
	ID             string
	Priority       int
	Agent          string
	URL            string
	DisableDecoder bool
	StreamSetting  StreamSetting
	StreamInfo     map[string]StreamInfo
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

type StreamInfo struct {
	Packet int
	Drop   int
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

type ProcessUptime struct {
	Index         int
	Name          string
	ChannelType   string
	ChannelID     string
	UptimeSeconds int64
}

type ProcessUptimeStatus interface {
	ProcessStartedAt() time.Time
}

type userContextKey struct{}
type streamInfoReporterKey struct{}

type streamInfoReporter func(userID, key string, info StreamInfo)

func WithUser(ctx context.Context, user User) context.Context {
	return context.WithValue(ctx, userContextKey{}, user)
}

func UserFromContext(ctx context.Context) (User, bool) {
	user, ok := ctx.Value(userContextKey{}).(User)
	return user, ok
}

func WithStreamInfoReporter(ctx context.Context, report func(userID, key string, info StreamInfo)) context.Context {
	return context.WithValue(ctx, streamInfoReporterKey{}, streamInfoReporter(report))
}

func WithoutStreamInfoReporter(ctx context.Context) context.Context {
	return context.WithValue(ctx, streamInfoReporterKey{}, streamInfoReporter(nil))
}

func ReportStreamInfo(ctx context.Context, key string, info StreamInfo) bool {
	user, ok := UserFromContext(ctx)
	if !ok || user.ID == "" || key == "" {
		return false
	}
	report, ok := ctx.Value(streamInfoReporterKey{}).(streamInfoReporter)
	if !ok || report == nil {
		return false
	}
	report(user.ID, key, info)
	return true
}

type trackedUser struct {
	user User
	refs int
}

type tunerRuntime struct {
	inUse               bool
	running             bool
	stopped             bool
	fault               bool
	reservationPriority int
	device              Device
	requested           *config.ChannelConfig
	tuned               *config.ChannelConfig
	users               map[string]*trackedUser
}

func (r *tunerRuntime) effectivePriority() int {
	priority := r.reservationPriority
	for _, tracked := range r.users {
		if tracked.user.Priority > priority {
			priority = tracked.user.Priority
		}
	}
	return priority
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

func (tm *TunerManager) ProcessUptimes() []ProcessUptime {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	now := time.Now()
	var result []ProcessUptime
	for i, item := range tm.tuners {
		runtime := tm.runtime[item]
		uptime, ok := runtime.device.(ProcessUptimeStatus)
		if !ok || !runtime.running {
			continue
		}
		startedAt := uptime.ProcessStartedAt()
		if startedAt.IsZero() {
			continue
		}
		channel := runtime.tuned
		result = append(result, ProcessUptime{
			Index:         i,
			Name:          item.Name(),
			ChannelType:   channelTypeOf(channel),
			ChannelID:     channelID(channel),
			UptimeSeconds: int64(now.Sub(startedAt).Seconds()),
		})
	}
	return result
}

func (tm *TunerManager) statusLocked(index int) Status {
	item := tm.tuners[index]
	return tm.statusLockedByTuner(item)
}

func (tm *TunerManager) statusLockedByTuner(item *Tuner) Status {
	runtime := tm.runtime[item]
	index := -1
	for i, candidate := range tm.tuners {
		if candidate == item {
			index = i
			break
		}
	}
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
	var status Status
	tm.mu.Lock()
	runtime := tm.runtime[item]
	if tracked := runtime.users[user.ID]; tracked != nil {
		tracked.refs++
		if user.StreamInfo == nil {
			user.StreamInfo = tracked.user.StreamInfo
		}
		tracked.user = user
		slog.Debug("tuner user reference added", "name", item.Name(), "userId", user.ID, "refs", tracked.refs)
		status = tm.statusLockedByTuner(item)
		tm.mu.Unlock()
		tm.publishStatus(eventTypeUpdate, status)
		return
	}
	runtime.users[user.ID] = &trackedUser{user: user, refs: 1}
	slog.Debug("tuner user added", "name", item.Name(), "userId", user.ID, "agent", user.Agent, "url", user.URL, "priority", user.Priority, "disableDecoder", user.DisableDecoder)
	status = tm.statusLockedByTuner(item)
	tm.mu.Unlock()
	tm.publishStatus(eventTypeUpdate, status)
}

func (tm *TunerManager) updateUserStreamInfo(item *Tuner, userID, key string, info StreamInfo) {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	runtime := tm.runtime[item]
	tracked := runtime.users[userID]
	if tracked == nil {
		return
	}
	if tracked.user.StreamInfo == nil {
		tracked.user.StreamInfo = map[string]StreamInfo{}
	}
	tracked.user.StreamInfo[key] = info
}

func (tm *TunerManager) removeUser(item *Tuner, id string) {
	var status Status
	publish := false
	tm.mu.Lock()
	runtime := tm.runtime[item]
	tracked := runtime.users[id]
	if tracked == nil {
		tm.mu.Unlock()
		return
	}
	tracked.refs--
	if tracked.refs == 0 {
		delete(runtime.users, id)
		slog.Debug("tuner user removed", "name", item.Name(), "userId", id)
		status = tm.statusLockedByTuner(item)
		publish = true
		tm.mu.Unlock()
		if publish {
			tm.publishStatus(eventTypeUpdate, status)
		}
		return
	}
	slog.Debug("tuner user reference removed", "name", item.Name(), "userId", id, "refs", tracked.refs)
	status = tm.statusLockedByTuner(item)
	publish = true
	tm.mu.Unlock()
	if publish {
		tm.publishStatus(eventTypeUpdate, status)
	}
}

func (tm *TunerManager) SeedEventLog() {
	if tm.events == nil {
		return
	}
	for _, status := range tm.Statuses() {
		tm.publishStatus(eventTypeCreate, status)
	}
}

func (tm *TunerManager) publishStatus(typ string, status Status) {
	if tm.events == nil {
		return
	}
	tm.events.PublishTunerStatusEvent(typ, status)
}
