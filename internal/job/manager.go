package job

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/21S1298001/mahiron/internal/job/run"
	"github.com/21S1298001/mahiron/internal/observability"
	"github.com/go-co-op/gocron/v2"
	"github.com/google/uuid"
)

type eventPublisher interface {
	PublishJobEvent(typ string, data map[string]any)
	PublishJobScheduleEvent(typ string, data map[string]any)
}

type JobManager struct {
	scheduler           gocron.Scheduler
	definitions         map[string]*JobDefinition
	gocronIDs           map[string]uuid.UUID
	history             []*Job
	queue               []*Job
	active              map[string]context.CancelFunc
	activeKeys          map[string]bool
	activeExclusiveKeys map[string]bool
	running             int
	maxConcurrent       int
	maxHistory          int
	mu                  sync.Mutex
	wg                  sync.WaitGroup
	shutdownCtx         context.Context
	shutdownCancel      context.CancelFunc
	changed             chan struct{}
	events              eventPublisher
}

type Config struct {
	MaxHistory        int
	MaxConcurrentJobs int
}

func NewManager(cfg Config, events ...eventPublisher) (*JobManager, error) {
	if cfg.MaxHistory <= 0 {
		cfg.MaxHistory = 100
	}
	if cfg.MaxConcurrentJobs <= 0 {
		cfg.MaxConcurrentJobs = 1
	}
	scheduler, err := gocron.NewScheduler()
	if err != nil {
		return nil, err
	}
	shutdownCtx, shutdownCancel := context.WithCancel(context.Background())
	var publisher eventPublisher
	if len(events) > 0 {
		publisher = events[0]
	}
	return &JobManager{
		scheduler:           scheduler,
		definitions:         make(map[string]*JobDefinition),
		gocronIDs:           make(map[string]uuid.UUID),
		active:              make(map[string]context.CancelFunc),
		activeKeys:          make(map[string]bool),
		activeExclusiveKeys: make(map[string]bool),
		maxConcurrent:       cfg.MaxConcurrentJobs,
		maxHistory:          cfg.MaxHistory,
		shutdownCtx:         shutdownCtx,
		shutdownCancel:      shutdownCancel,
		changed:             make(chan struct{}),
		events:              publisher,
	}, nil
}

func (m *JobManager) Register(definition JobDefinition) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.definitions[definition.Key] = &definition
}

func (m *JobManager) AddSchedule(key, schedule string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	def, ok := m.definitions[key]
	if !ok {
		return ErrDefinitionNotFound
	}
	gocronJob, err := m.scheduler.NewJob(
		gocron.CronJob(schedule, false),
		gocron.NewTask(m.wrapHandler(def)),
		gocron.WithName(def.Name),
		gocron.WithIdentifier(uuid.NewSHA1(uuid.NameSpaceOID, []byte(key))),
		gocron.WithSingletonMode(gocron.LimitModeReschedule),
	)
	if err != nil {
		return err
	}
	m.gocronIDs[key] = gocronJob.ID()
	slog.Info("job schedule added", "key", key, "name", def.Name, "schedule", schedule)
	m.publishJobScheduleLocked(ScheduleInfo{Key: key, Schedule: schedule, JobKey: key, JobName: def.Name})
	return nil
}

func (m *JobManager) Start() { m.scheduler.Start() }

func (m *JobManager) Shutdown(ctx context.Context) error {
	m.shutdownCancel()
	m.mu.Lock()
	m.abortQueuedAndStandbyLocked()
	m.cancelActiveLocked()
	m.mu.Unlock()
	if err := m.scheduler.ShutdownWithContext(ctx); err != nil {
		return err
	}
	done := make(chan struct{})
	go func() {
		m.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (m *JobManager) Enqueue(key string) (string, error) {
	m.mu.Lock()
	def, ok := m.definitions[key]
	if !ok {
		m.mu.Unlock()
		return "", ErrDefinitionNotFound
	}
	id, err := m.enqueueLocked(def)
	m.mu.Unlock()
	return id, err
}

func (m *JobManager) EnqueueDefinition(definition JobDefinition) (string, error) {
	m.mu.Lock()
	id, err := m.enqueueLocked(&definition)
	m.mu.Unlock()
	return id, err
}

func (m *JobManager) enqueueLocked(def *JobDefinition) (string, error) {
	if m.shutdownCtx.Err() != nil {
		return "", ErrManagerShutdown
	}
	if def.Key == "" || def.Handler == nil {
		return "", ErrInvalidDefinition
	}
	if m.activeKeys[def.Key] {
		return "", ErrJobAlreadyRunning
	}
	now := time.Now()
	item := &Job{
		ID: uuid.New().String(), Key: def.Key, Name: def.Name,
		Status: StatusQueued, CreatedAt: now, UpdatedAt: now, definition: def,
		done: make(chan struct{}),
	}
	m.addHistoryLocked(item)
	m.enqueueItemLocked(item)
	m.activeKeys[def.Key] = true
	m.publishJobChangeLocked("create", item)
	slog.Info("job queued", "key", item.Key, "name", item.Name, "id", item.ID)
	m.dispatchLocked()
	return item.ID, nil
}

func (m *JobManager) dispatchLocked() {
	for m.running < m.maxConcurrent && len(m.queue) > 0 && m.shutdownCtx.Err() == nil {
		item := m.popRunnableQueueLocked()
		if item == nil {
			return
		}
		ctx := m.startJobLocked(item)
		go m.run(ctx, item)
	}
}

func (m *JobManager) run(ctx context.Context, item *Job) {
	defer m.wg.Done()
	ctx, span := observability.StartSpan(ctx, observability.SpanJobRun,
		observability.AttrJobID.String(item.ID),
		observability.AttrJobKey.String(item.Key),
		observability.AttrJobName.String(item.Name),
		observability.AttrJobRetryCount.Int(item.RetryCount),
	)
	ctx = run.WithReporter(ctx, jobResultReporter{manager: m, item: item})
	ctx = run.WithJob(ctx, run.JobInfo{ID: item.ID, Key: item.Key, Name: item.Name})
	err := item.definition.Handler(ctx)
	observability.EndSpan(span, err)
	m.mu.Lock()
	m.completeActiveLocked(item)
	if !item.HasAborted && m.shouldRetryLocked(item, err) {
		m.standbyLocked(item, err)
	} else {
		m.finishLocked(item, err, item.HasAborted)
	}
	m.dispatchLocked()
	m.mu.Unlock()
}

func (m *JobManager) shouldRetryLocked(item *Job, err error) bool {
	if err == nil || errors.Is(err, context.Canceled) || item.RetryCount >= len(item.definition.RetryDelays) || m.shutdownCtx.Err() != nil {
		return false
	}
	return item.definition.RetryIf == nil || item.definition.RetryIf(err)
}

func (m *JobManager) standbyLocked(item *Job, err error) {
	delay := item.definition.RetryDelays[item.RetryCount]
	next := time.Now().Add(delay)
	item.RetryCount++
	item.Status = StatusStandby
	item.UpdatedAt = time.Now()
	item.NextRunAt = &next
	item.Error = err.Error()
	m.publishJobChangeLocked("update", item)
	slog.Warn("job retry scheduled", "key", item.Key, "id", item.ID, "retry", item.RetryCount, "nextRunAt", next, "err", err)
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		timer := time.NewTimer(time.Until(next))
		defer timer.Stop()
		select {
		case <-timer.C:
		case <-m.shutdownCtx.Done():
			return
		}
		m.mu.Lock()
		defer m.mu.Unlock()
		if item.Status != StatusStandby || item.HasAborted || m.shutdownCtx.Err() != nil {
			return
		}
		m.retryToQueueLocked(item)
		slog.Info("job retry enqueued", "key", item.Key, "name", item.Name, "id", item.ID, "retry", item.RetryCount)
		m.dispatchLocked()
	}()
}

func (m *JobManager) finishLocked(item *Job, err error, aborted bool) {
	if item.Status == StatusFinished {
		return
	}
	now := time.Now()
	item.Status = StatusFinished
	item.UpdatedAt = now
	item.FinishedAt = &now
	item.NextRunAt = nil
	item.HasAborted = item.HasAborted || aborted
	if err != nil && !item.HasAborted && !errors.Is(err, context.Canceled) {
		item.HasFailed = true
		item.Error = err.Error()
	}
	delete(m.activeKeys, item.Key)
	close(item.done)
	m.publishJobChangeLocked("update", item)
	m.trimHistory()
	if item.StartedAt != nil {
		result := "success"
		if item.HasAborted {
			result = "aborted"
		} else if item.HasFailed {
			result = "failure"
		}
		observability.RecordJobRun(context.Background(), item.Key, result, now.Sub(*item.StartedAt).Milliseconds())
		if item.HasFailed {
			slog.Error("job failed", "key", item.Key, "id", item.ID, "err", err, "duration", now.Sub(*item.StartedAt))
		} else {
			slog.Info("job completed", "key", item.Key, "id", item.ID, "duration", now.Sub(*item.StartedAt))
		}
	}
}

func (m *JobManager) addHistoryLocked(item *Job) {
	m.history = append(m.history, item)
	m.trimHistory()
}

func (m *JobManager) enqueueItemLocked(item *Job) {
	m.queue = append(m.queue, item)
}

func (m *JobManager) popRunnableQueueLocked() *Job {
	for i, item := range m.queue {
		if !m.dependenciesSatisfiedLocked(item.definition.DependsOn, item.definition.ExclusiveKeys) {
			continue
		}
		m.queue = append(m.queue[:i], m.queue[i+1:]...)
		return item
	}
	return nil
}

func (m *JobManager) dependenciesSatisfiedLocked(dependencies, exclusiveKeys []string) bool {
	for _, key := range dependencies {
		if m.activeKeys[key] {
			return false
		}
	}
	for _, key := range exclusiveKeys {
		if m.activeExclusiveKeys[key] {
			return false
		}
	}
	return true
}

func (m *JobManager) startJobLocked(item *Job) context.Context {
	now := time.Now()
	item.Status = StatusRunning
	item.StartedAt = &now
	item.UpdatedAt = now
	ctx, cancel := context.WithCancel(m.shutdownCtx)
	m.active[item.ID] = cancel
	for _, key := range item.definition.ExclusiveKeys {
		m.activeExclusiveKeys[key] = true
	}
	m.running++
	m.publishJobChangeLocked("update", item)
	m.wg.Add(1)
	slog.Info("job started", "key", item.Key, "name", item.Name, "id", item.ID)
	return ctx
}

func (m *JobManager) completeActiveLocked(item *Job) {
	delete(m.active, item.ID)
	for _, key := range item.definition.ExclusiveKeys {
		delete(m.activeExclusiveKeys, key)
	}
	m.running--
}

func (m *JobManager) retryToQueueLocked(item *Job) {
	item.Status = StatusQueued
	item.UpdatedAt = time.Now()
	item.NextRunAt = nil
	m.enqueueItemLocked(item)
	m.publishJobChangeLocked("update", item)
}

func (m *JobManager) abortQueuedAndStandbyLocked() {
	queued := append([]*Job(nil), m.queue...)
	m.queue = nil
	for _, item := range queued {
		m.abortPendingLocked(item)
	}
	for _, item := range m.history {
		if item.Status == StatusStandby {
			m.abortPendingLocked(item)
		}
	}
}

func (m *JobManager) abortPendingLocked(item *Job) {
	item.IsAborting = true
	m.finishLocked(item, context.Canceled, true)
}

func (m *JobManager) cancelActiveLocked() {
	for _, cancel := range m.active {
		cancel()
	}
}

// Changes returns a notification channel that is closed on the next job state
// transition. Callers should take a fresh channel after every notification.
func (m *JobManager) Changes() <-chan struct{} {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.changed
}

func (m *JobManager) notifyLocked() {
	close(m.changed)
	m.changed = make(chan struct{})
}

func (m *JobManager) publishJobLocked(typ string, item *Job) {
	if m.events == nil {
		return
	}
	m.events.PublishJobEvent(typ, item.EventData())
}

func (m *JobManager) publishJobChangeLocked(typ string, item *Job) {
	m.notifyLocked()
	m.publishJobLocked(typ, item)
}

func (m *JobManager) publishJobScheduleLocked(schedule ScheduleInfo) {
	if m.events == nil {
		return
	}
	m.events.PublishJobScheduleEvent("create", schedule.EventData())
}

// Wait blocks until the identified execution reaches its terminal state.  It
// is also the synchronization boundary for callers that need to observe a
// completed Job without polling the manager's internal state.
func (m *JobManager) Wait(ctx context.Context, id string) (*Job, error) {
	m.mu.Lock()
	item := m.findJob(id)
	if item == nil {
		m.mu.Unlock()
		return nil, ErrJobNotFound
	}
	done := item.done
	m.mu.Unlock()

	select {
	case <-done:
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	copy := *item
	copy.definition = nil
	copy.done = nil
	copy.Result = run.Clone(item.Result)
	return &copy, nil
}

func (m *JobManager) Abort(id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.abortActiveLocked(id) {
		return nil
	}
	if m.abortQueuedLocked(id) {
		return nil
	}
	if m.abortStandbyLocked(id) {
		return nil
	}
	slog.Debug("job abort skipped", "id", id, "err", ErrJobNotRunning)
	return ErrJobNotRunning
}

func (m *JobManager) abortActiveLocked(id string) bool {
	cancel, ok := m.active[id]
	if !ok {
		return false
	}
	if item := m.findJob(id); item != nil {
		item.IsAborting = true
		item.HasAborted = true
		item.UpdatedAt = time.Now()
		m.publishJobChangeLocked("update", item)
		slog.Info("job abort requested", "key", item.Key, "name", item.Name, "id", item.ID, "status", item.Status)
	}
	cancel()
	return true
}

func (m *JobManager) abortQueuedLocked(id string) bool {
	for i, item := range m.queue {
		if item.ID != id {
			continue
		}
		m.queue = append(m.queue[:i], m.queue[i+1:]...)
		slog.Info("queued job aborted", "key", item.Key, "name", item.Name, "id", item.ID)
		m.abortPendingLocked(item)
		return true
	}
	return false
}

func (m *JobManager) abortStandbyLocked(id string) bool {
	item := m.findJob(id)
	if item == nil || item.Status != StatusStandby {
		return false
	}
	item.HasAborted = true
	slog.Info("standby job aborted", "key", item.Key, "name", item.Name, "id", item.ID)
	m.abortPendingLocked(item)
	return true
}

func (m *JobManager) Rerun(id string) error {
	m.mu.Lock()
	item := m.findJob(id)
	if item == nil {
		m.mu.Unlock()
		return ErrJobNotFound
	}
	def := item.definition
	key := item.Key
	name := item.Name
	m.mu.Unlock()
	if def == nil || !def.IsRerunnable {
		slog.Debug("job rerun skipped", "key", key, "name", name, "id", id, "err", ErrJobNotRerunnable)
		return ErrJobNotRerunnable
	}
	newID, err := m.EnqueueDefinition(*def)
	if err != nil {
		slog.Warn("failed to rerun job", "key", key, "name", name, "id", id, "err", err)
		return err
	}
	slog.Info("job rerun queued", "key", key, "name", name, "id", id, "newId", newID)
	return err
}

func (m *JobManager) RunSchedule(key string) error {
	m.mu.Lock()
	gocronID, ok := m.gocronIDs[key]
	m.mu.Unlock()
	if !ok {
		slog.Debug("job schedule run skipped", "key", key, "err", ErrDefinitionNotFound)
		return ErrDefinitionNotFound
	}
	for _, scheduled := range m.scheduler.Jobs() {
		if scheduled.ID() == gocronID {
			slog.Info("job schedule run requested", "key", key)
			return scheduled.RunNow()
		}
	}
	slog.Debug("job schedule run skipped", "key", key, "err", ErrJobNotFound)
	return ErrJobNotFound
}

func (m *JobManager) GetJobs() []*Job {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := make([]*Job, len(m.history))
	for i, item := range m.history {
		copy := *item
		copy.definition = nil
		copy.done = nil
		copy.Result = run.Clone(item.Result)
		result[i] = &copy
	}
	return result
}

func (m *JobManager) GetActiveJobKeysByPrefix(prefix string) []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	seen := make(map[string]struct{})
	result := make([]string, 0)
	for _, item := range m.history {
		if item.Status == StatusFinished {
			continue
		}
		if !strings.HasPrefix(item.Key, prefix) {
			continue
		}
		if _, ok := seen[item.Key]; ok {
			continue
		}
		seen[item.Key] = struct{}{}
		result = append(result, item.Key)
	}
	return result
}

func (m *JobManager) GetJobSchedules() []ScheduleInfo {
	scheduled := m.scheduler.Jobs()
	m.mu.Lock()
	defer m.mu.Unlock()
	var result []ScheduleInfo
	for _, item := range scheduled {
		cronSchedule, ok := item.Schedule().(gocron.CronJobSchedule)
		if !ok {
			continue
		}
		key := ""
		for candidate, id := range m.gocronIDs {
			if id == item.ID() {
				key = candidate
				break
			}
		}
		name := item.Name()
		if def, ok := m.definitions[key]; ok {
			name = def.Name
		}
		result = append(result, ScheduleInfo{Key: key, Schedule: cronSchedule.Crontab, JobKey: key, JobName: name})
	}
	return result
}

type jobResultReporter struct {
	manager *JobManager
	item    *Job
}

func (r jobResultReporter) SetJobResult(result run.Result) {
	if r.manager == nil || r.item == nil {
		return
	}
	r.manager.setJobResult(r.item, result)
}

func (m *JobManager) setJobResult(item *Job, result run.Result) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if item.Status == StatusFinished {
		return
	}
	item.Result = run.Clone(&result)
	item.UpdatedAt = time.Now()
	m.publishJobChangeLocked("update", item)
	observability.RecordJobItems(context.Background(), item.Key, result)
}

func (m *JobManager) findJob(id string) *Job {
	for _, item := range m.history {
		if item.ID == id {
			return item
		}
	}
	return nil
}

func (m *JobManager) trimHistory() {
	for len(m.history) > m.maxHistory {
		removed := false
		for i, item := range m.history {
			if item.Status == StatusFinished {
				m.history = append(m.history[:i], m.history[i+1:]...)
				removed = true
				break
			}
		}
		if !removed {
			return
		}
	}
}

func (m *JobManager) wrapHandler(def *JobDefinition) func(context.Context) error {
	return func(context.Context) error {
		_, err := m.Enqueue(def.Key)
		if errors.Is(err, ErrJobAlreadyRunning) {
			slog.Info("job already queued or running, skipping scheduled execution", "key", def.Key)
			return nil
		}
		return err
	}
}
