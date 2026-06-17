package job

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/go-co-op/gocron/v2"
	"github.com/google/uuid"
)

type JobStatus string

const (
	StatusQueued   JobStatus = "queued"
	StatusRunning  JobStatus = "running"
	StatusFinished JobStatus = "finished"
)

type Job struct {
	ID         string
	Key        string
	Name       string
	Status     JobStatus
	RetryCount int
	IsAborting bool
	HasAborted bool
	HasFailed  bool
	Error      string
	CreatedAt  time.Time
	UpdatedAt  time.Time
	StartedAt  *time.Time
	FinishedAt *time.Time
}

type JobDefinition struct {
	Key          string
	Name         string
	Handler      func(ctx context.Context) error
	IsRerunnable bool
}

type JobManager struct {
	scheduler      gocron.Scheduler
	definitions    map[string]*JobDefinition
	gocronIDs      map[string]uuid.UUID
	history        []*Job
	active         map[string]context.CancelFunc
	activeKeys     map[string]bool
	maxHistory     int
	mu             sync.Mutex
	shutdownCtx    context.Context
	shutdownCancel context.CancelFunc
}

type Config struct {
	MaxHistory int
}

func NewManager(cfg Config) (*JobManager, error) {
	if cfg.MaxHistory <= 0 {
		cfg.MaxHistory = 100
	}

	scheduler, err := gocron.NewScheduler()
	if err != nil {
		return nil, err
	}

	shutdownCtx, shutdownCancel := context.WithCancel(context.Background())

	return &JobManager{
		scheduler:      scheduler,
		definitions:    make(map[string]*JobDefinition),
		gocronIDs:      make(map[string]uuid.UUID),
		active:         make(map[string]context.CancelFunc),
		activeKeys:     make(map[string]bool),
		maxHistory:     cfg.MaxHistory,
		shutdownCtx:    shutdownCtx,
		shutdownCancel: shutdownCancel,
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
	return nil
}

func (m *JobManager) Start() {
	m.scheduler.Start()
}

func (m *JobManager) Shutdown(ctx context.Context) error {
	m.shutdownCancel()
	return m.scheduler.ShutdownWithContext(ctx)
}

func (m *JobManager) Enqueue(key string) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	def, ok := m.definitions[key]
	if !ok {
		return "", ErrDefinitionNotFound
	}

	if m.activeKeys[key] {
		return "", ErrJobAlreadyRunning
	}

	executionID := uuid.New().String()
	now := time.Now()

	job := &Job{
		ID:        executionID,
		Key:       def.Key,
		Name:      def.Name,
		Status:    StatusRunning,
		CreatedAt: now,
		UpdatedAt: now,
		StartedAt: &now,
	}

	m.history = append(m.history, job)
	m.trimHistory()

	ctx, cancel := context.WithCancel(m.shutdownCtx)
	m.active[executionID] = cancel
	m.activeKeys[key] = true

	slog.Info("job started", "key", key, "name", def.Name, "id", executionID, "source", "enqueue")

	go func() {
		defer func() {
			m.mu.Lock()
			delete(m.active, executionID)
			delete(m.activeKeys, key)
			m.mu.Unlock()
			cancel()
		}()

		err := def.Handler(ctx)
		finishedAt := time.Now()

		m.mu.Lock()
		job.FinishedAt = &finishedAt
		job.UpdatedAt = finishedAt
		if err != nil {
			job.Status = StatusFinished
			job.HasFailed = true
			job.Error = err.Error()
			m.mu.Unlock()
			slog.Error("job failed", "key", key, "id", executionID, "err", err, "duration", finishedAt.Sub(*job.StartedAt))
		} else {
			job.Status = StatusFinished
			m.mu.Unlock()
			slog.Info("job completed", "key", key, "id", executionID, "duration", finishedAt.Sub(*job.StartedAt))
		}
	}()

	return executionID, nil
}

func (m *JobManager) Abort(id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	cancel, ok := m.active[id]
	if !ok {
		return ErrJobNotRunning
	}

	job := m.findJob(id)
	if job != nil {
		job.IsAborting = true
		job.HasAborted = true
		job.UpdatedAt = time.Now()
		slog.Info("job aborting", "key", job.Key, "id", id)
	}

	cancel()
	return nil
}

func (m *JobManager) Rerun(id string) error {
	m.mu.Lock()
	job := m.findJob(id)
	if job == nil {
		m.mu.Unlock()
		return ErrJobNotFound
	}

	def, ok := m.definitions[job.Key]
	m.mu.Unlock()

	if !ok || !def.IsRerunnable {
		return ErrJobNotRerunnable
	}

	slog.Info("job rerun requested", "key", job.Key, "originalId", id)
	_, err := m.Enqueue(job.Key)
	return err
}

func (m *JobManager) RunSchedule(key string) error {
	m.mu.Lock()
	gocronID, ok := m.gocronIDs[key]
	m.mu.Unlock()

	if !ok {
		return ErrDefinitionNotFound
	}

	slog.Info("job schedule triggered manually", "key", key)

	gocronJobs := m.scheduler.Jobs()
	for _, gj := range gocronJobs {
		if gj.ID() == gocronID {
			return gj.RunNow()
		}
	}

	return ErrJobNotFound
}

func (m *JobManager) GetJobs() []*Job {
	m.mu.Lock()
	defer m.mu.Unlock()

	result := make([]*Job, len(m.history))
	copy(result, m.history)
	return result
}

func (m *JobManager) GetJobSchedules() []ScheduleInfo {
	gocronJobs := m.scheduler.Jobs()

	m.mu.Lock()
	defer m.mu.Unlock()

	var result []ScheduleInfo
	for _, gj := range gocronJobs {
		sched := gj.Schedule()
		cronSched, ok := sched.(gocron.CronJobSchedule)
		if !ok {
			continue
		}

		key := ""
		for k, id := range m.gocronIDs {
			if id == gj.ID() {
				key = k
				break
			}
		}

		defName := gj.Name()
		if def, ok := m.definitions[key]; ok {
			defName = def.Name
		}

		result = append(result, ScheduleInfo{
			Key:      key,
			Schedule: cronSched.Crontab,
			JobKey:   key,
			JobName:  defName,
		})
	}

	return result
}

type ScheduleInfo struct {
	Key      string
	Schedule string
	JobKey   string
	JobName  string
}

func (m *JobManager) findJob(id string) *Job {
	for _, j := range m.history {
		if j.ID == id {
			return j
		}
	}
	return nil
}

func (m *JobManager) trimHistory() {
	if len(m.history) > m.maxHistory {
		m.history = m.history[len(m.history)-m.maxHistory:]
	}
}

func (m *JobManager) wrapHandler(def *JobDefinition) func(context.Context) error {
	return func(ctx context.Context) error {
		executionID := uuid.New().String()
		now := time.Now()

		job := &Job{
			ID:        executionID,
			Key:       def.Key,
			Name:      def.Name,
			Status:    StatusRunning,
			CreatedAt: now,
			UpdatedAt: now,
			StartedAt: &now,
		}

		m.mu.Lock()
		if m.activeKeys[def.Key] {
			m.mu.Unlock()
			slog.Info("job already running, skipping scheduled execution", "key", def.Key)
			return nil
		}
		m.history = append(m.history, job)
		m.trimHistory()
		m.activeKeys[def.Key] = true
		m.mu.Unlock()

		slog.Info("job started", "key", def.Key, "name", def.Name, "id", executionID, "source", "schedule")

		ctx, cancel := context.WithCancel(ctx)
		m.mu.Lock()
		m.active[executionID] = cancel
		m.mu.Unlock()

		defer func() {
			m.mu.Lock()
			delete(m.active, executionID)
			delete(m.activeKeys, def.Key)
			m.mu.Unlock()
			cancel()
		}()

		err := def.Handler(ctx)
		finishedAt := time.Now()

		m.mu.Lock()
		job.FinishedAt = &finishedAt
		job.UpdatedAt = finishedAt
		if err != nil {
			job.Status = StatusFinished
			job.HasFailed = true
			job.Error = err.Error()
			m.mu.Unlock()
			slog.Error("job failed", "key", def.Key, "id", executionID, "err", err, "duration", finishedAt.Sub(*job.StartedAt))
		} else {
			job.Status = StatusFinished
			m.mu.Unlock()
			slog.Info("job completed", "key", def.Key, "id", executionID, "duration", finishedAt.Sub(*job.StartedAt))
		}

		return err
	}
}

var (
	ErrDefinitionNotFound = &JobError{Code: "DEFINITION_NOT_FOUND", Message: "job definition not found"}
	ErrJobNotFound        = &JobError{Code: "JOB_NOT_FOUND", Message: "job not found"}
	ErrJobNotRunning      = &JobError{Code: "JOB_NOT_RUNNING", Message: "job is not running"}
	ErrJobNotRerunnable   = &JobError{Code: "JOB_NOT_RERUNNABLE", Message: "job is not rerunnable"}
	ErrJobAlreadyRunning  = &JobError{Code: "JOB_ALREADY_RUNNING", Message: "job is already running"}
)

type JobError struct {
	Code    string
	Message string
}

func (e *JobError) Error() string {
	return e.Message
}
