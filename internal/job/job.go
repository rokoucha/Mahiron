package job

import (
	"context"
	"time"

	"github.com/21S1298001/mahiron/internal/job/run"
)

type JobStatus string

const (
	StatusQueued   JobStatus = "queued"
	StatusStandby  JobStatus = "standby"
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
	HasSkipped bool
	HasFailed  bool
	Error      string
	Result     *run.Result
	CreatedAt  time.Time
	UpdatedAt  time.Time
	StartedAt  *time.Time
	FinishedAt *time.Time
	NextRunAt  *time.Time
	definition *JobDefinition
	done       chan struct{}
}

type JobDefinition struct {
	Key           string
	Name          string
	Handler       func(ctx context.Context) error
	DependsOn     []string
	ExclusiveKeys []string
	IsRerunnable  bool
	RetryDelays   []time.Duration
	RetryIf       func(error) bool
}

type ScheduleInfo struct {
	Key, Schedule, JobKey, JobName string
}

func (j *Job) EventData() map[string]any {
	data := map[string]any{
		"key":        j.Key,
		"name":       j.Name,
		"id":         j.ID,
		"status":     string(j.Status),
		"retryCount": j.RetryCount,
		"isAborting": j.IsAborting,
		"hasAborted": j.HasAborted,
		"hasSkipped": j.HasSkipped,
		"hasFailed":  j.HasFailed,
		"createdAt":  j.CreatedAt.UnixMilli(),
		"updatedAt":  j.UpdatedAt.UnixMilli(),
	}
	if j.definition != nil {
		data["isRerunnable"] = j.definition.IsRerunnable
		data["retryOnAbort"] = false
		if len(j.definition.RetryDelays) > 0 {
			data["retryOnFail"] = true
			data["retryMax"] = len(j.definition.RetryDelays)
			data["retryDelay"] = int(j.definition.RetryDelays[0].Milliseconds())
		}
	}
	if j.Error != "" {
		data["error"] = j.Error
	}
	if j.Result != nil {
		data["result"] = run.Clone(j.Result)
	}
	if j.StartedAt != nil {
		data["startedAt"] = j.StartedAt.UnixMilli()
	}
	if j.FinishedAt != nil {
		data["finishedAt"] = j.FinishedAt.UnixMilli()
		if j.StartedAt != nil {
			data["duration"] = int(j.FinishedAt.Sub(*j.StartedAt).Milliseconds())
		}
	}
	if j.NextRunAt != nil {
		data["nextRunAt"] = j.NextRunAt.UnixMilli()
	}
	return data
}

func (s ScheduleInfo) EventData() map[string]any {
	return map[string]any{
		"key":      s.Key,
		"schedule": s.Schedule,
		"job": map[string]any{
			"key":  s.JobKey,
			"name": s.JobName,
		},
	}
}

type JobError struct{ Code, Message string }

func (e *JobError) Error() string { return e.Message }

var (
	ErrDefinitionNotFound = &JobError{Code: "DEFINITION_NOT_FOUND", Message: "job definition not found"}
	ErrInvalidDefinition  = &JobError{Code: "INVALID_DEFINITION", Message: "invalid job definition"}
	ErrManagerShutdown    = &JobError{Code: "MANAGER_SHUTDOWN", Message: "job manager is shut down"}
	ErrJobNotFound        = &JobError{Code: "JOB_NOT_FOUND", Message: "job not found"}
	ErrJobNotRunning      = &JobError{Code: "JOB_NOT_RUNNING", Message: "job is not running"}
	ErrJobNotRerunnable   = &JobError{Code: "JOB_NOT_RERUNNABLE", Message: "job is not rerunnable"}
	ErrJobAlreadyRunning  = &JobError{Code: "JOB_ALREADY_RUNNING", Message: "job is already queued or running"}
)
