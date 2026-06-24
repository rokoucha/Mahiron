package job

import (
	"context"
	"time"
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
	HasFailed  bool
	Error      string
	CreatedAt  time.Time
	UpdatedAt  time.Time
	StartedAt  *time.Time
	FinishedAt *time.Time
	NextRunAt  *time.Time
	definition *JobDefinition
	done       chan struct{}
}

type JobDefinition struct {
	Key          string
	Name         string
	Handler      func(ctx context.Context) error
	IsRerunnable bool
	RetryDelays  []time.Duration
	RetryIf      func(error) bool
}

type ScheduleInfo struct {
	Key, Schedule, JobKey, JobName string
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
