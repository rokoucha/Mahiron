package api

import (
	"context"
	"net/http"

	"github.com/21S1298001/Mahiron5/job"
	apigen "github.com/21S1298001/Mahiron5/web/api/gen"
)

func GetJobs(ctx context.Context, h *Handler) (apigen.GetJobsRes, error) {
	jobs := h.jobManager.GetJobs()
	result := make(apigen.GetJobsOKApplicationJSON, len(jobs))
	for i, j := range jobs {
		result[i] = *apiJobItem(j)
	}
	return &result, nil
}

func GetJobSchedules(ctx context.Context, h *Handler) (apigen.GetJobSchedulesRes, error) {
	schedules := h.jobManager.GetJobSchedules()
	result := make(apigen.GetJobSchedulesOKApplicationJSON, len(schedules))
	for i, s := range schedules {
		result[i] = apigen.JobScheduleItem{
			Key:      s.Key,
			Schedule: s.Schedule,
			Job: apigen.JobScheduleItemJob{
				Key:  s.JobKey,
				Name: s.JobName,
			},
		}
	}
	return &result, nil
}

func AbortJob(ctx context.Context, h *Handler, params apigen.AbortJobParams) (apigen.AbortJobRes, error) {
	if err := h.jobManager.Abort(params.ID); err != nil {
		return conflict(err.Error()), nil
	}
	return &apigen.AbortJobAccepted{}, nil
}

func RerunJob(ctx context.Context, h *Handler, params apigen.RerunJobParams) (apigen.RerunJobRes, error) {
	if err := h.jobManager.Rerun(params.ID); err != nil {
		if e, ok := err.(*job.JobError); ok && e.Code == "JOB_NOT_FOUND" {
			return notFound("job not found"), nil
		}
		return conflict(err.Error()), nil
	}
	return &apigen.RerunJobAccepted{}, nil
}

func RunJobSchedule(ctx context.Context, h *Handler, params apigen.RunJobScheduleParams) (apigen.RunJobScheduleRes, error) {
	if err := h.jobManager.RunSchedule(params.Key); err != nil {
		if e, ok := err.(*job.JobError); ok && (e.Code == "DEFINITION_NOT_FOUND" || e.Code == "JOB_NOT_FOUND") {
			return notFound("job schedule not found"), nil
		}
		return conflict(err.Error()), nil
	}
	return &apigen.RunJobScheduleAccepted{}, nil
}

func apiJobItem(j *job.Job) *apigen.JobItem {
	item := &apigen.JobItem{
		Key:        j.Key,
		Name:       j.Name,
		ID:         j.ID,
		Status:     apiJobStatus(j.Status),
		RetryCount: j.RetryCount,
		IsAborting: j.IsAborting,
		HasAborted: apigen.NewOptBool(j.HasAborted),
		HasFailed:  apigen.NewOptBool(j.HasFailed),
		CreatedAt:  apigen.UnixtimeMS(j.CreatedAt.UnixMilli()),
		UpdatedAt:  apigen.UnixtimeMS(j.UpdatedAt.UnixMilli()),
	}
	if j.Error != "" {
		item.Error = apigen.NewOptString(j.Error)
	}
	if j.StartedAt != nil {
		item.StartedAt = apigen.NewOptUnixtimeMS(apigen.UnixtimeMS(j.StartedAt.UnixMilli()))
	}
	if j.FinishedAt != nil {
		item.FinishedAt = apigen.NewOptUnixtimeMS(apigen.UnixtimeMS(j.FinishedAt.UnixMilli()))
		if j.StartedAt != nil {
			item.Duration = apigen.NewOptInt(int(j.FinishedAt.Sub(*j.StartedAt).Milliseconds()))
		}
	}
	return item
}

func apiJobStatus(status job.JobStatus) apigen.JobItemStatus {
	switch status {
	case job.StatusQueued:
		return apigen.JobItemStatusQueued
	case job.StatusRunning:
		return apigen.JobItemStatusRunning
	case job.StatusFinished:
		return apigen.JobItemStatusFinished
	default:
		return apigen.JobItemStatusQueued
	}
}

func conflict(reason string) *apigen.ErrorStatusCode {
	return &apigen.ErrorStatusCode{
		StatusCode: http.StatusConflict,
		Response: apigen.Error{
			Code:   apigen.NewOptInt(http.StatusConflict),
			Reason: apigen.NewOptString(reason),
		},
	}
}
