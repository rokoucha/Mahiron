package api

import (
	"context"
	"net/http"

	"github.com/21S1298001/mahiron/internal/job"
	"github.com/21S1298001/mahiron/internal/job/run"
	apigen "github.com/21S1298001/mahiron/internal/web/api/gen"
)

func (h *Handler) GetJobs(ctx context.Context) (apigen.GetJobsRes, error) {
	jobs := h.jobManager.GetJobs()
	result := make(apigen.GetJobsOKApplicationJSON, len(jobs))
	for i, j := range jobs {
		result[i] = *apiJobItem(j)
	}
	return &result, nil
}

func (h *Handler) GetJobSchedules(ctx context.Context) (apigen.GetJobSchedulesRes, error) {
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

func (h *Handler) AbortJob(ctx context.Context, params apigen.AbortJobParams) (apigen.AbortJobRes, error) {
	if err := h.jobManager.Abort(params.ID); err != nil {
		return conflict(err.Error()), nil
	}
	return &apigen.AbortJobAccepted{}, nil
}

func (h *Handler) RerunJob(ctx context.Context, params apigen.RerunJobParams) (apigen.RerunJobRes, error) {
	if err := h.jobManager.Rerun(params.ID); err != nil {
		if e, ok := err.(*job.JobError); ok && e.Code == "JOB_NOT_FOUND" {
			return notFound("job not found"), nil
		}
		return conflict(err.Error()), nil
	}
	return &apigen.RerunJobAccepted{}, nil
}

func (h *Handler) RunJobSchedule(ctx context.Context, params apigen.RunJobScheduleParams) (apigen.RunJobScheduleRes, error) {
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
	if j.Result != nil {
		item.Result = apigen.NewOptJobResult(apiJobResult(j.Result))
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
	if j.NextRunAt != nil {
		item.NextRunAt = apigen.NewOptUnixtimeMS(apigen.UnixtimeMS(j.NextRunAt.UnixMilli()))
	}
	return item
}

func apiJobResult(result *run.Result) apigen.JobResult {
	out := apigen.JobResult{Kind: result.Kind}
	if result.Summary != "" {
		out.Summary = apigen.NewOptString(result.Summary)
	}
	if len(result.Counts) > 0 {
		counts := make(apigen.JobResultCounts, len(result.Counts))
		for key, value := range result.Counts {
			counts[key] = value
		}
		out.Counts = apigen.NewOptJobResultCounts(counts)
	}
	if len(result.Items) > 0 {
		out.Items = make([]apigen.JobResultItem, len(result.Items))
		for i, item := range result.Items {
			out.Items[i] = apigen.JobResultItem{Kind: item.Kind}
			if item.Summary != "" {
				out.Items[i].Summary = apigen.NewOptString(item.Summary)
			}
			if len(item.Data) > 0 {
				data := make(apigen.JobResultItemData, len(item.Data))
				for key, value := range item.Data {
					data[key] = apiRawJSON(value)
				}
				out.Items[i].Data = apigen.NewOptJobResultItemData(data)
			}
		}
	}
	if len(result.Warnings) > 0 {
		out.Warnings = append([]string(nil), result.Warnings...)
	}
	return out
}

func apiJobStatus(status job.JobStatus) apigen.JobItemStatus {
	switch status {
	case job.StatusQueued:
		return apigen.JobItemStatusQueued
	case job.StatusStandby:
		return apigen.JobItemStatusStandby
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
