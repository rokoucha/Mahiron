package job

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/21S1298001/mahiron/internal/job/run"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
)

func newTestManager(t *testing.T) *JobManager {
	t.Helper()
	mgr, err := NewManager(Config{MaxHistory: 10})
	if err != nil {
		t.Fatal(err)
	}
	return mgr
}

func waitJob(t *testing.T, mgr *JobManager, id string) *Job {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	item, err := mgr.Wait(ctx, id)
	if err != nil {
		t.Fatalf("Wait(%q): %v", id, err)
	}
	return item
}

func TestEnqueueAndComplete(t *testing.T) {
	mgr := newTestManager(t)

	done := make(chan struct{})
	mgr.Register(JobDefinition{
		Key:  "test-job",
		Name: "Test Job",
		Handler: func(ctx context.Context) error {
			close(done)
			return nil
		},
		IsRerunnable: true,
	})

	id, err := mgr.Enqueue("test-job")
	if err != nil {
		t.Fatal(err)
	}
	if id == "" {
		t.Fatal("expected non-empty execution id")
	}

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("handler not called")
	}

	job := waitJob(t, mgr, id)
	if job.Status != StatusFinished {
		t.Errorf("expected status finished, got %s", job.Status)
	}
	if job.HasFailed {
		t.Error("expected job not to have failed")
	}
}

func TestEnqueueUnknownKey(t *testing.T) {
	mgr := newTestManager(t)
	_, err := mgr.Enqueue("nonexistent")
	if !errors.Is(err, ErrDefinitionNotFound) {
		t.Errorf("expected ErrDefinitionNotFound, got %v", err)
	}
}

func TestAbort(t *testing.T) {
	mgr := newTestManager(t)

	handlerStarted := make(chan struct{})
	handlerCancelled := make(chan struct{})

	mgr.Register(JobDefinition{
		Key:  "long-job",
		Name: "Long Job",
		Handler: func(ctx context.Context) error {
			close(handlerStarted)
			<-ctx.Done()
			close(handlerCancelled)
			return ctx.Err()
		},
		IsRerunnable: true,
	})

	id, err := mgr.Enqueue("long-job")
	if err != nil {
		t.Fatal(err)
	}

	<-handlerStarted

	if err := mgr.Abort(id); err != nil {
		t.Fatal(err)
	}

	select {
	case <-handlerCancelled:
	case <-time.After(time.Second):
		t.Fatal("handler not cancelled")
	}

	job := waitJob(t, mgr, id)
	if !job.HasAborted {
		t.Error("expected HasAborted to be true")
	}
	if !job.IsAborting {
		t.Error("expected IsAborting to be true")
	}
}

func TestAbortNotRunning(t *testing.T) {
	mgr := newTestManager(t)
	err := mgr.Abort("nonexistent")
	if !errors.Is(err, ErrJobNotRunning) {
		t.Errorf("expected ErrJobNotRunning, got %v", err)
	}
}

func TestRerun(t *testing.T) {
	mgr := newTestManager(t)

	callCount := 0
	done := make(chan struct{}, 2)

	mgr.Register(JobDefinition{
		Key:  "rerun-job",
		Name: "Rerun Job",
		Handler: func(ctx context.Context) error {
			callCount++
			done <- struct{}{}
			return nil
		},
		IsRerunnable: true,
	})

	id, err := mgr.Enqueue("rerun-job")
	if err != nil {
		t.Fatal(err)
	}

	<-done
	waitJob(t, mgr, id)

	if err := mgr.Rerun(id); err != nil {
		t.Fatal(err)
	}

	<-done
	jobs := mgr.GetJobs()
	waitJob(t, mgr, jobs[len(jobs)-1].ID)

	if callCount != 2 {
		t.Errorf("expected 2 calls, got %d", callCount)
	}
}

func TestRerunNotRerunnable(t *testing.T) {
	mgr := newTestManager(t)

	done := make(chan struct{})
	mgr.Register(JobDefinition{
		Key:  "no-rerun",
		Name: "No Rerun",
		Handler: func(ctx context.Context) error {
			close(done)
			return nil
		},
		IsRerunnable: false,
	})

	id, err := mgr.Enqueue("no-rerun")
	if err != nil {
		t.Fatal(err)
	}

	<-done
	waitJob(t, mgr, id)

	err = mgr.Rerun(id)
	if !errors.Is(err, ErrJobNotRerunnable) {
		t.Errorf("expected ErrJobNotRerunnable, got %v", err)
	}
}

func TestHandlerError(t *testing.T) {
	mgr := newTestManager(t)

	done := make(chan struct{})
	mgr.Register(JobDefinition{
		Key:  "fail-job",
		Name: "Fail Job",
		Handler: func(ctx context.Context) error {
			close(done)
			return errors.New("something went wrong")
		},
		IsRerunnable: true,
	})

	id, err := mgr.Enqueue("fail-job")
	if err != nil {
		t.Fatal(err)
	}

	<-done
	job := waitJob(t, mgr, id)
	if !job.HasFailed {
		t.Error("expected HasFailed to be true")
	}
	if job.Error != "something went wrong" {
		t.Errorf("expected error message, got %s", job.Error)
	}
}

func TestJobResultReportedInHistoryAndEvents(t *testing.T) {
	publisher := &fakeEventPublisher{}
	mgr, err := NewManager(Config{MaxHistory: 10}, publisher)
	if err != nil {
		t.Fatal(err)
	}
	reported := run.Result{
		Kind:    "service_scan",
		Summary: "GR/27: 1 services",
		Counts:  map[string]int{"services": 1},
		Items: []run.Item{{
			Kind:    "service",
			Summary: "NHK",
			Data:    map[string]any{"name": "NHK"},
		}},
	}
	mgr.Register(JobDefinition{
		Key:  "report-job",
		Name: "Report Job",
		Handler: func(ctx context.Context) error {
			run.Set(ctx, reported)
			return nil
		},
	})

	id, err := mgr.Enqueue("report-job")
	if err != nil {
		t.Fatal(err)
	}
	finished := waitJob(t, mgr, id)
	if finished.Result == nil || finished.Result.Summary != reported.Summary {
		t.Fatalf("job result = %#v, want %#v", finished.Result, reported)
	}
	if len(publisher.events) == 0 {
		t.Fatal("expected job update events")
	}
	found := false
	for _, event := range publisher.events {
		result, ok := event.data["result"].(*run.Result)
		if ok && result.Summary == reported.Summary {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("result event not found in %#v", publisher.events)
	}
}

func TestGetJobSchedules(t *testing.T) {
	mgr := newTestManager(t)

	mgr.Register(JobDefinition{
		Key:          "scheduled-job",
		Name:         "Scheduled Job",
		Handler:      func(ctx context.Context) error { return nil },
		IsRerunnable: true,
	})

	if err := mgr.AddSchedule("scheduled-job", "5 6 * * *"); err != nil {
		t.Fatal(err)
	}

	mgr.Start()
	defer func() { _ = mgr.Shutdown(context.Background()) }()

	schedules := mgr.GetJobSchedules()
	if len(schedules) != 1 {
		t.Fatalf("expected 1 schedule, got %d", len(schedules))
	}
	if schedules[0].Key != "scheduled-job" {
		t.Errorf("expected key scheduled-job, got %s", schedules[0].Key)
	}
	if schedules[0].Schedule != "5 6 * * *" {
		t.Errorf("expected schedule '5 6 * * *', got %s", schedules[0].Schedule)
	}
	if schedules[0].JobName != "Scheduled Job" {
		t.Errorf("expected job name 'Scheduled Job', got %s", schedules[0].JobName)
	}
}

func TestRunSchedule(t *testing.T) {
	mgr := newTestManager(t)

	done := make(chan struct{})
	mgr.Register(JobDefinition{
		Key:  "manual-job",
		Name: "Manual Job",
		Handler: func(ctx context.Context) error {
			close(done)
			return nil
		},
		IsRerunnable: true,
	})

	if err := mgr.AddSchedule("manual-job", "0 0 1 1 *"); err != nil {
		t.Fatal(err)
	}

	mgr.Start()
	defer func() { _ = mgr.Shutdown(context.Background()) }()

	if err := mgr.RunSchedule("manual-job"); err != nil {
		t.Fatal(err)
	}

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("manual run did not execute")
	}
}

func TestAddScheduleUnknownKey(t *testing.T) {
	mgr := newTestManager(t)
	err := mgr.AddSchedule("nonexistent", "* * * * *")
	if !errors.Is(err, ErrDefinitionNotFound) {
		t.Errorf("expected ErrDefinitionNotFound, got %v", err)
	}
}

func TestMaxHistory(t *testing.T) {
	mgr := newTestManager(t)

	done := make(chan struct{}, 15)
	mgr.Register(JobDefinition{
		Key:  "history-job",
		Name: "History Job",
		Handler: func(ctx context.Context) error {
			done <- struct{}{}
			return nil
		},
		IsRerunnable: true,
	})

	for i := 0; i < 15; i++ {
		id, err := mgr.Enqueue("history-job")
		if err != nil {
			t.Fatal(err)
		}
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("history job did not complete")
		}
		waitJob(t, mgr, id)
	}

	jobs := mgr.GetJobs()
	if len(jobs) > 10 {
		t.Errorf("expected at most 10 jobs in history, got %d", len(jobs))
	}
}

func TestScheduleInfoFields(t *testing.T) {
	si := ScheduleInfo{
		Key:      "test-key",
		Schedule: "*/5 * * * *",
		JobKey:   "test-key",
		JobName:  "Test",
	}

	if diff := cmp.Diff(ScheduleInfo{
		Key:      "test-key",
		Schedule: "*/5 * * * *",
		JobKey:   "test-key",
		JobName:  "Test",
	}, si); diff != "" {
		t.Errorf("ScheduleInfo mismatch (-want +got):\n%s", diff)
	}
}

func TestEnqueueSingleton(t *testing.T) {
	mgr := newTestManager(t)

	block := make(chan struct{})
	mgr.Register(JobDefinition{
		Key:  "singleton-job",
		Name: "Singleton Job",
		Handler: func(ctx context.Context) error {
			<-block
			return nil
		},
		IsRerunnable: true,
	})

	id, err := mgr.Enqueue("singleton-job")
	if err != nil {
		t.Fatal(err)
	}

	_, err = mgr.Enqueue("singleton-job")
	if !errors.Is(err, ErrJobAlreadyRunning) {
		t.Errorf("expected ErrJobAlreadyRunning, got %v", err)
	}

	close(block)
	waitJob(t, mgr, id)

	_, err = mgr.Enqueue("singleton-job")
	if err != nil {
		t.Errorf("expected no error after job completed, got %v", err)
	}
}

func TestJobsRunIndependently(t *testing.T) {
	mgr, err := NewManager(Config{MaxHistory: 10, MaxConcurrentJobs: 3})
	if err != nil {
		t.Fatal(err)
	}
	started := make(chan string, 3)
	release := make(chan struct{})
	for _, key := range []string{"one", "two", "three"} {
		key := key
		mgr.Register(JobDefinition{Key: key, Name: key, Handler: func(context.Context) error {
			started <- key
			<-release
			return nil
		}})
		if _, err := mgr.Enqueue(key); err != nil {
			t.Fatal(err)
		}
	}
	for range 3 {
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatal("jobs did not start independently")
		}
	}
	close(release)
}

func TestMaxConcurrentJobsQueuesExcessHandlers(t *testing.T) {
	mgr, err := NewManager(Config{MaxHistory: 10, MaxConcurrentJobs: 2})
	if err != nil {
		t.Fatal(err)
	}
	started := make(chan string, 3)
	release := make(chan struct{}, 3)
	for _, key := range []string{"one", "two", "three"} {
		key := key
		mgr.Register(JobDefinition{Key: key, Handler: func(context.Context) error {
			started <- key
			<-release
			return nil
		}})
		if _, err := mgr.Enqueue(key); err != nil {
			t.Fatal(err)
		}
	}
	for range 2 {
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatal("first two jobs did not start")
		}
	}
	select {
	case key := <-started:
		t.Fatalf("job %q started above the concurrency limit", key)
	case <-time.After(50 * time.Millisecond):
	}
	release <- struct{}{}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("queued job did not start after a slot was released")
	}
	release <- struct{}{}
	release <- struct{}{}
}

func TestJobWaitsForDependenciesWhileOtherJobsCanRun(t *testing.T) {
	mgr, err := NewManager(Config{MaxConcurrentJobs: 2})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = mgr.Shutdown(context.Background()) }()

	release := make(chan struct{})
	started := make(chan string, 2)
	if _, err := mgr.EnqueueDefinition(JobDefinition{Key: "scan", Handler: func(context.Context) error {
		started <- "scan"
		<-release
		return nil
	}}); err != nil {
		t.Fatal(err)
	}
	if got := <-started; got != "scan" {
		t.Fatalf("started job = %q, want scan", got)
	}
	if _, err := mgr.EnqueueDefinition(JobDefinition{Key: "gather", DependsOn: []string{"scan"}, Handler: func(context.Context) error {
		started <- "gather"
		return nil
	}}); err != nil {
		t.Fatal(err)
	}
	if _, err := mgr.EnqueueDefinition(JobDefinition{Key: "independent", Handler: func(context.Context) error {
		started <- "independent"
		return nil
	}}); err != nil {
		t.Fatal(err)
	}
	if got := <-started; got != "independent" {
		t.Fatalf("started job while scan is blocked = %q, want independent", got)
	}
	close(release)
	if got := <-started; got != "gather" {
		t.Fatalf("dependent started job = %q, want gather", got)
	}
}

func TestRetryStandbyReleasesConcurrencySlot(t *testing.T) {
	mgr, err := NewManager(Config{MaxHistory: 10, MaxConcurrentJobs: 1})
	if err != nil {
		t.Fatal(err)
	}
	firstAttempt := make(chan struct{})
	retryStarted := make(chan struct{})
	attempts := 0
	mgr.Register(JobDefinition{
		Key: "retry", RetryDelays: []time.Duration{200 * time.Millisecond},
		Handler: func(context.Context) error {
			attempts++
			if attempts == 1 {
				close(firstAttempt)
				return errors.New("retry me")
			}
			close(retryStarted)
			return nil
		},
	})
	otherStarted := make(chan struct{})
	releaseOther := make(chan struct{})
	mgr.Register(JobDefinition{Key: "other", Handler: func(context.Context) error {
		close(otherStarted)
		<-releaseOther
		return nil
	}})
	if _, err := mgr.Enqueue("retry"); err != nil {
		t.Fatal(err)
	}
	if _, err := mgr.Enqueue("other"); err != nil {
		t.Fatal(err)
	}
	<-firstAttempt
	select {
	case <-otherStarted:
	case <-time.After(time.Second):
		t.Fatal("standby retry retained the concurrency slot")
	}
	select {
	case <-retryStarted:
		t.Fatal("retry started while the only slot was occupied")
	case <-time.After(250 * time.Millisecond):
	}
	close(releaseOther)
	select {
	case <-retryStarted:
	case <-time.After(time.Second):
		t.Fatal("retry did not start after the slot was released")
	}
}

func TestShutdownFinishesQueuedRunningAndStandbyJobs(t *testing.T) {
	mgr, err := NewManager(Config{MaxHistory: 10, MaxConcurrentJobs: 1})
	if err != nil {
		t.Fatal(err)
	}
	standbyID, err := mgr.EnqueueDefinition(JobDefinition{
		Key: "standby", RetryDelays: []time.Duration{time.Hour},
		Handler: func(context.Context) error { return errors.New("retry later") },
	})
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		jobs := mgr.GetJobs()
		if len(jobs) == 1 && jobs[0].Status == StatusStandby {
			break
		}
		time.Sleep(time.Millisecond)
	}
	runningStarted := make(chan struct{})
	runningID, err := mgr.EnqueueDefinition(JobDefinition{Key: "running", Handler: func(ctx context.Context) error {
		close(runningStarted)
		<-ctx.Done()
		return ctx.Err()
	}})
	if err != nil {
		t.Fatal(err)
	}
	<-runningStarted
	queuedID, err := mgr.EnqueueDefinition(JobDefinition{Key: "queued", Handler: func(context.Context) error {
		t.Fatal("queued handler ran during shutdown")
		return nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	if err := mgr.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{standbyID, runningID, queuedID} {
		job := waitJob(t, mgr, id)
		if job.Status != StatusFinished || job.HasFailed {
			t.Fatalf("job %q after shutdown = %#v", id, job)
		}
	}
	for _, id := range []string{standbyID, queuedID} {
		job := waitJob(t, mgr, id)
		if !job.HasAborted {
			t.Fatalf("non-running job %q was not aborted during shutdown: %#v", id, job)
		}
	}
}

func TestGetActiveJobKeysByPrefix(t *testing.T) {
	mgr := newTestManager(t)
	release := make(chan struct{})
	mgr.Register(JobDefinition{Key: "epg-gather:nid:1", Handler: func(context.Context) error { <-release; return nil }})
	mgr.Register(JobDefinition{Key: "epg-gather:nid:2", Handler: func(context.Context) error { <-release; return nil }})
	mgr.Register(JobDefinition{Key: "service-scan:GR:27", Handler: func(context.Context) error { <-release; return nil }})
	for _, key := range []string{"epg-gather:nid:1", "epg-gather:nid:2", "service-scan:GR:27"} {
		if _, err := mgr.Enqueue(key); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() { close(release) })
	keys := mgr.GetActiveJobKeysByPrefix("epg-gather:")
	if diff := cmp.Diff([]string{"epg-gather:nid:1", "epg-gather:nid:2"}, keys, cmpopts.SortSlices(func(a, b string) bool { return a < b })); diff != "" {
		t.Errorf("GetActiveJobKeysByPrefix mismatch (-want +got):\n%s\nall jobs: %#v", diff, mgr.GetJobs())
	}
}
