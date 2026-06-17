package job

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
)

func newTestManager(t *testing.T) *JobManager {
	t.Helper()
	mgr, err := NewManager(Config{MaxHistory: 10})
	if err != nil {
		t.Fatal(err)
	}
	return mgr
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

	time.Sleep(50 * time.Millisecond)

	jobs := mgr.GetJobs()
	if len(jobs) != 1 {
		t.Fatalf("expected 1 job, got %d", len(jobs))
	}
	if jobs[0].Status != StatusFinished {
		t.Errorf("expected status finished, got %s", jobs[0].Status)
	}
	if jobs[0].HasFailed {
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

	time.Sleep(50 * time.Millisecond)

	jobs := mgr.GetJobs()
	if len(jobs) != 1 {
		t.Fatalf("expected 1 job, got %d", len(jobs))
	}
	if !jobs[0].HasAborted {
		t.Error("expected HasAborted to be true")
	}
	if !jobs[0].IsAborting {
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
	time.Sleep(50 * time.Millisecond)

	if err := mgr.Rerun(id); err != nil {
		t.Fatal(err)
	}

	<-done
	time.Sleep(50 * time.Millisecond)

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
	time.Sleep(50 * time.Millisecond)

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

	_, err := mgr.Enqueue("fail-job")
	if err != nil {
		t.Fatal(err)
	}

	<-done
	time.Sleep(50 * time.Millisecond)

	jobs := mgr.GetJobs()
	if len(jobs) != 1 {
		t.Fatalf("expected 1 job, got %d", len(jobs))
	}
	if !jobs[0].HasFailed {
		t.Error("expected HasFailed to be true")
	}
	if jobs[0].Error != "something went wrong" {
		t.Errorf("expected error message, got %s", jobs[0].Error)
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

	for i := 0; i < 15; i++ {
		done := make(chan struct{})
		mgr.Register(JobDefinition{
			Key:  "history-job",
			Name: "History Job",
			Handler: func(ctx context.Context) error {
				close(done)
				return nil
			},
			IsRerunnable: true,
		})
		_, _ = mgr.Enqueue("history-job")
		<-done
	}

	time.Sleep(50 * time.Millisecond)

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

	_, err := mgr.Enqueue("singleton-job")
	if err != nil {
		t.Fatal(err)
	}

	_, err = mgr.Enqueue("singleton-job")
	if !errors.Is(err, ErrJobAlreadyRunning) {
		t.Errorf("expected ErrJobAlreadyRunning, got %v", err)
	}

	close(block)
	time.Sleep(50 * time.Millisecond)

	_, err = mgr.Enqueue("singleton-job")
	if err != nil {
		t.Errorf("expected no error after job completed, got %v", err)
	}
}
