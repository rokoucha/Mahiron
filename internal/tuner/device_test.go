package tuner

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestTunerDeviceCopiesCommandStdout(t *testing.T) {
	device := NewCommandDevice(nil, "printf command-ts")
	var dst bytes.Buffer
	if err := device.Start(context.Background(), &dst); err != nil {
		t.Fatal(err)
	}
	select {
	case <-device.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("tuner device did not finish")
	}
	if err := device.Err(); err != nil {
		t.Fatal(err)
	}
	if got, want := dst.String(), "command-ts"; got != want {
		t.Fatalf("stream = %q, want %q", got, want)
	}
}

func TestTunerDeviceCopiesDVBDevicePath(t *testing.T) {
	path := t.TempDir() + "/dvr0"
	if err := os.WriteFile(path, []byte("dvb-ts"), 0o600); err != nil {
		t.Fatal(err)
	}
	device := NewDVBDevice(nil, "printf command-ts", path)
	var dst bytes.Buffer
	if err := device.Start(context.Background(), &dst); err != nil {
		t.Fatal(err)
	}
	select {
	case <-device.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("tuner device did not finish")
	}
	if err := device.Err(); err != nil {
		t.Fatal(err)
	}
	if got, want := dst.String(), "dvb-ts"; got != want {
		t.Fatalf("stream = %q, want %q", got, want)
	}
}

func TestTunerDeviceStopTerminatesDVBCommand(t *testing.T) {
	device := NewDVBDevice(nil, "sleep 10", "/dev/null")
	if err := device.Start(context.Background(), bytes.NewBuffer(nil)); err != nil {
		t.Fatal(err)
	}
	stopCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := device.Stop(stopCtx); err != nil {
		t.Fatal(err)
	}
	select {
	case <-device.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("tuner device did not stop")
	}
}

func TestTunerDeviceProcessStartedAt(t *testing.T) {
	device := NewCommandDevice(nil, "sleep 10")
	if err := device.Start(context.Background(), bytes.NewBuffer(nil)); err != nil {
		t.Fatal(err)
	}
	startedAt := device.(ProcessUptimeStatus).ProcessStartedAt()
	if startedAt.IsZero() {
		t.Fatal("ProcessStartedAt() is zero for running process")
	}
	stopCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := device.Stop(stopCtx); err != nil {
		t.Fatal(err)
	}
	select {
	case <-device.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("tuner device did not stop")
	}
	if got := device.(ProcessUptimeStatus).ProcessStartedAt(); !got.IsZero() {
		t.Fatalf("ProcessStartedAt() after stop = %v, want zero", got)
	}
}

func TestTunerDeviceStartupRetryCommandPreservesFirstChunk(t *testing.T) {
	dir := t.TempDir()
	countPath := filepath.Join(dir, "count")
	script := writeTestScript(t, dir, fmt.Sprintf(`count=0
if [ -f %[1]q ]; then
  count=$(cat %[1]q)
fi
count=$((count + 1))
printf '%%s' "$count" > %[1]q
if [ "$count" -lt 2 ]; then
  exit 0
fi
printf command-ts
`, countPath))

	device := NewCommandDevice(nil, "sh "+fmt.Sprintf("%q", script), StartupRetryConfig{
		Max:     1,
		Timeout: 200 * time.Millisecond,
		Delay:   time.Millisecond,
	})
	var dst bytes.Buffer
	if err := device.Start(context.Background(), &dst); err != nil {
		t.Fatal(err)
	}
	select {
	case <-device.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("tuner device did not finish")
	}
	if err := device.Err(); err != nil {
		t.Fatal(err)
	}
	if got, want := dst.String(), "command-ts"; got != want {
		t.Fatalf("stream = %q, want %q", got, want)
	}
}

func TestTunerDeviceStartupRetryDVBRespawnsCommand(t *testing.T) {
	dir := t.TempDir()
	script := writeTestScript(t, dir, "sleep 1\n")

	device := NewDVBDevice(nil, "sh "+fmt.Sprintf("%q", script), "/dev/null", StartupRetryConfig{
		Max:     1,
		Timeout: 200 * time.Millisecond,
		Delay:   time.Millisecond,
	})
	dvb := device.(*dvbDevice)
	openAttempts := 0
	dvb.openAfterStart = func() (io.ReadCloser, error) {
		openAttempts++
		if openAttempts == 1 {
			return nil, errors.New("dvr not ready")
		}
		return io.NopCloser(bytes.NewReader([]byte("dvb-ts"))), nil
	}
	var dst bytes.Buffer
	if err := device.Start(context.Background(), &dst); err != nil {
		t.Fatal(err)
	}
	select {
	case <-device.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("tuner device did not finish")
	}
	if err := device.Err(); err != nil {
		t.Fatal(err)
	}
	if got, want := dst.String(), "dvb-ts"; got != want {
		t.Fatalf("stream = %q, want %q", got, want)
	}
	if got, want := openAttempts, 2; got != want {
		t.Fatalf("open attempts = %d, want %d", got, want)
	}
}

func TestTunerDeviceStartupRetryFailsWhenNoDataArrives(t *testing.T) {
	device := NewCommandDevice(nil, "sleep 10", StartupRetryConfig{
		Max:     1,
		Timeout: 20 * time.Millisecond,
		Delay:   time.Millisecond,
	})
	err := device.Start(context.Background(), bytes.NewBuffer(nil))
	if err == nil {
		t.Fatal("Start() error = nil, want error")
	}
	if errors.Is(err, context.Canceled) {
		t.Fatalf("Start() error = %v, want startup failure", err)
	}
	if status := device.(ProcessStatus).ProcessStatus(); status.PID != 0 {
		t.Fatalf("pid = %d, want stopped process", status.PID)
	}
}

func TestClassifyProcessExit(t *testing.T) {
	tests := []struct {
		name        string
		err         error
		copiedBytes int64
		want        string
	}{
		{name: "success with data", err: nil, copiedBytes: 1, want: tunerProcessExitSuccess},
		{name: "success with no data", err: nil, copiedBytes: 0, want: tunerProcessExitEmpty},
		{name: "canceled with no data", err: context.Canceled, copiedBytes: 0, want: tunerProcessExitCanceled},
		{name: "failure with no data", err: errors.New("boom"), copiedBytes: 0, want: tunerProcessExitFailure},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := classifyProcessExit(tt.err, tt.copiedBytes); got != tt.want {
				t.Fatalf("classifyProcessExit(%v, %d) = %q, want %q", tt.err, tt.copiedBytes, got, tt.want)
			}
		})
	}
}

func TestTunerDeviceSucceedsWithNoDataDoesNotError(t *testing.T) {
	// A command like `curl -fsSL <url>` can exit 0 while writing nothing to
	// stdout (e.g. an upstream HTTP error the shell command itself doesn't
	// treat as a failure). Device.Err() must stay untouched by the "empty"
	// classification: it is a metrics/log concern, not a stream error.
	device := NewCommandDevice(nil, "true")
	if err := device.Start(context.Background(), bytes.NewBuffer(nil)); err != nil {
		t.Fatal(err)
	}
	select {
	case <-device.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("tuner device did not finish")
	}
	if err := device.Err(); err != nil {
		t.Fatalf("Err() = %v, want nil", err)
	}
}

func writeTestScript(t *testing.T, dir, body string) string {
	t.Helper()
	path := filepath.Join(dir, "script.sh")
	if err := os.WriteFile(path, []byte("#!/bin/sh\n"+body), 0o700); err != nil {
		t.Fatal(err)
	}
	return path
}
