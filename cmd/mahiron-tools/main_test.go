package main

import (
	"bytes"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/21S1298001/mahiron/internal/probe"
	"github.com/21S1298001/mahiron/ts"
)

func TestProbeFileAndJSON(t *testing.T) {
	pat, err := ts.BuildPATSection(1, 101, 0x1000, 0)
	if err != nil {
		t.Fatal(err)
	}
	p := bytes.Repeat([]byte{0xff}, 188)
	p[0], p[1], p[2], p[3], p[4] = 0x47, 0x40, 0, 0x10, 0
	copy(p[5:], pat)
	path := t.TempDir() + "/input.ts"
	if err := os.WriteFile(path, p, 0600); err != nil {
		t.Fatal(err)
	}
	for _, jsonOutput := range []bool{false, true} {
		var out, stderr bytes.Buffer
		args := []string{"probe", "--duration", "0"}
		if jsonOutput {
			args = append(args, "--json")
		}
		args = append(args, path)
		if code := run(args, &out, &stderr); code != 0 {
			t.Fatalf("exit %d: %s", code, &stderr)
		}
		if jsonOutput {
			var r probe.Report
			if err := json.Unmarshal(out.Bytes(), &r); err != nil {
				t.Fatal(err)
			}
			if len(r.Services) != 1 || r.Services[0].ID != 101 || r.Services[0].Scrambling != "unknown" {
				t.Fatalf("report: %+v", r)
			}
		} else if !strings.Contains(out.String(), "Service 101") || !strings.Contains(out.String(), "PMT/MPT not received") {
			t.Fatalf("output: %s", &out)
		}
	}
}
func TestProbeDurationInterruptsIdlePipe(t *testing.T) {
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = r.Close() }()
	defer func() { _ = w.Close() }()
	original := os.Stdin
	os.Stdin = r
	defer func() { os.Stdin = original }()
	// Enough data to detect TS, then keep the producer open without more bytes.
	p := bytes.Repeat([]byte{0xff}, 188)
	p[0], p[1], p[2], p[3] = 0x47, 0x1f, 0xff, 0x10
	if _, err := w.Write(bytes.Repeat(p, 5)); err != nil {
		t.Fatal(err)
	}
	var out, stderr bytes.Buffer
	done := make(chan int, 1)
	go func() { done <- run([]string{"probe", "--duration", "30ms", "--json", "-"}, &out, &stderr) }()
	select {
	case code := <-done:
		if code != 0 {
			t.Fatalf("exit %d: %s", code, &stderr)
		}
		var report probe.Report
		if err := json.Unmarshal(out.Bytes(), &report); err != nil {
			t.Fatal(err)
		}
		if report.Format != "ts" || report.Packets == 0 {
			t.Fatalf("report: %+v", report)
		}
	case <-time.After(3 * time.Second):
		_ = r.Close()
		<-done
		t.Fatal("duration did not interrupt idle pipe")
	}
}
func TestInvalidArguments(t *testing.T) {
	for _, args := range [][]string{{"scan"}, {"probe", "--relative-ts", "16"}, {"probe", "--format", "mmt"}, {"probe", "--duration", "-1s"}, {"probe", "--format", "tlv", "--relative-ts", "1"}} {
		var out, stderr bytes.Buffer
		if code := run(args, &out, &stderr); code != 2 {
			t.Errorf("%v: exit %d", args, code)
		}
	}
}

func TestProbeDurationRetainsDetectionBuffer(t *testing.T) {
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = r.Close() }()
	defer func() { _ = w.Close() }()
	original := os.Stdin
	os.Stdin = r
	defer func() { os.Stdin = original }()
	pat, err := ts.BuildPATSection(1, 101, 0x1000, 0)
	if err != nil {
		t.Fatal(err)
	}
	p := bytes.Repeat([]byte{0xff}, 188)
	p[0], p[1], p[2], p[3], p[4] = 0x47, 0x40, 0, 0x10, 0
	copy(p[5:], pat)
	if _, err := w.Write(p); err != nil {
		t.Fatal(err)
	}
	var out, stderr bytes.Buffer
	code := run([]string{"probe", "--duration", "30ms", "--json", "-"}, &out, &stderr)
	if code != 0 {
		t.Fatalf("exit %d: %s", code, &stderr)
	}
	var report probe.Report
	if err := json.Unmarshal(out.Bytes(), &report); err != nil {
		t.Fatal(err)
	}
	if len(report.Services) != 1 {
		t.Fatalf("buffer lost: %s", &out)
	}
}

func TestProbeDurationWithoutInputReturnsReport(t *testing.T) {
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = r.Close() }()
	defer func() { _ = w.Close() }()
	original := os.Stdin
	os.Stdin = r
	defer func() { os.Stdin = original }()
	var out, stderr bytes.Buffer
	if code := run([]string{"probe", "--duration", "30ms", "--json", "-"}, &out, &stderr); code != 0 {
		t.Fatalf("exit %d: %s", code, &stderr)
	}
	var report probe.Report
	if err := json.Unmarshal(out.Bytes(), &report); err != nil {
		t.Fatal(err)
	}
	if report.Format != "unknown" || report.StopReason != "duration" || report.TimeBasis != "read_elapsed" || len(report.Warnings) == 0 {
		t.Fatalf("report: %+v", report)
	}
}
