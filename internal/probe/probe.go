package probe

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sort"
	"time"

	"github.com/21S1298001/mahiron/mmt"
	"github.com/21S1298001/mahiron/packet"
	"github.com/21S1298001/mahiron/ts"
)

type Options struct {
	Format     string
	RelativeTS uint8
}

// Run reads until EOF or cancellation. The caller must interrupt a blocking
// Read when cancelling (for example by closing its input).
func Run(ctx context.Context, r io.Reader, opt Options) (*Report, error) {
	start := time.Now()
	report := &Report{TimeBasis: "read_elapsed", Services: []*Service{}}
	parseCtx := ctx
	format := opt.Format
	if format == "" {
		format = "auto"
	}
	if format == "auto" {
		var err error
		r, format, err = detect(ctx, r)
		if err != nil {
			if ctx.Err() == nil {
				return nil, err
			}
			report.Format = "unknown"
			report.Duration = time.Since(start)
			report.StopReason = stopReason(ctx, err)
			report.Warnings = []string{"no recognizable stream observed before cancellation"}
			return report, nil
		}
		if ctx.Err() != nil {
			parseCtx = context.WithoutCancel(ctx)
		}
	}
	report.Format = format
	var err error
	switch format {
	case "ts", "tsmf":
		err = readTS(parseCtx, r, opt, report, start)
	case "tlv":
		if opt.RelativeTS != 0 {
			return nil, fmt.Errorf("--relative-ts requires TS/TSMF input")
		}
		err = readTLV(parseCtx, r, report, start)
	default:
		return nil, fmt.Errorf("unknown format %q", format)
	}
	report.Duration = time.Since(start)
	report.StopReason = stopReason(ctx, err)
	sort.SliceStable(report.Services, func(i, j int) bool {
		a, b := report.Services[i], report.Services[j]
		if a.RelativeTS != b.RelativeTS {
			return a.RelativeTS < b.RelativeTS
		}
		if a.ID != b.ID {
			return a.ID < b.ID
		}
		return a.Flow < b.Flow
	})
	for _, s := range report.Services {
		s.Scrambling = "unknown"
		if s.MapComplete && len(s.Components) > 0 {
			s.Scrambling = "no scrambling observed"
		}
		for i := range s.Components {
			c := &s.Components[i]
			c.Scrambling = c.Stats.Status()
			if c.Scrambling == "scrambling remains" {
				s.Scrambling = c.Scrambling
			} else if c.Scrambling == "unknown" && s.Scrambling != "scrambling remains" {
				s.Scrambling = "unknown"
			}
		}
		label := fmt.Sprintf("service %d", s.ID)
		if s.RelativeTS != 0 {
			label += fmt.Sprintf(" (relative TS %d)", s.RelativeTS)
		}
		if s.Flow != "" {
			label += fmt.Sprintf(" (flow %s)", s.Flow)
		}
		if !s.MapReceived {
			report.Warnings = append(report.Warnings, label+": PMT/MPT not received")
		} else if !s.MapComplete {
			report.Warnings = append(report.Warnings, label+": MPT subsets observed; complete composition unknown")
		}
		if s.Name == "" {
			report.Warnings = append(report.Warnings, label+": service name unavailable")
		}
	}
	if len(report.Services) == 0 {
		report.Warnings = append(report.Warnings, "no services discovered in the observed interval")
	}
	if errors.Is(err, io.EOF) || (ctx.Err() != nil && err != nil) {
		err = nil
	}
	return report, err
}
func detect(ctx context.Context, r io.Reader) (io.Reader, string, error) {
	var buf []byte
	tmp := make([]byte, 4096)
	for ctx.Err() == nil {
		n, err := r.Read(tmp)
		buf = append(buf, tmp[:n]...)
		final := err != nil || len(buf) >= 256<<10
		for _, d := range []packet.Detector{ts.Detector, mmt.Detector} {
			if d.Detect(buf, final) == packet.Matched {
				if ctx.Err() != nil {
					return bytes.NewReader(buf), d.Name(), nil
				}
				return io.MultiReader(bytes.NewReader(buf), r), d.Name(), nil
			}
		}
		if err != nil && !errors.Is(err, io.EOF) {
			return nil, "", err
		}
		if final {
			return nil, "", fmt.Errorf("input is neither TS nor TLV")
		}
	}
	return nil, "", ctx.Err()
}
func readTS(ctx context.Context, r io.Reader, opt Options, report *Report, start time.Time) error {
	reader := ts.NewPacketReader(r)
	states := map[uint8]*tsState{}
	var frame ts.TSMFFrame
	isTSMF := report.Format == "tsmf"
	var losses uint64
	defer func() {
		if opt.RelativeTS != 0 && !isTSMF {
			report.Warnings = append(report.Warnings, fmt.Sprintf("TSMF header not observed; --relative-ts %d cannot be applied", opt.RelativeTS))
		}
		for _, state := range states {
			state.finish(report)
		}
	}()
	for ctx.Err() == nil {
		p, err := reader.Next()
		if err != nil {
			return err
		}
		report.Packets++
		report.PacketStride = reader.PacketStride()
		report.SyncLosses = reader.SyncLosses()
		if reader.SyncLosses() != losses {
			frame.Reset()
			losses = reader.SyncLosses()
		}
		relative, ok := frame.Feed(p)
		if p.PID() == 0x2f && ok {
			isTSMF = true
			report.Format = "tsmf"
			delete(states, 0)
			continue
		}
		if isTSMF {
			if !ok || relative == 0 {
				continue
			}
		} else {
			relative = 0
		}
		if opt.RelativeTS != 0 && opt.RelativeTS != relative {
			continue
		}
		state := states[relative]
		if state == nil {
			state = newTSState(relative)
			states[relative] = state
		}
		report.ParseErrors += state.feed(p, time.Since(start))
	}
	return ctx.Err()
}
func readTLV(ctx context.Context, r io.Reader, report *Report, start time.Time) error {
	reader := mmt.NewTLVReader(r)
	state := newTLVState()
	defer state.finish(report)
	for ctx.Err() == nil {
		p, err := reader.Next()
		if err != nil {
			return err
		}
		report.Packets++
		report.ParseErrors += state.feed(p, time.Since(start))
	}
	return ctx.Err()
}

func stopReason(ctx context.Context, err error) string {
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return "duration"
	}
	if ctx.Err() != nil {
		return "cancelled"
	}
	if errors.Is(err, io.EOF) || err == nil {
		return "eof"
	}
	return "error"
}
