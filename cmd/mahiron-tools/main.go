package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
	"time"

	"github.com/21S1298001/mahiron/internal/probe"
	"github.com/21S1298001/mahiron/ts"
)

func main() { os.Exit(run(os.Args[1:], os.Stdout, os.Stderr)) }
func run(args []string, out, stderr io.Writer) int {
	if len(args) == 0 || args[0] == "help" || args[0] == "--help" || args[0] == "-h" {
		fmt.Fprintln(out, "Usage: mahiron-tools probe [options] [file|-]\n\nCommands:\n  probe  Inspect services, components and residual scrambling in TS/TSMF/TLV")
		return 0
	}
	if args[0] != "probe" {
		fmt.Fprintf(stderr, "unknown command %q\n", args[0])
		return 2
	}
	fs := flag.NewFlagSet("probe", flag.ContinueOnError)
	fs.SetOutput(stderr)
	format := fs.String("format", "auto", "input format: auto, ts, tsmf, tlv")
	duration := fs.Duration("duration", 10*time.Second, "observation time; 0 reads until EOF or interruption")
	jsonOutput := fs.Bool("json", false, "write JSON")
	relative := fs.Uint("relative-ts", 0, "relative TS to inspect (1..15); 0 inspects all")
	if err := fs.Parse(args[1:]); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if fs.NArg() > 1 || *relative > 15 || *duration < 0 {
		fmt.Fprintln(stderr, "expected at most one input, --relative-ts 0..15, and nonnegative --duration")
		return 2
	}
	switch *format {
	case "auto", "ts", "tsmf", "tlv":
	default:
		fmt.Fprintln(stderr, "--format must be auto, ts, tsmf or tlv")
		return 2
	}
	if *format == "tlv" && *relative != 0 {
		fmt.Fprintln(stderr, "--relative-ts requires TS/TSMF input")
		return 2
	}
	input := os.Stdin
	if fs.NArg() == 1 && fs.Arg(0) != "-" {
		var err error
		input, err = os.Open(fs.Arg(0))
		if err != nil {
			fmt.Fprintln(stderr, err)
			return 1
		}
		defer input.Close()
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	if *duration > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, *duration)
		defer cancel()
	}
	// Closing the owned input also releases a read blocked on an idle pipe.
	done := make(chan struct{})
	defer close(done)
	go func() {
		select {
		case <-ctx.Done():
			input.Close()
		case <-done:
		}
	}()
	report, err := probe.Run(ctx, input, probe.Options{Format: *format, RelativeTS: uint8(*relative)})
	if report != nil {
		if *jsonOutput {
			encoder := json.NewEncoder(out)
			encoder.SetIndent("", "  ")
			if e := encoder.Encode(report); e != nil {
				fmt.Fprintln(stderr, e)
				return 1
			}
		} else if e := writeText(out, report); e != nil {
			fmt.Fprintln(stderr, e)
			return 1
		}
	}
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	return 0
}
func writeText(dst io.Writer, r *probe.Report) error {
	var b strings.Builder
	out := &b
	fmt.Fprintf(out, "Format: %s\nPackets: %d\nRead elapsed: %s\nTime basis: wall clock since input started\nStop reason: %s\nParse errors: %d\n", r.Format, r.Packets, r.Duration.Round(time.Millisecond), r.StopReason, r.ParseErrors)
	if r.PacketStride != 0 {
		fmt.Fprintf(out, "Packet size: %d bytes\nSync losses: %d\n", r.PacketStride, r.SyncLosses)
	}
	var relative uint8
	for i, s := range r.Services {
		if r.Format == "tsmf" {
			if i == 0 || relative != s.RelativeTS {
				relative = s.RelativeTS
				fmt.Fprintf(out, "\nRelative TS %d\n", relative)
			} else {
				fmt.Fprintln(out)
			}
			var service strings.Builder
			writeService(&service, s)
			for line := range strings.SplitSeq(strings.TrimSuffix(service.String(), "\n"), "\n") {
				if line != "" {
					fmt.Fprint(out, "  ")
				}
				fmt.Fprintln(out, line)
			}
		} else {
			fmt.Fprintln(out)
			writeService(out, s)
		}
	}
	if len(r.Warnings) > 0 {
		fmt.Fprintln(out)
	}
	for _, w := range r.Warnings {
		fmt.Fprintf(out, "Warning: %s\n", w)
	}
	fmt.Fprintln(out, "\nScrambling check:\n  Basis: packet flags\n  Payload validation: not performed\n  CAS failure diagnosis: not performed")
	_, err := io.WriteString(dst, b.String())
	return err
}
func writeService(out io.Writer, s *probe.Service) {
	fmt.Fprintf(out, "Service %d: %s\n", s.ID, s.Name)
	if s.Provider != "" {
		fmt.Fprintf(out, "  Provider: %s\n", s.Provider)
	}
	if s.StreamID != nil {
		fmt.Fprintf(out, "  Stream ID: %d\n", *s.StreamID)
	}
	if s.NetworkID != nil {
		fmt.Fprintf(out, "  Network ID: %d\n", *s.NetworkID)
	}
	if s.Type != nil {
		fmt.Fprintf(out, "  Service type: 0x%02x\n", *s.Type)
	}
	fmt.Fprintf(out, "  Listed: %t | PMT/MPT received: %t | Complete: %t\n", s.Listed, s.MapReceived, s.MapComplete)
	if s.MapID != nil {
		fmt.Fprintf(out, "  PMT/MPT packet ID: 0x%04x\n", *s.MapID)
	}
	if s.PCRPID != nil {
		fmt.Fprintf(out, "  PCR PID: 0x%04x\n", *s.PCRPID)
	}
	if s.Flow != "" {
		fmt.Fprintf(out, "  Flow: %s\n", s.Flow)
	}
	ca := "unknown"
	if s.CADeclared != nil {
		ca = fmt.Sprint(*s.CADeclared)
	}
	fmt.Fprintf(out, "  CA declared (SDT): %s | CA descriptors: %t\n", ca, s.CASignaled)
	fmt.Fprintf(out, "  Scrambling: %s\n", s.Scrambling)
	writeCA(out, s.CA)
	for _, c := range s.Components {
		st := c.Stats
		fmt.Fprintf(out, "\n  %s 0x%04x (%s)\n", c.Kind, c.ID, c.Codec)
		if c.Language != "" {
			fmt.Fprintf(out, "    Language: %s\n", c.Language)
		}
		if c.Tag != nil {
			fmt.Fprintf(out, "    Component tag: 0x%x\n", *c.Tag)
		}
		fmt.Fprintf(out, "    Scrambling: %s\n", st.Status())
		fmt.Fprintf(out, "    Packets: %d | Scrambled: %d", st.Packets, st.Scrambled)
		if st.Packets > 0 {
			fmt.Fprintf(out, " (%.2f%%)", 100*float64(st.Scrambled)/float64(st.Packets))
		}
		fmt.Fprintf(out, "\n    Unknown: %d | Transport errors: %d\n", st.Unknown, st.TransportErrors)
		if c.CASignaled {
			fmt.Fprintln(out, "    CA descriptors: true")
		}
		if c.URL != "" {
			fmt.Fprintf(out, "    URL: %s\n", c.URL)
		}
		if c.Flow != "" && c.Flow != s.Flow {
			fmt.Fprintf(out, "    Flow: %s\n", c.Flow)
		}
		if c.AssetID != "" {
			fmt.Fprintf(out, "    Asset ID: %s\n", c.AssetID)
		}
		if st.FirstScrambled != nil {
			fmt.Fprintf(out, "    First scrambled (read elapsed): %s\n    Last scrambled (read elapsed): %s\n", st.FirstScrambled.Round(time.Millisecond), st.LastScrambled.Round(time.Millisecond))
		}
		writeCA(out, c.CA)
	}
}
func writeCA(out io.Writer, entries []probe.ConditionalAccess) {
	for _, ca := range entries {
		fmt.Fprintf(out, "    CA system: 0x%04x\n", ca.SystemID)
		if ca.PID == ts.PIDNull {
			fmt.Fprintln(out, "    ECM PID: none (0x1fff)")
		} else {
			fmt.Fprintf(out, "    ECM PID: 0x%04x | Received: %d\n", ca.PID, ca.Packets)
		}
	}
}
