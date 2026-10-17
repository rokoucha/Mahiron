package main

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/21S1298001/mahiron/internal/agent"
)

func main() {
	generate := flag.Bool("generate-token", false, "generate a token and its server-side SHA-256 hash")
	flag.Parse()
	if *generate {
		var random [32]byte
		if _, err := rand.Read(random[:]); err != nil {
			slog.Error("token generation failed")
			os.Exit(1)
		}
		token := hex.EncodeToString(random[:])
		fmt.Printf("token: %s\ntokenHash: %x\n", token, sha256.Sum256([]byte(token)))
		return
	}
	if flag.NArg() != 0 {
		slog.Error("unexpected arguments")
		os.Exit(2)
	}
	token := os.Getenv("MAHIRON_AGENT_TOKEN")
	if path := os.Getenv("MAHIRON_AGENT_TOKEN_FILE"); path != "" {
		data, err := os.ReadFile(path)
		if err != nil {
			slog.Error("cannot read token file", "err", err)
			os.Exit(1)
		}
		token = strings.TrimSpace(string(data))
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt)
	defer stop()
	if err := agent.Run(ctx, os.Getenv("MAHIRON_URL"), token); err != nil {
		slog.Error("agent stopped", "err", err)
		os.Exit(1)
	}
}
