package main

import (
	"bufio"
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/21S1298001/Mahiron5/config"
	"github.com/21S1298001/Mahiron5/util/dynamicmultiwriter"
	"github.com/asticode/go-astits"
)

func main() {
	serverConfig, err := config.LoadAndParseServerConfig("server.yml")
	if err != nil {
		slog.Error("failed to load config", "err", err)
	}

	level := slog.LevelInfo
	switch serverConfig.LogLevel {
	case "debug":
		level = slog.LevelDebug
	case "info":
		level = slog.LevelInfo
	case "warn":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	}
	h := slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level})
	slog.SetDefault(slog.New(h))

	mux := http.NewServeMux()
	mux.HandleFunc("/", stream)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt, os.Kill)
	defer stop()

	servers := make([]*http.Server, len(serverConfig.Addresses))
	for i, addr := range serverConfig.Addresses {
		if addr.Http != "" {
			srv := &http.Server{
				Addr:    addr.Http,
				Handler: mux,
			}
			servers[i] = srv
			slog.Info("starting HTTP server", "address", addr.Http)
			go func(srv *http.Server) {
				err := srv.ListenAndServe()
				if err != nil && !errors.Is(err, http.ErrServerClosed) {
					slog.Error("failed to start HTTP server", "address", addr.Http, "err", err)
					return
				}
			}(srv)
		}

		if addr.Unix != "" {
			srv := &http.Server{
				Handler: mux,
			}
			servers[i] = srv
			slog.Info("starting Unix socket server", "address", addr.Unix)
			go func(addr string) {
				l, err := net.Listen("unix", addr)
				if err != nil {
					slog.Error("failed to listen UNIX domain socket", "address", addr, "err", err)
					return
				}
				defer l.Close()

				err = srv.Serve(l)
				if err != nil && !errors.Is(err, http.ErrServerClosed) {
					slog.Error("failed to start UNIX domain socket server", "address", addr, "err", err)
					return
				}
			}(addr.Unix)
		}
	}

	<-ctx.Done()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	slog.Info("shutting down servers")
	for _, srv := range servers {
		if srv != nil {
			if err := srv.Shutdown(ctx); err != nil && !errors.Is(err, context.DeadlineExceeded) {
				slog.Error("failed to shut down server gracefully", "address", srv.Addr, "err", err)
			}
		}
	}
	slog.Info("all servers shut down")
	slog.Info("exiting")
	os.Exit(0)
}

func stream(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	slog.Info("start streaming")
	resp, err := http.Get("http://v6.haruka.dns.ggrel.net:40772/api/services/3273601024/stream")
	if err != nil {
		slog.Error("failed to get stream", "err", err)
		return
	}
	if resp.StatusCode != http.StatusOK {
		slog.Error("failed to get stream", "status", resp.StatusCode)
		return
	}
	defer resp.Body.Close()

	slog.Info("successfully got stream", "status", resp.StatusCode)

	w.Header().Set("Content-Type", resp.Header.Get("Content-Type"))
	w.Header().Set("Content-Length", resp.Header.Get("Content-Length"))
	w.WriteHeader(resp.StatusCode)

	if r.Method == http.MethodHead {
		slog.Info("HEAD request, returning")
		return
	}

	slog.Info("streaming data")

	pr, pw := io.Pipe()
	defer pr.Close()

	dmwr := dynamicmultiwriter.New(
		map[string]io.Writer{
			"http":   w,
			"parser": pw,
		},
	)

	defer dmwr.CloseAll()

	copyCh := make(chan error)
	go func() {
		_, err := io.Copy(dmwr, resp.Body)
		if err != nil {
			copyCh <- err
			return
		}
		copyCh <- nil
	}()

	go func() {
		dmx := astits.NewDemuxer(ctx, bufio.NewReader(pr))
		for {
			_, err := dmx.NextData()
			if err != nil {
				continue
			}

			// if d.EIT != nil {
			// 	slog.Info("EIT detected")
			// 	for _, e := range d.EIT.Events {
			// 		slog.Info("Event detected", "event_id", e.EventID)
			// 		for _, d := range e.Descriptors {
			// 			if d.Content != nil {
			// 				for _, i := range d.Content.Items {
			// 					slog.Info("Content items", "item", i)
			// 				}
			// 			}
			// 		}
			// 	}
			// }
		}
	}()

	slog.Info("waiting for copy to finish")
	select {
	case <-ctx.Done():
		dmwr.CloseAll()
		slog.Info("connection closed by client")
		return

	case err := <-copyCh:
		if err == nil {
			slog.Info("copy finished")
			return
		}

		if opErr, ok := err.(*net.OpError); ok {
			if sysErr, ok := opErr.Err.(*os.SyscallError); ok && sysErr.Err == syscall.EPIPE {
				slog.Info("connection closed by remote")
				return
			}
		}
		slog.Error("copy error", "err", err)
		return
	}
}
