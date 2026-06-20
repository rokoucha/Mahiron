package server

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"

	"github.com/21S1298001/Mahiron5/internal/server/middleware"
	"golang.org/x/sync/errgroup"
)

type ListenAddress struct {
	Http string
	Unix string
}

type Server struct {
	addresses []ListenAddress
	handler   http.Handler
	servers   []*http.Server
}

func NewServer(addresses []ListenAddress, handler http.Handler) *Server {
	middleware := middleware.Synthesis(
		middleware.RequestInfoMiddleware(),
		middleware.AccessLogMiddleware(),
	)

	return &Server{
		addresses: addresses,
		handler:   middleware(handler),
		servers:   make([]*http.Server, len(addresses)),
	}
}

func (s *Server) ListenAndServe() {
	for i, addr := range s.addresses {
		if addr.Http != "" {
			srv := &http.Server{
				Addr:    addr.Http,
				Handler: s.handler,
			}
			s.servers[i] = srv
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
				Handler: s.handler,
			}
			s.servers[i] = srv
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
}

func (s *Server) Shutdown(ctx context.Context) error {
	var eg errgroup.Group
	for _, srv := range s.servers {
		if srv != nil {
			eg.Go(func() error {
				if err := srv.Shutdown(ctx); err != nil && !errors.Is(err, context.DeadlineExceeded) {
					slog.Error("failed to shut down server gracefully", "address", srv.Addr, "err", err)
					return err
				}
				return nil
			})
		}
	}
	return eg.Wait()
}
