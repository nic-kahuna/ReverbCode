package httpd

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/config"
	"github.com/aoagents/agent-orchestrator/backend/internal/runfile"
	"github.com/aoagents/agent-orchestrator/backend/internal/terminal"
)

// Server is the daemon's HTTP server together with its lifecycle: bind the
// loopback port, publish the running.json handshake, serve until the context
// is cancelled, then shut down gracefully and clean up the handshake file.
type Server struct {
	cfg    config.Config
	log    *slog.Logger
	http   *http.Server
	listen net.Listener

	shutdownRequested chan struct{}
	shutdownOnce      sync.Once
}

// RouterFactory builds one handler around the Server's graceful-shutdown hook.
// Separating listener acquisition from handler construction lets daemon boot
// bind the exact configured port before it opens storage or constructs any
// external/mutation-capable subsystem.
type RouterFactory func(ControlDeps) (http.Handler, error)

// Listen acquires the daemon's exact configured loopback address. Every bind
// failure is fatal; in particular, EADDRINUSE never falls back to an ephemeral
// port whose identity clients could confuse with another boot.
func Listen(cfg config.Config) (net.Listener, error) {
	ln, err := net.Listen("tcp", cfg.Addr())
	if err != nil {
		return nil, fmt.Errorf("bind %s: %w", cfg.Addr(), err)
	}
	return ln, nil
}

// NewWithDeps constructs a Server with API dependencies supplied by the daemon
// and binds the listener immediately, before any running.json is written. The
// caller owns the returned Server's lifecycle via Run. termMgr may be nil, in
// which case the /mux terminal surface is not mounted.
//
// This compatibility constructor captures a normal-mode proof on behalf of old
// callers. New daemon wiring should acquire Listen early and call NewWithListener
// with an explicit daemon-provided ProbeContext/router factory.
func NewWithDeps(cfg config.Config, log *slog.Logger, termMgr *terminal.Manager, deps APIDeps) (*Server, error) {
	proof, err := compatibilityNormalProbe(cfg)
	if err != nil {
		return nil, fmt.Errorf("capture readiness proof: %w", err)
	}
	return NewWithDepsAndProbe(cfg, log, termMgr, deps, proof)
}

// NewWithDepsAndProbe is the explicit normal-mode convenience constructor. It
// binds the exact configured address and validates the supplied proof.
func NewWithDepsAndProbe(cfg config.Config, log *slog.Logger, termMgr *terminal.Manager, deps APIDeps, proof ProbeContext) (*Server, error) {
	ln, err := Listen(cfg)
	if err != nil {
		return nil, err
	}
	return NewWithListener(cfg, log, ln, func(control ControlDeps) (http.Handler, error) {
		return NewRouterWithControlAndProbe(cfg, log, termMgr, deps, control, proof)
	})
}

// NewWithListener completes Server construction around an already-bound exact
// listener. It owns ln on entry and closes it if handler construction fails.
func NewWithListener(cfg config.Config, log *slog.Logger, ln net.Listener, factory RouterFactory) (*Server, error) {
	log = loggerOrDefault(log)
	if ln == nil {
		return nil, fmt.Errorf("listener is required")
	}
	if factory == nil {
		_ = ln.Close()
		return nil, fmt.Errorf("router factory is required")
	}
	srv := &Server{
		cfg:               cfg,
		log:               log,
		listen:            ln,
		shutdownRequested: make(chan struct{}),
	}
	handler, err := factory(ControlDeps{RequestShutdown: srv.requestShutdown})
	if err != nil {
		_ = ln.Close()
		return nil, fmt.Errorf("construct HTTP router: %w", err)
	}
	if handler == nil {
		_ = ln.Close()
		return nil, fmt.Errorf("construct HTTP router: nil handler")
	}
	srv.http = &http.Server{
		Handler: handler,
		// ReadHeaderTimeout guards against slow-loris even on loopback;
		// per-request body/handler timeouts are applied per-surface.
		ReadHeaderTimeout: 10 * time.Second,
	}
	return srv, nil
}

// Addr returns the actual bound address (useful when the configured port was 0
// and the OS chose one — primarily in tests).
func (s *Server) Addr() net.Addr { return s.listen.Addr() }

// Handler returns the loopback server's built router so the daemon can share
// the exact same handler instance with the LAN listener (via NewMobileLAN),
// keeping the loopback and LAN surfaces identical.
func (s *Server) Handler() http.Handler { return s.http.Handler }

// Run serves until ctx is cancelled (SIGINT/SIGTERM via signal.NotifyContext),
// then performs a graceful shutdown bounded by cfg.ShutdownTimeout. It writes
// running.json before serving and removes it on the way out. Run blocks until
// shutdown is complete.
func (s *Server) Run(ctx context.Context) error {
	info := runfile.Info{
		PID:       os.Getpid(),
		Port:      s.boundPort(),
		StartedAt: time.Now().UTC(),
		Owner:     os.Getenv("AO_OWNER"),
	}
	if err := runfile.Write(s.cfg.RunFilePath, info); err != nil {
		_ = s.listen.Close()
		return fmt.Errorf("write run-file: %w", err)
	}
	defer func() {
		if err := runfile.RemoveIfOwned(s.cfg.RunFilePath, info.PID); err != nil {
			s.log.Warn("failed to remove run-file", "path", s.cfg.RunFilePath, "err", err)
		}
	}()

	serveErr := make(chan error, 1)
	go func() {
		s.log.Info("daemon listening", "addr", s.Addr().String(), "pid", info.PID)
		// Serve returns ErrServerClosed on a clean Shutdown; that is success.
		if err := s.http.Serve(s.listen); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serveErr <- err
			return
		}
		serveErr <- nil
	}()

	select {
	case err := <-serveErr:
		// Serve died on its own (bind already happened, so this is a real
		// runtime failure) before any shutdown signal.
		return err
	case <-s.shutdownRequested:
		s.log.Info("shutdown requested over HTTP", "timeout", s.cfg.ShutdownTimeout)
	case <-ctx.Done():
		s.log.Info("shutdown signal received, draining connections", "timeout", s.cfg.ShutdownTimeout)
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), s.cfg.ShutdownTimeout)
	defer cancel()

	if err := s.http.Shutdown(shutdownCtx); err != nil {
		// The deadline elapsed with connections still open; force them closed.
		s.log.Warn("graceful shutdown timed out, forcing close", "err", err)
		_ = s.http.Close()
		return fmt.Errorf("graceful shutdown exceeded %s: %w", s.cfg.ShutdownTimeout, err)
	}

	s.log.Info("daemon stopped cleanly")
	return <-serveErr
}

func (s *Server) boundPort() int {
	if tcp, ok := s.listen.Addr().(*net.TCPAddr); ok {
		return tcp.Port
	}
	return s.cfg.Port
}

func (s *Server) requestShutdown() {
	s.shutdownOnce.Do(func() {
		close(s.shutdownRequested)
	})
}

// RequestShutdown triggers the same clean shutdown as POST /shutdown: it makes
// Run return so the daemon exits without tearing down sessions. Idempotent.
func (s *Server) RequestShutdown() { s.requestShutdown() }
