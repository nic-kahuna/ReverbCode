package httpd

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/config"
	"github.com/aoagents/agent-orchestrator/backend/internal/runfile"
)

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestHealthProbes(t *testing.T) {
	router := newTestRouter(config.Config{}, discardLogger(), nil)
	srv := httptest.NewServer(router)
	defer srv.Close()

	client := &http.Client{Timeout: 2 * time.Second}
	for _, path := range []string{"/healthz", "/readyz"} {
		resp, err := client.Get(srv.URL + path)
		if err != nil {
			t.Fatalf("GET %s: %v", path, err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Errorf("GET %s = %d, want 200", path, resp.StatusCode)
		}
		if ct := resp.Header.Get("Content-Type"); ct != "application/json; charset=utf-8" {
			t.Errorf("GET %s Content-Type = %q, want JSON", path, ct)
		}
	}
}

func TestHealthProbesIncludeDaemonIdentity(t *testing.T) {
	router := newTestRouter(config.Config{}, discardLogger(), nil)
	srv := httptest.NewServer(router)
	defer srv.Close()

	client := &http.Client{Timeout: 2 * time.Second}
	var instanceID string
	for _, path := range []string{"/healthz", "/readyz"} {
		resp, err := client.Get(srv.URL + path)
		if err != nil {
			t.Fatalf("GET %s: %v", path, err)
		}
		defer resp.Body.Close()
		var body DaemonReadyResponse
		if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
			t.Fatalf("decode %s: %v", path, err)
		}
		if body.SchemaVersion != ReadinessSchemaVersion || body.Mode != DaemonModeNormal {
			t.Errorf("GET %s proof = %#v, want schema %d normal mode", path, body, ReadinessSchemaVersion)
		}
		if body.DiagnosticBase != RecoveryDiagnosticBase {
			t.Errorf("GET %s diagnosticBase = %q, want %q", path, body.DiagnosticBase, RecoveryDiagnosticBase)
		}
		if body.ExecutablePath != body.Build.Executable.Path || body.WorkingDirectory == "" {
			t.Errorf("GET %s legacy identity fields do not match immutable proof: %#v", path, body)
		}
		if instanceID == "" {
			instanceID = body.InstanceID
		} else if body.InstanceID != instanceID {
			t.Errorf("GET %s instanceId = %q, want immutable %q", path, body.InstanceID, instanceID)
		}
		if err := (ProbeContext{
			InstanceID:       body.InstanceID,
			Mode:             body.Mode,
			WorkingDirectory: body.WorkingDirectory,
			DataDir:          body.DataDir,
			Build:            body.Build,
			Fence:            body.Fence,
		}).Validate(); err != nil {
			t.Errorf("GET %s returned invalid proof: %v", path, err)
		}
	}
}

// TestServerLifecycle exercises the full Run loop: bind an ephemeral port,
// publish running.json, serve a request, then cancel the context and confirm a
// clean shutdown that removes the handshake file.
func TestServerLifecycle(t *testing.T) {
	runPath := filepath.Join(t.TempDir(), "running.json")
	cfg := config.Config{
		Host:            "127.0.0.1",
		Port:            0, // let the OS pick a free port — no conflict with a real daemon
		ShutdownTimeout: 5 * time.Second,
		RunFilePath:     runPath,
	}

	srv, err := NewWithDeps(cfg, discardLogger(), nil, APIDeps{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runErr := make(chan error, 1)
	go func() { runErr <- srv.Run(ctx) }()

	// Wait for the handshake file to confirm the server is up.
	base := "http://" + srv.Addr().String()
	waitForHealth(t, base)

	info, err := runfile.Read(runPath)
	if err != nil {
		t.Fatalf("read run-file: %v", err)
	}
	if info == nil {
		t.Fatal("run-file not written while server running")
		return
	}
	if info.Port == 0 {
		t.Error("run-file recorded port 0; want the actual bound port")
	}

	cancel()

	select {
	case err := <-runErr:
		if err != nil {
			t.Fatalf("Run returned error on graceful shutdown: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not return after context cancel")
	}

	if after, _ := runfile.Read(runPath); after != nil {
		t.Error("run-file still present after shutdown; want it removed")
	}
}

func TestServerShutdownEndpoint(t *testing.T) {
	runPath := filepath.Join(t.TempDir(), "running.json")
	cfg := config.Config{
		Host:            "127.0.0.1",
		Port:            0,
		ShutdownTimeout: 5 * time.Second,
		RunFilePath:     runPath,
	}

	srv, err := NewWithDeps(cfg, discardLogger(), nil, APIDeps{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	runErr := make(chan error, 1)
	go func() { runErr <- srv.Run(context.Background()) }()

	base := "http://" + srv.Addr().String()
	waitForHealth(t, base)

	resp, err := http.Post(base+"/shutdown", "application/json", nil)
	if err != nil {
		t.Fatalf("POST /shutdown: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("POST /shutdown = %d, want 202", resp.StatusCode)
	}

	select {
	case err := <-runErr:
		if err != nil {
			t.Fatalf("Run returned error on shutdown endpoint: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not return after shutdown endpoint")
	}

	if after, _ := runfile.Read(runPath); after != nil {
		t.Error("run-file still present after shutdown endpoint; want it removed")
	}
}

func waitForHealth(t *testing.T, base string) {
	t.Helper()
	// Per-request timeout so a stalled connect or hung handshake doesn't park
	// the test for the full Go test timeout; the outer deadline only bounds
	// the polling loop, not any single GET.
	client := &http.Client{Timeout: 500 * time.Millisecond}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := client.Get(base + "/healthz")
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("server did not become healthy within timeout")
}

func TestListenFailsOnPortConflictWithoutFallback(t *testing.T) {
	cfg := config.Config{Host: "127.0.0.1", Port: 0, RunFilePath: filepath.Join(t.TempDir(), "r.json")}

	first, err := Listen(cfg)
	if err != nil {
		t.Fatalf("first Listen: %v", err)
	}
	defer first.Close()

	port := first.Addr().(*net.TCPAddr).Port
	conflict := config.Config{Host: "127.0.0.1", Port: port, RunFilePath: cfg.RunFilePath}
	second, err := Listen(conflict)
	if err == nil {
		_ = second.Close()
		t.Fatalf("second Listen unexpectedly acquired already-bound port %d", port)
	}
}

func TestNewWithListenerOwnsListenerOnFactoryFailure(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()

	_, err = NewWithListener(config.Config{}, discardLogger(), ln, func(ControlDeps) (http.Handler, error) {
		return nil, errors.New("router failed")
	})
	if err == nil {
		t.Fatal("NewWithListener factory error = nil")
	}

	rebound, bindErr := net.Listen("tcp", addr)
	if bindErr != nil {
		t.Fatalf("listener was not closed after factory failure: %v", bindErr)
	}
	_ = rebound.Close()
}
