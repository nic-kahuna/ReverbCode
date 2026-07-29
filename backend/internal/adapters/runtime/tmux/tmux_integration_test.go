package tmux

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
)

func TestControlServerIntegration(t *testing.T) {
	tmuxPath, err := exec.LookPath("tmux")
	if err != nil {
		t.Skip("tmux unavailable")
	}

	dataDir := t.TempDir()
	socketFile, err := os.CreateTemp("", "ao-control-*.sock")
	if err != nil {
		t.Fatalf("reserve control socket path: %v", err)
	}
	socket := socketFile.Name()
	if err := socketFile.Close(); err != nil {
		t.Fatalf("close control socket placeholder: %v", err)
	}
	if err := os.Remove(socket); err != nil {
		t.Fatalf("remove control socket placeholder: %v", err)
	}
	r := New(Options{Binary: tmuxPath, CommandDir: dataDir, ControlSocket: socket, Timeout: 5 * time.Second})
	t.Cleanup(func() {
		_ = exec.Command(tmuxPath, "-N", "-S", socket, "kill-server").Run()
	})

	if err := r.EnsureControlServer(context.Background()); err != nil {
		t.Fatalf("EnsureControlServer: %v", err)
	}
	for option, want := range map[string]string{
		"@ao-control-version":  ControlServerVersion,
		"@ao-control-data-dir": r.controlDataDir,
		"exit-empty":           "off",
	} {
		out, err := exec.Command(tmuxPath, controlShowOptionArgs(socket, option)...).CombinedOutput()
		if err != nil {
			t.Fatalf("show %s: %v\n%s", option, err, out)
		}
		if got := strings.TrimSpace(string(out)); got != want {
			t.Fatalf("%s = %q, want %q", option, got, want)
		}
	}

	outputPath := filepath.Join(dataDir, "run-shell-cwd")
	command := "pwd > " + shellQuote(outputPath)
	if out, err := exec.Command(tmuxPath, "-N", "-S", socket, "run-shell", "-b", "-c", dataDir, command).CombinedOutput(); err != nil {
		t.Fatalf("run-shell: %v\n%s", err, out)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		out, err := os.ReadFile(outputPath)
		if err == nil {
			got := strings.TrimSpace(string(out))
			if resolved, resolveErr := filepath.EvalSymlinks(got); resolveErr == nil {
				got = resolved
			}
			if got != r.controlDataDir {
				t.Fatalf("run-shell cwd = %q, want %q", got, r.controlDataDir)
			}
			break
		}
		if !os.IsNotExist(err) {
			t.Fatalf("read run-shell output: %v", err)
		}
		if time.Now().After(deadline) {
			t.Fatal("run-shell did not produce output")
		}
		time.Sleep(50 * time.Millisecond)
	}

	// No hidden session is needed to retain the server.
	if out, err := exec.Command(tmuxPath, "-N", "-S", socket, "list-sessions").CombinedOutput(); err != nil {
		t.Fatalf("list sessionless control server: %v\n%s", err, out)
	} else if strings.TrimSpace(string(out)) != "" {
		t.Fatalf("control server unexpectedly has sessions: %s", out)
	}
}

func TestRuntimeIntegration(t *testing.T) {
	if _, err := exec.LookPath("tmux"); err != nil {
		t.Skip("tmux unavailable")
	}

	ctx := context.Background()
	id := strings.ReplaceAll(t.Name(), "/", "_")
	r := New(Options{Timeout: 5 * time.Second})

	// Ensure clean slate: ignore errors (session may not exist).
	_ = r.Destroy(ctx, ports.RuntimeHandle{ID: id})

	t.Cleanup(func() {
		// Always destroy so a test failure never leaks a tmux session.
		_ = r.Destroy(context.Background(), ports.RuntimeHandle{ID: id})
	})

	h, err := r.Create(ctx, ports.RuntimeConfig{
		SessionID:     domain.SessionID(id),
		WorkspacePath: t.TempDir(),
		// Run a trivial command then drop into an interactive shell (the keep-alive
		// exec is added by buildLaunchCommand, but we also verify here that output
		// appears).
		Argv: []string{"sh", "-c", "echo hello-from-tmux"},
		Env:  map[string]string{"AO_SESSION_ID": id},
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	alive, err := r.IsAlive(ctx, h)
	if err != nil {
		t.Fatalf("IsAlive: %v", err)
	}
	if !alive {
		t.Fatal("alive = false, want true after create")
	}

	// Wait for the echo output to appear (the session may take a moment to
	// write it to the pane history).
	out := waitForOutput(t, r, h, "hello-from-tmux", 5*time.Second)
	if !strings.Contains(out, "hello-from-tmux") {
		t.Fatalf("output = %q, want hello-from-tmux", out)
	}

	// Send a command and verify it echoes back.
	if err := r.SendMessage(ctx, h, "echo hello-send"); err != nil {
		t.Fatalf("SendMessage: %v", err)
	}
	out = waitForOutput(t, r, h, "hello-send", 5*time.Second)
	if !strings.Contains(out, "hello-send") {
		t.Fatalf("output after SendMessage = %q, want hello-send", out)
	}

	// Destroy and verify liveness goes false.
	if err := r.Destroy(ctx, h); err != nil {
		t.Fatalf("Destroy: %v", err)
	}
	alive, err = r.IsAlive(ctx, h)
	if err != nil {
		t.Fatalf("IsAlive after destroy: %v", err)
	}
	if alive {
		t.Fatal("alive after destroy = true, want false")
	}
}

// TestRuntimeIntegrationExactSessionParsing verifies that IsAlive uses exact
// session matching and does not treat a prefix as a live session.
func TestRuntimeIntegrationExactSessionParsing(t *testing.T) {
	if _, err := exec.LookPath("tmux"); err != nil {
		t.Skip("tmux unavailable")
	}

	ctx := context.Background()
	base := strings.ReplaceAll(t.Name(), "/", "_")
	longID := base + "_long"
	prefixID := base

	r := New(Options{Timeout: 5 * time.Second})
	_ = r.Destroy(ctx, ports.RuntimeHandle{ID: longID})
	_ = r.Destroy(ctx, ports.RuntimeHandle{ID: prefixID})

	t.Cleanup(func() {
		_ = r.Destroy(context.Background(), ports.RuntimeHandle{ID: longID})
		_ = r.Destroy(context.Background(), ports.RuntimeHandle{ID: prefixID})
	})

	h, err := r.Create(ctx, ports.RuntimeConfig{
		SessionID:     domain.SessionID(longID),
		WorkspacePath: t.TempDir(),
		Argv:          []string{"sh", "-c", "echo ready"},
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	// tmux has-session -t <prefix> should NOT match <longID> because tmux
	// requires the exact session name when using -t with a plain string (not a
	// glob). Verify by probing the prefix handle directly.
	prefixAlive, err := r.IsAlive(ctx, ports.RuntimeHandle{ID: prefixID})
	if err != nil {
		// tmux may return an error (session not found) rather than exit 0.
		// That is acceptable here: the point is the prefix must not be alive.
		t.Logf("IsAlive prefix returned error (acceptable): %v", err)
	}
	if prefixAlive {
		_ = r.Destroy(ctx, h)
		t.Fatal("prefix handle reported alive; tmux session matching is not exact")
	}
}

// waitForOutput polls GetOutput until out contains want or the deadline passes.
func waitForOutput(t *testing.T, r *Runtime, h ports.RuntimeHandle, want string, deadline time.Duration) string {
	t.Helper()
	end := time.Now().Add(deadline)
	var out string
	for time.Now().Before(end) {
		var err error
		out, err = r.GetOutput(context.Background(), h, 50)
		if err != nil {
			t.Fatalf("GetOutput: %v", err)
		}
		if strings.Contains(out, want) {
			return out
		}
		time.Sleep(100 * time.Millisecond)
	}
	return out
}
