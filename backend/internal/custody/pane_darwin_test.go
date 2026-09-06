//go:build darwin && cgo

package custody

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/adapters/runtime/tmux"
	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
)

// This finite helper is a disposable writer, not a simulated Codex conversation.
func TestManagedPaneWriterHelper(t *testing.T) {
	path := os.Getenv("AO_PRIVATE_PANE_WRITER")
	if path == "" {
		t.Skip("private subprocess only")
	}
	done := make(chan struct{})
	go func() {
		s := bufio.NewScanner(os.Stdin)
		for s.Scan() {
			if strings.TrimSpace(s.Text()) == "exit" {
				close(done)
				return
			}
		}
	}()
	timer := time.NewTicker(20 * time.Millisecond)
	defer timer.Stop()
	deadline := time.NewTimer(15 * time.Second)
	defer deadline.Stop()
	for n := 0; ; n++ {
		select {
		case <-done:
			return
		case <-deadline.C:
			return
		case <-timer.C:
			if err := os.WriteFile(path, []byte(fmt.Sprint(n)), 0600); err != nil {
				t.Fatal(err)
			}
		}
	}
}
func TestManagedPaneExactStopRetentionAndUnrelatedProgress(t *testing.T) {
	binary, err := exec.LookPath("tmux")
	if err != nil {
		t.Skip("tmux unavailable")
	}
	dir, err := os.MkdirTemp("/tmp", "ao16-pane-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	t.Setenv("TMUX_TMPDIR", dir)
	t.Setenv("TMUX", "")
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	executable, err = filepath.EvalSymlinks(executable)
	if err != nil {
		t.Fatal(err)
	}
	rt := tmux.New(tmux.Options{Binary: binary, CommandDir: dir, EnterDelay: time.Millisecond})
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
	defer cancel()
	var identities []ProcessIdentity
	handles := []ports.RuntimeHandle{}
	t.Cleanup(func() {
		for _, p := range identities {
			_ = SignalProcess(p, true)
		}
		for _, h := range handles {
			_ = rt.Destroy(context.Background(), h)
		}
	})
	for _, id := range []string{"private-custody-worker", "private-custody-sibling"} {
		h, e := rt.Create(ctx, ports.RuntimeConfig{SessionID: domain.SessionID(id), WorkspacePath: dir, Managed: true, Argv: []string{executable, "-test.run=^TestManagedPaneWriterHelper$"}, Env: map[string]string{"AO_PRIVATE_PANE_WRITER": filepath.Join(dir, id)}})
		if e != nil {
			t.Fatal(e)
		}
		handles = append(handles, h)
	}
	wait := func(check func() bool) {
		t.Helper()
		deadline := time.Now().Add(2 * time.Second)
		for !check() {
			if time.Now().After(deadline) {
				t.Fatal("private fixture did not reach expected boundary")
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	first, second := filepath.Join(dir, "private-custody-worker"), filepath.Join(dir, "private-custody-sibling")
	wait(func() bool { _, a := os.Stat(first); _, b := os.Stat(second); return a == nil && b == nil })
	info, err := rt.RuntimeProcesses(ctx, handles[0])
	if err != nil {
		t.Fatal(err)
	}
	var provider ProcessIdentity
	wait(func() bool {
		topology, _, e := ProcessInventory()
		if e != nil {
			return false
		}
		for _, p := range topology {
			if p.ParentPID == info.PanePID {
				identity, e := ObserveProcess(p.PID)
				if e == nil && identity.Executable == executable {
					provider = identity
					return true
				}
			}
		}
		return false
	})
	identities = append(identities, provider)
	if err = SignalProcess(provider, false); err != nil {
		t.Fatal(err)
	}
	wait(func() bool {
		p, e := ObserveProcess(provider.PID)
		return e == nil && p.Same(provider) && p.Status == 4
	})
	before, err := os.ReadFile(first)
	if err != nil {
		t.Fatal(err)
	}
	otherBefore, _ := os.ReadFile(second)
	time.Sleep(120 * time.Millisecond)
	after, _ := os.ReadFile(first)
	otherAfter, _ := os.ReadFile(second)
	if string(before) != string(after) || string(otherBefore) == string(otherAfter) {
		t.Fatal("stopped writer changed or unrelated sibling did not progress")
	}
	if err = SignalProcess(provider, true); err != nil {
		t.Fatal(err)
	}
	wait(func() bool { b, _ := os.ReadFile(first); return string(b) != string(before) })
	if err = rt.SendMessage(ctx, handles[0], "exit"); err != nil {
		t.Fatal(err)
	}
	wait(func() bool { p, e := rt.RuntimeProcesses(ctx, handles[0]); return e == nil && p.Dead })
	// The retained dead pane proves the managed tail did not open an interactive
	// shell or discard its candidate when the finite provider exited.
	retained, err := rt.RuntimeProcesses(ctx, handles[0])
	if err != nil || !retained.Dead {
		t.Fatalf("provider exit discarded retained pane: %+v %v", retained, err)
	}
	if _, err = os.Stat(first); err != nil {
		t.Fatal("partial writer artifact disappeared")
	}
}
