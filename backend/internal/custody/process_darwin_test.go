//go:build darwin && cgo

package custody

import (
	"os/exec"
	"testing"
	"time"
)

func observedEventually(t *testing.T, pid int, accept func(ProcessIdentity) bool) ProcessIdentity {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		p, err := ObserveProcess(pid)
		if err == nil && accept(p) {
			return p
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("owned process identity did not become observable")
	return ProcessIdentity{}
}
func TestAuditTokenStopContinueOnlyOwnedGeneration(t *testing.T) {
	cmd := exec.Command("/bin/sleep", "0.4")
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	p := observedEventually(t, cmd.Process.Pid, func(p ProcessIdentity) bool { return p.Executable == "/bin/sleep" })
	t.Cleanup(func() { _ = SignalProcess(p, true); _ = cmd.Wait() })
	if err := SignalProcess(p, false); err != nil {
		t.Fatal(err)
	}
	stopped := observedEventually(t, p.PID, func(q ProcessIdentity) bool { return q.Same(p) && q.Status == 4 })
	if !stopped.Same(p) {
		t.Fatal("process generation changed")
	}
	if err := SignalProcess(p, true); err != nil {
		t.Fatal(err)
	}
	if err := cmd.Wait(); err != nil {
		t.Fatal(err)
	}
	gone, err := ProcessGone(p)
	if err != nil || !gone {
		t.Fatalf("positive exit proof: %v %v", gone, err)
	}
}
func TestAuditTokenRejectsSamePIDExec(t *testing.T) {
	cmd := exec.Command("/bin/sh", "-c", "read barrier; exec /bin/sleep 0.3")
	input, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err = cmd.Start(); err != nil {
		t.Fatal(err)
	}
	old := observedEventually(t, cmd.Process.Pid, func(p ProcessIdentity) bool { return p.Executable == "/bin/sh" })
	if _, err = input.Write([]byte("go\n")); err != nil {
		t.Fatal(err)
	}
	_ = input.Close()
	current := observedEventually(t, old.PID, func(p ProcessIdentity) bool { return p.Executable == "/bin/sleep" })
	if current.Same(old) || current.StartSeconds != old.StartSeconds || current.StartMicroseconds != old.StartMicroseconds {
		t.Fatal("same-PID exec did not preserve birth/change execution token")
	}
	if err = SignalProcess(old, false); err == nil {
		_ = SignalProcess(current, true)
		t.Fatal("stale token signalled successor")
	}
	if err = cmd.Wait(); err != nil {
		t.Fatal(err)
	}
}
