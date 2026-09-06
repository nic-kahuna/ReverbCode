package cli

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/bootguard"
	"github.com/aoagents/agent-orchestrator/backend/internal/config"
	"github.com/aoagents/agent-orchestrator/backend/internal/daemon"
	"github.com/aoagents/agent-orchestrator/backend/internal/runfile"
)

func TestCompatibilityCommandIsReadOnlyAndTyped(t *testing.T) {
	setConfigEnv(t)
	cfg, err := config.Load()
	if err != nil {
		t.Fatal(err)
	}
	out, _, err := executeCLI(t, Deps{}, "compatibility", "--json")
	if err != nil {
		t.Fatal(err)
	}
	var status bootguard.Inspection
	if err := json.Unmarshal([]byte(out), &status); err != nil {
		t.Fatal(err)
	}
	if status.Schema != "ao-compatibility/v1" || status.State != "missing_legacy" || !status.InspectionOnly || !status.StartPausedSupported || status.SupportedProtocol != 2 {
		t.Fatalf("wire %s", out)
	}
	if _, err := os.Stat(cfg.DataDir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("inspection created data: %v", err)
	}
	if err := os.MkdirAll(cfg.DataDir, 0750); err != nil {
		t.Fatal(err)
	}
	future := []byte("{\"schema\":\"ao-data-compatibility/v1\",\"requiredProtocol\":3}\n")
	if err := os.WriteFile(filepath.Join(cfg.DataDir, bootguard.MarkerName), future, 0600); err != nil {
		t.Fatal(err)
	}
	out, _, err = executeCLI(t, Deps{}, "compatibility", "--json")
	if !errors.Is(err, bootguard.ErrUnsupported) {
		t.Fatalf("future accepted: %s,%v", out, err)
	}
	if err := json.Unmarshal([]byte(out), &status); err != nil {
		t.Fatal(err)
	}
	if status.State != "unsupported" || status.RequiredProtocol != 3 {
		t.Fatalf("future wire %s", out)
	}
}

func TestOfflineCommandsNeverEmitInvocationOrUsageTelemetry(t *testing.T) {
	cfg := setConfigEnv(t)
	if err := runfile.Write(cfg.runFile, runfile.Info{PID: os.Getpid(), Port: 3001, StartedAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	calls := 0
	deps := Deps{Out: &bytes.Buffer{}, Err: &bytes.Buffer{}, ProcessAlive: func(int) bool { return true }, HTTPClient: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) { calls++; return jsonResponse(200, "{}"), nil })}}
	for _, args := range [][]string{
		{"compatibility", "--json"}, {"compatibility", "--invalid"}, {"prepare-start-paused", "--json"}, {"prepare-start-paused", "--invalid"}, {"import", "--from", t.TempDir(), "--yes"}, {"import", "--invalid"}, {"daemon", "--invalid"},
		{"--help=false", "import", "--invalid"}, {"--help=false", "compatibility", "--invalid"}, {"--help=false", "prepare-start-paused", "--invalid"}, {"--help=false", "daemon", "--invalid"},
		{"-h=false", "import", "extra"}, {"start", "--invalid"}, {"--help=false", "start", "--invalid"}, {"--help=false", "--version=false", "compatibility", "--invalid"},
	} {
		_ = executeWithDeps(deps, args)
	}
	if calls != 0 {
		t.Fatalf("offline command contacted daemon %d times", calls)
	}
}

func TestPrepareStartPausedCommandEmitsNativeProofWithoutLanes(t *testing.T) {
	setConfigEnv(t)
	out, _, err := executeCLI(t, Deps{}, "prepare-start-paused", "--json")
	if err != nil {
		t.Fatal(err)
	}
	var proof daemon.StartPausedPreparation
	if err := json.Unmarshal([]byte(out), &proof); err != nil {
		t.Fatal(err)
	}
	if proof.Schema != "ao-start-paused-preparation/v1" || !proof.AdmissionPaused || proof.MutationLanesStarted || !proof.PreparationOnly || proof.ProjectIDs == nil {
		t.Fatalf("proof %s", out)
	}
}

func TestOfflineOwnershipProcess(t *testing.T) {
	dir := os.Getenv("AO_TEST_OFFLINE_LOCK_DIR")
	if dir == "" {
		return
	}
	g, err := bootguard.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = g.Close() }()
	if err := os.WriteFile(filepath.Join(dir, "ready"), []byte("ready"), 0600); err != nil {
		t.Fatal(err)
	}
	time.Sleep(30 * time.Second)
}

func TestImportAndPreparerRefuseOtherProcessThroughAlias(t *testing.T) {
	setConfigEnv(t)
	dir := t.TempDir()
	cmd := exec.Command(os.Args[0], "-test.run=^TestOfflineOwnershipProcess$")
	cmd.Env = append(os.Environ(), "AO_TEST_OFFLINE_LOCK_DIR="+dir)
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cmd.Process.Kill(); _ = cmd.Wait() })
	deadline := time.Now().Add(10 * time.Second)
	for {
		if _, err := os.Stat(filepath.Join(dir, "ready")); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("child did not acquire lock")
		}
		time.Sleep(10 * time.Millisecond)
	}
	alias := filepath.Join(t.TempDir(), "alias")
	if err := os.Symlink(dir, alias); err != nil {
		t.Skipf("symlink: %v", err)
	}
	t.Setenv("AO_DATA_DIR", alias)
	legacy := writeLegacyProject(t)
	for _, args := range [][]string{{"import", "--from", legacy, "--yes", "--json"}, {"prepare-start-paused", "--json"}} {
		if _, _, err := executeCLI(t, Deps{}, args...); err == nil {
			t.Fatalf("contender succeeded: %v", args)
		}
	}
	if _, err := os.Stat(filepath.Join(dir, "ao.db")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("contender created database: %v", err)
	}
}
