package custody

import (
	"bytes"
	"context"
	"encoding/json"
	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

func fixtureGit(t *testing.T, dir string, args ...string) string {
	t.Helper()
	cmd := exec.Command("git", append([]string{"-C", dir}, args...)...)
	cmd.Env = append(os.Environ(), "GIT_CONFIG_GLOBAL=/dev/null", "GIT_CONFIG_NOSYSTEM=1", "GIT_OPTIONAL_LOCKS=0")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git %v: %v: %s", args, err, out)
	}
	return string(bytes.TrimSpace(out))
}
func candidateFixture(t *testing.T) (Attempt, Candidate) {
	t.Helper()
	dir := t.TempDir()
	if root := os.Getenv("AO_CUSTODY_CANARY_DIR"); root != "" {
		var err error
		dir, err = os.MkdirTemp(root, "candidate-")
		if err != nil {
			t.Fatal(err)
		}
	}
	fixtureGit(t, dir, "init", "--initial-branch=main")
	fixtureGit(t, dir, "remote", "add", "origin", "git@github.com:fixture/repository.git")
	fixtureGit(t, dir, "config", "user.email", "fixture@example.invalid")
	fixtureGit(t, dir, "config", "user.name", "Custody fixture")
	write := func(path string, b []byte, mode os.FileMode) {
		t.Helper()
		if err := os.WriteFile(filepath.Join(dir, path), b, mode); err != nil {
			t.Fatal(err)
		}
	}
	write("progress.txt", []byte("base\n"), 0644)
	write("deleted.txt", []byte("keep deletion\n"), 0644)
	write("executable", []byte("#!/bin/sh\nexit 0\n"), 0755)
	write(".gitignore", []byte("ignored/\n"), 0644)
	fixtureGit(t, dir, "add", ".")
	fixtureGit(t, dir, "commit", "-m", "fixture base")
	base := fixtureGit(t, dir, "rev-parse", "HEAD")
	write("progress.txt", []byte("staged progress\n"), 0644)
	fixtureGit(t, dir, "add", "progress.txt")
	write("progress.txt", []byte("unstaged progress\n"), 0644)
	if err := os.Remove(filepath.Join(dir, "deleted.txt")); err != nil {
		t.Fatal(err)
	}
	write("new space ü.bin", []byte{0, 1, 255, 0, 10}, 0600)
	if err := os.Symlink("progress.txt", filepath.Join(dir, "new-link")); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(dir, "ignored"), 0755); err != nil {
		t.Fatal(err)
	}
	write("ignored/artifact.bin", []byte{0, 99, 255}, 0640)
	fixtureGit(t, dir, "update-index", "--split-index")
	physical, err := filepath.EvalSymlinks(dir)
	if err != nil {
		t.Fatal(err)
	}
	a := Attempt{Project: "fixture", SessionID: "fixture-1", AttemptID: "0123456789abcdef0123456789abcdef", Operation: "spawn", IssueNumber: 1, Generation: 2, Revision: 1, Phase: "preparing", Fence: "requested", RequestID: "fedcba9876543210fedcba9876543210", ForegroundID: "foreground-fixture", ClaimID: "fixture:fixture-1:attempt", Workspace: physical, OriginalBaseSHA: base}
	before := fixtureFileHashes(t, physical)
	candidate, err := CaptureCandidate(context.Background(), a, "fixture/repository")
	if err != nil {
		t.Fatal(err)
	}
	after := fixtureFileHashes(t, physical)
	b1, _ := json.Marshal(before)
	b2, _ := json.Marshal(after)
	if !bytes.Equal(b1, b2) {
		t.Fatal("capture changed candidate bytes")
	}
	return a, candidate
}
func fixtureFileHashes(t *testing.T, dir string) map[string]string {
	t.Helper()
	out := map[string]string{}
	err := filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.Type().IsRegular() {
			b, e := os.ReadFile(path)
			if e != nil {
				return e
			}
			out[path] = hashBytes(b)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return out
}
func TestCaptureCandidateRetainsSplitIndexAndExactProgress(t *testing.T) {
	a, c := candidateFixture(t)
	if c.Head != a.OriginalBaseSHA || len(c.SharedIndex) != 1 || len(c.Index) == 0 || !bytes.Contains(c.StagedDiff, []byte("staged progress")) || !bytes.Contains(c.UnstagedDiff, []byte("unstaged progress")) {
		t.Fatalf("incomplete capture: %+v", c)
	}
	if len(c.Untracked) != 2 || len(c.Ignored) != 1 {
		t.Fatalf("manifest counts: %+v %+v", c.Untracked, c.Ignored)
	}
	b, err := json.Marshal(c)
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range c.Ignored {
		if f.Retention != "retained_in_place" || f.SHA256 != "" {
			t.Fatal("ignored entry pretends backup")
		}
	}
	if root := os.Getenv("AO_CUSTODY_CANARY_DIR"); root != "" {
		route, _ := json.Marshal(domain.AgentRoute{Harness: "codex", Model: "gpt-6-astra", ReasoningEffort: "high"})
		payload := PreservationPayload{Schema: "ao-custody-preservation/v1", ExecutionOrigin: "preparing_without_runtime", Project: a.Project, SessionID: a.SessionID, AttemptID: a.AttemptID, Generation: a.Generation, RequestID: a.RequestID, ForegroundID: a.ForegroundID, Candidate: c, ProviderID: "", Route: route, Transcript: "", TurnID: "", CompletedContextSHA256: "", Children: []ChildPreservation{}, Members: []ProcessIdentity{}}
		if err = os.WriteFile(filepath.Join(root, "candidate.json"), b, 0600); err != nil {
			t.Fatal(err)
		}
		data, e := json.Marshal(payload)
		if e != nil {
			t.Fatal(e)
		}
		if e = os.WriteFile(filepath.Join(root, "preservation.json"), append(data, '\n'), 0600); e != nil {
			t.Fatal(e)
		}
	}
}
func TestCaptureCandidateCleanDiffEncodesEmptyString(t *testing.T) {
	a, _ := candidateFixture(t)
	fixtureGit(t, a.Workspace, "add", "-A")
	fixtureGit(t, a.Workspace, "commit", "-m", "settled fixture")
	c, err := CaptureCandidate(context.Background(), a, "fixture/repository")
	if err != nil {
		t.Fatal(err)
	}
	b, _ := json.Marshal(c)
	if !bytes.Contains(b, []byte(`"staged_diff":""`)) || !bytes.Contains(b, []byte(`"unstaged_diff":""`)) {
		t.Fatal("empty diff is not a base64 string")
	}
}

func TestCaptureRejectsApplicableExecutableFilterBeforeRunningIt(t *testing.T) {
	a, _ := candidateFixture(t)
	marker := filepath.Join(a.Workspace, "filter-ran")
	fixtureGit(t, a.Workspace, "config", "filter.probe.clean", "touch '"+marker+"'; cat")
	if err := os.WriteFile(filepath.Join(a.Workspace, ".gitattributes"), []byte("progress.txt filter=probe\n"), 0644); err != nil {
		t.Fatal(err)
	}
	if _, err := CaptureCandidate(context.Background(), a, "fixture/repository"); err == nil {
		t.Fatal("executable clean filter accepted")
	}
	if _, err := os.Stat(marker); !os.IsNotExist(err) {
		t.Fatal("observation executed a filter")
	}
}
func TestRawTrackedFingerprintCatchesAssumeUnchangedAndPermissions(t *testing.T) {
	a, original := candidateFixture(t)
	fixtureGit(t, a.Workspace, "update-index", "--assume-unchanged", "executable")
	before, err := CaptureCandidate(context.Background(), a, "fixture/repository")
	if err != nil {
		t.Fatal(err)
	}
	if before.RawTrackedSHA256 != original.RawTrackedSHA256 {
		t.Fatal("index flag changed raw byte fingerprint")
	}
	if err = os.WriteFile(filepath.Join(a.Workspace, "executable"), []byte("hidden raw progress\r\n"), 0755); err != nil {
		t.Fatal(err)
	}
	after, err := CaptureCandidate(context.Background(), a, "fixture/repository")
	if err != nil {
		t.Fatal(err)
	}
	if after.RawTrackedSHA256 == before.RawTrackedSHA256 {
		t.Fatal("assume-unchanged concealed changed bytes")
	}
	if err = os.Chmod(filepath.Join(a.Workspace, "executable"), 0700); err != nil {
		t.Fatal(err)
	}
	modes, err := CaptureCandidate(context.Background(), a, "fixture/repository")
	if err != nil {
		t.Fatal(err)
	}
	if modes.RawTrackedSHA256 == after.RawTrackedSHA256 {
		t.Fatal("non-executable mode changes concealed")
	}
}
func TestCaptureAllowsUnusedFilterButRejectsGitlinks(t *testing.T) {
	a, _ := candidateFixture(t)
	fixtureGit(t, a.Workspace, "config", "filter.unused.clean", "false")
	if _, err := CaptureCandidate(context.Background(), a, "fixture/repository"); err != nil {
		t.Fatalf("unused filter refused: %v", err)
	}
	fixtureGit(t, a.Workspace, "update-index", "--add", "--cacheinfo", "160000,"+a.OriginalBaseSHA+",nested")
	if _, err := CaptureCandidate(context.Background(), a, "fixture/repository"); err == nil {
		t.Fatal("unproven submodule custody accepted")
	}
}

// The exported historical identities below exercise the cross-language wire;
// they are deliberately synthetic and are never used as live signal evidence.
func TestCandidateChildCertificateWireFixture(t *testing.T) {
	root := os.Getenv("AO_CUSTODY_CANARY_DIR")
	if root == "" {
		t.Skip("explicit wire fixture export only")
	}
	a, candidate := candidateFixture(t)
	member := func(pid, parent int, path, role string) ProcessIdentity {
		return ProcessIdentity{PID: pid, ParentPID: parent, GroupID: parent, UID: uint32(os.Getuid()), StartSeconds: 1, StartMicroseconds: 2, AuditToken: [8]uint32{0, uint32(os.Getuid()), 0, 0, 0, uint32(pid), 0, 1}, Status: 4, Executable: path, Role: role}
	}
	members := []ProcessIdentity{member(41001, 41000, "/bin/sh", "pane_parent"), member(41002, 41001, "/tmp/wire-only/codex", "writer")}
	members[0].Status = 2
	childMembers := []ProcessIdentity{member(42001, 41000, "/bin/sh", "pane_parent"), member(42002, 42001, "/tmp/wire-only/codex", "writer")}
	childMembers[0].Status = 2
	route, err := json.Marshal(&domain.AgentRoute{Harness: domain.HarnessCodex, Model: "gpt-6-astra", ReasoningEffort: domain.ReasoningEffortHigh})
	if err != nil {
		t.Fatal(err)
	}
	payload := PreservationPayload{Schema: "ao-custody-preservation/v1", ExecutionOrigin: "retained_provider", Project: a.Project, SessionID: a.SessionID, AttemptID: a.AttemptID, Generation: a.Generation, RequestID: a.RequestID, ForegroundID: a.ForegroundID, Candidate: candidate, ProviderID: "11111111-1111-4111-8111-111111111111", Route: route, Transcript: filepath.Join(root, "wire-parent-transcript.jsonl"), TurnID: "parent-turn", CompletedContextSHA256: hashBytes([]byte("synthetic parent wire context")), Members: members, Children: []ChildPreservation{{Operation: "reviewer:fixture-run", HandleID: "review-" + a.SessionID, ProviderID: "22222222-2222-4222-8222-222222222222", Transcript: filepath.Join(root, "wire-child-transcript.jsonl"), TurnID: "child-turn", CompletedContextSHA256: hashBytes([]byte("synthetic child wire context")), Members: childMembers}}}
	data, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(root, "preservation-children-wire.json"), append(data, '\n'), 0600); err != nil {
		t.Fatal(err)
	}
}
