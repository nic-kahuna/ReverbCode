package gitworktree

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
)

func TestWorkspaceExactRestoreEnforcesPersistedPhysicalDisposition(t *testing.T) {
	git := requireGit(t)
	tmp := t.TempDir()
	repo := setupOriginClone(t, git, tmp)
	ws, err := New(Options{Binary: git, ManagedRoot: filepath.Join(tmp, "managed"), RepoResolver: StaticRepoResolver{"proj": repo}})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx := context.Background()
	cfg := ports.WorkspaceConfig{ProjectID: "proj", SessionID: "sess-exact", Branch: "feature/exact"}
	info, err := ws.Create(ctx, cfg)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	cfg.Path = info.Path

	if err := ws.PreflightExactRestore(ctx, cfg, ports.WorkspaceRecoveryInPlace, ""); err != nil {
		t.Fatalf("in-place preflight: %v", err)
	}
	if _, err := ws.RestoreExact(ctx, cfg, ports.WorkspaceRecoveryInPlace, ""); err != nil {
		t.Fatalf("in-place restore: %v", err)
	}
	if err := ws.PreflightExactRestore(ctx, cfg, ports.WorkspaceRecoveryRemoved, ""); !errors.Is(err, ports.ErrWorkspaceRestoreAmbiguous) {
		t.Fatalf("removed disposition against registered path error = %v, want ambiguity", err)
	}

	if err := ws.ForceDestroy(ctx, info); err != nil {
		t.Fatalf("ForceDestroy fixture: %v", err)
	}
	if err := ws.PreflightExactRestore(ctx, cfg, ports.WorkspaceRecoveryInPlace, ""); !errors.Is(err, ports.ErrWorkspaceRestoreAmbiguous) {
		t.Fatalf("in-place disposition against absent path error = %v, want ambiguity", err)
	}
	if err := ws.PreflightExactRestore(ctx, cfg, ports.WorkspaceRecoveryRemoved, ""); err != nil {
		t.Fatalf("removed preflight: %v", err)
	}
	restored, err := ws.RestoreExact(ctx, cfg, ports.WorkspaceRecoveryRemoved, "")
	if err != nil {
		t.Fatalf("removed exact restore: %v", err)
	}
	if restored.Path != info.Path || restored.Branch != info.Branch {
		t.Fatalf("restored = %#v, want exact %#v", restored, info)
	}
}

func TestWorkspaceExactRestoreRejectsStrayWithoutMovingOrDeleting(t *testing.T) {
	git := requireGit(t)
	tmp := t.TempDir()
	repo := setupOriginClone(t, git, tmp)
	ws, err := New(Options{Binary: git, ManagedRoot: filepath.Join(tmp, "managed"), RepoResolver: StaticRepoResolver{"proj": repo}})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx := context.Background()
	cfg := ports.WorkspaceConfig{ProjectID: "proj", SessionID: "sess-stray", Branch: "feature/stray"}
	info, err := ws.Create(ctx, cfg)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if err := ws.ForceDestroy(ctx, info); err != nil {
		t.Fatalf("ForceDestroy fixture: %v", err)
	}
	cfg.Path = info.Path
	if err := os.MkdirAll(info.Path, 0o755); err != nil {
		t.Fatalf("MkdirAll stray: %v", err)
	}
	keep := filepath.Join(info.Path, "do-not-move.txt")
	if err := os.WriteFile(keep, []byte("preserve me\n"), 0o644); err != nil {
		t.Fatalf("WriteFile stray: %v", err)
	}

	if err := ws.PreflightExactRestore(ctx, cfg, ports.WorkspaceRecoveryRemoved, ""); !errors.Is(err, ports.ErrWorkspaceRestoreAmbiguous) {
		t.Fatalf("preflight error = %v, want ambiguity", err)
	}
	if _, err := ws.RestoreExact(ctx, cfg, ports.WorkspaceRecoveryRemoved, ""); !errors.Is(err, ports.ErrWorkspaceRestoreAmbiguous) {
		t.Fatalf("restore error = %v, want ambiguity", err)
	}
	if got, err := os.ReadFile(keep); err != nil || string(got) != "preserve me\n" {
		t.Fatalf("stray content changed: data=%q err=%v", got, err)
	}
	if matches, err := filepath.Glob(info.Path + ".stray*"); err != nil || len(matches) != 0 {
		t.Fatalf("exact restore moved stray path aside: matches=%v err=%v", matches, err)
	}
}

func TestWorkspaceExactRestoreValidatesPreservedRefBeforeMaterializing(t *testing.T) {
	git := requireGit(t)
	tmp := t.TempDir()
	repo := setupOriginClone(t, git, tmp)
	ws, err := New(Options{Binary: git, ManagedRoot: filepath.Join(tmp, "managed"), RepoResolver: StaticRepoResolver{"proj": repo}})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx := context.Background()
	cfg := ports.WorkspaceConfig{ProjectID: "proj", SessionID: "sess-ref", Branch: "feature/ref"}
	info, err := ws.Create(ctx, cfg)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if err := os.WriteFile(filepath.Join(info.Path, "README.md"), []byte("preserved edit\n"), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	ref, err := ws.StashUncommitted(ctx, info)
	if err != nil || ref == "" {
		t.Fatalf("StashUncommitted = %q, %v", ref, err)
	}
	if err := ws.ForceDestroy(ctx, info); err != nil {
		t.Fatalf("ForceDestroy fixture: %v", err)
	}
	cfg.Path = info.Path

	if err := ws.PreflightExactRestore(ctx, cfg, ports.WorkspaceRecoveryRemoved, ref); err != nil {
		t.Fatalf("valid preserved ref preflight: %v", err)
	}
	wrongOwner := "refs/ao/preserved/another-session"
	if err := ws.PreflightExactRestore(ctx, cfg, ports.WorkspaceRecoveryRemoved, wrongOwner); !errors.Is(err, ports.ErrWorkspaceRestoreAmbiguous) {
		t.Fatalf("wrong-owner ref error = %v, want ambiguity", err)
	}
	missing := "refs/ao/preserved/sess-ref/missing"
	if err := ws.PreflightExactRestore(ctx, cfg, ports.WorkspaceRecoveryRemoved, missing); !errors.Is(err, ports.ErrWorkspaceRestoreAmbiguous) {
		t.Fatalf("descendant ref error = %v, want ambiguity", err)
	}
	if _, err := os.Stat(info.Path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("read-only preflight materialized path: %v", err)
	}
}

func TestWorkspaceApplyPreservedExactRevalidatesRefAtMutationBoundary(t *testing.T) {
	git := requireGit(t)
	tmp := t.TempDir()
	repo := setupOriginClone(t, git, tmp)
	ws, err := New(Options{Binary: git, ManagedRoot: filepath.Join(tmp, "managed"), RepoResolver: StaticRepoResolver{"proj": repo}})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx := context.Background()
	info, err := ws.Create(ctx, ports.WorkspaceConfig{ProjectID: "proj", SessionID: "sess-toctou", Branch: "feature/toctou"})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if err := os.WriteFile(filepath.Join(info.Path, "README.md"), []byte("saved\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	ref, err := ws.StashUncommitted(ctx, info)
	if err != nil {
		t.Fatal(err)
	}
	if err := ws.PreflightExactRestore(ctx, ports.WorkspaceConfig{ProjectID: "proj", SessionID: info.SessionID, Branch: info.Branch, Path: info.Path}, ports.WorkspaceRecoveryInPlace, ref); err != nil {
		t.Fatalf("preflight: %v", err)
	}
	runGit(t, git, info.Path, "reset", "--hard", "HEAD")
	if err := os.WriteFile(filepath.Join(info.Path, "moved.txt"), []byte("move\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	runGit(t, git, info.Path, "add", "moved.txt")
	runGit(t, git, info.Path, "commit", "-m", "move after preflight")
	if err := ws.ApplyPreservedExact(ctx, info, ref); !errors.Is(err, ports.ErrWorkspaceRestoreAmbiguous) {
		t.Fatalf("ApplyPreservedExact error = %v, want ambiguity", err)
	}
	if got, err := os.ReadFile(filepath.Join(info.Path, "README.md")); err != nil || string(got) == "saved\n" {
		t.Fatalf("failed revalidation applied preserved content: data=%q err=%v", got, err)
	}
}

func TestWorkspaceApplyPreservedExactRejectsDetachedWorktreeAtMutationBoundary(t *testing.T) {
	git := requireGit(t)
	tmp := t.TempDir()
	repo := setupOriginClone(t, git, tmp)
	ws, err := New(Options{Binary: git, ManagedRoot: filepath.Join(tmp, "managed"), RepoResolver: StaticRepoResolver{"proj": repo}})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	info, err := ws.Create(ctx, ports.WorkspaceConfig{ProjectID: "proj", SessionID: "sess-detached", Branch: "feature/detached"})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(info.Path, "README.md"), []byte("saved\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	ref, err := ws.StashUncommitted(ctx, info)
	if err != nil {
		t.Fatal(err)
	}
	runGit(t, git, info.Path, "reset", "--hard", "HEAD")
	runGit(t, git, info.Path, "checkout", "--detach", "HEAD")
	if err := ws.ApplyPreservedExact(ctx, info, ref); !errors.Is(err, ports.ErrWorkspaceRestoreAmbiguous) {
		t.Fatalf("ApplyPreservedExact error = %v, want ambiguity", err)
	}
	got, err := os.ReadFile(filepath.Join(info.Path, "README.md"))
	if err != nil || string(got) == "saved\n" {
		t.Fatalf("detached apply changed worktree: data=%q err=%v", got, err)
	}
}

func TestWorkspaceExactRestoreProvesBaseAncestry(t *testing.T) {
	git := requireGit(t)
	tmp := t.TempDir()
	repo := setupOriginClone(t, git, tmp)
	ws, err := New(Options{Binary: git, ManagedRoot: filepath.Join(tmp, "managed"), RepoResolver: StaticRepoResolver{"proj": repo}})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx := context.Background()
	cfg := ports.WorkspaceConfig{ProjectID: "proj", SessionID: "sess-base", Branch: "feature/base"}
	info, err := ws.Create(ctx, cfg)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	base, err := ws.revParse(ctx, repo, "HEAD")
	if err != nil {
		t.Fatalf("resolve base: %v", err)
	}
	if err := os.WriteFile(filepath.Join(info.Path, "descendant.txt"), []byte("descendant\n"), 0o644); err != nil {
		t.Fatalf("write descendant: %v", err)
	}
	runGit(t, git, info.Path, "add", "descendant.txt")
	runGit(t, git, info.Path, "commit", "-m", "descendant")
	cfg.Path, cfg.BaseSHA = info.Path, base
	if err := ws.PreflightExactRestore(ctx, cfg, ports.WorkspaceRecoveryInPlace, ""); err != nil {
		t.Fatalf("ancestor base rejected for descendant branch: %v", err)
	}

	runGit(t, git, repo, "branch", "unrelated-base", "main")
	runGit(t, git, repo, "checkout", "unrelated-base")
	if err := os.WriteFile(filepath.Join(repo, "unrelated.txt"), []byte("unrelated\n"), 0o644); err != nil {
		t.Fatalf("write unrelated: %v", err)
	}
	runGit(t, git, repo, "add", "unrelated.txt")
	runGit(t, git, repo, "commit", "-m", "unrelated")
	unrelated, err := ws.revParse(ctx, repo, "HEAD")
	if err != nil {
		t.Fatalf("resolve unrelated: %v", err)
	}
	cfg.BaseSHA = unrelated
	if err := ws.PreflightExactRestore(ctx, cfg, ports.WorkspaceRecoveryInPlace, ""); !errors.Is(err, ports.ErrWorkspaceRestoreAmbiguous) {
		t.Fatalf("unrelated base error = %v, want ambiguity", err)
	}
	if _, err := os.Stat(filepath.Join(info.Path, "descendant.txt")); err != nil {
		t.Fatalf("failed ancestry preflight changed worktree: %v", err)
	}
}

func TestWorkspaceExactRestoreRejectsMovedBranchHeadForPreservedRef(t *testing.T) {
	git := requireGit(t)
	tmp := t.TempDir()
	repo := setupOriginClone(t, git, tmp)
	ws, err := New(Options{Binary: git, ManagedRoot: filepath.Join(tmp, "managed"), RepoResolver: StaticRepoResolver{"proj": repo}})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx := context.Background()
	cfg := ports.WorkspaceConfig{ProjectID: "proj", SessionID: "sess-moved", Branch: "feature/moved"}
	info, err := ws.Create(ctx, cfg)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if err := os.WriteFile(filepath.Join(info.Path, "README.md"), []byte("preserve me\n"), 0o644); err != nil {
		t.Fatalf("write preserved edit: %v", err)
	}
	ref, err := ws.StashUncommitted(ctx, info)
	if err != nil || ref == "" {
		t.Fatalf("StashUncommitted = %q, %v", ref, err)
	}
	if err := os.WriteFile(filepath.Join(info.Path, "after.txt"), []byte("moves head\n"), 0o644); err != nil {
		t.Fatalf("write head move: %v", err)
	}
	runGit(t, git, info.Path, "add", "after.txt")
	runGit(t, git, info.Path, "commit", "-m", "move branch head")
	cfg.Path = info.Path
	if err := ws.PreflightExactRestore(ctx, cfg, ports.WorkspaceRecoveryInPlace, ref); !errors.Is(err, ports.ErrWorkspaceRestoreAmbiguous) {
		t.Fatalf("moved branch head error = %v, want ambiguity", err)
	}
	if _, err := ws.revParse(ctx, repo, ref); err != nil {
		t.Fatalf("failed preflight consumed preserved ref: %v", err)
	}
}

func TestWorkspaceApplyPreservedExactRetainsRefUntilExplicitDelete(t *testing.T) {
	git := requireGit(t)
	tmp := t.TempDir()
	repo := setupOriginClone(t, git, tmp)
	ws, err := New(Options{Binary: git, ManagedRoot: filepath.Join(tmp, "managed"), RepoResolver: StaticRepoResolver{"proj": repo}})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx := context.Background()
	info, err := ws.Create(ctx, ports.WorkspaceConfig{ProjectID: "proj", SessionID: "sess-retain", Branch: "feature/retain"})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if err := os.WriteFile(filepath.Join(info.Path, "README.md"), []byte("managed edit\n"), 0o644); err != nil {
		t.Fatalf("write edit: %v", err)
	}
	ref, err := ws.StashUncommitted(ctx, info)
	if err != nil || ref == "" {
		t.Fatalf("StashUncommitted = %q, %v", ref, err)
	}
	if err := ws.ForceDestroy(ctx, info); err != nil {
		t.Fatalf("ForceDestroy fixture: %v", err)
	}
	cfg := ports.WorkspaceConfig{ProjectID: "proj", SessionID: "sess-retain", Branch: "feature/retain", Path: info.Path}
	restored, err := ws.RestoreExact(ctx, cfg, ports.WorkspaceRecoveryRemoved, ref)
	if err != nil {
		t.Fatalf("RestoreExact: %v", err)
	}
	if err := ws.ApplyPreservedExact(ctx, restored, ref); err != nil {
		t.Fatalf("ApplyPreservedExact: %v", err)
	}
	if _, err := ws.revParse(ctx, repo, ref); err != nil {
		t.Fatalf("exact apply consumed ref before durable commit: %v", err)
	}
	if err := ws.DeletePreservedRef(ctx, restored, ref); err != nil {
		t.Fatalf("DeletePreservedRef: %v", err)
	}
	if _, err := ws.revParse(ctx, repo, ref); err == nil {
		t.Fatalf("preserved ref %q still exists after explicit delete", ref)
	}
	got, err := os.ReadFile(filepath.Join(restored.Path, "README.md"))
	if err != nil || string(got) != "managed edit\n" {
		t.Fatalf("applied work changed by ref cleanup: data=%q err=%v", got, err)
	}
}
