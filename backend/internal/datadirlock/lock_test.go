package datadirlock

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestAcquireIsExclusiveAndReacquirable(t *testing.T) {
	dir := t.TempDir()
	first, err := Acquire(dir)
	if err != nil {
		t.Fatalf("first Acquire: %v", err)
	}

	second, err := Acquire(dir)
	if second != nil {
		_ = second.Close()
		t.Fatal("second Acquire unexpectedly returned a lock")
	}
	if !errors.Is(err, ErrLocked) {
		t.Fatalf("second Acquire error = %v, want ErrLocked", err)
	}

	if err := first.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	third, err := Acquire(dir)
	if err != nil {
		t.Fatalf("reacquire after Close: %v", err)
	}
	if err := third.Close(); err != nil {
		t.Fatalf("third Close: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, lockFileName)); err != nil {
		t.Fatalf("retained lock file: %v", err)
	}
}

func TestAcquireCanonicalizesSymlinkAliases(t *testing.T) {
	parent := t.TempDir()
	realDir := filepath.Join(parent, "real")
	if err := os.Mkdir(realDir, 0o750); err != nil {
		t.Fatal(err)
	}
	alias := filepath.Join(parent, "alias")
	if err := os.Symlink(realDir, alias); err != nil {
		if errors.Is(err, os.ErrPermission) {
			t.Skipf("symlink unavailable: %v", err)
		}
		t.Fatal(err)
	}

	lock, err := Acquire(alias)
	if err != nil {
		t.Fatalf("Acquire alias: %v", err)
	}
	t.Cleanup(func() { _ = lock.Close() })
	canonicalReal, err := filepath.EvalSymlinks(realDir)
	if err != nil {
		t.Fatal(err)
	}
	if lock.DataDir() != canonicalReal {
		t.Fatalf("DataDir = %q, want canonical %q", lock.DataDir(), canonicalReal)
	}
	if contender, err := Acquire(realDir); contender != nil || !errors.Is(err, ErrLocked) {
		if contender != nil {
			_ = contender.Close()
		}
		t.Fatalf("real-path contender = (%v, %v), want nil ErrLocked", contender, err)
	}
}

func TestAcquireDistinctDirectoriesAreIndependent(t *testing.T) {
	one, err := Acquire(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = one.Close() })
	two, err := Acquire(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = two.Close() })
}
