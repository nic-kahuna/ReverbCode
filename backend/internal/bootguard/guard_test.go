package bootguard

import (
	"bytes"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

func TestInspectDoesNotCreateState(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "absent")
	got, err := Inspect(dir)
	if err != nil || got.State != "missing_legacy" || !got.InspectionOnly {
		t.Fatalf("Inspect = %+v, %v", got, err)
	}
	if _, err := os.Stat(dir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("inspection created state: %v", err)
	}
}

func TestGuardRejectsInvalidEvidenceWithoutChangingBytes(t *testing.T) {
	for _, tc := range []struct {
		name, data string
		want       error
	}{
		{"future", string(encode(Marker{MarkerSchema, 2})), ErrUnsupported},
		{"zero", `{"schema":"ao-data-compatibility/v1","requiredProtocol":0}` + "\n", ErrMalformed},
		{"bool", `{"schema":"ao-data-compatibility/v1","requiredProtocol":true}` + "\n", ErrMalformed},
		{"duplicate", `{"schema":"ao-data-compatibility/v1","requiredProtocol":2,"requiredProtocol":1}` + "\n", ErrMalformed},
		{"unknown", `{"schema":"ao-data-compatibility/v1","requiredProtocol":1,"extra":true}` + "\n", ErrMalformed},
		{"truncated", `{"schema":`, ErrMalformed},
		{"future_schema", `{"schema":"ao-data-compatibility/v2","requiredProtocol":1}` + "\n", ErrMalformed},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			marker := filepath.Join(dir, MarkerName)
			db := filepath.Join(dir, "ao.db")
			if err := os.WriteFile(marker, []byte(tc.data), 0600); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(db, []byte("database must not be opened"), 0600); err != nil {
				t.Fatal(err)
			}
			g, err := Open(dir)
			if g != nil || !errors.Is(err, tc.want) {
				t.Fatalf("Open = %v,%v", g, err)
			}
			got, _ := os.ReadFile(marker)
			if !bytes.Equal(got, []byte(tc.data)) {
				t.Fatal("marker changed")
			}
			got, _ = os.ReadFile(db)
			if string(got) != "database must not be opened" {
				t.Fatal("database changed")
			}
		})
	}
}

func TestGuardRatchetIsMonotonicAndRequiresLiveOwnership(t *testing.T) {
	dir := t.TempDir()
	g, err := open(dir, 2)
	if err != nil {
		t.Fatal(err)
	}
	if err := g.Ratchet(2); err != nil {
		t.Fatal(err)
	}
	if err := g.Ratchet(1); !errors.Is(err, ErrDowngrade) {
		t.Fatalf("downgrade: %v", err)
	}
	if err := g.Ratchet(3); !errors.Is(err, ErrUnsupported) {
		t.Fatalf("unknown format: %v", err)
	}
	if err := g.Ratchet(2); err != nil {
		t.Fatalf("idempotent durability confirmation: %v", err)
	}
	if err := g.Close(); err != nil {
		t.Fatal(err)
	}
	if err := g.Ratchet(2); !errors.Is(err, ErrOwnership) {
		t.Fatalf("closed ratchet: %v", err)
	}
	if g, err := Open(dir); g != nil || !errors.Is(err, ErrUnsupported) {
		t.Fatalf("older supported build accepted newer state: %v,%v", g, err)
	}
}

func TestGuardMissingHeldMarkerCannotBeRecreatedByRatchet(t *testing.T) {
	dir := t.TempDir()
	g, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = g.Close() }()
	if err := os.Remove(filepath.Join(dir, MarkerName)); err != nil {
		t.Fatal(err)
	}
	if err := g.Ratchet(1); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("missing marker: %v", err)
	}
}

func TestGuardRejectsMarkerSymlink(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(t.TempDir(), "target")
	if err := os.WriteFile(target, encode(Marker{MarkerSchema, 1}), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, filepath.Join(dir, MarkerName)); err != nil {
		t.Skipf("symlink: %v", err)
	}
	if g, err := Open(dir); g != nil || !errors.Is(err, ErrMalformed) {
		t.Fatalf("symlink accepted: %v,%v", g, err)
	}
}

// Child uses the real process lock. Killing it after publication simulates a
// crash between the protocol ratchet and a later product-state write.
func TestGuardProcess(t *testing.T) {
	dir := os.Getenv("AO_TEST_GUARD_DIR")
	if dir == "" {
		return
	}
	g, err := open(dir, 2)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = g.Close() }()
	if err := g.Ratchet(2); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "ready"), []byte("ready"), 0600); err != nil {
		t.Fatal(err)
	}
	time.Sleep(30 * time.Second)
}

func TestGuardRatchetSurvivesProcessDeathAndAliasCannotContend(t *testing.T) {
	dir := t.TempDir()
	cmd := exec.Command(os.Args[0], "-test.run=^TestGuardProcess$")
	cmd.Env = append(os.Environ(), "AO_TEST_GUARD_DIR="+dir)
	var output bytes.Buffer
	cmd.Stdout = &output
	cmd.Stderr = &output
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
			t.Fatalf("child not ready: %s", output.String())
		}
		time.Sleep(10 * time.Millisecond)
	}
	alias := filepath.Join(t.TempDir(), "alias")
	if err := os.Symlink(dir, alias); err != nil {
		t.Skipf("symlink: %v", err)
	}
	if g, err := open(alias, 2); g != nil || err == nil {
		t.Fatalf("alias contender accepted: %v,%v", g, err)
	}
	if err := cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	_ = cmd.Wait()
	if g, err := Open(dir); g != nil || !errors.Is(err, ErrUnsupported) {
		t.Fatalf("crashed ratchet lost: %v,%v", g, err)
	}
}

func TestInspectionCanonicalMissingAliasChild(t *testing.T) {
	root := t.TempDir()
	target := filepath.Join(root, "target")
	if err := os.Mkdir(target, 0o700); err != nil {
		t.Fatal(err)
	}
	alias := filepath.Join(root, "alias")
	if err := os.Symlink(target, alias); err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(alias, "absent", "data")
	before, err := Inspect(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(target, "absent")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("inspection wrote: %v", err)
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	after, err := Inspect(dir)
	if err != nil || before.MarkerPath != after.MarkerPath {
		t.Fatalf("alias inspection drift: %+v %+v %v", before, after, err)
	}
}

func TestInspectionUnavailableAliasRetainsTypedFailure(t *testing.T) {
	root := t.TempDir()
	alias := filepath.Join(root, "dangling")
	if err := os.Symlink(filepath.Join(root, "absent"), alias); err != nil {
		t.Fatal(err)
	}
	got, err := Inspect(filepath.Join(alias, "data"))
	if !errors.Is(err, ErrUnavailable) || got.State != "unavailable" || got.Schema != "ao-compatibility/v1" || !got.InspectionOnly || got.SupportedProtocol != SupportedProtocol {
		t.Fatalf("unavailable wire: %+v %v", got, err)
	}
	if _, err := os.Stat(filepath.Join(root, "absent")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("inspection repaired alias: %v", err)
	}
}
