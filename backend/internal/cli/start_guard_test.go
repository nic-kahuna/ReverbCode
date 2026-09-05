package cli

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/aoagents/agent-orchestrator/backend/internal/bootguard"
)

func managedFixture(t *testing.T) (string, string) {
	t.Helper()
	bundle := filepath.Join(t.TempDir(), "AO.app")
	executable := filepath.Join(bundle, "Contents", "Resources", "daemon", "ao")
	for _, p := range []string{executable, filepath.Join(bundle, "Contents", "Info.plist"), filepath.Join(bundle, "Contents", "MacOS", "agent-orchestrator")} {
		if err := os.MkdirAll(filepath.Dir(p), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte("fixture"), 0o700); err != nil {
			t.Fatal(err)
		}
	}
	canonical, err := filepath.EvalSymlinks(bundle)
	if err != nil {
		t.Fatal(err)
	}
	return canonical, executable
}

func TestStartBindsContainingBundleBeforeDiscovery(t *testing.T) {
	cfg := setConfigEnv(t)
	t.Setenv("HOME", t.TempDir())
	bundle, executable := managedFixture(t)
	link := filepath.Join(t.TempDir(), "ao")
	if err := os.Symlink(executable, link); err != nil {
		t.Fatal(err)
	}
	writeMarker(t, cfg, "/stale/Old.app")
	t.Cleanup(swapScanLocations(func() []string { t.Fatal("managed launch scanned alternative bundles"); return nil }))
	var opened []string
	deps := Deps{StartProcess: func(cfg processStartConfig) error { opened = append([]string{cfg.Path}, cfg.Args...); return nil }, Executable: func() (string, error) { return link, nil }, CommandOutput: func(_ context.Context, name string, args ...string) ([]byte, error) {
		opened = append([]string{name}, args...)
		return nil, nil
	}}.withDefaults()
	c := &commandContext{deps: deps}
	got, err := c.authorizedStartBundle()
	if err != nil || got != bundle {
		t.Fatalf("bound bundle = %q, %v", got, err)
	}
	if _, err := os.Stat(cfg.dataDir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("inspection created data: %v", err)
	}
	// The host's real platform open command is injected, never invoked.
	if _, err := c.openApp(context.Background(), got); err != nil {
		t.Fatal(err)
	}
	if len(opened) == 0 || !strings.Contains(strings.Join(opened, " "), bundle) {
		t.Fatalf("opened %v", opened)
	}
}

func TestStartRefusalBeforeAnyOpenOrFetch(t *testing.T) {
	for _, tc := range []struct {
		name, marker, data string
		managed            bool
	}{
		{name: "future", marker: "{\"schema\":\"ao-data-compatibility/v1\",\"requiredProtocol\":2}\n", managed: true},
		{name: "malformed", marker: "{}", managed: true},
		{name: "legacy database", data: "ao.db"},
		{name: "other durable entry", data: "ao.lock"},
		{name: "compatible standalone", marker: "{\"schema\":\"ao-data-compatibility/v1\",\"requiredProtocol\":1}\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := setConfigEnv(t)
			t.Setenv("HOME", t.TempDir())
			if err := os.MkdirAll(cfg.dataDir, 0o700); err != nil {
				t.Fatal(err)
			}
			if tc.marker != "" {
				if err := os.WriteFile(filepath.Join(cfg.dataDir, bootguard.MarkerName), []byte(tc.marker), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			if tc.data != "" {
				if err := os.WriteFile(filepath.Join(cfg.dataDir, tc.data), []byte("preserved"), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			_, exe := managedFixture(t)
			if !tc.managed {
				exe = os.Args[0]
			}
			t.Cleanup(swapScanLocations(func() []string { t.Fatal("refused launch scanned"); return nil }))
			_, _, err := executeCLI(t, Deps{Executable: func() (string, error) { return exe, nil }, CommandOutput: func(context.Context, string, ...string) ([]byte, error) {
				t.Fatal("refused launch opened or unpacked")
				return nil, nil
			}}, "start", "--json")
			if err == nil {
				t.Fatal("unsafe launch succeeded")
			}
			if tc.marker != "" {
				got, e := os.ReadFile(filepath.Join(cfg.dataDir, bootguard.MarkerName))
				if e != nil || string(got) != tc.marker {
					t.Fatalf("marker changed: %q, %v", got, e)
				}
			}
		})
	}
}

func TestStartFreshStandaloneAndIncompleteBundle(t *testing.T) {
	setConfigEnv(t)
	t.Setenv("HOME", t.TempDir())
	c := &commandContext{deps: Deps{}.withDefaults()}
	if got, err := c.authorizedStartBundle(); err != nil || got != "" {
		t.Fatalf("fresh bootstrap blocked: %q %v", got, err)
	}
	bundle, exe := managedFixture(t)
	if err := os.Remove(filepath.Join(bundle, "Contents", "Info.plist")); err != nil {
		t.Fatal(err)
	}
	c.deps.Executable = func() (string, error) { return exe, nil }
	if _, err := c.authorizedStartBundle(); err == nil || !strings.Contains(err.Error(), "AO_START_REPAIR_REQUIRED") {
		t.Fatalf("incomplete bundle: %v", err)
	}
}
