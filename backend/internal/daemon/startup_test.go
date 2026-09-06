package daemon

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/bootguard"
	"github.com/aoagents/agent-orchestrator/backend/internal/config"
	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/runfile"
	"github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite"
)

func startupConfig(t *testing.T) config.Config {
	t.Helper()
	dir := t.TempDir()
	return config.Config{DataDir: filepath.Join(dir, "data"), RunFilePath: filepath.Join(dir, "running.json")}
}

func TestPrepareStartPausedPersistsAdmissionAndPreservesOtherConfig(t *testing.T) {
	cfg := startupConfig(t)
	ctx := context.Background()
	s, err := sqlite.Open(cfg.DataDir)
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"legacy", "configured", "archived"} {
		row := domain.ProjectRecord{ID: id, Path: "/repos/" + id, DisplayName: id, RegisteredAt: time.Now()}
		if id == "archived" {
			row.ArchivedAt = time.Now()
		}
		if err := s.UpsertProject(ctx, row); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	raw, err := sql.Open("sqlite", filepath.Join(cfg.DataDir, "ao.db"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := raw.Exec(`UPDATE projects SET config='{"defaultBranch":"develop","env":{"KEEP":"true"},"admissionPaused":false}' WHERE id='configured'`); err != nil {
		t.Fatal(err)
	}
	_ = raw.Close()
	proof, err := PrepareStartPaused(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(proof.ProjectIDs, []string{"archived", "configured", "legacy"}) || !proof.AdmissionPaused || proof.MutationLanesStarted || !proof.PreparationOnly || !proof.ExistingSessionsMayBeRunning {
		t.Fatalf("proof %+v", proof)
	}
	if _, err := os.Stat(cfg.RunFilePath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("preparation started daemon: %v", err)
	}
	guard, err := bootguard.Open(cfg.DataDir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = guard.Close() }()
	s, _, err = openGuardedStartupStore(ctx, guard, false)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()
	for _, id := range proof.ProjectIDs {
		row, ok, err := s.GetProject(ctx, id)
		if err != nil || !ok || !row.Config.AdmissionPaused {
			t.Fatalf("persisted pause %s: %+v,%v", id, row, err)
		}
	}
	raw, err = sql.Open("sqlite", filepath.Join(cfg.DataDir, "ao.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	var value string
	if err := raw.QueryRow(`SELECT config FROM projects WHERE id='configured'`).Scan(&value); err != nil {
		t.Fatal(err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal([]byte(value), &fields); err != nil {
		t.Fatal(err)
	}
	if string(fields["env"]) != `{"KEEP":"true"}` || string(fields["admissionPaused"]) != "true" {
		t.Fatalf("unrelated config changed: %s", value)
	}
}

func TestGuardedStoreHoldRatchetsCompatibilityFloor(t *testing.T) {
	cfg := startupConfig(t)
	ctx := context.Background()
	guard, err := bootguard.Open(cfg.DataDir)
	if err != nil {
		t.Fatal(err)
	}
	store, _, err := openGuardedStartupStore(ctx, guard, false)
	if err != nil {
		t.Fatal(err)
	}
	project := domain.ProjectRecord{ID: "hold", Path: "/repos/hold", RegisteredAt: time.Now()}
	if err := store.UpsertProject(ctx, project); err != nil {
		t.Fatal(err)
	}
	rec, err := store.CreateSession(ctx, domain.SessionRecord{ProjectID: "hold", Kind: domain.KindWorker, CreatedAt: time.Now(), UpdatedAt: time.Now()})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.SetWorkerSchedulingHold(ctx, rec.ID, time.Now()); err != nil {
		t.Fatal(err)
	}
	status, err := bootguard.Inspect(cfg.DataDir)
	if err != nil {
		t.Fatal(err)
	}
	if status.RequiredProtocol != 2 || status.SupportedProtocol != 3 {
		t.Fatalf("compatibility after hold = %+v", status)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if err := guard.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestStartupPauseNewProjectsAndLaterOrdinaryRestart(t *testing.T) {
	cfg := startupConfig(t)
	ctx := context.Background()
	g, err := bootguard.Open(cfg.DataDir)
	if err != nil {
		t.Fatal(err)
	}
	s, _, err := openGuardedStartupStore(ctx, g, true)
	if err != nil {
		t.Fatal(err)
	}
	for _, workspace := range []bool{false, true} {
		id := "new"
		if workspace {
			id = "workspace"
		}
		row := domain.ProjectRecord{ID: id, Path: "/repos/" + id, DisplayName: id, RegisteredAt: time.Now()}
		if workspace {
			err = s.UpsertWorkspaceProject(ctx, row, nil)
		} else {
			err = s.UpsertProject(ctx, row)
		}
		if err != nil {
			t.Fatal(err)
		}
		got, _, err := s.GetProject(ctx, id)
		if err != nil || !got.Config.AdmissionPaused {
			t.Fatalf("new project %s not paused: %+v,%v", id, got, err)
		}
		// Existing project may be explicitly resumed during this same boot.
		got.Config.AdmissionPaused = false
		if err := s.UpsertProject(ctx, got); err != nil {
			t.Fatal(err)
		}
	}
	_ = s.Close()
	_ = g.Close()
	g, err = bootguard.Open(cfg.DataDir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = g.Close() }()
	s, _, err = openGuardedStartupStore(ctx, g, false)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()
	for _, id := range []string{"new", "workspace"} {
		got, _, err := s.GetProject(ctx, id)
		if err != nil || got.Config.AdmissionPaused {
			t.Fatalf("ordinary restart repaused %s", id)
		}
	}
	if s.NewProjectAdmissionPaused() {
		t.Fatal("maintenance default leaked to ordinary boot")
	}
}

func TestStartupRejectsFutureMarkerBeforeDatabaseOrRuntime(t *testing.T) {
	for _, marker := range []string{`{"schema":"ao-data-compatibility/v1","requiredProtocol":4}` + "\n", `malformed`} {
		t.Run(marker, func(t *testing.T) {
			cfg := startupConfig(t)
			if err := os.MkdirAll(cfg.DataDir, 0750); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(cfg.DataDir, bootguard.MarkerName), []byte(marker), 0600); err != nil {
				t.Fatal(err)
			}
			sentinel := []byte("not a database; must not be opened")
			if err := os.WriteFile(filepath.Join(cfg.DataDir, "ao.db"), sentinel, 0600); err != nil {
				t.Fatal(err)
			}
			t.Setenv("AO_DATA_DIR", cfg.DataDir)
			t.Setenv("AO_RUN_FILE", cfg.RunFilePath)
			t.Setenv("AO_START_PAUSED", "true")
			err := RunWithOptions(Options{StartPaused: true})
			if !errors.Is(err, bootguard.ErrUnsupported) && !errors.Is(err, bootguard.ErrMalformed) {
				t.Fatalf("wrong early error: %v", err)
			}
			if _, err := PrepareStartPaused(context.Background(), cfg); !errors.Is(err, bootguard.ErrUnsupported) && !errors.Is(err, bootguard.ErrMalformed) {
				t.Fatalf("preparer bypass: %v", err)
			}
			got, _ := os.ReadFile(filepath.Join(cfg.DataDir, "ao.db"))
			if string(got) != string(sentinel) {
				t.Fatal("database changed")
			}
			entries, _ := os.ReadDir(cfg.DataDir)
			for _, entry := range entries {
				if entry.Name() != "ao.db" && entry.Name() != bootguard.MarkerName && entry.Name() != "ao.lock" {
					t.Fatalf("pre-proof side effect: %s", entry.Name())
				}
			}
		})
	}
}

func TestStartupPauseMalformedProjectConfigFailsWithoutPartialPause(t *testing.T) {
	cfg := startupConfig(t)
	ctx := context.Background()
	s, err := sqlite.Open(cfg.DataDir)
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"a", "z"} {
		if err := s.UpsertProject(ctx, domain.ProjectRecord{ID: id, Path: "/" + id, DisplayName: id, RegisteredAt: time.Now()}); err != nil {
			t.Fatal(err)
		}
	}
	_ = s.Close()
	raw, err := sql.Open("sqlite", filepath.Join(cfg.DataDir, "ao.db"))
	if err != nil {
		t.Fatal(err)
	}
	_, err = raw.Exec(`UPDATE projects SET config='broken' WHERE id='z'`)
	if err != nil {
		t.Fatal(err)
	}
	_ = raw.Close()
	if _, err := PrepareStartPaused(ctx, cfg); err == nil {
		t.Fatal("accepted malformed config")
	}
	s, err = sqlite.Open(cfg.DataDir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	a, _, _ := s.GetProject(ctx, "a")
	if a.Config.AdmissionPaused {
		t.Fatal("partially paused preceding project")
	}
}

func TestPrepareRefusesLiveLegacyRunfileBeforeDatabase(t *testing.T) {
	cfg := startupConfig(t)
	if err := runfile.Write(cfg.RunFilePath, runfile.Info{PID: os.Getpid(), Port: 3001, StartedAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	if _, err := PrepareStartPaused(context.Background(), cfg); err == nil {
		t.Fatal("accepted live old daemon")
	}
	if _, err := os.Stat(filepath.Join(cfg.DataDir, "ao.db")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("opened database: %v", err)
	}
}

func TestStartupPauseRejectsEffectiveFalseFromAmbiguousJSONAtomically(t *testing.T) {
	for _, ambiguous := range []string{
		`{"admissionPaused":false,"admissionPaused":false}`,
		`{"admissionPaused":false,"AdmissionPaused":false}`,
		`{"admissionPaused":false,"ADMISSIONPAUSED":false}`,
	} {
		t.Run(ambiguous, func(t *testing.T) {
			cfg := startupConfig(t)
			ctx := context.Background()
			store, err := sqlite.Open(cfg.DataDir)
			if err != nil {
				t.Fatal(err)
			}
			for _, id := range []string{"a-valid", "z-ambiguous"} {
				if err := store.UpsertProject(ctx, domain.ProjectRecord{ID: id, Path: "/repos/" + id, RegisteredAt: time.Now()}); err != nil {
					t.Fatal(err)
				}
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			raw, err := sql.Open("sqlite", filepath.Join(cfg.DataDir, "ao.db"))
			if err != nil {
				t.Fatal(err)
			}
			defer raw.Close()
			if _, err := raw.Exec(`UPDATE projects SET config=? WHERE id='z-ambiguous'`, ambiguous); err != nil {
				t.Fatal(err)
			}
			for attempt := 0; attempt < 2; attempt++ {
				if _, err := PrepareStartPaused(ctx, cfg); err == nil {
					t.Fatal("ambiguous policy reported successfully paused")
				}
				var configAfter string
				if err := raw.QueryRow(`SELECT config FROM projects WHERE id='z-ambiguous'`).Scan(&configAfter); err != nil {
					t.Fatal(err)
				}
				if configAfter != ambiguous {
					t.Fatalf("ambiguous config rewritten: %s", configAfter)
				}
				var validConfig sql.NullString
				if err := raw.QueryRow(`SELECT config FROM projects WHERE id='a-valid'`).Scan(&validConfig); err != nil {
					t.Fatal(err)
				}
				if validConfig.Valid {
					t.Fatalf("preceding valid row partially paused: %s", validConfig.String)
				}
			}
		})
	}
}
