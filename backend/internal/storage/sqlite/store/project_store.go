package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite/gen"
)

// UpsertProject inserts or replaces a registered project row.
func (s *Store) UpsertProject(ctx context.Context, r domain.ProjectRecord) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if err := s.applyNewProjectAdmission(ctx, &r); err != nil {
		return err
	}
	config, err := marshalProjectConfig(r.Config)
	if err != nil {
		return err
	}
	return upsertProject(ctx, s.qw, r, config)
}

// UpsertWorkspaceProject inserts or replaces a workspace project and its child
// repository registry in one transaction. The child set is authoritative.
func (s *Store) UpsertWorkspaceProject(ctx context.Context, r domain.ProjectRecord, repos []domain.WorkspaceRepoRecord) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if err := s.applyNewProjectAdmission(ctx, &r); err != nil {
		return err
	}
	config, err := marshalProjectConfig(r.Config)
	if err != nil {
		return err
	}
	return s.inTx(ctx, "upsert workspace project", func(q *gen.Queries) error {
		if err := upsertProject(ctx, q, r, config); err != nil {
			return err
		}
		if err := q.DeleteWorkspaceReposByProject(ctx, domain.ProjectID(r.ID)); err != nil {
			return err
		}
		for _, repo := range repos {
			if err := q.UpsertWorkspaceRepo(ctx, gen.UpsertWorkspaceRepoParams{
				ProjectID:     domain.ProjectID(r.ID),
				Name:          repo.Name,
				RelativePath:  repo.RelativePath,
				RepoOriginURL: repo.RepoOriginURL,
				RegisteredAt:  repo.RegisteredAt,
			}); err != nil {
				return err
			}
		}
		return nil
	})
}

// ListWorkspaceRepos returns the registered direct child repos for a workspace project.
func (s *Store) ListWorkspaceRepos(ctx context.Context, projectID string) ([]domain.WorkspaceRepoRecord, error) {
	rows, err := s.qr.ListWorkspaceRepos(ctx, domain.ProjectID(projectID))
	if err != nil {
		return nil, fmt.Errorf("list workspace repos for %s: %w", projectID, err)
	}
	out := make([]domain.WorkspaceRepoRecord, 0, len(rows))
	for _, row := range rows {
		out = append(out, domain.WorkspaceRepoRecord{
			ProjectID:     row.ProjectID,
			Name:          row.Name,
			RelativePath:  row.RelativePath,
			RepoOriginURL: row.RepoOriginURL,
			RegisteredAt:  row.RegisteredAt,
		})
	}
	return out, nil
}

func upsertProject(ctx context.Context, q *gen.Queries, r domain.ProjectRecord, config sql.NullString) error {
	kind := r.Kind.WithDefault()
	return q.UpsertProject(ctx, gen.UpsertProjectParams{
		ID:            domain.ProjectID(r.ID),
		Path:          r.Path,
		RepoOriginURL: r.RepoOriginURL,
		DisplayName:   r.DisplayName,
		RegisteredAt:  r.RegisteredAt,
		ArchivedAt:    nullTime(r.ArchivedAt),
		Config:        config,
		Kind:          string(kind),
	})
}

// GetProject returns a project by id, active or archived.
func (s *Store) GetProject(ctx context.Context, id string) (domain.ProjectRecord, bool, error) {
	p, err := s.qr.GetProject(ctx, domain.ProjectID(id))
	if errors.Is(err, sql.ErrNoRows) {
		return domain.ProjectRecord{}, false, nil
	}
	if err != nil {
		return domain.ProjectRecord{}, false, fmt.Errorf("get project %s: %w", id, err)
	}
	return projectRowFromGen(p), true, nil
}

// FindProjectByPath returns a project registered at path, active or archived.
func (s *Store) FindProjectByPath(ctx context.Context, path string) (domain.ProjectRecord, bool, error) {
	p, err := s.qr.FindProjectByPath(ctx, path)
	if errors.Is(err, sql.ErrNoRows) {
		return domain.ProjectRecord{}, false, nil
	}
	if err != nil {
		return domain.ProjectRecord{}, false, fmt.Errorf("find project by path %s: %w", path, err)
	}
	return projectRowFromGen(p), true, nil
}

// ListProjects returns active projects ordered by id.
func (s *Store) ListProjects(ctx context.Context) ([]domain.ProjectRecord, error) {
	rows, err := s.qr.ListProjects(ctx)
	if err != nil {
		return nil, fmt.Errorf("list projects: %w", err)
	}
	out := make([]domain.ProjectRecord, 0, len(rows))
	for _, p := range rows {
		out = append(out, projectRowFromGen(p))
	}
	return out, nil
}

// ArchiveProject soft-deletes a project and reports whether a row was affected.
func (s *Store) ArchiveProject(ctx context.Context, id string, at time.Time) (bool, error) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	n, err := s.qw.ArchiveProject(ctx, gen.ArchiveProjectParams{
		ArchivedAt: nullTime(at),
		ID:         domain.ProjectID(id),
	})
	if err != nil {
		return false, err
	}
	return n > 0, nil
}

func projectRowFromGen(p gen.Project) domain.ProjectRecord {
	r := domain.ProjectRecord{
		ID:            string(p.ID),
		Path:          p.Path,
		RepoOriginURL: p.RepoOriginURL,
		DisplayName:   p.DisplayName,
		RegisteredAt:  p.RegisteredAt,
		Kind:          domain.ProjectKind(p.Kind).WithDefault(),
		Config:        unmarshalProjectConfig(p.Config),
	}
	if p.Config.Valid {
		var cfg domain.ProjectConfig
		if err := json.Unmarshal([]byte(p.Config.String), &cfg); err != nil {
			r.ConfigDecodeError = err.Error()
		} else if err := cfg.Validate(); err != nil {
			r.ConfigDecodeError = err.Error()
		}
	}
	if p.ArchivedAt.Valid {
		r.ArchivedAt = p.ArchivedAt.Time
	}
	return r
}

// marshalProjectConfig encodes the typed per-project config into the nullable
// JSON column. An IsZero config stores SQL NULL so an unset config round-trips
// back to a zero value rather than an empty object.
func marshalProjectConfig(cfg domain.ProjectConfig) (sql.NullString, error) {
	if cfg.IsZero() {
		return sql.NullString{}, nil
	}
	data, err := json.Marshal(cfg)
	if err != nil {
		return sql.NullString{}, fmt.Errorf("marshal project config: %w", err)
	}
	return sql.NullString{String: string(data), Valid: true}, nil
}

// unmarshalProjectConfig decodes the nullable JSON column back into the typed
// struct. SQL NULL (an unset config) decodes to a zero value. A damaged config
// (invalid JSON from a direct DB edit or migration bug) also degrades to a zero
// config rather than erroring — a corrupt config must never block access to the
// project row, nor fail an entire ListProjects.
func unmarshalProjectConfig(s sql.NullString) domain.ProjectConfig {
	if !s.Valid || s.String == "" {
		return domain.ProjectConfig{}
	}
	var cfg domain.ProjectConfig
	if err := json.Unmarshal([]byte(s.String), &cfg); err != nil {
		return domain.ProjectConfig{}
	}
	cfg.AdmissionPausedSet = false
	return cfg
}

func nullTime(t time.Time) sql.NullTime {
	if t.IsZero() {
		return sql.NullTime{}
	}
	return sql.NullTime{Time: t, Valid: true}
}

// PauseAdmissionForStartup runs before any observer or runtime lane exists.
// One transaction preserves unrelated config fields and includes archived rows.
func (s *Store) PauseAdmissionForStartup(ctx context.Context) (ids []string, resultErr error) {
	ids = []string{}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	// A published offline preparation must survive a crash. Ordinary AO writes
	// use NORMAL; this transaction explicitly syncs its WAL before success.
	if _, err := s.writeDB.ExecContext(ctx, "PRAGMA synchronous=FULL"); err != nil {
		return nil, err
	}
	defer func() {
		_, err := s.writeDB.ExecContext(context.Background(), "PRAGMA synchronous=NORMAL")
		resultErr = errors.Join(resultErr, err)
	}()
	err := s.inTx(ctx, "startup admission pause", func(q *gen.Queries) error {
		rows, err := q.ListProjectConfigsForStartup(ctx)
		if err != nil {
			return err
		}
		for _, row := range rows {
			ids = append(ids, string(row.ID))
			if !row.Config.Valid {
				continue
			}
			var existing domain.ProjectConfig
			if err := json.Unmarshal([]byte(row.Config.String), &existing); err != nil {
				return fmt.Errorf("project %s has unsupported configuration; startup refused: %w", row.ID, err)
			}
			if err := existing.Validate(); err != nil {
				return fmt.Errorf("project %s has invalid configuration; startup refused: %w", row.ID, err)
			}
		}
		if err := q.PauseAllProjectAdmission(ctx); err != nil {
			return err
		}
		// SQLite json_set and Go JSON decoding differ on duplicate/case-variant
		// keys. Verify the effective policy with the same decoder admission uses,
		// in this transaction, before publishing a successful pause.
		updated, err := q.ListProjectConfigsForStartup(ctx)
		if err != nil {
			return err
		}
		for _, row := range updated {
			var effective domain.ProjectConfig
			if !row.Config.Valid {
				return fmt.Errorf("project %s pause was not persisted", row.ID)
			}
			if err := json.Unmarshal([]byte(row.Config.String), &effective); err != nil {
				return fmt.Errorf("project %s effective admission is invalid: %w", row.ID, err)
			}
			if !effective.AdmissionPaused {
				return fmt.Errorf("project %s has ambiguous admission keys; startup pause refused", row.ID)
			}
		}
		return nil
	})
	if err == nil {
		s.newProjectsPaused = true
	}
	return ids, err
}

// NewProjectAdmissionPaused is fixed for this boot after startup preparation.
func (s *Store) NewProjectAdmissionPaused() bool {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	return s.newProjectsPaused
}

// applyNewProjectAdmission is called only with writeMu held.
func (s *Store) applyNewProjectAdmission(ctx context.Context, r *domain.ProjectRecord) error {
	if !s.newProjectsPaused {
		return nil
	}
	previous, err := s.qw.GetProject(ctx, domain.ProjectID(r.ID))
	if errors.Is(err, sql.ErrNoRows) || (err == nil && previous.ArchivedAt.Valid) {
		r.Config.AdmissionPaused = true
		return nil
	}
	return err
}
