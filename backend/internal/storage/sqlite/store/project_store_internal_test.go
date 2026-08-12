package store

import (
	"database/sql"
	"strings"
	"testing"

	"github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite/gen"
)

func TestDecodeProjectConfigDegradesGracefully(t *testing.T) {
	// SQL NULL / empty → zero config.
	if got, decodeErr := decodeProjectConfig(sql.NullString{}); !got.IsZero() || decodeErr != "" {
		t.Fatalf("NULL config = %#v decodeErr=%q, want zero with no error", got, decodeErr)
	}

	// Valid JSON decodes.
	if got, decodeErr := decodeProjectConfig(sql.NullString{String: `{"defaultBranch":"develop"}`, Valid: true}); got.DefaultBranch != "develop" || decodeErr != "" {
		t.Fatalf("valid config = %#v decodeErr=%q, want develop with no error", got, decodeErr)
	}

	// Corrupt JSON must NOT error — it degrades to a zero config so the project
	// row (and ListProjects) stay accessible.
	if got, decodeErr := decodeProjectConfig(sql.NullString{String: `{not json`, Valid: true}); !got.IsZero() || decodeErr == "" {
		t.Fatalf("corrupt config = %#v decodeErr=%q, want zero plus durable error fact", got, decodeErr)
	}

	// Typed-invalid JSON is also degraded. Unknown startup policy must never be
	// mistaken for an omitted/default automatic policy on a later read.
	if got, decodeErr := decodeProjectConfig(sql.NullString{String: `{"startupRestorePolicy":"future_policy"}`, Valid: true}); !got.IsZero() || !strings.Contains(decodeErr, "unknown policy") {
		t.Fatalf("unknown policy = %#v decodeErr=%q, want zero plus validation error", got, decodeErr)
	}
}

func TestProjectRowFromGenCarriesConfigDecodeError(t *testing.T) {
	row := projectRowFromGen(gen.Project{
		ID:     "demo",
		Kind:   "single_repo",
		Config: sql.NullString{String: `{not json`, Valid: true},
	})
	if !row.Config.IsZero() || row.ConfigDecodeError == "" {
		t.Fatalf("mapped project config = %#v decodeErr=%q, want zero plus decode-error fact", row.Config, row.ConfigDecodeError)
	}
}
