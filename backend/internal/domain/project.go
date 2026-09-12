package domain

import "time"

const (
	// ProjectKindSingleRepo is the existing one-repository project shape.
	ProjectKindSingleRepo ProjectKind = "single_repo"
	// ProjectKindWorkspace is a parent root-as-repo plus child repositories.
	ProjectKindWorkspace ProjectKind = "workspace"
	// RootWorkspaceRepoName is the reserved repo_name used for the parent root repo.
	RootWorkspaceRepoName = "__root__"
	// SessionWorktreeStatePreserved marks a recoverable worktree journaled by
	// preserve-only startup reconciliation at its exact registered path.
	SessionWorktreeStatePreserved = "preserved"
	// SessionWorktreeStatePreservedRemoved marks a recoverable worktree whose
	// exact registered path is absent and may be recreated by explicit restore.
	SessionWorktreeStatePreservedRemoved = "preserved_removed"
	// SessionWorktreeStatePreservedPartial marks managed recovery whose runtime,
	// workspace disposition, or durable transition is ambiguous. It covers both
	// startup quarantine and an explicit restore that could not safely complete;
	// it is never eligible for automatic or retry restore.
	SessionWorktreeStatePreservedPartial = "preserved_partial"
)

// ProjectKind describes how a registered project materialises session workspaces.
type ProjectKind string

// WithDefault returns ProjectKindSingleRepo when the stored value predates the kind column.
func (k ProjectKind) WithDefault() ProjectKind {
	if k == "" {
		return ProjectKindSingleRepo
	}
	return k
}

// ProjectRecord is the durable project registry row used by storage and services.
type ProjectRecord struct {
	ID            string
	Path          string
	RepoOriginURL string
	DisplayName   string
	RegisteredAt  time.Time
	ArchivedAt    time.Time
	Kind          ProjectKind
	// Config holds the typed per-project configuration AO resolves at spawn. An
	// IsZero value means unset.
	Config ProjectConfig
	// ConfigDecodeError is non-empty when storage kept the project accessible but
	// could not decode its persisted config JSON. Startup reconciliation must
	// treat this as unknown policy and fail closed.
	ConfigDecodeError string
}

// WorkspaceRepoRecord is a child repo registered under a workspace project.
// The root repo itself is represented by ProjectRecord and by session_worktrees
// rows using RootWorkspaceRepoName; workspace_repos contains direct children.
type WorkspaceRepoRecord struct {
	ProjectID     ProjectID
	Name          string
	RelativePath  string
	RepoOriginURL string
	RegisteredAt  time.Time
}

// SessionWorktreeRecord tracks one repo worktree in a session. Workspace
// projects create one root row plus one child row per WorkspaceRepoRecord.
type SessionWorktreeRecord struct {
	SessionID    SessionID
	RepoName     string
	Branch       string
	BaseSHA      string
	WorktreePath string
	PreservedRef string
	// State mirrors session_worktrees.state. In addition to physical worktree
	// lifecycle values, preserved, preserved_removed, and preserved_partial are
	// durable managed-recovery journal states.
	State string
}
