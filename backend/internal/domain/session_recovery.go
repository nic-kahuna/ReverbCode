package domain

// SessionRecoveryState is the machine-readable recovery disposition derived
// for a session. Recovery state is never stored on the session row.
type SessionRecoveryState string

// SessionRecoveryRuntimeState reports what the durable journal can safely say
// about the prior runtime. Unknown never means alive or dead; it means AO will
// not infer either without a fresh exact probe at the restore boundary.
type SessionRecoveryRuntimeState string

const (
	// SessionRecoveryAwaitingRecovery means AO has preserved the session's
	// recovery journal and will not relaunch it until the exact session is
	// explicitly restored.
	SessionRecoveryAwaitingRecovery SessionRecoveryState = "awaiting_recovery"
	// SessionRecoveryRuntimeAbsent means the managed transition proved the saved
	// runtime absent (or stopped it exactly) before publishing recovery.
	SessionRecoveryRuntimeAbsent SessionRecoveryRuntimeState = "absent"
	// SessionRecoveryRuntimeUnknown means a partial transition or probe failure
	// prevents AO from claiming the saved runtime is absent.
	SessionRecoveryRuntimeUnknown SessionRecoveryRuntimeState = "unknown"
)

// SessionRecovery is the read-only evidence an external controller needs to
// decide whether to invoke the existing exact-session restore action. It
// intentionally reports only whether a provider session id was saved, never
// the provider's opaque identifier itself.
type SessionRecovery struct {
	State                SessionRecoveryState        `json:"state" enum:"awaiting_recovery"`
	Policy               StartupRestorePolicy        `json:"policy" enum:"preserve_only"`
	RuntimeState         SessionRecoveryRuntimeState `json:"runtimeState" enum:"absent,unknown"`
	ProviderSessionSaved bool                        `json:"providerSessionSaved"`
	Worktrees            []RecoveryWorktree          `json:"worktrees"`
}

// RecoveryWorktree is one exact durable session_worktrees journal row. Empty
// values (notably preservedRef for a clean worktree) remain present on the wire
// so inventory consumers can distinguish clean preservation from missing data.
type RecoveryWorktree struct {
	RepoName     string `json:"repoName"`
	Branch       string `json:"branch"`
	BaseSHA      string `json:"baseSha"`
	WorktreePath string `json:"worktreePath"`
	PreservedRef string `json:"preservedRef"`
	State        string `json:"state"`
}
