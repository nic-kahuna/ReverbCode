package domain

// RecoveryFenceProtocolVersion is the only recovery-fence protocol this
// bridge understands. A different value is durable future evidence and must
// never be interpreted as permission to run mutation-capable code.
const RecoveryFenceProtocolVersion int64 = 1

// RecoveryFenceCanonicalPayload is the byte-exact protocol-v1 payload. Keeping
// the supported representation canonical makes readiness proofs and compare-
// and-swap transitions unambiguous while preserving unknown payload bytes.
const RecoveryFenceCanonicalPayload = "{}"

// RecoveryFenceCanonicalPayloadSHA256 is sha256("{}"). It is part of the
// readiness contract consumed by the desktop bridge.
const RecoveryFenceCanonicalPayloadSHA256 = "44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a"

// RecoveryFenceState is the persisted state vocabulary understood by protocol
// v1. The database intentionally does not constrain this column so a newer
// protocol can write a value this bridge will retain and quarantine.
type RecoveryFenceState string

const (
	RecoveryFenceStateInactive RecoveryFenceState = "inactive"
	RecoveryFenceStateActive   RecoveryFenceState = "active"
)

// RecoveryFenceDisposition is the exhaustive boot classification. Its zero
// value is deliberately invalid: only RecoveryFenceDispositionInactive grants
// permission to run ordinary migrations or construct mutation-capable lanes.
type RecoveryFenceDisposition string

const (
	RecoveryFenceDispositionInactive    RecoveryFenceDisposition = "inactive"
	RecoveryFenceDispositionActive      RecoveryFenceDisposition = "active"
	RecoveryFenceDispositionUnsupported RecoveryFenceDisposition = "unsupported"
	RecoveryFenceDispositionMalformed   RecoveryFenceDisposition = "malformed"
	RecoveryFenceDispositionUnavailable RecoveryFenceDisposition = "unavailable"
)

// RecoveryFenceReason is a stable machine-readable explanation for the
// disposition. Clients must authorize from the complete supported-inactive
// representation, never from a reason string alone.
type RecoveryFenceReason string

const (
	RecoveryFenceReasonSupportedInactive          RecoveryFenceReason = "supported_inactive"
	RecoveryFenceReasonSupportedActive            RecoveryFenceReason = "supported_active"
	RecoveryFenceReasonTableMissing               RecoveryFenceReason = "table_missing"
	RecoveryFenceReasonSingletonMissing           RecoveryFenceReason = "singleton_missing"
	RecoveryFenceReasonSingletonDuplicate         RecoveryFenceReason = "singleton_duplicate"
	RecoveryFenceReasonStorageTypeInvalid         RecoveryFenceReason = "storage_type_invalid"
	RecoveryFenceReasonProtocolUnsupported        RecoveryFenceReason = "protocol_unsupported"
	RecoveryFenceReasonStateUnsupported           RecoveryFenceReason = "state_unsupported"
	RecoveryFenceReasonPayloadMalformed           RecoveryFenceReason = "payload_malformed"
	RecoveryFenceReasonPayloadUnsupported         RecoveryFenceReason = "payload_unsupported"
	RecoveryFenceReasonDatabaseVersionUnsupported RecoveryFenceReason = "database_version_unsupported"
	RecoveryFenceReasonQueryFailed                RecoveryFenceReason = "query_failed"
)

// RecoveryFenceStatus is the stable diagnostic/readiness representation. Raw
// payload bytes are never exposed; their length and digest identify them while
// the storage layer retains the bytes verbatim.
type RecoveryFenceStatus struct {
	SupportedProtocolVersion       int64                    `json:"supportedProtocolVersion"`
	SupportedDatabaseSchemaVersion int64                    `json:"supportedDatabaseSchemaVersion"`
	DatabaseSchemaVersion          int64                    `json:"databaseSchemaVersion"`
	ProtocolVersion                *int64                   `json:"protocolVersion,omitempty"`
	State                          RecoveryFenceState       `json:"state,omitempty"`
	Disposition                    RecoveryFenceDisposition `json:"disposition"`
	ReasonCode                     RecoveryFenceReason      `json:"reasonCode"`
	RowCount                       int                      `json:"rowCount"`
	ProtocolStorageClass           string                   `json:"protocolStorageClass,omitempty"`
	StateStorageClass              string                   `json:"stateStorageClass,omitempty"`
	PayloadStorageClass            string                   `json:"payloadStorageClass,omitempty"`
	GenerationStorageClass         string                   `json:"generationStorageClass,omitempty"`
	ActivationIDStorageClass       string                   `json:"activationIdStorageClass,omitempty"`
	PayloadByteLength              int                      `json:"payloadByteLength,omitempty"`
	PayloadSHA256                  string                   `json:"payloadSha256,omitempty"`
	Generation                     *int64                   `json:"generation,omitempty"`
	ActivationID                   string                   `json:"activationId,omitempty"`
}

// AllowsNormalBoot is intentionally strict and redundant. It protects callers
// from accidentally treating a partially populated or hand-constructed status
// with disposition="inactive" as a mutation grant.
func (s RecoveryFenceStatus) AllowsNormalBoot() bool {
	return s.Disposition == RecoveryFenceDispositionInactive &&
		s.ReasonCode == RecoveryFenceReasonSupportedInactive &&
		s.ProtocolVersion != nil &&
		*s.ProtocolVersion == RecoveryFenceProtocolVersion &&
		s.State == RecoveryFenceStateInactive &&
		s.RowCount == 1 &&
		s.ProtocolStorageClass == "integer" &&
		s.StateStorageClass == "text" &&
		s.PayloadStorageClass == "blob" &&
		s.GenerationStorageClass == "integer" &&
		s.ActivationIDStorageClass == "null" &&
		s.PayloadByteLength == len(RecoveryFenceCanonicalPayload) &&
		s.PayloadSHA256 == RecoveryFenceCanonicalPayloadSHA256 &&
		s.Generation != nil && *s.Generation >= 0 &&
		s.ActivationID == "" &&
		s.DatabaseSchemaVersion <= s.SupportedDatabaseSchemaVersion
}

// RecoveryInventorySchemaVersion freezes the compatibility projection. Later
// additive columns are ignored; missing, renamed, or retyped required columns
// make the inventory unavailable while the daemon remains recovery-fenced.
const RecoveryInventorySchemaVersion = 1

// RecoveryInventoryProjectionManifest is the canonical, ordered fixed
// projection understood by recovery protocol v1. "required" means SQLite's
// table_xinfo notnull bit must be set; additive columns are intentionally not
// represented and are ignored by compatible readers.
const RecoveryInventoryProjectionManifest = `recovery-inventory-v1
projects|id|TEXT|nullable
projects|path|TEXT|required
projects|repo_origin_url|TEXT|required
projects|display_name|TEXT|required
projects|registered_at|TIMESTAMP|required
projects|archived_at|TIMESTAMP|nullable
projects|config|TEXT|nullable
projects|kind|TEXT|required
sessions|id|TEXT|nullable
sessions|project_id|TEXT|required
sessions|num|INTEGER|required
sessions|issue_id|TEXT|required
sessions|kind|TEXT|required
sessions|harness|TEXT|required
sessions|display_name|TEXT|required
sessions|activity_state|TEXT|required
sessions|activity_last_at|TIMESTAMP|required
sessions|first_signal_at|TIMESTAMP|nullable
sessions|is_terminated|BOOLEAN|required
sessions|branch|TEXT|required
sessions|workspace_path|TEXT|required
sessions|runtime_handle_id|TEXT|required
sessions|agent_session_id|TEXT|required
sessions|prompt|TEXT|required
sessions|preview_url|TEXT|required
sessions|preview_revision|INTEGER|required
sessions|requested_harness|TEXT|required
sessions|requested_model|TEXT|required
sessions|requested_reasoning_effort|TEXT|required
sessions|launch_model|TEXT|required
sessions|launch_reasoning_effort|TEXT|required
sessions|launch_route_recorded|BOOLEAN|required
sessions|created_at|TIMESTAMP|required
sessions|updated_at|TIMESTAMP|required
workspace_repos|project_id|TEXT|required
workspace_repos|name|TEXT|required
workspace_repos|relative_path|TEXT|required
workspace_repos|repo_origin_url|TEXT|required
workspace_repos|registered_at|TIMESTAMP|required
session_worktrees|session_id|TEXT|required
session_worktrees|repo_name|TEXT|required
session_worktrees|branch|TEXT|required
session_worktrees|base_sha|TEXT|required
session_worktrees|worktree_path|TEXT|required
session_worktrees|preserved_ref|TEXT|required
session_worktrees|state|TEXT|required
`

// RecoveryInventorySchemaFingerprint is sha256 of
// RecoveryInventoryProjectionManifest.
const RecoveryInventorySchemaFingerprint = "b243ed4715376b24fc545fe8542c076c839f81a5539b22febc91ed5e720cfbc2"

// RecoveryInventoryStatus describes whether the fixed projection was safely
// validated against sqlite_schema/table_xinfo.
type RecoveryInventoryStatus struct {
	SchemaVersion int    `json:"schemaVersion"`
	Fingerprint   string `json:"fingerprint"`
	Available     bool   `json:"available"`
	ReasonCode    string `json:"reasonCode,omitempty"`
}

// RecoveryProject is the version-1 fixed persisted project projection. Config
// is base64 because its raw database bytes are authoritative in recovery mode.
type RecoveryProject struct {
	ID            string  `json:"id"`
	Path          string  `json:"path"`
	RepoOriginURL string  `json:"repoOriginUrl"`
	DisplayName   string  `json:"displayName"`
	RegisteredAt  string  `json:"registeredAt"`
	ArchivedAt    *string `json:"archivedAt"`
	ConfigBase64  *string `json:"configBase64"`
	Kind          string  `json:"kind"`
}

// RecoverySession is the version-1 fixed persisted session projection. Values
// remain opaque strings/integers; no domain status derivation or runtime/SCM
// lookup is permitted.
type RecoverySession struct {
	ID                       string  `json:"id"`
	ProjectID                string  `json:"projectId"`
	Num                      int64   `json:"num"`
	IssueID                  string  `json:"issueId"`
	Kind                     string  `json:"kind"`
	Harness                  string  `json:"harness"`
	DisplayName              string  `json:"displayName"`
	ActivityState            string  `json:"activityState"`
	ActivityLastAt           string  `json:"activityLastAt"`
	FirstSignalAt            *string `json:"firstSignalAt"`
	IsTerminated             int64   `json:"isTerminated"`
	Branch                   string  `json:"branch"`
	WorkspacePath            string  `json:"workspacePath"`
	RuntimeHandleID          string  `json:"runtimeHandleId"`
	AgentSessionID           string  `json:"agentSessionId"`
	PromptBase64             string  `json:"promptBase64"`
	PreviewURL               string  `json:"previewUrl"`
	PreviewRevision          int64   `json:"previewRevision"`
	RequestedHarness         string  `json:"requestedHarness"`
	RequestedModel           string  `json:"requestedModel"`
	RequestedReasoningEffort string  `json:"requestedReasoningEffort"`
	LaunchModel              string  `json:"launchModel"`
	LaunchReasoningEffort    string  `json:"launchReasoningEffort"`
	LaunchRouteRecorded      int64   `json:"launchRouteRecorded"`
	CreatedAt                string  `json:"createdAt"`
	UpdatedAt                string  `json:"updatedAt"`
}

type RecoveryWorkspaceRepo struct {
	ProjectID     string `json:"projectId"`
	Name          string `json:"name"`
	RelativePath  string `json:"relativePath"`
	RepoOriginURL string `json:"repoOriginUrl"`
	RegisteredAt  string `json:"registeredAt"`
}

type RecoverySessionWorktree struct {
	SessionID    string `json:"sessionId"`
	RepoName     string `json:"repoName"`
	Branch       string `json:"branch"`
	BaseSHA      string `json:"baseSha"`
	WorktreePath string `json:"worktreePath"`
	PreservedRef string `json:"preservedRef"`
	State        string `json:"state"`
}
