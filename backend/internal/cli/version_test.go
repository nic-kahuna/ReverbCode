package cli

import (
	"testing"

	"github.com/aoagents/agent-orchestrator/backend/internal/buildinfo"
)

func TestVersionStringUsesSharedBuildMetadata(t *testing.T) {
	originalVersion, originalCommit, originalDate := buildinfo.Version, buildinfo.Commit, buildinfo.Date
	t.Cleanup(func() {
		buildinfo.Version = originalVersion
		buildinfo.Commit = originalCommit
		buildinfo.Date = originalDate
	})

	buildinfo.Version = "1.2.3"
	buildinfo.Commit = "release-sha"
	buildinfo.Date = "2026-08-12T00:00:00Z"

	const want = "1.2.3 commit release-sha built 2026-08-12T00:00:00Z"
	if got := VersionString(); got != want {
		t.Fatalf("VersionString() = %q, want %q", got, want)
	}
}
