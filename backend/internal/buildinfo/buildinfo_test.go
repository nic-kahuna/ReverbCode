package buildinfo

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"runtime/debug"
	"testing"
)

func TestMetadataForBuild(t *testing.T) {
	tests := []struct {
		name    string
		version string
		commit  string
		date    string
		info    *debug.BuildInfo
		ok      bool
		want    Metadata
	}{
		{
			name:    "release stamps override identity but preserve modified state",
			version: "1.2.3",
			commit:  "release-sha",
			date:    "2026-08-12T00:00:00Z",
			info: buildInfo("v9.9.9",
				debug.BuildSetting{Key: "vcs.revision", Value: "embedded-sha"},
				debug.BuildSetting{Key: "vcs.time", Value: "2020-01-01T00:00:00Z"},
				debug.BuildSetting{Key: "vcs.modified", Value: "true"},
			),
			ok: true,
			want: Metadata{
				Version:        "1.2.3",
				SourceCommit:   "release-sha",
				BuiltAt:        "2026-08-12T00:00:00Z",
				SourceModified: true,
			},
		},
		{
			name:    "dirty developer build uses embedded vcs identity",
			version: "dev",
			info: buildInfo("(devel)",
				debug.BuildSetting{Key: "vcs.revision", Value: "developer-sha"},
				debug.BuildSetting{Key: "vcs.time", Value: "2026-08-11T12:34:56Z"},
				debug.BuildSetting{Key: "vcs.modified", Value: "true"},
			),
			ok: true,
			want: Metadata{
				Version:        "dev",
				SourceCommit:   "developer-sha",
				BuiltAt:        "2026-08-11T12:34:56Z",
				SourceModified: true,
			},
		},
		{
			name:    "clean developer build remains clean",
			version: "dev",
			info: buildInfo("(devel)",
				debug.BuildSetting{Key: "vcs.revision", Value: "developer-sha"},
				debug.BuildSetting{Key: "vcs.modified", Value: "false"},
			),
			ok:   true,
			want: Metadata{Version: "dev", SourceCommit: "developer-sha"},
		},
		{
			name:    "module version fills an unstamped dev version",
			version: "dev",
			info:    buildInfo("v4.5.6"),
			ok:      true,
			want:    Metadata{Version: "v4.5.6"},
		},
		{
			name: "missing metadata has a stable dev version",
			want: Metadata{Version: "dev"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := metadataForBuild(tt.version, tt.commit, tt.date, tt.info, tt.ok)
			if got != tt.want {
				t.Fatalf("metadataForBuild() = %#v, want %#v", got, tt.want)
			}
		})
	}
}

func TestFormatVersion(t *testing.T) {
	tests := []struct {
		name string
		meta Metadata
		want string
	}{
		{name: "version only", meta: Metadata{Version: "dev"}, want: "dev"},
		{name: "complete", meta: Metadata{Version: "1.2.3", SourceCommit: "abc", BuiltAt: "2026-08-12T00:00:00Z"}, want: "1.2.3 commit abc built 2026-08-12T00:00:00Z"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := formatVersion(tt.meta); got != tt.want {
				t.Fatalf("formatVersion() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestCaptureExecutable(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "ao-test")
	contents := []byte("exact bridge executable bytes\n")
	if err := os.WriteFile(path, contents, 0o700); err != nil {
		t.Fatal(err)
	}
	meta := Metadata{Version: "1.2.3", SourceCommit: "abc"}

	got, err := captureExecutable(path, meta)
	if err != nil {
		t.Fatalf("captureExecutable: %v", err)
	}
	wantHash := sha256.Sum256(contents)
	wantPath, err := filepath.Abs(path)
	if err != nil {
		t.Fatal(err)
	}
	if resolved, resolveErr := filepath.EvalSymlinks(wantPath); resolveErr == nil {
		wantPath = resolved
	}
	if got.Build != meta {
		t.Fatalf("Build = %#v, want %#v", got.Build, meta)
	}
	if got.Executable.Path != filepath.Clean(wantPath) {
		t.Fatalf("Executable.Path = %q, want %q", got.Executable.Path, filepath.Clean(wantPath))
	}
	if got.Executable.SHA256 != hex.EncodeToString(wantHash[:]) {
		t.Fatalf("Executable.SHA256 = %q, want %q", got.Executable.SHA256, hex.EncodeToString(wantHash[:]))
	}
}

func buildInfo(version string, settings ...debug.BuildSetting) *debug.BuildInfo {
	return &debug.BuildInfo{
		Main:     debug.Module{Version: version},
		Settings: settings,
	}
}
