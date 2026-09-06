package custody

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
)

const maxCandidateCapture = 64 << 20

type CandidateFile struct {
	Path          string `json:"path"`
	Mode          uint32 `json:"mode"`
	Size          int64  `json:"size"`
	SHA256        string `json:"sha256,omitempty"`
	SymlinkTarget string `json:"symlink_target,omitempty"`
	Retention     string `json:"retention"`
}
type Candidate struct {
	Repository       string            `json:"repository"`
	CommonDir        string            `json:"common_dir"`
	Worktree         string            `json:"worktree"`
	Branch           string            `json:"branch"`
	Head             string            `json:"head"`
	Tree             string            `json:"tree"`
	OriginalBaseSHA  string            `json:"original_base_sha"`
	Index            []byte            `json:"index"`
	SharedIndex      map[string][]byte `json:"shared_index"`
	StagedDiff       []byte            `json:"staged_diff"`
	UnstagedDiff     []byte            `json:"unstaged_diff"`
	Untracked        []CandidateFile   `json:"untracked"`
	Ignored          []CandidateFile   `json:"ignored"`
	RawTrackedSHA256 string            `json:"raw_tracked_sha256"`
	CaptureBytes     int64             `json:"capture_bytes"`
}

type boundedBuffer struct{ bytes.Buffer }

func (b *boundedBuffer) Write(p []byte) (int, error) {
	if b.Len()+len(p) > maxCandidateCapture {
		return 0, fmt.Errorf("capture exceeds %d bytes", maxCandidateCapture)
	}
	return b.Buffer.Write(p)
}
func gitBytes(ctx context.Context, path string, args ...string) ([]byte, error) {
	return gitBytesInput(ctx, path, nil, args...)
}
func gitBytesInput(ctx context.Context, path string, input []byte, args ...string) ([]byte, error) {
	cmd := exec.CommandContext(ctx, "git", append([]string{"-C", path, "--no-optional-locks", "-c", "core.fsmonitor=false"}, args...)...)
	cmd.Stdin = bytes.NewReader(input)
	cmd.Env = []string{}
	for _, entry := range os.Environ() {
		if !strings.HasPrefix(entry, "GIT_") {
			cmd.Env = append(cmd.Env, entry)
		}
	}
	cmd.Env = append(cmd.Env, "GIT_OPTIONAL_LOCKS=0")
	var stdout boundedBuffer
	var stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("candidate git %v: %w: %s", args, err, stderr.String())
	}
	return append([]byte{}, stdout.Bytes()...), nil
}
func gitValue(ctx context.Context, path string, args ...string) (string, error) {
	b, err := gitBytes(ctx, path, args...)
	return strings.TrimSpace(string(b)), err
}
func limitedRead(path string) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()
	b, err := io.ReadAll(io.LimitReader(f, maxCandidateCapture+1))
	if len(b) > maxCandidateCapture {
		return nil, ErrUnknown
	}
	return b, err
}

// CaptureCandidate preserves the actual worktree in place. It captures bounded
// Git progress, not copies of generated/ignored dependency trees. Ignored entries
// explicitly mean retained in place, not backed up or recursively hashed.
func CaptureCandidate(ctx context.Context, a Attempt, repository string) (Candidate, error) {
	out := Candidate{Repository: repository, OriginalBaseSHA: a.OriginalBaseSHA, SharedIndex: map[string][]byte{}, Untracked: []CandidateFile{}, Ignored: []CandidateFile{}}
	if a.Workspace == "" || a.OriginalBaseSHA == "" {
		return out, ErrUnknown
	}
	var err error
	out.Worktree, err = filepath.EvalSymlinks(a.Workspace)
	if err != nil {
		return out, err
	}
	origin, e := gitValue(ctx, out.Worktree, "remote", "get-url", "origin")
	if e != nil {
		return out, e
	}
	actualRepo := strings.TrimSuffix(strings.TrimPrefix(strings.TrimPrefix(origin, "https://github.com/"), "git@github.com:"), ".git")
	if actualRepo != repository {
		return out, fmt.Errorf("%w: candidate repository identity differs", ErrConflict)
	}
	out.CommonDir, err = gitValue(ctx, out.Worktree, "rev-parse", "--path-format=absolute", "--git-common-dir")
	if err != nil {
		return out, err
	}
	out.Branch, err = gitValue(ctx, out.Worktree, "symbolic-ref", "HEAD")
	if err != nil {
		return out, err
	}
	out.Head, err = gitValue(ctx, out.Worktree, "rev-parse", "HEAD")
	if err != nil {
		return out, err
	}
	out.Tree, err = gitValue(ctx, out.Worktree, "rev-parse", "HEAD^{tree}")
	if err != nil {
		return out, err
	}
	index, err := gitValue(ctx, out.Worktree, "rev-parse", "--path-format=absolute", "--git-path", "index")
	if err != nil {
		return out, err
	}
	out.Index, err = limitedRead(index)
	if err != nil {
		return out, err
	}
	shared, err := gitValue(ctx, out.Worktree, "rev-parse", "--shared-index-path")
	if err != nil {
		return out, err
	}
	if shared != "" {
		if !filepath.IsAbs(shared) {
			shared = filepath.Join(out.Worktree, shared)
		}
		b, e := limitedRead(shared)
		if e != nil {
			return out, e
		}
		out.SharedIndex[filepath.Base(shared)] = b
	}
	tracked, err := trackedPaths(ctx, out.Worktree)
	if err != nil {
		return out, err
	}
	if err = validateObservation(ctx, out.Worktree, tracked); err != nil {
		return out, err
	}
	out.RawTrackedSHA256, err = rawTrackedFingerprint(out.Worktree, tracked)
	if err != nil {
		return out, err
	}
	out.StagedDiff, err = gitBytes(ctx, out.Worktree, "diff", "--cached", "--binary", "--no-ext-diff", "--no-textconv")
	if err != nil {
		return out, err
	}
	out.UnstagedDiff, err = gitBytes(ctx, out.Worktree, "diff", "--binary", "--no-ext-diff", "--no-textconv")
	if err != nil {
		return out, err
	}
	for _, ignored := range []bool{false, true} {
		args := []string{"ls-files", "--others", "--exclude-standard", "-z"}
		if ignored {
			args = append(args, "--ignored", "--directory")
		}
		b, e := gitBytes(ctx, out.Worktree, args...)
		if e != nil {
			return out, e
		}
		paths := strings.Split(string(b), "\x00")
		sort.Strings(paths)
		for _, path := range paths {
			if path == "" {
				continue
			}
			clean := filepath.Clean(path)
			if filepath.IsAbs(path) || clean == ".." || strings.HasPrefix(clean, "../") {
				return out, ErrUnknown
			}
			full := filepath.Join(out.Worktree, clean)
			info, e := os.Lstat(full)
			if e != nil {
				return out, e
			}
			entry := CandidateFile{Path: path, Mode: uint32(info.Mode()), Size: info.Size(), Retention: "retained_in_place"}
			if info.Mode()&os.ModeSymlink != 0 {
				entry.SymlinkTarget, e = os.Readlink(full)
				if e != nil {
					return out, e
				}
			}
			if !ignored && info.Mode().IsRegular() {
				data, e := limitedRead(full)
				if e != nil {
					return out, e
				}
				entry.SHA256 = hashBytes(data)
			}
			if ignored {
				out.Ignored = append(out.Ignored, entry)
			} else {
				out.Untracked = append(out.Untracked, entry)
			}
		}
	}
	out.CaptureBytes = int64(len(out.Index) + len(out.StagedDiff) + len(out.UnstagedDiff))
	for _, b := range out.SharedIndex {
		out.CaptureBytes += int64(len(b))
	}
	if out.CaptureBytes > maxCandidateCapture {
		return out, ErrUnknown
	}
	return out, nil
}
