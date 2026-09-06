package custody

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"unicode/utf8"
)

// trackedPaths includes HEAD removals and index additions independently of
// assume-unchanged/skip-worktree flags. Gitlinks and unmerged indexes need a
// separately designed nested-custody boundary and cannot be certified here.
func trackedPaths(ctx context.Context, dir string) ([]string, error) {
	paths := map[string]bool{}
	for _, index := range []bool{false, true} {
		args := []string{"ls-tree", "-rz", "--full-tree", "HEAD"}
		if index {
			args = []string{"ls-files", "--stage", "-z"}
		}
		b, err := gitBytes(ctx, dir, args...)
		if err != nil {
			return nil, err
		}
		for _, line := range bytes.Split(b, []byte{0}) {
			if len(line) == 0 {
				continue
			}
			parts := bytes.SplitN(line, []byte{'\t'}, 2)
			if len(parts) != 2 {
				return nil, ErrUnknown
			}
			meta := strings.Fields(string(parts[0]))
			if len(meta) != 3 {
				return nil, ErrUnknown
			}
			if meta[0] == "160000" {
				return nil, fmt.Errorf("%w: gitlink/submodule custody", ErrUnsupported)
			}
			if index && meta[2] != "0" {
				return nil, fmt.Errorf("%w: unmerged index custody", ErrUnsupported)
			}
			path := string(parts[1])
			if !utf8.ValidString(path) || filepath.IsAbs(path) || filepath.Clean(path) != path || path == ".." || strings.HasPrefix(path, "../") {
				return nil, ErrUnknown
			}
			paths[path] = true
		}
	}
	out := make([]string, 0, len(paths))
	for path := range paths {
		out = append(out, path)
	}
	sort.Strings(out)
	return out, nil
}

// validateObservation runs before diff: --no-ext-diff/--no-textconv do not stop
// clean/process filters. Check applicability rather than silently disabling
// existing transforms or executing them while certifying a quiet candidate.
func validateObservation(ctx context.Context, dir string, paths []string) error {
	config, err := gitBytes(ctx, dir, "config", "--null", "--list", "--includes")
	if err != nil {
		return err
	}
	executable := map[string]bool{}
	for _, entry := range bytes.Split(config, []byte{0}) {
		parts := bytes.SplitN(entry, []byte{'\n'}, 2)
		if len(parts) != 2 {
			continue
		}
		key := string(parts[0])
		value := string(parts[1])
		if value == "" || !strings.HasPrefix(key, "filter.") {
			continue
		}
		for _, suffix := range []string{".clean", ".process"} {
			if strings.HasSuffix(key, suffix) {
				name := strings.TrimSuffix(strings.TrimPrefix(key, "filter."), suffix)
				executable[name] = true
			}
		}
	}
	if len(executable) == 0 {
		return nil
	}
	input := []byte{}
	for _, path := range paths {
		input = append(input, []byte(path)...)
		input = append(input, 0)
	}
	for _, cached := range []bool{false, true} {
		args := []string{"check-attr", "-z", "--stdin", "filter"}
		if cached {
			args = []string{"check-attr", "--cached", "-z", "--stdin", "filter"}
		}
		attributes, err := gitBytesInput(ctx, dir, input, args...)
		if err != nil {
			return err
		}
		records := bytes.Split(attributes, []byte{0})
		if len(records) > 0 && len(records[len(records)-1]) == 0 {
			records = records[:len(records)-1]
		}
		if len(records) != 3*len(paths) {
			return ErrUnknown
		}
		for i := 0; i < len(records); i += 3 {
			if string(records[i]) != paths[i/3] || string(records[i+1]) != "filter" {
				return ErrUnknown
			}
			if executable[string(records[i+2])] {
				return fmt.Errorf("%w: executable Git filter applies to %s", ErrUnsupported, string(records[i]))
			}
		}
	}

	return nil
}

func rawTrackedFingerprint(dir string, paths []string) (string, error) {
	hash := sha256.New()
	_, _ = hash.Write([]byte("ao-raw-tracked/v1\x00"))
	var length [8]byte
	var mode [4]byte
	for _, path := range paths {
		binary.BigEndian.PutUint64(length[:], uint64(len([]byte(path))))
		_, _ = hash.Write(length[:])
		_, _ = hash.Write([]byte(path))
		full := filepath.Join(dir, path)
		for parent := filepath.Dir(full); parent != dir; parent = filepath.Dir(parent) {
			info, e := os.Lstat(parent)
			if os.IsNotExist(e) {
				continue
			}
			if e != nil {
				return "", e
			}
			if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
				return "", ErrUnsupported
			}
		}
		info, err := os.Lstat(full)
		state := byte('M')
		var fileMode uint32
		var size uint64
		content := []byte{}
		if err != nil && !os.IsNotExist(err) {
			return "", err
		}
		if err == nil {
			fileMode = uint32(info.Mode())
			size = uint64(info.Size())
			switch {
			case info.Mode().IsRegular():
				state = 'R'
				content, err = limitedRead(full)
			case info.Mode()&os.ModeSymlink != 0:
				state = 'L'
				var target string
				target, err = os.Readlink(full)
				content = []byte(target)
			default:
				return "", fmt.Errorf("%w: unsupported tracked node %s", ErrUnsupported, path)
			}
			if err != nil {
				return "", err
			}
			if uint64(len(content)) != size {
				return "", ErrConflict
			}
		}
		_, _ = hash.Write([]byte{state})
		binary.BigEndian.PutUint32(mode[:], fileMode)
		_, _ = hash.Write(mode[:])
		binary.BigEndian.PutUint64(length[:], size)
		_, _ = hash.Write(length[:])
		sum := sha256.Sum256(content)
		_, _ = hash.Write(sum[:])
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}
