// Package datadirlock provides the process-wide ownership boundary for one AO
// data directory. The lock is OS-enforced and remains held until Close.
package datadirlock

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

const lockFileName = "ao.lock"

// ErrLocked means another process already owns this data directory.
var ErrLocked = errors.New("AO data directory is already locked")

// Lock is an exclusive lease on one data directory. The lock file is retained
// after release so a successor always locks the same inode; Close is idempotent.
type Lock struct {
	dataDir  string
	path     string
	file     *os.File
	platform *platformLock
	once     sync.Once
	closeErr error
}

// Acquire creates the data directory if needed and attempts a non-blocking,
// OS-exclusive lock. Callers must acquire this before opening ao.db and hold it
// for the complete daemon or offline-writer lifetime.
func Acquire(dataDir string) (*Lock, error) {
	abs, err := filepath.Abs(dataDir)
	if err != nil {
		return nil, fmt.Errorf("resolve data directory: %w", err)
	}
	abs = filepath.Clean(abs)
	if err := os.MkdirAll(abs, 0o750); err != nil {
		return nil, fmt.Errorf("create data directory for lock: %w", err)
	}
	if resolved, resolveErr := filepath.EvalSymlinks(abs); resolveErr == nil {
		abs = filepath.Clean(resolved)
	}
	path := filepath.Join(abs, lockFileName)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open data-directory lock: %w", err)
	}
	state, held, err := tryLockFile(f)
	if err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("lock data directory: %w", err)
	}
	if !held {
		_ = f.Close()
		return nil, fmt.Errorf("%w: %s", ErrLocked, abs)
	}
	return &Lock{dataDir: abs, path: path, file: f, platform: state}, nil
}

// DataDir is the absolute, cleaned directory protected by the lock.
func (l *Lock) DataDir() string {
	if l == nil {
		return ""
	}
	return l.dataDir
}

// Path is the retained lock-file path, useful for diagnostics and tests.
func (l *Lock) Path() string {
	if l == nil {
		return ""
	}
	return l.path
}

// Close releases the OS lock and closes its file descriptor.
func (l *Lock) Close() error {
	if l == nil {
		return nil
	}
	l.once.Do(func() {
		unlockErr := unlockFile(l.file, l.platform)
		closeErr := l.file.Close()
		if unlockErr != nil {
			l.closeErr = fmt.Errorf("unlock data directory: %w", unlockErr)
		} else if closeErr != nil {
			l.closeErr = fmt.Errorf("close data-directory lock: %w", closeErr)
		}
	})
	return l.closeErr
}
