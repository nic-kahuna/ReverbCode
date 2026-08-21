//go:build windows

package datadirlock

import (
	"errors"
	"os"

	"golang.org/x/sys/windows"
)

type platformLock struct {
	overlapped windows.Overlapped
}

func tryLockFile(f *os.File) (*platformLock, bool, error) {
	state := &platformLock{}
	err := windows.LockFileEx(
		windows.Handle(f.Fd()),
		windows.LOCKFILE_EXCLUSIVE_LOCK|windows.LOCKFILE_FAIL_IMMEDIATELY,
		0,
		1,
		0,
		&state.overlapped,
	)
	if errors.Is(err, windows.ERROR_LOCK_VIOLATION) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	return state, true, nil
}

func unlockFile(f *os.File, state *platformLock) error {
	return windows.UnlockFileEx(windows.Handle(f.Fd()), 0, 1, 0, &state.overlapped)
}
