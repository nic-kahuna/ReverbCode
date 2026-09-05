//go:build !windows

package datadirlock

import (
	"errors"
	"os"

	"golang.org/x/sys/unix"
)

type platformLock struct{}

func tryLockFile(f *os.File) (*platformLock, bool, error) {
	err := unix.Flock(int(f.Fd()), unix.LOCK_EX|unix.LOCK_NB)
	if errors.Is(err, unix.EWOULDBLOCK) || errors.Is(err, unix.EAGAIN) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	return &platformLock{}, true, nil
}

func unlockFile(f *os.File, _ *platformLock) error {
	return unix.Flock(int(f.Fd()), unix.LOCK_UN)
}
