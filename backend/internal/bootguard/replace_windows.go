//go:build windows

package bootguard

import "golang.org/x/sys/windows"

// Windows has no POSIX directory-fsync equivalent. Use its synchronous move
// primitive after flushing the temporary file; no copy/across-volume mode.
// https://learn.microsoft.com/en-us/windows/win32/api/winbase/nf-winbase-movefileexw
func replaceDurably(source, destination string) error {
	from, err := windows.UTF16PtrFromString(source)
	if err != nil {
		return err
	}
	to, err := windows.UTF16PtrFromString(destination)
	if err != nil {
		return err
	}
	return windows.MoveFileEx(from, to, windows.MOVEFILE_REPLACE_EXISTING|windows.MOVEFILE_WRITE_THROUGH)
}
