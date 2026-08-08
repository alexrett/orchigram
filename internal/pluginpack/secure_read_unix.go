//go:build unix

package pluginpack

import (
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"golang.org/x/sys/unix"
)

// secureReadRegularAt walks every relative path component from an already
// opened root directory. O_NOFOLLOW prevents both intermediate and final
// symlinks, while reading through the final descriptor closes path-swap races.
func secureReadRegularAt(root, relative string, limit int64, beforeOpen func(string)) ([]byte, error) {
	rootDirectory, err := os.Open(root) //nolint:gosec // Root is the operator-selected manifest directory.
	if err != nil {
		return nil, err
	}
	defer func() { _ = rootDirectory.Close() }()
	components := strings.Split(relative, "/")
	parent := rootDirectory
	for index, component := range components {
		if beforeOpen != nil {
			beforeOpen(component)
		}
		flags := unix.O_RDONLY | unix.O_CLOEXEC | unix.O_NOFOLLOW
		if index < len(components)-1 {
			flags |= unix.O_DIRECTORY
		}
		fd, openErr := unix.Openat(int(parent.Fd()), component, flags, 0)
		if openErr != nil {
			return nil, fmt.Errorf("open path component %q without symlinks: %w", component, openErr)
		}
		opened := os.NewFile(uintptr(fd), component)
		if parent != rootDirectory {
			_ = parent.Close()
		}
		parent = opened
	}
	if parent == rootDirectory {
		return nil, errors.New("platform path is empty")
	}
	defer func() { _ = parent.Close() }()
	info, err := parent.Stat()
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() {
		return nil, errors.New("input is not a regular file")
	}
	if info.Size() < 0 || info.Size() > limit {
		return nil, fmt.Errorf("input exceeds %d-byte limit", limit)
	}
	data, err := io.ReadAll(io.LimitReader(parent, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > limit {
		return nil, fmt.Errorf("input exceeds %d-byte limit", limit)
	}
	return data, nil
}
