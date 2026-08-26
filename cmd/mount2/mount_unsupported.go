//go:build !linux && (!darwin || !amd64)

// Package mount2 implements a FUSE mounting system for rclone remotes.
//
// Build for mount for unsupported platforms to stop go complaining
// about "no buildable Go source files".
package mount2

import (
	"context"
	"errors"

	"github.com/rclone/rclone/cmd/mountlib"
	"github.com/rclone/rclone/vfs"
)

// Mount reports that mount2 is unavailable on this platform.
func Mount(*vfs.VFS, string, *mountlib.Options) (<-chan error, func() error, string, error) {
	return nil, nil, "", errors.New("mount2 is only supported on Linux and Darwin AMD64")
}

// MountWithContext reports that mount2 is unavailable on this platform.
func MountWithContext(context.Context, *vfs.VFS, string, *mountlib.Options) (<-chan error, func() error, string, error) {
	return nil, nil, "", errors.New("mount2 is only supported on Linux and Darwin AMD64")
}

// DisableCommandAndRC is a no-op on platforms where mount2 is unavailable.
func DisableCommandAndRC() {}
