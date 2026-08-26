//go:build linux || (darwin && amd64)

package mount2

import (
	"context"
	"testing"

	"github.com/rclone/rclone/backend/memory"
	"github.com/rclone/rclone/cmd/mountlib"
	"github.com/rclone/rclone/fs/config/configmap"
	"github.com/rclone/rclone/vfs"
	"github.com/rclone/rclone/vfs/vfscommon"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMountOptionsDirectMountStrict(t *testing.T) {
	ctx := context.Background()
	backend, err := memory.NewFs(ctx, "direct-options-test", "", configmap.Simple{})
	require.NoError(t, err)
	vfsInstance := vfs.New(ctx, backend, &vfscommon.Opt)
	defer vfsInstance.Shutdown()

	opt := &mountlib.Options{DirectMountStrict: true}
	got := mountOptions(NewFS(vfsInstance, opt), backend, opt)
	assert.True(t, got.DirectMount)
	assert.True(t, got.DirectMountStrict)
}

func TestDisableCommandAndRC(t *testing.T) {
	parent := mountCommand.Parent()
	require.NotNil(t, parent)
	_, mountFn := mountlib.ResolveMountMethod("mount2")
	require.NotNil(t, mountFn)
	t.Cleanup(func() {
		parent.AddCommand(mountCommand)
		mountlib.AddRc("mount2", mount)
	})

	DisableCommandAndRC()
	assert.Nil(t, mountCommand.Parent())
	_, mountFn = mountlib.ResolveMountMethod("mount2")
	assert.Nil(t, mountFn)
}
