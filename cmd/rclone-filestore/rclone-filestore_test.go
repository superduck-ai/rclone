package main

import (
	"testing"

	"github.com/rclone/rclone/cmd"
	"github.com/rclone/rclone/cmd/mountlib"
	"github.com/stretchr/testify/assert"
)

func TestConfigureRootDisablesGenericMount2EntryPoints(t *testing.T) {
	configureRoot()
	for _, command := range cmd.Root.Commands() {
		assert.NotEqual(t, "mount2", command.Name())
	}
	_, mountFn := mountlib.ResolveMountMethod("mount2")
	assert.Nil(t, mountFn)
}
