// Command rclone-filestore mounts Anthropic Filestore filesystems with rclone VFS.
package main

import (
	_ "github.com/rclone/rclone/backend/filestore"
	_ "github.com/rclone/rclone/backend/local"
	"github.com/rclone/rclone/cmd"
	"github.com/rclone/rclone/cmd/mount2"
	_ "github.com/rclone/rclone/cmd/multimount"
)

func configureRoot() {
	mount2.DisableCommandAndRC()
	cmd.Root.Use = "rclone-filestore"
	cmd.Root.Short = "Mount Anthropic Filestore filesystems."
}

func main() {
	configureRoot()
	cmd.Main()
}
