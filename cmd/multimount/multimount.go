package multimount

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/rclone/rclone/cmd"
	"github.com/rclone/rclone/fs"
	"github.com/spf13/cobra"
)

type mountResult struct {
	state *mountState
	err   error
	spec  MountSpec
}

type startMountFunction func(context.Context, *MultiMountConfig, MountSpec) (*mountState, error)
type waitSignalFunction func() os.Signal

var configPath string

func runMultiMount() error {
	return runMultiMountWith(configPath, startMount, waitForShutdownSignal)
}

func runMultiMountWith(configPath string, start startMountFunction, waitSignal waitSignalFunction) error {
	cfg, err := loadConfig(configPath)
	if err != nil {
		return fmt.Errorf("loading config: %w", err)
	}

	fs.Logf(nil, "multimount: starting %d mount(s)", len(cfg.Mounts))
	if err := os.MkdirAll(cfg.StateDir, 0o755); err != nil {
		return fmt.Errorf("creating state dir: %w", err)
	}

	results := make([]mountResult, len(cfg.Mounts))
	var waitGroup sync.WaitGroup
	for i := range cfg.Mounts {
		spec := cfg.Mounts[i]
		waitGroup.Add(1)
		go func(index int, spec MountSpec) {
			defer waitGroup.Done()
			fs.Logf(nil, "multimount: mounting %s -> %s", spec.FilesystemID, spec.Destination)
			state, err := start(context.Background(), cfg, spec)
			results[index] = mountResult{state: state, err: err, spec: spec}
		}(i, spec)
	}
	waitGroup.Wait()

	states := make([]*mountState, 0, len(results))
	failed := false
	for _, result := range results {
		if result.err != nil {
			fs.Logf(nil, "multimount: failed to mount %s: %v", result.spec.Destination, result.err)
			failed = true
			continue
		}
		fs.Logf(nil, "multimount: mounted %s (%s) successfully", result.spec.Destination, result.spec.FilesystemID)
		states = append(states, result.state)
	}

	defer func() {
		cleanupMounts(states)
		fs.Logf(nil, "multimount: shutdown complete")
	}()
	if failed {
		return fmt.Errorf("some mounts failed, cleaning up")
	}
	if err := touchFile(cfg.ReadyFile); err != nil {
		return fmt.Errorf("failed to touch ready file %s: %w", cfg.ReadyFile, err)
	}
	fs.Logf(nil, "multimount: all %d mount(s) ready, touched %s", len(cfg.Mounts), cfg.ReadyFile)

	sig := waitSignal()
	fs.Logf(nil, "multimount: received %v, shutting down", sig)
	return nil
}

func waitForShutdownSignal() os.Signal {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
	return <-signals
}

func cleanupMounts(states []*mountState) {
	for _, state := range states {
		fs.Logf(nil, "multimount: unmounting %s", state.spec.Destination)
		if err := state.unmountFn(); err != nil {
			fs.Logf(nil, "multimount: unmount %s failed: %v", state.spec.Destination, err)
			continue
		}
		if err := os.Remove(state.spec.Destination); err != nil && !errors.Is(err, os.ErrNotExist) {
			fs.Logf(nil, "multimount: removing mount target %s: %v", state.spec.Destination, err)
		}
		if err := os.RemoveAll(state.stateDir); err != nil {
			fs.Logf(nil, "multimount: removing state dir %s: %v", state.stateDir, err)
		}
	}
}

func touchFile(path string) error {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0o666)
	if err != nil {
		return fmt.Errorf("creating ready file: %w", err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("closing ready file: %w", err)
	}
	return nil
}

func init() {
	command := &cobra.Command{
		Use:   "multimount",
		Short: "Mount multiple filestore filesystems simultaneously",
		Long: `Mount multiple filestore filesystems as FUSE mounts from a single process.

Designed for Firecracker containers where a single init script manages all
mounts. Uses go-fuse/v2 with strict direct mount (no fusermount subprocess) to
avoid chroot deadlocks.

The --config flag points to a JSON file describing all mounts. Once every
mount is ready, a ready_file is touched to signal the init script.

On SIGTERM/SIGINT, all mounts are gracefully unmounted before exit.`,
		Args: cobra.NoArgs,
		Run: func(_ *cobra.Command, _ []string) {
			if err := runMultiMount(); err != nil {
				fs.Logf(nil, "multimount: %v", err)
				os.Exit(1)
			}
		},
	}
	command.Flags().StringVar(&configPath, "config", "", "Path to the multimount JSON configuration")
	_ = command.MarkFlagRequired("config")
	cmd.Root.AddCommand(command)
}
