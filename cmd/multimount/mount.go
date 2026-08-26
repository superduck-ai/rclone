package multimount

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/rclone/rclone/cmd/mount2"
	"github.com/rclone/rclone/cmd/mountlib"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/config/configmap"
	"github.com/rclone/rclone/vfs"
	"github.com/rclone/rclone/vfs/vfscommon"
)

type mountState struct {
	spec      MountSpec
	unmountFn func() error
	stateDir  string
}

type mountFunction func(*vfs.VFS, string, *mountlib.Options) (<-chan error, func() error, string, error)

var mountDirect mountFunction = mount2.Mount

func (spec MountSpec) identifier() string {
	if spec.MemoryStoreID != "" {
		return spec.MemoryStoreID
	}
	return spec.FilesystemID
}

func mountSlug(spec MountSpec) string {
	cleaned := strings.ReplaceAll(spec.Destination, "/", "_")
	cleaned = strings.Trim(cleaned, "_")
	if cleaned == "" {
		cleaned = "root"
	}
	return spec.identifier() + "_" + cleaned
}

func resolveBackend(cfg *MultiMountConfig, spec MountSpec) (string, configmap.Simple) {
	values := configmap.Simple{
		"url":        cfg.ServiceURL,
		"auth_token": spec.AuthToken,
	}
	if spec.MemoryStoreID != "" {
		values["memory_store_id"] = spec.MemoryStoreID
		if spec.BackendCacheTTL != "" {
			values["cache_ttl"] = spec.BackendCacheTTL
		}
		return "rclone-memory", values
	}
	values["filesystem_id"] = spec.FilesystemID
	return "rclone-filestore", values
}

func parseCacheMode(value string) vfscommon.CacheMode {
	switch strings.ToLower(value) {
	case "minimal":
		return vfscommon.CacheModeMinimal
	case "writes":
		return vfscommon.CacheModeWrites
	case "full":
		return vfscommon.CacheModeFull
	default:
		return vfscommon.CacheModeOff
	}
}

func parseDirCacheTime(seconds float64) time.Duration {
	if seconds <= 0 {
		return 5 * time.Minute
	}
	return time.Duration(seconds * float64(time.Second))
}

func parseSizeSuffix(value string) fs.SizeSuffix {
	if value == "" {
		return 0
	}
	var size fs.SizeSuffix
	if err := size.Set(value); err != nil {
		fs.Logf(nil, "invalid size %q, using 0: %v", value, err)
		return 0
	}
	return size
}

func parseFileMode(value string) vfscommon.FileMode {
	mode, err := strconv.ParseUint(value, 8, 32)
	if err != nil {
		fs.Logf(nil, "invalid file mode %q, using 0: %v", value, err)
		return 0
	}
	return vfscommon.FileMode(mode)
}

func startMount(ctx context.Context, cfg *MultiMountConfig, spec MountSpec) (*mountState, error) {
	slug := mountSlug(spec)
	stateDir := filepath.Join(cfg.StateDir, slug)
	if err := os.MkdirAll(stateDir, 0o755); err != nil {
		return nil, fmt.Errorf("creating state dir %s: %w", stateDir, err)
	}
	if err := os.MkdirAll(spec.Destination, 0o755); err != nil {
		return nil, fmt.Errorf("creating mount target %s: %w", spec.Destination, err)
	}

	source := spec.Source
	if source == "" {
		source = "/"
	}
	backendName, values := resolveBackend(cfg, spec)
	regInfo, err := fs.Find(backendName)
	if err != nil {
		return nil, fmt.Errorf("finding %s backend: %w", backendName, err)
	}
	backend, err := regInfo.NewFs(ctx, slug, source, values)
	if err != nil {
		return nil, fmt.Errorf("creating backend for %s: %w", spec.Destination, err)
	}

	vfsOpt := vfscommon.Opt
	vfsOpt.ReadOnly = spec.ReadOnly
	vfsOpt.CacheMode = parseCacheMode(spec.VFSCacheMode)
	vfsOpt.CacheMaxSize = parseSizeSuffix(spec.VFSCacheMaxSize)
	duration := parseDirCacheTime(spec.CacheDurationS)
	vfsOpt.DirCacheTime = fs.Duration(duration)
	vfsOpt.CachePollInterval = fs.Duration(duration)
	if spec.MemoryStoreID != "" && !spec.ReadOnly {
		vfsOpt.WriteBack = 0
		vfsOpt.CachePollInterval = vfscommon.Opt.CachePollInterval
	}
	if spec.UID != 0 {
		vfsOpt.UID = spec.UID
	}
	if spec.GID != 0 {
		vfsOpt.GID = spec.GID
	}
	if spec.DirPerms != "" {
		vfsOpt.DirPerms = parseFileMode(spec.DirPerms)
	}
	if spec.FilePerms != "" {
		vfsOpt.FilePerms = parseFileMode(spec.FilePerms)
	}

	vfsInstance := vfs.New(ctx, backend, &vfsOpt)
	mountOpt := mountlib.Options{
		AllowNonEmpty:     true,
		AllowOther:        true,
		AttrTimeout:       fs.Duration(time.Second),
		DeviceName:        fmt.Sprintf("%s:%s:%s", backendName, spec.identifier(), spec.Destination),
		DirectMountStrict: true,
	}
	serveErr, unmountFn, _, err := mountDirect(vfsInstance, spec.Destination, &mountOpt)
	if err != nil {
		vfsInstance.Shutdown()
		return nil, fmt.Errorf("mounting %s: %w", spec.Destination, err)
	}

	go func() {
		if err := <-serveErr; err != nil {
			fs.Logf(nil, "mount %s serve error: %v", spec.Destination, err)
		}
	}()
	go func() {
		entries, err := os.ReadDir(spec.Destination)
		if err != nil {
			fs.Logf(nil, "cache prime for %s: ReadDir returned %v (mount may still be initializing)", spec.Destination, err)
			return
		}
		fs.Logf(nil, "cache primed for %s: %d entries", spec.Destination, len(entries))
	}()

	return &mountState{spec: spec, unmountFn: unmountFn, stateDir: stateDir}, nil
}
