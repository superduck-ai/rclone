// Package multimount mounts one or more Filestore filesystems from a JSON config.
package multimount

import (
	"encoding/json"
	"fmt"
	"os"
)

// MountSpec describes one filesystem or memory-store mount.
type MountSpec struct {
	FilesystemID    string  `json:"filesystem_id"`
	MemoryStoreID   string  `json:"memory_store_id,omitempty"`
	BackendCacheTTL string  `json:"backend_cache_ttl,omitempty"`
	Source          string  `json:"source"`
	Destination     string  `json:"destination"`
	AuthToken       string  `json:"auth_token"`
	ReadOnly        bool    `json:"readonly"`
	CacheDurationS  float64 `json:"cache_duration_s"`
	DirPerms        string  `json:"dir_perms"`
	FilePerms       string  `json:"file_perms"`
	UID             uint32  `json:"uid"`
	GID             uint32  `json:"gid"`
	VFSCacheMode    string  `json:"vfs_cache_mode"`
	VFSCacheMaxSize string  `json:"vfs_cache_max_size"`
}

// MultiMountConfig is the top-level multimount configuration.
type MultiMountConfig struct { //nolint:revive // The fully qualified name is intentionally descriptive.
	StateDir   string      `json:"state_dir"`
	ReadyFile  string      `json:"ready_file"`
	ServiceURL string      `json:"service_url"`
	Mounts     []MountSpec `json:"mounts"`
}

func loadConfig(configPath string) (*MultiMountConfig, error) {
	data, err := os.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("reading config %s: %w", configPath, err)
	}

	cfg := new(MultiMountConfig)
	if err := json.Unmarshal(data, cfg); err != nil {
		return nil, fmt.Errorf("parsing config %s: %w", configPath, err)
	}
	if cfg.StateDir == "" {
		return nil, fmt.Errorf("config: state_dir is required")
	}
	if cfg.ReadyFile == "" {
		return nil, fmt.Errorf("config: ready_file is required")
	}
	if cfg.ServiceURL == "" {
		return nil, fmt.Errorf("config: service_url is required")
	}
	if len(cfg.Mounts) == 0 {
		return nil, fmt.Errorf("config: at least one mount is required")
	}
	for i := range cfg.Mounts {
		mount := &cfg.Mounts[i]
		if (mount.FilesystemID == "") == (mount.MemoryStoreID == "") {
			return nil, fmt.Errorf("config: mounts[%d] must set exactly one of filesystem_id or memory_store_id", i)
		}
		if mount.Destination == "" {
			return nil, fmt.Errorf("config: mounts[%d].destination is required", i)
		}
		if mount.AuthToken == "" {
			return nil, fmt.Errorf("config: mounts[%d].auth_token is required", i)
		}
	}
	return cfg, nil
}
