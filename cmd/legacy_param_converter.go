// Copyright 2024 Google Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cmd

import (
	"fmt"
	"reflect"

	"github.com/googlecloudplatform/gcsfuse/v2/cfg"
	"github.com/googlecloudplatform/gcsfuse/v2/internal/config"
	"github.com/mitchellh/mapstructure"
)

// cliContext is abstraction over the IsSet() method of cli.Context, Specially
// added to keep OverrideWithIgnoreInterruptsFlag method's unit test simple.
type cliContext interface {
	IsSet(string) bool
}

// PopulateNewConfigFromLegacyFlagsAndConfig takes cliContext, legacy flags and legacy MountConfig and resolves it into new cfg.Config Object.
func PopulateNewConfigFromLegacyFlagsAndConfig(c cliContext, flags *flagStorage, legacyConfig *config.MountConfig) (*cfg.Config, error) {
	resolvedConfig := &cfg.Config{}

	structuredFlags := &map[string]interface{}{
		"app-name": flags.AppName,
		"debug": &map[string]interface{}{
			"exit-on-invariant-violation": flags.DebugInvariants,
			"gcs":                         flags.DebugGCS,
			"log-mutex":                   flags.DebugMutex,
		},
		"file-system": map[string]interface{}{
			"dir-mode":  flags.DirMode,
			"file-mode": flags.FileMode,
			// Todo: "fuse-options":      nil,
			"gid":               flags.Gid,
			"ignore-interrupts": flags.IgnoreInterrupts,
			"rename-dir-limit":  flags.RenameDirLimit,
			"temp-dir":          flags.TempDir,
			"uid":               flags.Uid,
		},
		"foreground": flags.Foreground,
		"gcs-auth": map[string]interface{}{
			"anonymous-access":     flags.AnonymousAccess,
			"key-file":             flags.KeyFile,
			"reuse-token-from-url": flags.ReuseTokenFromUrl,
			"token-url":            flags.TokenUrl,
		},
		"gcs-connection": map[string]interface{}{
			"billing-project":               flags.BillingProject,
			"client-protocol":               string(flags.ClientProtocol),
			"custom-endpoint":               flags.CustomEndpoint,
			"experimental-enable-json-read": flags.ExperimentalEnableJsonRead,
			"http-client-timeout":           flags.HttpClientTimeout,
			"limit-bytes-per-sec":           flags.EgressBandwidthLimitBytesPerSecond,
			"limit-ops-per-sec":             flags.OpRateLimitHz,
			"max-conns-per-host":            flags.MaxConnsPerHost,
			"max-idle-conns-per-host":       flags.MaxIdleConnsPerHost,
			"sequential-read-size-mb":       flags.SequentialReadSizeMb,
		},
		"gcs-retries": map[string]interface{}{
			"max-retry-sleep": flags.MaxRetrySleep,
			"multiplier":      flags.RetryMultiplier,
		},
		"implicit-dirs": flags.ImplicitDirs,
		"list": map[string]interface{}{
			"kernel-list-cache-ttl-secs": flags.KernelListCacheTtlSeconds,
		},
		"logging": map[string]interface{}{
			"file-path": flags.LogFile,
			"format":    flags.LogFormat,
		},
		"metadata-cache": map[string]interface{}{
			"deprecated-stat-cache-capacity":          flags.StatCacheCapacity,
			"deprecated-stat-cache-ttl":               flags.StatCacheTTL,
			"deprecated-type-cache-ttl":               flags.TypeCacheTTL,
			"enable-nonexistent-type-cache":           flags.EnableNonexistentTypeCache,
			"experimental-metadata-prefetch-on-mount": flags.ExperimentalMetadataPrefetchOnMount,
		},
		"metrics": map[string]interface{}{
			"stackdriver-export-interval": flags.StackdriverExportInterval,
		},
		"monitoring": map[string]interface{}{
			"experimental-opentelemetry-collector-address": flags.OtelCollectorAddress,
		},
		"only-dir": flags.OnlyDir,
	}

	// Use decoder to convert flagStorage to cfg.Config.
	decoderConfig := &mapstructure.DecoderConfig{
		DecodeHook: cfg.DecodeHook(),
		Result:     resolvedConfig,
		TagName:    "yaml",
	}
	decoder, err := mapstructure.NewDecoder(decoderConfig)
	if err != nil {
		return nil, fmt.Errorf("mapstructure.NewDecoder: %v", err)
	}
	// Decoding flags.
	err = decoder.Decode(structuredFlags)
	if err != nil {
		return nil, fmt.Errorf("decoder.Decode(structuredFlags): %v", err)
	}

	// If config file is not present, no need to decode it.
	if legacyConfig == nil || reflect.DeepEqual(*legacyConfig, config.MountConfig{}) {
		return resolvedConfig, nil
	}

	// Save overlapping flags in a map to override the config value later.
	overlapFlags := map[string]interface{}{
		"log-file":                   resolvedConfig.Logging.FilePath,
		"log-format":                 resolvedConfig.Logging.Format,
		"ignore-interrupts":          resolvedConfig.FileSystem.IgnoreInterrupts,
		"anonymous-access":           resolvedConfig.GcsAuth.AnonymousAccess,
		"kernel-list-cache-ttl-secs": resolvedConfig.List.KernelListCacheTtlSecs,
	}

	// Decoding config to the same config structure.
	err = decoder.Decode(legacyConfig)
	if err != nil {
		return nil, fmt.Errorf("decoder.Decode(config): %v", err)
	}

	// Override/Give priority to flags in case of overlap in flags and config.
	for flagName, value := range overlapFlags {
		if c.IsSet(flagName) {
			switch flagName {
			case "log-file":
				resolvedConfig.Logging.FilePath = value.(cfg.ResolvedPath)
			case "log-format":
				resolvedConfig.Logging.Format = value.(string)
			case "ignore-interrupts":
				resolvedConfig.FileSystem.IgnoreInterrupts = value.(bool)
			case "anonymous-access":
				resolvedConfig.GcsAuth.AnonymousAccess = value.(bool)
			case "kernel-list-cache-ttl-secs":
				resolvedConfig.List.KernelListCacheTtlSecs = value.(int64)
			}
		}
	}

	return resolvedConfig, nil
}
