// SPDX-FileCopyrightText: SAP SE or an SAP affiliate company and IronCore contributors
// SPDX-License-Identifier: Apache-2.0

package subscriptions

import (
	"strings"

	"github.com/blang/semver/v4"
)

// IsExcluded reports whether the BMC matches any row in cfg.ExcludedHardware.
// Excluded BMCs are never subscribed regardless of Redfish capability.
func IsExcluded(ref BMCRef, cfg *Config) bool {
	if cfg == nil {
		return false
	}
	return findMatch(ref, cfg.ExcludedHardware) != nil
}

// FindOverride returns the first HardwareMatch row from cfg.HardwareOverrides
// or cfg.EventBasedHardware that matches the BMC. The result supplies optional
// delivery-mode overrides and vendor-specific SubmitTestEvent parameters.
// Returns nil when no override row matches.
func FindOverride(ref BMCRef, cfg *Config) *HardwareMatch {
	if cfg == nil {
		return nil
	}
	// HardwareOverrides takes precedence; fall back to EventBasedHardware
	// for backward compatibility with v1alpha1 configs.
	if m := findMatch(ref, cfg.HardwareOverrides); m != nil {
		return m
	}
	return findMatch(ref, cfg.EventBasedHardware)
}

// SubscribeToBMC is kept for backward compatibility with tests and callers
// that have not yet been migrated to FindOverride. It searches both
// HardwareOverrides and EventBasedHardware (the former takes precedence) and
// returns the first matching row, or nil if none match.
//
// Deprecated: use FindOverride for new code. The allowlist gate semantics
// (return nil = do not subscribe) are superseded by SelectStrategy.
func SubscribeToBMC(ref BMCRef, cfg *Config) *HardwareMatch {
	return FindOverride(ref, cfg)
}

// findMatch searches rows for the first entry that matches ref by
// vendor, model, and minimum firmware version.
func findMatch(ref BMCRef, rows []HardwareMatch) *HardwareMatch {
	if ref.Vendor == "" {
		return nil
	}
	for i := range rows {
		hw := &rows[i]
		if !vendorMatches(hw.Vendor, ref.Vendor) {
			continue
		}
		if !modelsMatch(hw.Models, ref.Model) {
			continue
		}
		if !firmwareSatisfies(hw.MinFirmware, ref.FirmwareVersion) {
			continue
		}
		return hw
	}
	return nil
}

// vendorMatches compares the configured vendor against the BMC's
// reported manufacturer.
func vendorMatches(configured, reported string) bool {
	if configured == "" || reported == "" {
		return false
	}
	return configured == reported
}

// modelsMatch returns true if model is in the configured list, or if
// the list contains "*". Comparison is case-insensitive — vendor
// product naming has the same casing chaos as vendor strings.
func modelsMatch(models []string, reported string) bool {
	if len(models) == 0 {
		// Defensive: schema validation rejects empty lists, but under-
		// specified rows should never match.
		return false
	}
	reported = strings.TrimSpace(reported)
	for _, m := range models {
		if m == "*" {
			return true
		}
		if strings.EqualFold(strings.TrimSpace(m), reported) {
			return true
		}
	}
	return false
}

// firmwareSatisfies returns true when minFirmware is empty, or when
// reported parses as a semver >= minFirmware.
func firmwareSatisfies(minFirmware, reported string) bool {
	if minFirmware == "" {
		return true
	}
	if reported == "" {
		return false
	}
	min, err := semver.ParseTolerant(minFirmware)
	if err != nil {
		// Config validation rejects invalid semver, but treat it as
		// "never matches" if one reaches us anyway.
		return false
	}
	got, err := semver.ParseTolerant(reported)
	if err != nil {
		return false
	}
	return got.GTE(min)
}
