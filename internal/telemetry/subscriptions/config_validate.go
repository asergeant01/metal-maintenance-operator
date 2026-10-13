// SPDX-FileCopyrightText: SAP SE or an SAP affiliate company and IronCore contributors
// SPDX-License-Identifier: Apache-2.0

package subscriptions

import (
	"fmt"
	"strings"
	"time"

	"github.com/blang/semver/v4"
	"github.com/ironcore-dev/metal-operator/bmc"
	goyaml "go.yaml.in/yaml/v3"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

const (
	minInterval = 5 * time.Second
	maxInterval = 24 * time.Hour

	minPerBMCTimeout = time.Second
	maxPerBMCTimeout = 10 * time.Minute

	minTestEventInterval = time.Minute
	maxTestEventInterval = 7 * 24 * time.Hour

	minTestEventTimeout = 5 * time.Second
	maxTestEventTimeout = 5 * time.Minute

	minCapabilityCacheTTL = 5 * time.Minute
	maxCapabilityCacheTTL = 7 * 24 * time.Hour
)

var knownVendors = map[bmc.Manufacturer]struct{}{
	bmc.ManufacturerDell:       {},
	bmc.ManufacturerLenovo:     {},
	bmc.ManufacturerHPE:        {},
	bmc.ManufacturerSupermicro: {},
	bmc.ManufacturerBull:       {},
}

// supportedVendorList returns the canonical vendor strings in a stable
// order for use in error messages. Built once, not allocated per error.
var supportedVendorList = []string{
	string(bmc.ManufacturerDell),
	string(bmc.ManufacturerHPE),
	string(bmc.ManufacturerLenovo),
	string(bmc.ManufacturerSupermicro),
	string(bmc.ManufacturerBull),
}

func isKnownVendor(v string) bool {
	_, ok := knownVendors[bmc.Manufacturer(v)]
	return ok
}

// Parse decodes raw YAML from the ConfigMap's data field using strict
// unmarshalling (unknown keys are rejected) and then validates all fields.
// It returns a fully-validated Config or a non-empty ErrorList.
func Parse(raw []byte) (*Config, field.ErrorList) {
	var cfg Config
	dec := goyaml.NewDecoder(strings.NewReader(string(raw)))
	dec.KnownFields(true)
	if err := dec.Decode(&cfg); err != nil {
		return nil, field.ErrorList{
			field.Invalid(field.NewPath(""), string(raw), fmt.Sprintf("YAML parse error: %v", err)),
		}
	}
	if errs := Validate(&cfg); len(errs) > 0 {
		return nil, errs
	}
	return &cfg, nil
}

// Validate checks all semantic constraints on a Config.
// All errors are collected into one pass so the caller sees them all at once.
func Validate(cfg *Config) field.ErrorList {
	var errs field.ErrorList
	root := field.NewPath("")

	// apiVersion
	if cfg.APIVersion != APIVersion && cfg.APIVersion != APIVersionV1Alpha1 {
		errs = append(errs, field.Invalid(root.Child("apiVersion"), cfg.APIVersion,
			fmt.Sprintf("must be %q (or %q for legacy configs)", APIVersion, APIVersionV1Alpha1)))
	}

	if cfg.SubscriptionReconcileInterval == 0 {
		errs = append(errs, field.Required(root.Child("subscriptionReconcileInterval"),
			"subscriptionReconcileInterval is required"))
	} else if cfg.SubscriptionReconcileInterval < minInterval || cfg.SubscriptionReconcileInterval > maxInterval {
		errs = append(errs, field.Invalid(root.Child("subscriptionReconcileInterval"), cfg.SubscriptionReconcileInterval,
			fmt.Sprintf("must be between %s and %s", minInterval, maxInterval)))
	}

	// perBMCTimeout
	if cfg.PerBMCTimeout != 0 && (cfg.PerBMCTimeout < minPerBMCTimeout || cfg.PerBMCTimeout > maxPerBMCTimeout) {
		errs = append(errs, field.Invalid(root.Child("perBMCTimeout"), cfg.PerBMCTimeout,
			fmt.Sprintf("must be between %s and %s when set", minPerBMCTimeout, maxPerBMCTimeout)))
	}

	// testEventInterval — optional; zero disables the health check.
	if cfg.TestEventInterval != 0 && (cfg.TestEventInterval < minTestEventInterval || cfg.TestEventInterval > maxTestEventInterval) {
		errs = append(errs, field.Invalid(root.Child("testEventInterval"), cfg.TestEventInterval,
			fmt.Sprintf("must be between %s and %s when set", minTestEventInterval, maxTestEventInterval)))
	}

	// testEventTimeout — only meaningful when testEventInterval is set.
	if cfg.TestEventTimeout != 0 && (cfg.TestEventTimeout < minTestEventTimeout || cfg.TestEventTimeout > maxTestEventTimeout) {
		errs = append(errs, field.Invalid(root.Child("testEventTimeout"), cfg.TestEventTimeout,
			fmt.Sprintf("must be between %s and %s when set", minTestEventTimeout, maxTestEventTimeout)))
	}

	// capabilityCacheTTL — optional; zero means use the default (24h).
	if cfg.CapabilityCacheTTL != 0 && (cfg.CapabilityCacheTTL < minCapabilityCacheTTL || cfg.CapabilityCacheTTL > maxCapabilityCacheTTL) {
		errs = append(errs, field.Invalid(root.Child("capabilityCacheTTL"), cfg.CapabilityCacheTTL,
			fmt.Sprintf("must be between %s and %s when set", minCapabilityCacheTTL, maxCapabilityCacheTTL)))
	}

	// Validate hardware match rows for all three lists.
	validateHardwareRows(&errs, root.Child("excludedHardware"), cfg.ExcludedHardware, false)
	validateHardwareRows(&errs, root.Child("hardwareOverrides"), cfg.HardwareOverrides, cfg.TestEventInterval > 0)
	validateHardwareRows(&errs, root.Child("eventBasedHardware"), cfg.EventBasedHardware, cfg.TestEventInterval > 0)

	return errs
}

// validateHardwareRows validates a slice of HardwareMatch rows at the given
// field path. requireTestMessageId enforces that each row has a TestMessageId
// when testEventInterval is configured.
func validateHardwareRows(errs *field.ErrorList, path *field.Path, rows []HardwareMatch, requireTestMessageId bool) {
	for i, hw := range rows {
		hwPath := path.Index(i)
		vendor := strings.TrimSpace(hw.Vendor)
		switch {
		case vendor == "":
			*errs = append(*errs, field.Required(hwPath.Child("vendor"), ""))
		case hw.Vendor != vendor || !isKnownVendor(vendor):
			*errs = append(*errs, field.NotSupported(hwPath.Child("vendor"), hw.Vendor, supportedVendorList))
		}
		if len(hw.Models) == 0 {
			*errs = append(*errs, field.Required(hwPath.Child("models"), "at least one model or \"*\" required"))
		} else {
			for j, model := range hw.Models {
				if strings.TrimSpace(model) == "" {
					*errs = append(*errs, field.Invalid(hwPath.Child("models").Index(j), model,
						"model entries must not be blank"))
				}
			}
		}
		if hw.MinFirmware != "" {
			if _, err := semver.ParseTolerant(hw.MinFirmware); err != nil {
				*errs = append(*errs, field.Invalid(hwPath.Child("minFirmware"), hw.MinFirmware,
					fmt.Sprintf("must be a valid semver: %v", err)))
			}
		}
		if requireTestMessageId && hw.TestMessageId == "" {
			*errs = append(*errs, field.Required(hwPath.Child("testMessageId"),
				"required when testEventInterval is set"))
		}
		switch hw.MetricDelivery {
		case "", "push", "sse":
		default:
			*errs = append(*errs, field.NotSupported(hwPath.Child("metricDelivery"), hw.MetricDelivery, []string{"push", "sse"}))
		}
	}
}
