// SPDX-FileCopyrightText: SAP SE or an SAP affiliate company and IronCore contributors
// SPDX-License-Identifier: Apache-2.0

package capability

import (
	"context"
	"encoding/json"
	"time"

	metalv1alpha1 "github.com/ironcore-dev/metal-operator/api/v1alpha1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	// AnnotationKey is the BMC annotation that stores the cached Capabilities JSON.
	AnnotationKey = "telemetry.metal.ironcore.dev/capabilities"

	// DefaultCacheTTL is how long a cached capability snapshot is considered
	// fresh. The reconciler overrides this via Config.CapabilityCacheTTL when set.
	DefaultCacheTTL = 24 * time.Hour
)

// GetCached reads the cached Capabilities from a BMC annotation, validating
// freshness and firmware version. Returns (caps, true) on a cache hit.
// Returns (nil, false) when absent, expired, or firmware has changed since
// the snapshot was taken.
func GetCached(bmc *metalv1alpha1.BMC, ttl time.Duration) (*Capabilities, bool) {
	if ttl <= 0 {
		ttl = DefaultCacheTTL
	}
	raw, ok := bmc.Annotations[AnnotationKey]
	if !ok || raw == "" {
		return nil, false
	}
	var caps Capabilities
	if err := json.Unmarshal([]byte(raw), &caps); err != nil {
		return nil, false
	}
	if time.Since(caps.ProbedAt) > ttl {
		return nil, false
	}
	// Invalidate if firmware version has changed since we probed.
	if bmc.Status.FirmwareVersion != "" && caps.FirmwareVersion != bmc.Status.FirmwareVersion {
		return nil, false
	}
	return &caps, true
}

// PersistCached encodes caps as JSON and patches it onto the BMC's annotation.
// Failures are non-fatal: the caller should log and continue. The bmc object
// is updated in-place by the server response (ResourceVersion, etc.).
func PersistCached(ctx context.Context, c client.Client, bmc *metalv1alpha1.BMC, caps *Capabilities) error {
	data, err := json.Marshal(caps)
	if err != nil {
		return err
	}

	base := bmc.DeepCopy()
	if bmc.Annotations == nil {
		bmc.Annotations = make(map[string]string)
	}
	bmc.Annotations[AnnotationKey] = string(data)
	return c.Patch(ctx, bmc, client.MergeFrom(base))
}
