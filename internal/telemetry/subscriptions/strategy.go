// SPDX-FileCopyrightText: SAP SE or an SAP affiliate company and IronCore contributors
// SPDX-License-Identifier: Apache-2.0

package subscriptions

import (
	"github.com/ironcore-dev/metal-maintenance-operator/internal/telemetry/capability"
)

// DeliveryMode is how MetricReport data should be collected from the BMC.
type DeliveryMode int

const (
	// DeliveryNone means metric subscriptions are not wanted.
	DeliveryNone DeliveryMode = iota
	// DeliveryPush creates a Redfish HTTP POST subscription for MetricReport events.
	// This is the default delivery mode at fleet scale.
	DeliveryPush
	// DeliverySSE opens a persistent outbound SSE connection to the BMC's
	// ServerSentEventUri. Opt-in only via hardwareOverrides[].metricDelivery: sse.
	DeliverySSE
)

// SubscriptionStrategy is the output of SelectStrategy: it tells the reconciler
// exactly what subscriptions to create and how to deliver metric reports.
type SubscriptionStrategy struct {
	// WantEvents is true when an Event-format subscription should be created
	// for alerts / Critical events.
	WantEvents bool
	// WantMetrics is true when a MetricReport subscription (or SSE stream)
	// should be maintained.
	WantMetrics bool
	// DeliveryMode specifies push vs SSE delivery for metrics.
	// Only meaningful when WantMetrics is true.
	DeliveryMode DeliveryMode

	// SkipReason explains why WantMetrics is false, for log output.
	// Empty when WantMetrics is true.
	SkipReason string

	// TestEvent holds vendor-specific SubmitTestEvent parameters sourced
	// from a matching HardwareOverride row. Nil when no override is found
	// or the override has no TestMessageId.
	TestEvent *TestEventParams
}

// SelectStrategy decides what subscriptions to create for a BMC. It combines
// live capability data (caps) with config overrides (cfg) and the BMC identity
// (ref) to produce a SubscriptionStrategy.
//
// Decision tree:
//
//  1. If the BMC is in the ExcludedHardware list → skip everything.
//
//  2. If caps is nil or EventService is unavailable → fall back to the static
//     HardwareOverride / EventBasedHardware gate (backward compatibility for
//     BMCs that have not yet been probed or where probe auth fails).
//
//  3. Capability-driven path:
//     - WantEvents = true when EventService is present and enabled
//     - WantMetrics = true when TelemetryService is present AND EventService
//       supports MetricReport in EventFormatTypes
//     - Delivery is Push by default; SSE requires an explicit
//       metricDelivery: sse override AND caps.SupportsSSE
func SelectStrategy(caps *capability.Capabilities, ref BMCRef, cfg *Config) *SubscriptionStrategy {
	if cfg == nil {
		return &SubscriptionStrategy{SkipReason: "no config"}
	}

	// 1. Check exclusion list.
	if IsExcluded(ref, cfg) {
		return &SubscriptionStrategy{SkipReason: "excluded by config"}
	}

	// Find any override row: used for delivery-mode override and test event params.
	override := FindOverride(ref, cfg)

	// 2. No capability data — fall back to static gate.
	if caps == nil || caps.EventService == nil {
		if override == nil {
			return &SubscriptionStrategy{SkipReason: "EventService not probed and no config override"}
		}
		return strategyFromOverride(override)
	}

	// 3. Capability-driven path.
	if !caps.EventService.ServiceEnabled {
		return &SubscriptionStrategy{SkipReason: "EventService disabled"}
	}

	strategy := &SubscriptionStrategy{
		WantEvents: true,
	}

	switch {
	case caps.EventService.LegacyMetricReport:
		// iDRAC9 and similar: EventTypesForSubscription has MetricReport but
		// EventFormatTypes is absent. The modern EventFormatType POST field that
		// MO sends is not honoured; skip metric subscriptions.
		strategy.SkipReason = "LegacyMetricReport: EventFormatType not supported by EventService (await MO fix)"

	case caps.TelemetryService == nil || !caps.TelemetryService.Present:
		strategy.SkipReason = "TelemetryService not present"

	case !caps.EventService.SupportsMetricReport:
		strategy.SkipReason = "EventService lacks MetricReport in EventFormatTypes"

	default:
		strategy.WantMetrics = true
		strategy.DeliveryMode = DeliveryPush // default at fleet scale

		// SSE is opt-in: the BMC must support it AND the override must request it.
		if caps.EventService.SupportsSSE && override != nil && override.MetricDelivery == "sse" {
			strategy.DeliveryMode = DeliverySSE
		}
	}

	// Carry test event params from override row (vendor-specific quirks).
	if override != nil && override.TestMessageId != "" {
		strategy.TestEvent = &TestEventParams{
			MessageId:         override.TestMessageId,
			Severity:          override.TestSeverity,
			OriginOfCondition: override.TestOriginOfCondition,
		}
	}

	return strategy
}

// strategyFromOverride converts a legacy HardwareMatch (from EventBasedHardware
// or HardwareOverrides) into a SubscriptionStrategy. Used in the fallback path
// when capability data is not yet available.
func strategyFromOverride(hw *HardwareMatch) *SubscriptionStrategy {
	s := &SubscriptionStrategy{
		WantEvents:  true,
		WantMetrics: true,
	}
	if hw.MetricDelivery == "sse" {
		s.DeliveryMode = DeliverySSE
	} else {
		s.DeliveryMode = DeliveryPush
	}
	if hw.TestMessageId != "" {
		s.TestEvent = &TestEventParams{
			MessageId:         hw.TestMessageId,
			Severity:          hw.TestSeverity,
			OriginOfCondition: hw.TestOriginOfCondition,
		}
	}
	return s
}
