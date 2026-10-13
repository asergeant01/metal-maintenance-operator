// SPDX-FileCopyrightText: SAP SE or an SAP affiliate company and IronCore contributors
// SPDX-License-Identifier: Apache-2.0

// Package capability probes and caches the Redfish telemetry capabilities of a BMC.
// It reads EventService and TelemetryService at reconcile time and derives a
// Capabilities snapshot that the subscription reconciler uses to choose a
// SubscriptionStrategy instead of relying on a static vendor/model allowlist.
package capability

import "time"

// Capabilities summarises the Redfish telemetry features discovered on a BMC.
type Capabilities struct {
	// RedfishVersion is the protocol version reported by the service root (e.g. "1.20.1").
	RedfishVersion string `json:"redfishVersion,omitempty"`

	EventService     *EventServiceCaps     `json:"eventService,omitempty"`
	TelemetryService *TelemetryServiceCaps `json:"telemetryService,omitempty"`

	// ProbedAt is when this snapshot was taken. Used for TTL-based cache invalidation.
	ProbedAt time.Time `json:"probedAt"`

	// FirmwareVersion is the BMC firmware at probe time. A version change
	// invalidates the cached capabilities, forcing a fresh probe.
	FirmwareVersion string `json:"firmwareVersion,omitempty"`
}

// EventServiceCaps summarises the EventService capabilities relevant to
// subscription management.
type EventServiceCaps struct {
	// SchemaVersion is extracted from the @odata.type string (e.g. "v1_10_2").
	SchemaVersion string `json:"schemaVersion,omitempty"`

	// EventFormatTypes are the EventFormatType values the service accepts
	// (e.g. ["Event", "MetricReport"]). Present on Redfish EventService v1.2.0+.
	EventFormatTypes []string `json:"eventFormatTypes,omitempty"`

	// SupportsMetricReport is true when EventFormatTypes includes "MetricReport".
	// This is the primary capability gate for metric-report subscriptions.
	SupportsMetricReport bool `json:"supportsMetricReport,omitempty"`

	// LegacyMetricReport is true when EventFormatTypes is absent but the
	// deprecated EventTypesForSubscription array includes "MetricReport".
	// This covers iDRAC9 and similar legacy firmware. MMO skips metric
	// subscriptions for these BMCs: the modern EventFormatType POST field
	// that MO sends is not honoured by legacy EventService implementations.
	// Tracked as a future MO-layer fix.
	LegacyMetricReport bool `json:"legacyMetricReport,omitempty"`

	// SSEUri is the ServerSentEventUri value from the EventService, or empty
	// when SSE is not supported.
	SSEUri string `json:"sseUri,omitempty"`

	// SupportsSSE is true when SSEUri is non-empty.
	SupportsSSE bool `json:"supportsSSE,omitempty"`

	// ServiceEnabled mirrors EventService.ServiceEnabled.
	ServiceEnabled bool `json:"serviceEnabled,omitempty"`
}

// TelemetryServiceCaps summarises the TelemetryService presence and configuration.
type TelemetryServiceCaps struct {
	// Present is true when the service root exposes a TelemetryService link.
	Present bool `json:"present"`

	// Enabled mirrors TelemetryService.ServiceEnabled.
	Enabled bool `json:"enabled,omitempty"`

	// MinCollectionInterval is the ISO 8601 minimum interval (e.g. "PT0H0M5S").
	MinCollectionInterval string `json:"minCollectionInterval,omitempty"`

	// MaxReports is the maximum number of MetricReports the service supports.
	MaxReports int `json:"maxReports,omitempty"`
}
