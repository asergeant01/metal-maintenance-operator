// SPDX-FileCopyrightText: SAP SE or an SAP affiliate company and IronCore contributors
// SPDX-License-Identifier: Apache-2.0

package capability

import (
	"context"
	"strings"
	"time"

	"github.com/stmcginnis/gofish"
	"github.com/stmcginnis/gofish/schemas"
)

// Probe reads the EventService and TelemetryService from the Redfish service
// root and returns a Capabilities snapshot. Failures on individual service
// endpoints (e.g. a BMC without TelemetryService) are handled gracefully: the
// corresponding Capabilities field is nil rather than an error. The error
// return is reserved for fatal failures such as being unable to reach the
// service root at all.
func Probe(ctx context.Context, api *gofish.APIClient, firmwareVersion string) (*Capabilities, error) {
	caps := &Capabilities{
		ProbedAt:        time.Now(),
		FirmwareVersion: firmwareVersion,
	}

	svc := api.GetService()
	if svc == nil {
		return caps, nil
	}
	caps.RedfishVersion = svc.RedfishVersion

	caps.EventService = probeEventService(svc)
	caps.TelemetryService = probeTelemetryService(svc)

	return caps, nil
}

func probeEventService(svc interface {
	EventService() (*schemas.EventService, error)
}) *EventServiceCaps {
	es, err := svc.EventService()
	if err != nil || es == nil {
		return nil
	}

	ec := &EventServiceCaps{
		SchemaVersion:  schemaVersionFrom(es.ODataType),
		SSEUri:         es.ServerSentEventURI,
		SupportsSSE:    es.ServerSentEventURI != "",
		ServiceEnabled: es.ServiceEnabled,
	}

	if len(es.EventFormatTypes) > 0 {
		for _, eft := range es.EventFormatTypes {
			ec.EventFormatTypes = append(ec.EventFormatTypes, string(eft))
			if eft == schemas.MetricReportEventFormatType {
				ec.SupportsMetricReport = true
			}
		}
	} else {
		// Legacy EventService: check deprecated EventTypesForSubscription.
		for _, et := range es.EventTypesForSubscription {
			if string(et) == "MetricReport" {
				ec.LegacyMetricReport = true
				break
			}
		}
	}

	return ec
}

func probeTelemetryService(svc interface {
	TelemetryService() (*schemas.TelemetryService, error)
}) *TelemetryServiceCaps {
	ts, err := svc.TelemetryService()
	if err != nil || ts == nil {
		// TelemetryService absent or inaccessible — not an error.
		return &TelemetryServiceCaps{Present: false}
	}

	tc := &TelemetryServiceCaps{
		Present:               true,
		Enabled:               ts.ServiceEnabled,
		MinCollectionInterval: ts.MinCollectionInterval,
	}
	if ts.MaxReports != nil {
		tc.MaxReports = *ts.MaxReports
	}
	return tc
}

// schemaVersionFrom extracts the version segment from a Redfish @odata.type
// string such as "#EventService.v1_10_2.EventService" → "v1_10_2".
// Returns an empty string if the pattern is not found.
func schemaVersionFrom(odataType string) string {
	parts := strings.SplitN(odataType, ".", 3)
	if len(parts) >= 2 && strings.HasPrefix(parts[1], "v") {
		return parts[1]
	}
	return ""
}
