// SPDX-FileCopyrightText: SAP SE or an SAP affiliate company and IronCore contributors
// SPDX-License-Identifier: Apache-2.0

package runtime

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/go-logr/logr"
	"github.com/ironcore-dev/metal-maintenance-operator/internal/telemetry/subscriptions"
)

// BMCMetricFetcher fetches a full Redfish MetricReport body from a BMC via
// authenticated GET. Used when a MetricReport POST notification arrives with
// empty MetricValues but a non-empty @odata.id (Dell 17G iDRAC 10 behaviour).
type BMCMetricFetcher struct {
	Resolver subscriptions.Resolver
	Log      logr.Logger
	client   *http.Client
}

// NewBMCMetricFetcher constructs a BMCMetricFetcher with a shared http.Client
// whose transport is cloned from http.DefaultTransport so idle-connection
// pooling and timeouts are retained.
func NewBMCMetricFetcher(resolver subscriptions.Resolver, insecureTLS bool, log logr.Logger) *BMCMetricFetcher {
	return &BMCMetricFetcher{
		Resolver: resolver,
		Log:      log,
		client: &http.Client{
			Transport:     subscriptions.NewBMCTransport(insecureTLS, 0),
			Timeout:       30 * time.Second,
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		},
	}
}

func (f *BMCMetricFetcher) FetchMetricReport(ctx context.Context, bmcName, odataID string) ([]byte, error) {
	r, err := f.Resolver.Resolve(ctx, bmcName)
	if err != nil {
		return nil, fmt.Errorf("resolve BMC %q: %w", bmcName, err)
	}
	rawURL, err := resolveODataURL(odataID, subscriptions.BMCEndpoint(r.BMC))
	if err != nil {
		return nil, fmt.Errorf("validate odataID for BMC %q: %w", bmcName, err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return nil, err
	}
	req.SetBasicAuth(r.Username, r.Password)

	resp, err := f.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("GET %s: status %d", rawURL, resp.StatusCode)
	}
	const maxFetchBytes = 1 << 20 // 1 MiB
	buf, err := io.ReadAll(io.LimitReader(resp.Body, maxFetchBytes+1))
	if err != nil {
		return nil, err
	}
	if len(buf) > maxFetchBytes {
		return nil, fmt.Errorf("GET %s: response exceeds %d bytes", rawURL, maxFetchBytes)
	}
	return buf, nil
}

// resolveODataURL validates odataID and resolves it against the BMC endpoint.
// The check against metricReportsPath is applied to abs.Path after URL
// normalisation so that dot-segment traversal (e.g. MetricReports/../../Systems/1)
// cannot bypass it. The resolved URL must also be same-origin with the BMC and
// carry no userinfo.
func resolveODataURL(odataID, bmcEndpoint string) (string, error) {
	const metricReportsPath = "/redfish/v1/TelemetryService/MetricReports/"
	// ResolveSameOriginURL validates same-origin and returns abs.String() — a
	// well-formed URL that url.Parse will always accept. Parse it again here
	// only to access the structured abs.Path and abs.User fields.
	rawAbs, err := subscriptions.ResolveSameOriginURL(odataID, bmcEndpoint)
	if err != nil {
		return "", err
	}
	abs, _ := url.Parse(rawAbs) // rawAbs is well-formed output of ResolveSameOriginURL
	if !strings.HasPrefix(abs.Path, metricReportsPath) {
		return "", fmt.Errorf("odataID %q is not under %s", odataID, metricReportsPath)
	}
	if abs.User != nil {
		return "", fmt.Errorf("odataID %q contains userinfo", odataID)
	}
	return rawAbs, nil
}
