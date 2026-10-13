// SPDX-FileCopyrightText: SAP SE or an SAP affiliate company and IronCore contributors
// SPDX-License-Identifier: Apache-2.0

package subscriptions

import (
	"crypto/tls"
	"fmt"
	"net/http"
	"net/url"
	"time"
)

// NewBMCTransport returns an http.RoundTripper cloned from http.DefaultTransport
// with an optional InsecureSkipVerify setting and a ResponseHeaderTimeout.
// Both the metric fetcher and the SSE collector use this to share transport
// construction logic without duplicating the TLS configuration.
func NewBMCTransport(insecureTLS bool, responseHeaderTimeout time.Duration) http.RoundTripper {
	t := http.DefaultTransport.(*http.Transport).Clone() //nolint:forcetypeassert
	t.ResponseHeaderTimeout = responseHeaderTimeout
	if insecureTLS {
		t.TLSClientConfig = &tls.Config{InsecureSkipVerify: insecureTLS} //nolint:gosec
	}
	return t
}

// ResolveSameOriginURL resolves uri (which may be relative) against bmcEndpoint
// and validates that the result is same-origin with the BMC. Returns the
// absolute URL string, or an error if either uri or bmcEndpoint cannot be
// parsed, or if the resolved URL's scheme or host differs from the BMC origin.
func ResolveSameOriginURL(uri, bmcEndpoint string) (string, error) {
	parsed, err := url.Parse(uri)
	if err != nil {
		return "", fmt.Errorf("parse uri: %w", err)
	}
	origin, err := url.Parse(bmcEndpoint)
	if err != nil {
		return "", fmt.Errorf("parse BMC endpoint: %w", err)
	}
	abs := origin.ResolveReference(parsed)
	if abs.Scheme != origin.Scheme || abs.Host != origin.Host {
		return "", fmt.Errorf("uri %q resolves outside BMC origin %s", uri, origin.Host)
	}
	return abs.String(), nil
}
