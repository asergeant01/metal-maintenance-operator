// SPDX-FileCopyrightText: SAP SE or an SAP affiliate company and IronCore contributors
// SPDX-License-Identifier: Apache-2.0

// Package ssestream manages per-BMC outbound SSE connections for MetricReport
// delivery. Dell 17G iDRAC 10 uses notification-only HTTP POST pushes with
// empty MetricValues; the SSE stream at ServerSentEventUri carries the full
// inline frames. This collector subscribes to that stream per BMC and forwards
// samples to the MetricReportSink.
package ssestream

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/go-logr/logr"

	"github.com/ironcore-dev/metal-maintenance-operator/internal/telemetry/events"
	"github.com/ironcore-dev/metal-maintenance-operator/internal/telemetry/sink"
	"github.com/ironcore-dev/metal-maintenance-operator/internal/telemetry/subscriptions"
)

const (
	// reconnectInitial is the starting delay between reconnect attempts.
	reconnectInitial = 5 * time.Second
	// reconnectMax is the ceiling on backoff delay.
	reconnectMax = 5 * time.Minute
	// stableThreshold: connections alive longer than this reset the backoff.
	stableThreshold = 60 * time.Second
	// maxTokenSize is the per-line scanner buffer for SSE frames.
	// Dell iDRAC MetricReports with many MetricValues can exceed bufio's
	// default 64 KB limit; 1 MB comfortably covers even the largest reports.
	maxTokenSize = 1 << 20 // 1 MB
	// maxFrameSize caps the assembled SSE frame (all data: lines joined).
	// Prevents unbounded memory growth from a buggy or adversarial BMC that
	// sends a frame split across many large data: lines.
	maxFrameSize = 10 << 20 // 10 MB
	// errorBodyLimit is the maximum bytes read from a non-200 response body
	// for inclusion in the error log.
	errorBodyLimit = 512
	// maxDrainBytes caps the body drain in the deferred close so a misbehaving
	// BMC cannot stall the reconnect loop indefinitely via an unbounded response.
	maxDrainBytes = 1 << 20 // 1 MB
	// idleTimeout is the per-connection watchdog window. The connection is
	// cancelled and control returns to the reconnect loop if no SSE line arrives
	// within this window. The timer resets on every line, including SSE comment
	// keepalives, so only a truly stalled BMC triggers it. 5 min covers
	// low-frequency streams while still recovering from a silent stall.
	idleTimeout = 5 * time.Minute
	// responseHeaderTimeout limits how long the HTTP transport waits for the
	// BMC to begin sending response headers after the request is delivered.
	responseHeaderTimeout = 30 * time.Second
)

// Collector manages per-BMC outbound SSE connections for MetricReport delivery.
// It implements manager.Runnable; controller-runtime's default treats plain
// Runnables as leader-only, so only the elected leader opens outbound SSE
// connections — the correct behaviour for a push-model metric stream.
type Collector struct {
	resolver   subscriptions.Resolver
	metricSink sink.MetricReportSink
	httpClient *http.Client
	log        logr.Logger

	// InitialStagger caps the random delay added before each BMC's first
	// connection attempt, spreading startup bursts across many goroutines.
	// Defaults to reconnectInitial (5s). Set to 0 in tests to avoid delays.
	InitialStagger time.Duration

	mu      sync.Mutex
	active  map[string]bmcEntry
	desired map[string]string // name → URL: what should be running
	nextGen uint64
	rootCtx context.Context // nil until Start; check instead of a started bool
	// ready is closed by Start once rootCtx is set, so tests can synchronise
	// without a sleep.
	ready chan struct{}
}

type bmcEntry struct {
	cancel     context.CancelFunc
	generation uint64
	sseURL     string
	// done is closed by the goroutine when it exits. startLocked waits for it
	// before launching the replacement so two goroutines never publish for the
	// same BMC simultaneously.
	done chan struct{}
}

// NewCollector creates a Collector. Call Start before SetBMC/RemoveBMC.
func NewCollector(resolver subscriptions.Resolver, metricSink sink.MetricReportSink, insecureTLS bool, log logr.Logger) *Collector {
	return &Collector{
		resolver:   resolver,
		metricSink: metricSink,
		httpClient: &http.Client{
			Transport: subscriptions.NewBMCTransport(insecureTLS, responseHeaderTimeout),
			// No timeout: SSE connections are indefinite. Context cancellation
			// via RemoveBMC or manager shutdown terminates the request.
			Timeout: 0,
		},
		log:            log,
		active:         make(map[string]bmcEntry),
		desired:        make(map[string]string),
		ready:          make(chan struct{}),
		InitialStagger: reconnectInitial,
	}
}

// Start holds the root context until it is cancelled (manager shutdown).
// Implements manager.Runnable.
func (c *Collector) Start(ctx context.Context) error {
	c.mu.Lock()
	c.rootCtx = ctx
	for name, url := range c.desired {
		c.startLocked(name, url)
	}
	// Close ready while still holding the mutex so that no goroutine can
	// observe rootCtx != nil (via SetBMC) before Ready() signals.
	close(c.ready)
	c.mu.Unlock()

	<-ctx.Done()

	// Cancel all active SSE goroutines and wait for them to exit.
	c.mu.Lock()
	dones := make([]chan struct{}, 0, len(c.active))
	for _, e := range c.active {
		e.cancel()
		dones = append(dones, e.done)
	}
	c.active = make(map[string]bmcEntry)
	c.mu.Unlock()

	for _, d := range dones {
		<-d
	}
	return nil
}

// Ready returns a channel that is closed once Start has initialised the
// collector and SetBMC calls are safe to make.
func (c *Collector) Ready() <-chan struct{} {
	return c.ready
}

// SetBMC starts a persistent SSE connection for bmcName at sseURL if one is
// not already running, or restarts it when the URL has changed (e.g. after a
// firmware update). If called before Start, the desired state is recorded and
// replayed when Start fires so no reconcile calls are silently dropped.
func (c *Collector) SetBMC(bmcName, sseURL string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.desired[bmcName] = sseURL
	if c.rootCtx != nil {
		c.startLocked(bmcName, sseURL)
	}
}

// startLocked launches or restarts the SSE goroutine for bmcName. Must be
// called with c.mu held and c.rootCtx set.
func (c *Collector) startLocked(bmcName, sseURL string) {
	var oldDone chan struct{}
	if e, ok := c.active[bmcName]; ok {
		if e.sseURL == sseURL {
			return // already running at this URL, no-op
		}
		e.cancel()
		oldDone = e.done
		delete(c.active, bmcName)
	}

	ctx, cancel := context.WithCancel(c.rootCtx)
	gen := c.nextGen
	c.nextGen++
	done := make(chan struct{})
	c.active[bmcName] = bmcEntry{cancel: cancel, generation: gen, sseURL: sseURL, done: done}

	go func() {
		// If a prior goroutine for this BMC is still winding down, wait for it
		// before starting so two goroutines never publish metrics simultaneously.
		if oldDone != nil {
			<-oldDone
		}
		// Stagger initial connect across [0, InitialStagger) to spread the
		// connection burst when the reconciler enqueues many BMCs at startup.
		if c.InitialStagger > 0 {
			stagger := time.Duration(rand.Int63n(int64(c.InitialStagger) + 1))
			select {
			case <-ctx.Done():
				close(done)
				c.mu.Lock()
				if e, ok := c.active[bmcName]; ok && e.generation == gen {
					delete(c.active, bmcName)
				}
				c.mu.Unlock()
				return
			case <-time.After(stagger):
			}
		}
		c.runSSE(ctx, bmcName, sseURL)
		close(done)
		c.mu.Lock()
		// Only delete our own entry; a concurrent SetBMC after RemoveBMC could
		// have already inserted a new entry for the same BMC.
		if e, ok := c.active[bmcName]; ok && e.generation == gen {
			delete(c.active, bmcName)
		}
		c.mu.Unlock()
	}()
}

// ActiveURL returns the SSE URL of the currently-running goroutine for
// bmcName, or "" if no goroutine is active.
func (c *Collector) ActiveURL(bmcName string) string {
	c.mu.Lock()
	defer c.mu.Unlock()
	if e, ok := c.active[bmcName]; ok {
		return e.sseURL
	}
	return ""
}

// RemoveBMC cancels the SSE goroutine for bmcName and waits for it to exit.
// Removes the desired entry so the goroutine is not started again on the next
// SetBMC call. Safe to call when no goroutine is running (no-op).
func (c *Collector) RemoveBMC(bmcName string) {
	c.mu.Lock()
	delete(c.desired, bmcName)
	e, ok := c.active[bmcName]
	if ok {
		e.cancel()
		delete(c.active, bmcName)
	}
	c.mu.Unlock()

	if ok {
		<-e.done // wait outside lock to avoid deadlock with goroutine cleanup
	}
}

// runSSE is the per-BMC reconnect loop. Backoff: 5s initial, ×2, 5min cap.
// Connections that stay alive longer than stableThreshold reset the backoff.
func (c *Collector) runSSE(ctx context.Context, bmcName, sseURL string) {
	log := c.log.WithValues("bmc", bmcName)
	backoff := reconnectInitial

	for {
		if ctx.Err() != nil {
			return
		}

		start := time.Now()
		err := c.connectAndStream(ctx, bmcName, sseURL)
		elapsed := time.Since(start)

		if ctx.Err() != nil {
			return
		}

		if elapsed >= stableThreshold {
			backoff = reconnectInitial
		}

		if err != nil {
			log.V(1).Info("SSE connection ended, reconnecting", "err", err, "backoff", backoff)
		}

		// Add ±25% jitter so 1000 goroutines don't all reconnect at the same instant.
		jitter := time.Duration(rand.Int63n(int64(backoff)/2+1)) - backoff/4
		sleep := backoff + jitter
		sleep = max(sleep, reconnectInitial/2)
		timer := time.NewTimer(sleep)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}

		backoff *= 2
		if backoff > reconnectMax {
			backoff = reconnectMax
		}
	}
}

// connectAndStream opens one SSE connection, reads frames until the connection
// breaks or ctx is cancelled, and publishes samples. It resolves credentials
// fresh on each call so credential rotation is handled automatically.
func (c *Collector) connectAndStream(ctx context.Context, bmcName, sseURL string) error {
	resolved, err := c.resolver.Resolve(ctx, bmcName)
	if err != nil {
		return fmt.Errorf("resolve credentials: %w", err)
	}

	// connCtx is a per-connection child context. The idle watchdog inside
	// readSSEFrames cancels it (with a descriptive cause) when no SSE line
	// arrives within idleTimeout. Using a child context keeps the outer
	// reconnect-loop context intact so runSSE can reconnect immediately.
	connCtx, connCancel := context.WithCancelCause(ctx)
	defer connCancel(nil)

	req, err := http.NewRequestWithContext(connCtx, http.MethodGet, sseURL, nil)
	if err != nil {
		return fmt.Errorf("build SSE request: %w", err)
	}
	req.Header.Set("Accept", "text/event-stream")
	req.SetBasicAuth(resolved.Username, resolved.Password)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("SSE connect: %w", err)
	}
	defer func() {
		// Drain the body before close so the TCP connection can be reused by
		// the HTTP transport on the next reconnect attempt. The limit prevents
		// a misbehaving BMC from stalling the reconnect loop indefinitely.
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, maxDrainBytes))
		_ = resp.Body.Close()
	}()

	if resp.StatusCode != http.StatusOK {
		// Read a short snippet for diagnostics, then discard the rest (handled
		// by the deferred drain above).
		snippet := make([]byte, errorBodyLimit)
		n, _ := io.ReadFull(resp.Body, snippet)
		return fmt.Errorf("SSE connect: HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(snippet[:n])))
	}

	c.log.V(1).Info("SSE connection established", "bmc", bmcName, "url", sseURL)
	streamErr := c.readSSEFrames(connCtx, connCancel, bmcName, resp)
	// If the idle watchdog fired (connCtx cancelled for a reason unrelated to
	// the outer ctx), surface the watchdog cause for an informative log entry.
	if cause := context.Cause(connCtx); cause != nil && ctx.Err() == nil {
		return cause
	}
	return streamErr
}

// publishFrame parses a MetricReport JSON body and publishes its samples to
// the sink. Extracted to avoid duplicating the parse-and-publish block between
// the normal frame path and the partial-frame-on-close path.
func (c *Collector) publishFrame(ctx context.Context, bmcName, data string) {
	samples, err := events.ParseMetricReportSamples([]byte(data), c.log)
	if err != nil {
		c.log.V(2).Info("SSE frame parse error", "bmc", bmcName, "err", err)
		return
	}
	if len(samples) == 0 {
		return
	}
	if err := c.metricSink.PublishSamples(ctx, bmcName, samples); err != nil {
		c.log.Error(err, "SSE metric sink publish failed", "bmc", bmcName)
	}
}

// readSSEFrames reads SSE frames line-by-line from the response body.
// Each frame is one or more "data: <json>" lines followed by a blank line.
// The full assembled data value is decoded as a MetricReport JSON body.
// cancel is the CancelCauseFunc for connCtx (the request context); the idle
// watchdog calls it when no SSE line arrives within idleTimeout.
func (c *Collector) readSSEFrames(ctx context.Context, cancel context.CancelCauseFunc, bmcName string, resp *http.Response) error {
	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, maxTokenSize), maxTokenSize)
	var dataBuf strings.Builder
	oversized := false

	// Idle watchdog: reset on every received line (including SSE keepalive
	// comments). If no line arrives within idleTimeout the watchdog cancels
	// connCtx, which breaks the scanner read and returns control to the
	// reconnect loop in runSSE.
	idle := time.AfterFunc(idleTimeout, func() {
		cancel(fmt.Errorf("SSE idle timeout: no line from BMC %s in %s", bmcName, idleTimeout))
	})
	defer idle.Stop()

	for scanner.Scan() {
		idle.Reset(idleTimeout)

		line := scanner.Text()

		switch {
		case strings.HasPrefix(line, "data:"):
			// Per SSE spec §9.2.6 the field value starts after the colon; a
			// single U+0020 space immediately following the colon is stripped.
			value := strings.TrimPrefix(line, "data:")
			if len(value) > 0 && value[0] == ' ' {
				value = value[1:]
			}
			if !oversized {
				// Account for the '\n' separator only when the frame already
				// has data; the first data: line in an empty frame has no
				// preceding newline, so +1 would incorrectly reject a frame
				// that is exactly maxFrameSize bytes long.
				sep := 0
				if dataBuf.Len() > 0 {
					sep = 1
				}
				if dataBuf.Len()+len(value)+sep > maxFrameSize {
					c.log.V(1).Info("SSE frame too large, dropping", "bmc", bmcName, "size", dataBuf.Len()+len(value))
					oversized = true
					dataBuf.Reset()
				} else {
					if dataBuf.Len() > 0 {
						dataBuf.WriteByte('\n')
					}
					dataBuf.WriteString(value)
				}
			}
		case line == "":
			if oversized {
				oversized = false
				continue
			}
			if dataBuf.Len() == 0 {
				continue
			}
			data := strings.TrimSpace(dataBuf.String())
			dataBuf.Reset()
			if data != "" {
				c.publishFrame(ctx, bmcName, data)
			}
			// Lines starting with ":" are SSE comments; other prefixes (event:,
			// id:, retry:) are ignored — we only consume MetricReport data frames.
		}
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("SSE read: %w", err)
	}

	// If the server closed the connection mid-frame (no trailing blank line),
	// attempt to process any buffered data rather than silently losing it.
	if dataBuf.Len() > 0 && !oversized {
		data := strings.TrimSpace(dataBuf.String())
		if data != "" {
			c.log.V(1).Info("SSE stream closed mid-frame; attempting to parse partial frame",
				"bmc", bmcName, "bytes", len(data))
			c.publishFrame(ctx, bmcName, data)
		}
	}

	return fmt.Errorf("SSE stream closed by server")
}
