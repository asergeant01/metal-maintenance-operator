// SPDX-FileCopyrightText: SAP SE or an SAP affiliate company and IronCore contributors
// SPDX-License-Identifier: Apache-2.0

package ssestream_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	"github.com/ironcore-dev/metal-maintenance-operator/internal/telemetry/sink"
	"github.com/ironcore-dev/metal-maintenance-operator/internal/telemetry/ssestream"
	"github.com/ironcore-dev/metal-maintenance-operator/internal/telemetry/subscriptions"
	metalv1alpha1 "github.com/ironcore-dev/metal-operator/api/v1alpha1"
)

// fakeResolver always returns a fixed credential pair.
type fakeResolver struct {
	username, password string
}

func (r *fakeResolver) Resolve(_ context.Context, _ string) (*subscriptions.Resolved, error) {
	return &subscriptions.Resolved{
		BMC:      &metalv1alpha1.BMC{},
		Username: r.username,
		Password: r.password,
	}, nil
}

// recordingMetricSink captures PublishSamples calls.
type recordingMetricSink struct {
	mu    sync.Mutex
	calls []publishCall
}

type publishCall struct {
	bmcName string
	samples []sink.Sample
}

func (s *recordingMetricSink) PublishSamples(_ context.Context, name string, samples []sink.Sample) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls = append(s.calls, publishCall{bmcName: name, samples: samples})
	return nil
}

func (s *recordingMetricSink) Forget(_ string) {}

func (s *recordingMetricSink) all() []publishCall {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]publishCall, len(s.calls))
	copy(out, s.calls)
	return out
}

// startCollector starts coll in the background and returns a cancel func.
// It blocks until Start has set rootCtx so that SetBMC calls are safe.
// InitialStagger is set to 0 so tests connect immediately without delay.
func startCollector(t *testing.T, coll *ssestream.Collector) context.CancelFunc {
	t.Helper()
	coll.InitialStagger = 0
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		_ = coll.Start(ctx)
	}()
	<-coll.Ready()
	return cancel
}

// TestSSEFrameParsing serves a two-frame SSE stream and asserts that samples
// from both frames reach the sink with the expected MetricID values.
func TestSSEFrameParsing(t *testing.T) {
	mSink := &recordingMetricSink{}
	coll := ssestream.NewCollector(&fakeResolver{"u", "p"}, mSink, false, logr.Discard())
	cancel := startCollector(t, coll)
	defer cancel()

	// Two MetricReport frames separated by a blank line.
	sseBody := `data: {"Id":"R1","MetricValues":[{"MetricId":"CPU","MetricValue":"42"}]}

data: {"Id":"R2","MetricValues":[{"MetricId":"MEM","MetricValue":"8"}]}

`
	done := make(chan struct{})
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = fmt.Fprint(w, sseBody)
		w.(http.Flusher).Flush()
		<-done
	}))
	defer ts.Close()
	defer close(done)

	coll.SetBMC("bmc-1", ts.URL)

	// Wait for both frames to be published.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if len(mSink.all()) >= 2 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()

	calls := mSink.all()
	if len(calls) < 2 {
		t.Fatalf("expected ≥2 publish calls, got %d", len(calls))
	}
	if calls[0].samples[0].MetricID != "CPU" {
		t.Errorf("frame1 MetricID: got %q, want CPU", calls[0].samples[0].MetricID)
	}
	if calls[1].samples[0].MetricID != "MEM" {
		t.Errorf("frame2 MetricID: got %q, want MEM", calls[1].samples[0].MetricID)
	}
}

// TestSetBMCNoopOnDuplicate verifies that calling SetBMC twice for the same
// BMC does not start a second goroutine (only one connection reaches the server).
func TestSetBMCNoopOnDuplicate(t *testing.T) {
	mSink := &recordingMetricSink{}
	coll := ssestream.NewCollector(&fakeResolver{"u", "p"}, mSink, false, logr.Discard())
	cancel := startCollector(t, coll)
	defer cancel()

	connections := make(chan struct{}, 10)
	done := make(chan struct{})
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		connections <- struct{}{}
		w.Header().Set("Content-Type", "text/event-stream")
		w.(http.Flusher).Flush()
		<-done
	}))
	defer ts.Close()
	defer close(done)

	coll.SetBMC("bmc-1", ts.URL)
	coll.SetBMC("bmc-1", ts.URL) // duplicate — must be no-op

	time.Sleep(100 * time.Millisecond)
	cancel()

	if len(connections) > 1 {
		t.Errorf("expected at most 1 connection, got %d", len(connections))
	}
}

// TestRemoveBMC_CancelsGoroutine verifies that RemoveBMC causes the SSE
// goroutine to exit without waiting for manager shutdown.
func TestRemoveBMC_CancelsGoroutine(t *testing.T) {
	mSink := &recordingMetricSink{}
	coll := ssestream.NewCollector(&fakeResolver{"u", "p"}, mSink, false, logr.Discard())
	cancel := startCollector(t, coll)
	defer cancel()

	connected := make(chan struct{})
	disconnected := make(chan struct{})
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.(http.Flusher).Flush()
		close(connected) // signal that the handler is running
		<-r.Context().Done()
		close(disconnected)
	}))
	defer ts.Close()

	coll.SetBMC("bmc-1", ts.URL)

	// Wait for the server handler to confirm the connection is established
	// before calling RemoveBMC, avoiding the race in a fixed sleep.
	select {
	case <-connected:
	case <-time.After(2 * time.Second):
		t.Fatal("SSE connection was not established within 2s")
	}

	coll.RemoveBMC("bmc-1")

	select {
	case <-disconnected:
		// success
	case <-time.After(2 * time.Second):
		t.Fatal("RemoveBMC did not disconnect the SSE goroutine within 2s")
	}
}

// TestCollector_NeedLeaderElection checks leader-election semantics. If the
// Collector implements manager.LeaderElectionRunnable it must return true
// (only the leader makes outbound SSE connections). If it does not implement
// the interface, controller-runtime's default is leader-only — also correct.
func TestCollector_NeedLeaderElection(t *testing.T) {
	coll := ssestream.NewCollector(&fakeResolver{}, &recordingMetricSink{}, false, logr.Discard())
	ler, ok := any(coll).(manager.LeaderElectionRunnable)
	if !ok {
		// Default (no interface) is leader-only — acceptable.
		return
	}
	if !ler.NeedLeaderElection() {
		t.Error("Collector.NeedLeaderElection should return true (leader-only)")
	}
}
