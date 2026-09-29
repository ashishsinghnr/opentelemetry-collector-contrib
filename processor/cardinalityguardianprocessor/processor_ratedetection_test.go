// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package cardinalityguardianprocessor

import (
	"context"
	"fmt"
	"math/rand/v2"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/processor/processortest"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// rateTest drives the "label" attribute of metric "metric" through epochs.
type rateTest struct{ p *cardinalityProcessor }

// newRateTest returns a rateTest with the given global limit.
func newRateTest(t *testing.T, limit int) *rateTest {
	t.Helper()
	return newRateTestWithConfig(t, &Config{MaxCardinalityDeltaPerEpoch: limit, EpochDurationSeconds: 300})
}

// newRateTestWithConfig runs one warm-up epoch, so scenarios start in the
// tracker's second epoch and do not depend on first-epoch behavior (#50406).
func newRateTestWithConfig(t *testing.T, cfg *Config) *rateTest {
	t.Helper()
	set := processortest.NewNopSettings(component.MustNewType("cardinality_guardian"))
	proc, err := newCardinalityProcessor(t.Context(), cfg, set, new(consumertest.MetricsSink))
	require.NoError(t, err)
	r := &rateTest{p: proc.(*cardinalityProcessor)}
	r.epoch("warmup", 1)
	return r
}

// feed sends n distinct values; the same prefix yields the same values. It
// returns the number dropped.
func (r *rateTest) feed(prefix string, n int) int {
	drops := 0
	for i := range n {
		if r.feedValue(fmt.Sprintf("%s-%d", prefix, i)) {
			drops++
		}
	}
	return drops
}

func (r *rateTest) feedValue(v string) bool {
	return r.p.shouldDrop("metric", "label", pcommon.NewValueStr(v))
}

// epoch feeds n values and rotates; n == 0 runs an idle epoch.
func (r *rateTest) epoch(prefix string, n int) int {
	drops := r.feed(prefix, n)
	r.p.rotate()
	return drops
}

// TestRateDetection_SustainedGrowthAcrossEpochs asserts a label growing at a
// constant rate is enforced in every active epoch, including after an idle one.
// Regression for #50403, where the delta was 0 from epoch 2 under constant growth.
func TestRateDetection_SustainedGrowthAcrossEpochs(t *testing.T) {
	r := newRateTest(t, 10)

	// A distinct prefix per epoch makes every value new.
	for i, idleAfter := range []bool{false, true, false, false} {
		assert.Positivef(t, r.epoch(fmt.Sprintf("growth%d", i), 100),
			"active epoch %d adds 100 new values and must be enforced", i+1)
		if idleAfter {
			r.epoch("", 0)
		}
	}
}

// TestRateDetection_MetricOverrideLimit asserts the metric's override, not the
// global limit, bounds the epoch.
func TestRateDetection_MetricOverrideLimit(t *testing.T) {
	const override, perEpoch = 10, 500
	r := newRateTestWithConfig(t, &Config{
		MaxCardinalityDeltaPerEpoch: 1000,
		EpochDurationSeconds:        300,
		MetricOverrides:             map[string]int{"metric": override},
	})

	kept := perEpoch - r.epoch("growth", perEpoch)
	assert.Equal(t, override, kept, "the override, not the global limit, must bound the epoch")
}

// TestRateDetection_LargeLimitOvershootBounded asserts a limit crossed after an
// epoch's first estimateInterval inserts is overshot by at most estimateInterval.
func TestRateDetection_LargeLimitOvershootBounded(t *testing.T) {
	const limit, perEpoch = 100, 500
	r := newRateTest(t, limit)

	for i := range 6 {
		kept := perEpoch - r.epoch(fmt.Sprintf("growth%d", i), perEpoch)
		assert.GreaterOrEqualf(t, kept, limit, "epoch %d: kept %d, below limit %d", i+1, kept, limit)
		assert.LessOrEqualf(t, kept, limit+estimateInterval,
			"epoch %d: kept %d, overshooting limit %d by more than estimateInterval", i+1, kept, limit)
	}
}

// TestRateDetection_SlowLeakOnLargeLabelEnforced asserts a leak that adds fewer
// than estimateInterval values per epoch to a large label is enforced: only the
// refresh in an epoch's first estimateInterval inserts can catch it.
func TestRateDetection_SlowLeakOnLargeLabelEnforced(t *testing.T) {
	const limit, perEpoch = 10, 40
	// largeLabel is past the ~7,600 values at which a sketch goes dense;
	// denseSlack allows for the dense sketch's estimate noise.
	const largeLabel, denseSlack = 20000, 5
	r := newRateTest(t, limit)

	r.epoch("base", largeLabel)
	for i := range 3 {
		kept := perEpoch - r.epoch(fmt.Sprintf("growth%d", i), perEpoch)
		assert.LessOrEqualf(t, kept, limit+denseSlack, "epoch %d: kept %d of %d new values", i+1, kept, perEpoch)
	}
}

// TestRateDetection_PartiallyRecurringLabelNotPenalized asserts a stable label
// whose values do not all appear every epoch, as with long-tail routes or pods
// with intermittent traffic, is not penalized. A baseline of only the previous
// epoch would count every value that skipped it as new.
func TestRateDetection_PartiallyRecurringLabelNotPenalized(t *testing.T) {
	const values = 2000
	r := newRateTest(t, 100)
	rng := rand.New(rand.NewPCG(1, 2))

	require.Positive(t, r.epoch("route", values), "the label's growth to its values must be enforced")
	for epoch := 1; epoch <= 5; epoch++ {
		drops := 0
		for i := range values {
			if rng.Float64() < 0.7 && r.feedValue(fmt.Sprintf("route-%d", i)) {
				drops++
			}
		}
		r.p.rotate()
		assert.Zerof(t, drops, "epoch %d: ~70%% of known values recurred; none are new", epoch)
	}
}

// TestRateDetection_StableLabelReturningAfterIdleNotPenalized asserts a stable
// label that skips one epoch is not penalized when it returns. Two idle epochs
// would evict the tracker.
func TestRateDetection_StableLabelReturningAfterIdleNotPenalized(t *testing.T) {
	r := newRateTest(t, 10)

	require.Positive(t, r.epoch("host", 500), "the label's growth to 500 values must be enforced")
	r.epoch("", 0)
	assert.Zero(t, r.epoch("host", 500), "the same 500 values returned after an idle epoch")
}

// gaugeTest returns a processor that reports top offenders without enforcing,
// after a warm-up epoch (#50406), and a func returning the exported gauge's
// value for a label key, or false when the label is absent from it.
func gaugeTest(t *testing.T) (*cardinalityProcessor, func(labelKey string) (int64, bool)) {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	// t.Context is canceled before cleanups run, so detach from its cancellation.
	t.Cleanup(func() { assert.NoError(t, provider.Shutdown(context.WithoutCancel(t.Context()))) })

	set := processortest.NewNopSettings(component.MustNewType("cardinality_guardian"))
	set.MeterProvider = provider
	cfg := &Config{
		MaxCardinalityDeltaPerEpoch: 1_000_000,
		EpochDurationSeconds:        300,
		TopOffendersCount:           3,
		EnforcementMode:             EnforcementTagOnly,
	}
	proc, err := newCardinalityProcessor(t.Context(), cfg, set, new(consumertest.MetricsSink))
	require.NoError(t, err)
	p := proc.(*cardinalityProcessor)
	p.shouldDrop("metric", "leaky_label", pcommon.NewValueStr("warmup"))
	p.rotate()

	reported := func(labelKey string) (int64, bool) {
		var collected metricdata.ResourceMetrics
		require.NoError(t, reader.Collect(t.Context(), &collected))
		for _, sm := range collected.ScopeMetrics {
			for _, m := range sm.Metrics {
				if m.Name != "otelcol_processor_cardinality_top.offenders" {
					continue
				}
				gauge, ok := m.Data.(metricdata.Gauge[int64])
				require.True(t, ok, "top.offenders must be a Gauge[int64]")
				for _, dp := range gauge.DataPoints {
					if lk, ok := dp.Attributes.Value(attribute.Key("label_key")); ok && lk.AsString() == labelKey {
						return dp.Value, true
					}
				}
			}
		}
		return 0, false
	}
	return p, reported
}

// TestRateDetection_TopOffendersReportsSustainedGrowth asserts the gauge keeps
// reporting a steady leak every epoch.
func TestRateDetection_TopOffendersReportsSustainedGrowth(t *testing.T) {
	const perEpoch = 100
	p, reported := gaugeTest(t)

	for epoch := 1; epoch <= 3; epoch++ {
		for i := range perEpoch {
			p.shouldDrop("metric", "leaky_label", pcommon.NewValueStr(fmt.Sprintf("e%d-%d", epoch, i)))
		}
		p.rotate()
		got, ok := reported("leaky_label")
		require.Truef(t, ok, "epoch %d: the growing label must be reported", epoch)
		// The delta is an HLL++ estimate, so allow 10%.
		assert.InDeltaf(t, perEpoch, got, perEpoch*0.1,
			"epoch %d: the gauge must report the ~%d values added this epoch", epoch, perEpoch)
	}
}

// TestRateDetection_TopOffendersClearsWhenGrowthStops asserts the gauge reports
// the current epoch rather than repeating the last epoch that had growth.
func TestRateDetection_TopOffendersClearsWhenGrowthStops(t *testing.T) {
	p, reported := gaugeTest(t)

	for epoch := 1; epoch <= 3; epoch++ {
		for i := range 300 {
			p.shouldDrop("metric", "leaky_label", pcommon.NewValueStr(fmt.Sprintf("value-%d", i)))
		}
		p.rotate()
		_, ok := reported("leaky_label")
		if epoch == 1 {
			require.True(t, ok, "epoch 1: the growing label must be reported")
			continue
		}
		assert.Falsef(t, ok, "epoch %d: no label grew, so the gauge must clear", epoch)
	}
}
