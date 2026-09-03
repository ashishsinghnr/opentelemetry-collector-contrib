// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package cardinalityguardianprocessor

import (
	"fmt"
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

// TestRateDetection_SustainedGrowthAcrossEpochs asserts that a label growing at a
// constant rate is detected in every epoch, not just the first.
//
// README.md documents the intended behavior as rate-based: "A label with 50K
// stable values is fine. A label that grew by 100 in the last epoch is a problem."
//
// The delta is computed from cachedCurr/cachedPrev. Both are per-epoch estimates
// (current is replaced by a fresh sketch in rotate()), so their difference is the
// change in rate — acceleration — rather than the rate itself. For a constant R
// new unique values per epoch, curr == prev == R, so delta == 0 from epoch 2
// onward and shouldDrop returns false at the `currCount <= prevCount` guard
// before the configured limit is ever consulted. No limit value changes this.
func TestRateDetection_SustainedGrowthAcrossEpochs(t *testing.T) {
	const (
		limit          = 10
		newPerEpoch    = 100 // deliberately far above limit
		epochsObserved = 3
	)

	cfg := &Config{
		MaxCardinalityDeltaPerEpoch: limit,
		EpochDurationSeconds:        300, // long: rotation is driven manually
	}

	next := new(consumertest.MetricsSink)
	set := processortest.NewNopSettings(component.MustNewType("cardinality_guardian"))
	proc, err := newCardinalityProcessor(t.Context(), cfg, set, next)
	require.NoError(t, err)
	p := proc.(*cardinalityProcessor)

	// unique feeds a distinct value every call, so cardinality grows by exactly
	// newPerEpoch per epoch and never repeats across epochs.
	seq := 0
	unique := func() pcommon.Value {
		seq++
		return pcommon.NewValueStr(fmt.Sprintf("value-%d", seq))
	}

	dropsPerEpoch := make([]int, 0, epochsObserved)
	for epoch := 1; epoch <= epochsObserved; epoch++ {
		drops := 0
		for range newPerEpoch {
			if p.shouldDrop("metric", "leaky_label", unique()) {
				drops++
			}
		}
		dropsPerEpoch = append(dropsPerEpoch, drops)
		p.rotate()
	}

	// Epoch 1 is expected to drop: cachedPrev == 0, so delta == newPerEpoch > limit.
	assert.Positive(t, dropsPerEpoch[0],
		"epoch 1: %d new unique values against limit %d should be detected",
		newPerEpoch, limit)

	// Epochs 2+ carry the same growth rate and must also be detected. On the
	// current implementation these are 0 because delta collapses to acceleration.
	for epoch := 2; epoch <= epochsObserved; epoch++ {
		assert.Positivef(t, dropsPerEpoch[epoch-1],
			"epoch %d: growth continued at %d new unique values per epoch (limit %d), "+
				"but nothing was detected — delta measured acceleration, not rate. "+
				"drops by epoch: %v",
			epoch, newPerEpoch, limit, dropsPerEpoch)
	}
}

// TestRateDetection_TopOffendersReportsSustainedGrowth covers the reporting
// path, which reads the same cachedCurr/cachedPrev pair as shouldDrop via
// collectShardDeltas and applies the same `curr <= prev` guard.
//
// TestTopOffenders already covers a single rotation, where cachedPrev is still
// zero and the delta is therefore correct by construction. The gap this closes
// is the second and later epochs: with per-epoch estimates the delta collapses
// to acceleration, so a label leaking at a constant rate drops out of the
// offender gauge entirely after the first rotation — the operator sees an empty
// or misleading top-N exactly while the leak continues.
//
// enforcement_mode is tag_only and the limit is high so nothing is stripped;
// this isolates reporting from enforcement.
func TestRateDetection_TopOffendersReportsSustainedGrowth(t *testing.T) {
	const (
		newPerEpoch = 100
		epochs      = 3
	)

	cfg := &Config{
		MaxCardinalityDeltaPerEpoch: 10_000, // high: nothing should be stripped
		EpochDurationSeconds:        300,
		TopOffendersCount:           3,
		EnforcementMode:             EnforcementTagOnly,
	}

	reader := sdkmetric.NewManualReader()
	sdkProvider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	defer func() {
		if err := sdkProvider.Shutdown(t.Context()); err != nil {
			t.Errorf("sdk provider shutdown: %v", err)
		}
	}()

	set := processortest.NewNopSettings(component.MustNewType("cardinality_guardian"))
	set.MeterProvider = sdkProvider

	next := new(consumertest.MetricsSink)
	proc, err := newCardinalityProcessor(t.Context(), cfg, set, next)
	require.NoError(t, err)
	p := proc.(*cardinalityProcessor)

	seq := 0
	unique := func() pcommon.Value {
		seq++
		return pcommon.NewValueStr(fmt.Sprintf("value-%d", seq))
	}

	// deltaFor returns the reported delta for the leaky label after a rotation,
	// or 0 when the label is absent from the gauge entirely.
	deltaFor := func(t *testing.T, labelKey string) int64 {
		t.Helper()
		var collected metricdata.ResourceMetrics
		require.NoError(t, reader.Collect(t.Context(), &collected))

		topMetric := findMetricByName(t, collected, "otelcol_processor_cardinality_top.offenders")
		gaugeData, ok := topMetric.Data.(metricdata.Gauge[int64])
		require.True(t, ok, "top.offenders must be a Gauge[int64]")

		for _, dp := range gaugeData.DataPoints {
			lk, lkOk := dp.Attributes.Value(attribute.Key("label_key"))
			if lkOk && lk.AsString() == labelKey {
				return dp.Value
			}
		}
		return 0
	}

	reported := make([]int64, 0, epochs)
	for epoch := 1; epoch <= epochs; epoch++ {
		for range newPerEpoch {
			p.shouldDrop("metric", "leaky_label", unique())
		}
		p.rotate()
		reported = append(reported, deltaFor(t, "leaky_label"))
	}

	// Epoch 1 is correct even on the buggy implementation (cachedPrev == 0).
	assert.Positive(t, reported[0],
		"epoch 1: leaky_label should appear in top.offenders")

	// Epochs 2+ must keep reporting the ongoing growth. The delta is an HLL++
	// estimate (~0.81% standard error at p=14), so assert the order of magnitude
	// rather than an exact count.
	for epoch := 2; epoch <= epochs; epoch++ {
		got := reported[epoch-1]
		assert.Positivef(t, got,
			"epoch %d: leaky_label grew by %d more unique values but was reported "+
				"with delta %d — top.offenders measured acceleration, not rate. "+
				"reported deltas by epoch: %v",
			epoch, newPerEpoch, got, reported)
		assert.InDeltaf(t, newPerEpoch, got, float64(newPerEpoch)*0.1,
			"epoch %d: reported delta %d should be within 10%% of the %d new "+
				"unique values added this epoch. reported deltas by epoch: %v",
			epoch, got, newPerEpoch, reported)
	}
}

// TestRateDetection_StableHighCardinalityNotPenalized is the other half of the
// documented contract and guards against overcorrecting the fix above: a label
// that reached a high count but stopped growing must not be dropped.
//
// A cumulative/lifetime sketch keeps cachedCurr saturated at the total ever
// seen, so once growth stops the per-epoch delta must fall back to ~0.
func TestRateDetection_StableHighCardinalityNotPenalized(t *testing.T) {
	const (
		limit        = 10
		distinctSeen = 500
	)

	cfg := &Config{
		MaxCardinalityDeltaPerEpoch: limit,
		EpochDurationSeconds:        300,
	}

	next := new(consumertest.MetricsSink)
	set := processortest.NewNopSettings(component.MustNewType("cardinality_guardian"))
	proc, err := newCardinalityProcessor(t.Context(), cfg, set, next)
	require.NoError(t, err)
	p := proc.(*cardinalityProcessor)

	values := make([]pcommon.Value, distinctSeen)
	for i := range values {
		values[i] = pcommon.NewValueStr(fmt.Sprintf("stable-%d", i))
	}

	// Epoch 1: the label reaches its full cardinality (growth happens here).
	for _, v := range values {
		p.shouldDrop("metric", "stable_label", v)
	}
	p.rotate()

	// Epochs 2 and 3: the same value set recurs, adding no new unique values.
	for epoch := 2; epoch <= 3; epoch++ {
		drops := 0
		for _, v := range values {
			if p.shouldDrop("metric", "stable_label", v) {
				drops++
			}
		}
		assert.Zerof(t, drops,
			"epoch %d: label held steady at %d values with no new uniques; "+
				"a stable high-cardinality label must not be penalized",
			epoch, distinctSeen)
		p.rotate()
	}
}

// TestRateDetection_TopOffendersClearsWhenGrowthStops asserts the gauge reports
// the current epoch, not the last epoch that had growth.
//
// publishTopOffenders used to return early on an empty buffer, leaving the
// previous epoch's entries in place. That became reachable once deltas correctly
// fall to zero for a label that stopped growing.
func TestRateDetection_TopOffendersClearsWhenGrowthStops(t *testing.T) {
	const distinct = 300

	cfg := &Config{
		MaxCardinalityDeltaPerEpoch: 1_000_000, // high: isolate reporting from enforcement
		EpochDurationSeconds:        300,
		TopOffendersCount:           3,
		EnforcementMode:             EnforcementTagOnly,
	}

	next := new(consumertest.MetricsSink)
	set := processortest.NewNopSettings(component.MustNewType("cardinality_guardian"))
	proc, err := newCardinalityProcessor(t.Context(), cfg, set, next)
	require.NoError(t, err)
	p := proc.(*cardinalityProcessor)

	values := make([]pcommon.Value, distinct)
	for i := range values {
		values[i] = pcommon.NewValueStr(fmt.Sprintf("value-%d", i))
	}

	// Epoch 1 grows to `distinct` unique values.
	for _, v := range values {
		p.shouldDrop("metric", "leaky_label", v)
	}
	p.rotate()

	p.topOffendersMu.RLock()
	afterGrowth := len(p.topOffenders)
	p.topOffendersMu.RUnlock()
	require.Positive(t, afterGrowth, "epoch 1: growing label should be reported")

	// Epochs 2-3 replay the same values, so no new uniques are added.
	for epoch := 2; epoch <= 3; epoch++ {
		for _, v := range values {
			p.shouldDrop("metric", "leaky_label", v)
		}
		p.rotate()

		p.topOffendersMu.RLock()
		got := len(p.topOffenders)
		p.topOffendersMu.RUnlock()
		assert.Zerof(t, got,
			"epoch %d: no label grew, so the gauge must clear rather than repeat "+
				"the last epoch that had growth", epoch)
	}
}

// TestRateDetection_EnforcementIsExactBelowDenseThreshold pins the accuracy of
// the refresh schedule in insert().
//
// While the cumulative estimate is below denseThreshold, Estimate is cheap and
// runs on every insert, so the limit is enforced exactly. Above it the estimate
// lags by up to estimateInterval-1 values, which is acceptable because a label
// that large has breached any usable limit by orders of magnitude. The lag must
// stay bounded rather than grow.
func TestRateDetection_EnforcementIsExactBelowDenseThreshold(t *testing.T) {
	const (
		limit    = 10
		perEpoch = 500
		epochs   = 12
	)

	cfg := &Config{
		MaxCardinalityDeltaPerEpoch: limit,
		EpochDurationSeconds:        300,
	}

	next := new(consumertest.MetricsSink)
	set := processortest.NewNopSettings(component.MustNewType("cardinality_guardian"))
	proc, err := newCardinalityProcessor(t.Context(), cfg, set, next)
	require.NoError(t, err)
	p := proc.(*cardinalityProcessor)

	shard := p.getShard("metric")
	key := trackerKey{metricName: "metric", attrKey: "leaky_label"}

	seq := 0
	kept := make([]int, 0, epochs)
	for range epochs {
		epochKept := 0
		for range perEpoch {
			seq++
			if !p.shouldDrop("metric", "leaky_label", pcommon.NewValueStr(fmt.Sprintf("value-%d", seq))) {
				epochKept++
			}
		}

		shard.mu.RLock()
		tr := shard.trackers[key]
		shard.mu.RUnlock()
		require.NotNil(t, tr)
		tr.mu.Lock()
		cumulative := tr.cachedCurr
		tr.mu.Unlock()

		if cumulative < denseThreshold {
			assert.Equalf(t, limit, epochKept,
				"cumulative estimate %d is below denseThreshold, so the limit must be "+
					"enforced exactly; kept %d", cumulative, epochKept)
		}
		assert.LessOrEqualf(t, epochKept, limit+estimateInterval,
			"kept %d against limit %d: lag must stay within estimateInterval (%d)",
			epochKept, limit, estimateInterval)

		kept = append(kept, epochKept)
		p.rotate()
	}

	// A steady leak must give a steady lag; a growing one would mean cachedPrev
	// is inheriting drift rather than being recomputed at rotation.
	assert.Equalf(t, kept[len(kept)-2], kept[len(kept)-1],
		"lag must not grow across epochs. kept by epoch: %v", kept)
}
