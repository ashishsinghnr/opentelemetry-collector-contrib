// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package cardinalityguardianprocessor // import "github.com/open-telemetry/opentelemetry-collector-contrib/processor/cardinalityguardianprocessor"

import (
	"context"
	"hash/maphash"
	"sync"
	"sync/atomic"
	"time"

	"github.com/axiomhq/hyperloglog"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/processor"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/cardinalityguardianprocessor/internal/metadata"
)

// staleSweepEpochs is how many consecutive zero-insert epochs a tracker must
// see before eviction. Two epochs so a tracker that fired at the tail of one
// epoch isn't reaped before traffic resumes.
const staleSweepEpochs = 2

// numShards must be a power of two so getShard reduces to a bitmask AND.
const numShards = 256

// overflowSentinel replaces the offending attribute value in
// EnforcementOverflowAttribute mode. The OTel SDK spec uses
// "otel.metric.overflow" as a boolean marker; we use a distinct value-side
// sentinel so consumers can distinguish "this dimension overflowed" from
// "the whole datapoint was marked overflow".
const overflowSentinel = "otel.cardinality_overflow"

// mustGetSketch returns a fresh HLL++ sketch (p=14 → ~0.81% standard error).
// The sketch starts sparse and grows with observed cardinality, converting to a
// dense ~16 KB representation at roughly 8k unique values; it is not a
// fixed-size allocation. Allocated directly because hyperloglog.Sketch lacks a
// safe Reset(), so pooling dirty sketches would corrupt estimates.
func mustGetSketch() *hyperloglog.Sketch {
	return hyperloglog.New14()
}

// trackerKey is a zero-allocation composite key for the trackers map.
// Using a struct key avoids the heap allocation that string concatenation
// (e.g. metricName + ":" + attrKey) would cause in the hot path: Go can hash
// a struct key inline without allocating a temporary string.
type trackerKey struct {
	metricName string
	attrKey    string
}

// estimateInterval is how often Sketch.Estimate() is recomputed once a sketch is
// large enough that the call is expensive. Power-of-two ⇒ bitmask check.
const estimateInterval = 64

// denseThreshold is the estimate above which Sketch.Estimate() is assumed to have
// switched to its dense representation. Measured against axiomhq/hyperloglog at
// p=14, the call stays under 400ns below ~7500 unique values and jumps to ~100µs
// by 8000, so tracker.insert stops refreshing on every insert above this bound.
const denseThreshold = 1 << 13

// tracker holds one HLL++ sketch for a (metric_name, label_key) pair plus a
// fine-grained mutex so the shard-level lock can be released before HLL work
// begins. Hot fields are at the front for cache-line locality.
type tracker struct {
	mu sync.Mutex
	// lifetime accumulates every value ever seen for this pair and is never
	// reset, so cachedCurr is cumulative and the delta against cachedPrev is the
	// number of unique values added during the epoch that just ended — a rate.
	// Two per-epoch sketches would instead give the change in rate, which is
	// zero under constant growth and hides sustained leaks.
	lifetime *hyperloglog.Sketch
	// cachedCurr is the cumulative estimate; cachedPrev is its value at the last
	// rotation. Their difference is the epoch delta.
	cachedCurr uint64
	cachedPrev uint64
	// insertCount resets each epoch; it drives idle detection and the refresh
	// interval in insert().
	insertCount uint64
	// idleEpochs counts consecutive zero-insert rotations; eviction at staleSweepEpochs.
	idleEpochs int
}

// insert feeds a pre-hashed value into the cumulative sketch via InsertHash —
// the []byte path would escape to the heap because the library's `hash`
// indirection blocks escape analysis.
//
// Estimate is refreshed every estimateInterval inserts, plus on every insert
// while the epoch's delta is still within estimateInterval of the limit. The
// second condition is what makes enforcement exact: it only holds in the narrow
// band just below a breach, so the extra calls are bounded, and without it a
// stale estimate would let up to estimateInterval-1 values through.
//
// The band is skipped once the sketch is past denseThreshold, where Estimate
// costs ~100µs instead of ~100ns. A label that large has already breached any
// usable limit by orders of magnitude, so per-insert precision buys nothing.
func (t *tracker) insert(hashVal, limit uint64) (curr, prev uint64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.lifetime.InsertHash(hashVal)
	t.insertCount++

	nearLimit := t.cachedCurr < denseThreshold &&
		t.cachedCurr+estimateInterval >= t.cachedPrev+limit
	if nearLimit || t.insertCount&(estimateInterval-1) == 0 {
		t.cachedCurr = t.lifetime.Estimate()
	}
	return t.cachedCurr, t.cachedPrev
}

// rotate closes out the epoch, returning its cardinality delta and whether the
// tracker received no inserts.
//
// The estimate is recomputed here rather than trusted from insert(), which skips
// refreshes between intervals and so can leave cachedCurr stale at an arbitrary
// epoch boundary. This is the only place the delta is computed, so enforcement
// and the top-offenders gauge cannot disagree.
func (t *tracker) rotate() (delta uint64, idle bool) {
	t.mu.Lock()
	defer t.mu.Unlock()

	idle = t.insertCount == 0
	if idle {
		t.idleEpochs++
	} else {
		t.idleEpochs = 0
		t.cachedCurr = t.lifetime.Estimate()
	}

	// cachedCurr is cumulative and must carry across the boundary; zeroing it
	// would make the next epoch's delta the full lifetime count.
	if t.cachedCurr > t.cachedPrev {
		delta = t.cachedCurr - t.cachedPrev
	}
	t.cachedPrev = t.cachedCurr
	t.insertCount = 0
	return delta, idle
}

func newTracker() *tracker {
	return &tracker{
		lifetime: mustGetSketch(),
	}
}

// offenderEntry is a snapshot of a high-delta tracker for telemetry reporting,
// produced in rotate() and consumed by the top-offenders callback.
type offenderEntry struct {
	metricName string
	labelKey   string
	delta      uint64
}

// trackerEntry is the (key, tracker) pair used to snapshot shard contents
// outside the shard lock.
type trackerEntry struct {
	key trackerKey
	t   *tracker
}

// trackerShard is an independently-locked partition of the trackers map.
// RWMutex because the common case (tracker exists) only needs a read lock.
type trackerShard struct {
	mu       sync.RWMutex
	trackers map[trackerKey]*tracker
}

type cardinalityProcessor struct {
	config          *Config
	logger          *zap.Logger
	next            consumer.Metrics
	protectedLabels map[string]struct{}
	// seed is randomized so shard distribution can't be gamed by malicious input.
	seed   maphash.Seed
	shards [numShards]*trackerShard
	// cancel stops the background rotation goroutine in Shutdown.
	cancel context.CancelFunc
	// enforcementMode mirrors config.resolvedEnforcementMode() to skip the
	// method call on every data point.
	enforcementMode EnforcementMode

	labelsStripped   atomic.Int64
	trackerCount     atomic.Int64
	trackersRejected atomic.Int64
	// rejectionWarned ensures the MaxTrackerCount warning fires only once per epoch.
	rejectionWarned atomic.Bool
	dropLogCount    atomic.Int64
	dropsThisEpoch  atomic.Int64

	// telemetry is held so Shutdown can unregister the observable callbacks;
	// without that the SDK keeps the processor (and its 256 shards) alive.
	telemetry *metadata.TelemetryBuilder

	topOffenders   []offenderEntry
	topOffendersMu sync.RWMutex

	// wg tracks the rotation goroutine so Shutdown blocks until it exits.
	wg sync.WaitGroup
}

// newCardinalityProcessor constructs a cardinalityProcessor and registers its
// internal OTel telemetry instruments. The logger is sourced from
// set.TelemetrySettings so log records carry the correct component attributes.
func newCardinalityProcessor(_ context.Context, cfg *Config, set processor.Settings, next consumer.Metrics) (processor.Metrics, error) {
	protected := make(map[string]struct{}, len(cfg.NeverDropLabels))
	for _, l := range cfg.NeverDropLabels {
		protected[l] = struct{}{}
	}

	_, cancel := context.WithCancel(context.Background())

	p := &cardinalityProcessor{
		config:          cfg,
		logger:          set.Logger,
		next:            next,
		protectedLabels: protected,
		seed:            maphash.MakeSeed(),
		cancel:          cancel,
		enforcementMode: cfg.resolvedEnforcementMode(),
	}

	for i := range p.shards {
		p.shards[i] = &trackerShard{
			trackers: make(map[trackerKey]*tracker),
		}
	}

	builder, err := metadata.NewTelemetryBuilder(set.TelemetrySettings)
	if err != nil {
		return nil, err
	}
	p.telemetry = builder

	err = builder.RegisterProcessorCardinalityTrackersActiveCallback(func(_ context.Context, o metric.Int64Observer) error {
		var totalActive int64
		for i := range p.shards {
			p.shards[i].mu.RLock()
			totalActive += int64(len(p.shards[i].trackers))
			p.shards[i].mu.RUnlock()
		}
		o.Observe(totalActive)
		return nil
	})
	if err != nil {
		return nil, err
	}

	err = builder.RegisterProcessorCardinalityLabelsStrippedCallback(func(_ context.Context, o metric.Int64Observer) error {
		o.Observe(p.labelsStripped.Load())
		return nil
	})
	if err != nil {
		return nil, err
	}

	err = builder.RegisterProcessorCardinalitySavingsEstimatedCallback(func(_ context.Context, o metric.Float64Observer) error {
		drops := p.labelsStripped.Load()
		o.Observe(float64(drops) * p.config.EstimatedCostPerMetricMonth)
		return nil
	})
	if err != nil {
		return nil, err
	}

	err = builder.RegisterProcessorCardinalityTrackersRejectedCallback(func(_ context.Context, o metric.Int64Observer) error {
		o.Observe(p.trackersRejected.Load())
		return nil
	})
	if err != nil {
		return nil, err
	}

	err = builder.RegisterProcessorCardinalityTopOffendersCallback(func(_ context.Context, o metric.Int64Observer) error {
		p.topOffendersMu.RLock()
		for _, entry := range p.topOffenders {
			o.Observe(int64(entry.delta),
				metric.WithAttributes(
					attribute.String("metric_name", entry.metricName),
					attribute.String("label_key", entry.labelKey),
				),
			)
		}
		p.topOffendersMu.RUnlock()
		return nil
	})
	if err != nil {
		return nil, err
	}

	return p, nil
}

// getShard routes a metric name to one of the numShards independent shard
// buckets. maphash.String is zero-allocation: it operates directly on the
// string header without converting to []byte, and the seed is a value type
// held on the processor struct (no pointer indirection). The seed is fixed at
// construction time so routing is deterministic within a process lifetime.
func (p *cardinalityProcessor) getShard(metricName string) *trackerShard {
	return p.shards[maphash.String(p.seed, metricName)&(numShards-1)]
}

// Capabilities tells the Collector that this processor mutates the data it
// receives. Setting MutatesData: true causes the Collector to clone the metric
// batch before passing it to this processor, ensuring that modifications to
// attributes do not race with other pipeline components that may be consuming
// the same in-memory batch concurrently.
func (*cardinalityProcessor) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: true}
}

// ConsumeMetrics is the entry point for incoming metric batches. It walks the
// three-level OTel hierarchy (ResourceMetrics → ScopeMetrics → Metric) and
// delegates per-metric enforcement to processMetric. After all enforcement is
// complete the (potentially mutated) batch is forwarded to the next consumer.
func (p *cardinalityProcessor) ConsumeMetrics(ctx context.Context, md pmetric.Metrics) error {
	rm := md.ResourceMetrics()
	for i := 0; i < rm.Len(); i++ {
		resMetrics := rm.At(i)
		sm := resMetrics.ScopeMetrics()
		for j := 0; j < sm.Len(); j++ {
			scopeMetrics := sm.At(j)
			ms := scopeMetrics.Metrics()
			for k := 0; k < ms.Len(); k++ {
				// Process each metric
				p.processMetric(ms.At(k))
			}
		}
	}
	return p.next.ConsumeMetrics(ctx, md)
}

// processMetric dispatches a single metric to the appropriate data-point
// handler based on its type. All five OpenTelemetry metric types (Gauge, Sum,
// Histogram, ExponentialHistogram, and Summary) are fully supported, and
// attribute cardinality limits are enforced on all of their data points.
//
// When enforcement mode is strip_and_reaggregate or overflow_attribute, inline
// spatial reaggregation is performed after attribute mutation for supported
// metric types (Delta Sum and Gauge). Unsupported metric types (Cumulative Sum,
// Histogram, ExponentialHistogram, Summary) fall back to tag_only behavior.
func (p *cardinalityProcessor) processMetric(m pmetric.Metric) {
	reaggMode := p.enforcementMode == EnforcementStripAndReaggregate || p.enforcementMode == EnforcementOverflowAttribute

	switch m.Type() {
	case pmetric.MetricTypeGauge:
		p.processNumberDataPoints(m.Name(), m.Gauge().DataPoints())
		if reaggMode {
			reaggregateNumberDataPoints(m.Gauge().DataPoints(), pmetric.MetricTypeGauge, false)
		}
	case pmetric.MetricTypeSum:
		isDelta := m.Sum().AggregationTemporality() == pmetric.AggregationTemporalityDelta
		if reaggMode && !isDelta {
			// Cumulative Sums are not yet supported for reaggregation.
			// Fall back to tag_only behavior for this specific metric to avoid collisions.
			p.processNumberDataPointsWithMode(m.Name(), m.Sum().DataPoints(), EnforcementTagOnly)
		} else {
			p.processNumberDataPoints(m.Name(), m.Sum().DataPoints())
			if reaggMode && isDelta {
				reaggregateNumberDataPoints(m.Sum().DataPoints(), pmetric.MetricTypeSum, true)
			}
		}
	case pmetric.MetricTypeHistogram:
		if reaggMode {
			// Histograms are not yet supported for reaggregation.
			p.processHistogramDataPointsWithMode(m.Name(), m.Histogram().DataPoints(), EnforcementTagOnly)
		} else {
			p.processHistogramDataPoints(m.Name(), m.Histogram().DataPoints())
		}
	case pmetric.MetricTypeExponentialHistogram:
		if reaggMode {
			p.processExponentialHistogramDataPointsWithMode(m.Name(), m.ExponentialHistogram().DataPoints(), EnforcementTagOnly)
		} else {
			p.processExponentialHistogramDataPoints(m.Name(), m.ExponentialHistogram().DataPoints())
		}
	case pmetric.MetricTypeSummary:
		if reaggMode {
			p.processSummaryDataPointsWithMode(m.Name(), m.Summary().DataPoints(), EnforcementTagOnly)
		} else {
			p.processSummaryDataPoints(m.Name(), m.Summary().DataPoints())
		}
	}
}

// processNumberDataPoints iterates over a NumberDataPointSlice and calls
// handleAttributes for each data point. Both Gauge and Sum metric types use
// this slice type, so a single method covers both.
func (p *cardinalityProcessor) processNumberDataPoints(metricName string, dps pmetric.NumberDataPointSlice) {
	for i := 0; i < dps.Len(); i++ {
		p.handleAttributes(metricName, dps.At(i).Attributes())
	}
}

// processHistogramDataPoints iterates over a HistogramDataPointSlice and calls
// handleAttributes for each data point.
func (p *cardinalityProcessor) processHistogramDataPoints(metricName string, dps pmetric.HistogramDataPointSlice) {
	for i := 0; i < dps.Len(); i++ {
		p.handleAttributes(metricName, dps.At(i).Attributes())
	}
}

// processExponentialHistogramDataPoints iterates over an ExponentialHistogramDataPointSlice
// and calls handleAttributes for each data point.
func (p *cardinalityProcessor) processExponentialHistogramDataPoints(metricName string, dps pmetric.ExponentialHistogramDataPointSlice) {
	for i := 0; i < dps.Len(); i++ {
		p.handleAttributes(metricName, dps.At(i).Attributes())
	}
}

// processSummaryDataPoints iterates over a SummaryDataPointSlice and calls
// handleAttributes for each data point.
func (p *cardinalityProcessor) processSummaryDataPoints(metricName string, dps pmetric.SummaryDataPointSlice) {
	for i := 0; i < dps.Len(); i++ {
		p.handleAttributes(metricName, dps.At(i).Attributes())
	}
}

// handleAttributes applies the cardinality decision from shouldDrop to every
// attribute on a single data point. Tag/replace mutations are deferred until
// after RemoveIf completes — a PutBool/PutStr inside the callback could
// reallocate the pdata KeyValueList while RemoveIf still holds its internal
// cursor.
func (p *cardinalityProcessor) handleAttributes(metricName string, attrs pcommon.Map) {
	p.handleAttributesWithMode(metricName, attrs, p.enforcementMode)
}

// handleAttributesWithMode is the mode-parameterized version of handleAttributes.
// It allows callers (like processMetric) to override the enforcement mode for
// specific metric types that don't support reaggregation.
func (p *cardinalityProcessor) handleAttributesWithMode(metricName string, attrs pcommon.Map, mode EnforcementMode) {
	// shouldTag is set when tag_only mode decides an attribute should be tagged.
	// overflowKeys collects keys whose values should be replaced with the sentinel.
	// Both are deferred until after RemoveIf completes to avoid mutating the map
	// while iterating.
	shouldTag := false
	var overflowKeys []string

	attrs.RemoveIf(func(k string, v pcommon.Value) bool {
		if p.isProtected(k) {
			return false
		}

		if p.shouldDrop(metricName, k, v) {
			switch mode {
			case EnforcementTagOnly:
				// DUAL-ROUTE MODE: record the decision, keep the attribute.
				// Check() gates v.AsString() so it never runs when Debug is off.
				shouldTag = true
				if ce := p.logger.Check(zap.DebugLevel, "Cardinality overflow (tag_only)"); ce != nil {
					ce.Write(
						zap.String("metric", metricName),
						zap.String("key", k),
						zap.String("value", v.AsString()),
					)
				}
				return false

			case EnforcementOverflowAttribute:
				// OVERFLOW MODE: defer value replacement until after iteration.
				overflowKeys = append(overflowKeys, k)
				if ce := p.logger.Check(zap.DebugLevel, "Cardinality overflow (overflow_attribute)"); ce != nil {
					ce.Write(
						zap.String("metric", metricName),
						zap.String("key", k),
						zap.String("value", v.AsString()),
					)
				}
				return false

			case EnforcementStripAndReaggregate:
				// STRIP MODE: log and signal RemoveIf to delete this attribute.
				// Increment dropLogCount unconditionally so the per-epoch
				// suppression summary is accurate even when
				// DropLogMaxPerEpoch == 0 (unlimited).
				p.labelsStripped.Add(1)
				p.dropsThisEpoch.Add(1)
				count := p.dropLogCount.Add(1)
				if maxLog := p.config.DropLogMaxPerEpoch; maxLog == 0 || count <= int64(maxLog) {
					p.logger.Warn("Dropping high-cardinality attribute",
						zap.String("metric", metricName),
						zap.String("key", k),
						zap.String("value", v.AsString()))
				}
				return true
			}
		}
		return false
	})

	// Apply deferred mutations after iteration is complete.
	if shouldTag {
		attrs.PutBool("otel.metric.overflow", true)
	}
	for _, k := range overflowKeys {
		attrs.PutStr(k, overflowSentinel)
	}
}

// processNumberDataPointsWithMode processes data points with an overridden
// enforcement mode. Used when the metric type doesn't support the configured
// mode (e.g., Cumulative Sums falling back to tag_only).
func (p *cardinalityProcessor) processNumberDataPointsWithMode(metricName string, dps pmetric.NumberDataPointSlice, mode EnforcementMode) {
	for i := 0; i < dps.Len(); i++ {
		p.handleAttributesWithMode(metricName, dps.At(i).Attributes(), mode)
	}
}

// processHistogramDataPointsWithMode processes histogram data points with an
// overridden enforcement mode.
func (p *cardinalityProcessor) processHistogramDataPointsWithMode(metricName string, dps pmetric.HistogramDataPointSlice, mode EnforcementMode) {
	for i := 0; i < dps.Len(); i++ {
		p.handleAttributesWithMode(metricName, dps.At(i).Attributes(), mode)
	}
}

// processExponentialHistogramDataPointsWithMode processes exponential histogram
// data points with an overridden enforcement mode.
func (p *cardinalityProcessor) processExponentialHistogramDataPointsWithMode(metricName string, dps pmetric.ExponentialHistogramDataPointSlice, mode EnforcementMode) {
	for i := 0; i < dps.Len(); i++ {
		p.handleAttributesWithMode(metricName, dps.At(i).Attributes(), mode)
	}
}

// processSummaryDataPointsWithMode processes summary data points with an
// overridden enforcement mode.
func (p *cardinalityProcessor) processSummaryDataPointsWithMode(metricName string, dps pmetric.SummaryDataPointSlice, mode EnforcementMode) {
	for i := 0; i < dps.Len(); i++ {
		p.handleAttributesWithMode(metricName, dps.At(i).Attributes(), mode)
	}
}

// isProtected reports whether key is in the NeverDropLabels set. The lookup
// is O(1) via the pre-built map on the processor struct.
func (p *cardinalityProcessor) isProtected(key string) bool {
	_, ok := p.protectedLabels[key]
	return ok
}

// rotate advances the sliding cardinality window by one epoch across all
// shards. It runs on the background ticker goroutine, never on the hot path.
//
// Per shard: snapshot tracker pointers under a brief RLock, release, then call
// tracker.rotate on each — which takes only the fine-grained per-tracker mutex.
// The shard-level write lock is taken only to evict stale trackers.
func (p *cardinalityProcessor) rotate() {
	p.logger.Debug("Rotating cardinality sketches")

	// topBuf holds the highest-delta trackers across all shards, bounded to topN.
	topN := p.config.TopOffendersCount
	var topBuf []offenderEntry
	if topN > 0 {
		topBuf = make([]offenderEntry, 0, topN)
	}

	for i := range p.shards {
		shard := p.shards[i]
		// Snapshot tracker references under a shard read lock — no sketch
		// allocations inside the lock.
		shard.mu.RLock()
		entries := make([]trackerEntry, len(shard.trackers))
		idx := 0
		for k, t := range shard.trackers {
			entries[idx] = trackerEntry{key: k, t: t}
			idx++
		}
		shard.mu.RUnlock()

		// Rotate under each tracker's own lock, not the shard lock, so
		// ConsumeMetrics is never blocked here. Trackers idle for
		// staleSweepEpochs consecutive rotations are collected for eviction.
		staleKeys := make([]trackerKey, 0, len(entries))
		deltas := make([]uint64, len(entries))
		for i, e := range entries {
			delta, idle := e.t.rotate()
			deltas[i] = delta
			if idle && e.t.idleEpochs >= staleSweepEpochs {
				staleKeys = append(staleKeys, e.key)
			}
		}

		topBuf = collectShardDeltas(entries, deltas, topBuf, topN)

		// Evict stale trackers under a write lock. This is rare — only
		// trackers that received zero inserts for staleSweepEpochs
		// consecutive epochs are removed.
		if len(staleKeys) > 0 {
			shard.mu.Lock()
			for _, k := range staleKeys {
				delete(shard.trackers, k)
				p.trackerCount.Add(-1)
			}
			shard.mu.Unlock()
			p.logger.Debug("Evicted stale trackers",
				zap.Int("count", len(staleKeys)))
		}
	}

	// Reset the warning flag so we can log again next epoch if still bounded
	p.rejectionWarned.Store(false)

	// Emit suppression summary and reset drop log counters for the new epoch
	totalDrops := p.dropsThisEpoch.Swap(0)
	logged := p.dropLogCount.Swap(0)
	suppressed := totalDrops - logged
	if suppressed > 0 {
		p.logger.Info("Suppressed drop log entries this epoch",
			zap.Int64("suppressed", suppressed),
			zap.Int64("total_drops", totalDrops))
	}

	p.publishTopOffenders(topBuf)
}

// collectShardDeltas maintains a bounded top-N buffer of the highest-delta
// trackers using a linear min-scan. The buffer never grows beyond topN entries,
// so memory usage is O(topN) regardless of how many trackers exist. For each
// candidate tracker, if the buffer is not yet full the entry is appended;
// otherwise the candidate replaces the current minimum only if its delta is
// larger. The min-element index is recomputed via a simple linear scan over
// the (tiny, typically 10-element) buffer — no heap or sort allocations.
//
// deltas must be parallel to entries and is produced by tracker.rotate, so this
// function neither locks nor estimates: the gauge and enforcement always agree.
func collectShardDeltas(entries []trackerEntry, deltas []uint64, topBuf []offenderEntry, topN int) []offenderEntry {
	if topN <= 0 {
		return topBuf
	}
	for i, e := range entries {
		delta := deltas[i]
		if delta == 0 {
			continue
		}

		if len(topBuf) < topN {
			// Buffer not full yet — just append.
			topBuf = append(topBuf, offenderEntry{
				metricName: e.key.metricName,
				labelKey:   e.key.attrKey,
				delta:      delta,
			})
			continue
		}

		// Buffer is full — find the minimum element via linear scan.
		minIdx := 0
		for i := 1; i < len(topBuf); i++ {
			if topBuf[i].delta < topBuf[minIdx].delta {
				minIdx = i
			}
		}
		// Replace only if the candidate beats the current minimum.
		if delta > topBuf[minIdx].delta {
			topBuf[minIdx] = offenderEntry{
				metricName: e.key.metricName,
				labelKey:   e.key.attrKey,
				delta:      delta,
			}
		}
	}
	return topBuf
}

// publishTopOffenders sorts the bounded top-N buffer by descending delta and
// stores the result under topOffendersMu for the telemetry callback to read.
// It also emits an Info-level log line for the single highest offender to aid
// grep-based debugging.
//
// An empty buffer is stored rather than skipped: when no label grew this epoch
// the gauge must clear, or it keeps reporting the last epoch that had growth.
func (p *cardinalityProcessor) publishTopOffenders(topBuf []offenderEntry) {
	sortOffenders(topBuf)
	p.topOffendersMu.Lock()
	p.topOffenders = topBuf
	p.topOffendersMu.Unlock()

	if len(topBuf) == 0 {
		return
	}
	p.logger.Info("Top cardinality offender",
		zap.String("metric", topBuf[0].metricName),
		zap.String("label", topBuf[0].labelKey),
		zap.Uint64("delta", topBuf[0].delta))
}

// sortOffenders performs an insertion sort on a small offenderEntry slice in
// descending delta order. Insertion sort is optimal here because the slice is
// bounded to TopOffendersCount (default 10) — well below the crossover point
// where quicksort becomes worthwhile — and it avoids the closure allocation
// that sort.Slice would incur.
func sortOffenders(s []offenderEntry) {
	for i := 1; i < len(s); i++ {
		key := s[i]
		j := i - 1
		for j >= 0 && s[j].delta < key.delta {
			s[j+1] = s[j]
			j--
		}
		s[j+1] = key
	}
}

// Start is called by the OTel Collector host when the pipeline is starting.
// It launches the background epoch-rotation goroutine and returns immediately.
// The goroutine exits when the internal cancel function is called, which
// happens in Shutdown().
func (p *cardinalityProcessor) Start(_ context.Context, _ component.Host) error {
	// Create a local cancelable context rooted in context.Background()
	// to ensure it is not affected by the ephemeral startup context.
	childCtx, cancel := context.WithCancel(context.Background())
	p.cancel = cancel

	p.logger.Info("Starting cardinality rotation ticker",
		zap.Int("interval_seconds", p.config.EpochDurationSeconds))

	p.wg.Go(func() {
		p.rotationLoop(childCtx)
	})

	return nil
}

// rotationLoop runs the background epoch-rotation ticker. It runs until
// the provided context is canceled.
func (p *cardinalityProcessor) rotationLoop(ctx context.Context) {
	ticker := time.NewTicker(time.Duration(p.config.EpochDurationSeconds) * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			p.rotate()
		case <-ctx.Done():
			return
		}
	}
}

// Shutdown is called by the OTel Collector host when the pipeline is stopping.
// It must complete two cleanup steps in order:
//
//  1. Unregister the observable gauge callback. The OTel SDK holds a strong
//     reference to every registered callback closure; without this call the
//     closure — which captures p via the p.shards slice — would keep the entire
//     processor alive until the SDK's MeterProvider is shut down, leaking all
//     256 shard maps and their tracker contents across pipeline reconfigurations.
//     A nil guard is included for safety (e.g. if construction failed after the
//     counter registrations but before RegisterCallback).
//
//  2. Cancel the child context. This signals the background ticker goroutine to
//     exit on its next select iteration, stopping epoch rotations cleanly.
func (p *cardinalityProcessor) Shutdown(_ context.Context) error {
	p.logger.Info("Shutting down cardinality processor")
	if p.telemetry != nil {
		p.telemetry.Shutdown()
	}
	p.cancel()
	p.wg.Wait()
	return nil
}

// shouldDrop returns true when the unique-value count for (metricName, attrKey)
// has grown by more than MaxCardinalityDeltaPerEpoch since the last epoch
// rotation. The fast path is a shard RLock; a missed tracker triggers
// double-checked locking to install one. curr ≤ prev is treated as no growth
// to guard against uint64 underflow from HLL variance near sketch boundaries.
func (p *cardinalityProcessor) shouldDrop(metricName, attrKey string, attrVal pcommon.Value) bool {
	key := trackerKey{metricName: metricName, attrKey: attrKey}

	// Hash the attribute value before acquiring any lock. hashAttrValue keeps
	// Str/Int/Double/Bool/Bytes on a zero-allocation path; Map/Slice fall back
	// to AsString (JSON-marshal).
	hashVal := hashAttrValue(attrVal)

	// Route to 1/256th of the total key space.
	shard := p.getShard(metricName)

	// Phase 1: read lock — fast path for already-tracked metric+label pairs.
	shard.mu.RLock()
	t, ok := shard.trackers[key]
	shard.mu.RUnlock()

	if !ok {
		// Phase 2: write lock — only for first-time insertion into the shard.
		// Double-check after acquiring the write lock: another goroutine may
		// have inserted the same key while we waited.
		shard.mu.Lock()
		t, ok = shard.trackers[key]
		if !ok {
			if maxT := p.config.MaxTrackerCount; maxT > 0 && p.trackerCount.Load() >= int64(maxT) {
				shard.mu.Unlock()
				p.trackersRejected.Add(1)
				if p.rejectionWarned.CompareAndSwap(false, true) {
					p.logger.Warn("MaxTrackerCount reached; ignoring new metric attributes until next epoch rotation",
						zap.Int("limit", maxT))
				}
				return false
			}
			t = newTracker()
			shard.trackers[key] = t
			p.trackerCount.Add(1)
		}
		shard.mu.Unlock()
	}

	// HLL insert and estimate happen under the per-tracker lock, completely
	// independent of the shard lock. The limit is passed in so insert can refresh
	// the estimate eagerly as the tracker approaches it.
	limit := p.getLimit(metricName)
	currCount, prevCount := t.insert(hashVal, limit)

	// Guard against uint64 underflow caused by HLL probabilistic estimation
	// variance — if the current count has not grown, nothing should be dropped.
	if currCount <= prevCount {
		return false
	}

	return (currCount - prevCount) > limit
}

// getLimit returns the per-metric cardinality limit for the given metric name,
// falling back to the global MaxCardinalityDeltaPerEpoch if no override exists.
// The map is read-only after construction — no lock needed.
func (p *cardinalityProcessor) getLimit(metricName string) uint64 {
	if v, ok := p.config.MetricOverrides[metricName]; ok {
		return uint64(v)
	}
	return uint64(p.config.MaxCardinalityDeltaPerEpoch)
}
