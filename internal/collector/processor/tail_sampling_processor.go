// Copyright 2018, OpenCensus Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package processor

import (
	"sync"
	"sync/atomic"
	"time"

	agenttracepb "github.com/census-instrumentation/opencensus-proto/gen-go/agent/trace/v1"
	tracepb "github.com/census-instrumentation/opencensus-proto/gen-go/trace/v1"
	"go.uber.org/zap"

	"github.com/census-instrumentation/opencensus-service/internal/collector/sampling"
)

// PolicyAndDestinations combines a sampling policy evaluator with the destinations to be
// used for that policy.
type PolicyAndDestinations struct {
	Evaluator    sampling.PolicyEvaluator
	Destinations []*SpanProcessor
}

// tailSamplingSpanProcessor handles the incoming trace data and uses the given sampling
// policies to sample traces.
type tailSamplingSpanProcessor struct {
	start        sync.Once
	policies     []*PolicyAndDestinations
	logger       *zap.Logger
	idToTrace    *concurrentMap
	policyTicker tTicker
}

var _ SpanProcessor = (*tailSamplingSpanProcessor)(nil)

// NewTailSamplingSpanProcessor creates a TailSamplingSpanProcessor from the variadic
// list of passed SpanProcessors.
func NewTailSamplingSpanProcessor(policies []*PolicyAndDestinations, logger *zap.Logger) SpanProcessor {
	tsp := &tailSamplingSpanProcessor{
		policies:  policies,
		logger:    logger,
		idToTrace: newConcurrentMap(),
	}
	tsp.setupPolicyTicker(30*time.Second, 1*time.Second)
	return tsp
}

// ProcessSpans is required by the SpanProcessor interface.
func (tsp *tailSamplingSpanProcessor) ProcessSpans(batch *agenttracepb.ExportTraceServiceRequest, spanFormat string) (uint64, error) {
	tsp.start.Do(func() {
		tsp.logger.Info("First trace data arrived, starting tail-sampling timers")
		tsp.policyTicker.Start(1 * time.Second)
	})

	// Groupd spans per their traceId to minize contention on idToTrace
	idToSpans := make(map[traceKey][]*tracepb.Span)
	for _, span := range batch.Spans {
		if len(span.TraceId) != 16 {
			tsp.logger.Warn("Span without valid TraceId", zap.String("spanFormat", spanFormat))
			continue
		}
		idToSpans[traceKey(span.TraceId)] = append(idToSpans[traceKey(span.TraceId)], span)
	}

	for id, spans := range idToSpans {
		lenSpans := int64(len(spans))
		actualData, loaded := tsp.idToTrace.LoadOrStore(id, func() *traceValue {
			traceData := new(traceValue)
			traceData.Add(lenSpans)
			return traceData
		})
		if loaded {
			// TODO: (@tail) anything related to traceData needs to be protected
			actualData.Add(lenSpans)
		} // TODO: (@tail) set other fields on demand for the first time.
	}

	return 0, nil
}

func (tsp *tailSamplingSpanProcessor) setupPolicyTicker(decisionWait time.Duration, tickerFrequency time.Duration) {
	tsp.policyTicker = newPolicyTicker(func() {
		tsp.logger.Debug("PoliceTicker: ticked")
	})
}

type batcher struct {
	pendingIds                chan []byte
	batches                   chan [][]byte
	cbMutex                   sync.Mutex
	currentBatch              [][]byte
	numBatches                uint64
	newBatchesInitialCapacity int
	stopchan                  chan bool
	stopped                   bool
}

func newBatchQueue(numBatches, newBatchesInitialCapacity, channelSize int) *batcher {
	batches := make(chan [][]byte, numBatches)
	// First numBatches batches will be empty
	for i := 0; i < numBatches; i++ {
		batches <- nil
	}

	batcher := &batcher{
		pendingIds:                make(chan []byte, channelSize),
		batches:                   batches,
		currentBatch:              make([][]byte, 0, newBatchesInitialCapacity),
		numBatches:                uint64(numBatches),
		newBatchesInitialCapacity: newBatchesInitialCapacity,
		stopchan:                  make(chan bool),
	}

	go func() {
		for id := range batcher.pendingIds {
			batcher.cbMutex.Lock()
			batcher.currentBatch = append(batcher.currentBatch, id)
			batcher.cbMutex.Unlock()
		}
		batcher.stopchan <- true
	}()

	return batcher
}

func (b *batcher) addToBatchQueue(id []byte) {
	b.pendingIds <- id
}

func (b *batcher) getAndSwitchBatch() ([][]byte, bool) {
	for readBatch := range b.batches {
		if !b.stopped {
			b.cbMutex.Lock()
			b.batches <- b.currentBatch
			b.currentBatch = make([][]byte, 0, b.newBatchesInitialCapacity)
			b.cbMutex.Unlock()
		}
		return readBatch, true
	}

	readBatch := b.currentBatch
	b.currentBatch = nil
	return readBatch, false
}

func (b *batcher) stop() {
	close(b.pendingIds)
	b.stopped = <-b.stopchan
	close(b.batches)
}

// tTicker interface allows easier testing of ticker related functionality used by tailSamplingProcessor
type tTicker interface {
	// Start sets the frequency of the ticker and starts the perioc calls to OnTick.
	Start(d time.Duration)
	// OnTick is called when the ticker fires.
	OnTick()
	// Stops firing the ticker.
	Stop()
}

type policyTicker struct {
	ticker *time.Ticker
	onTick func()
}

func newPolicyTicker(onTick func()) *policyTicker {
	return &policyTicker{onTick: onTick}
}

func (pt *policyTicker) Start(d time.Duration) {
	pt.ticker = time.NewTicker(d)
	go func() {
		for range pt.ticker.C {
			pt.OnTick()
		}
	}()
}
func (pt *policyTicker) OnTick() {
	pt.onTick()
}
func (pt *policyTicker) Stop() {
	pt.ticker.Stop()
}

var _ tTicker = (*policyTicker)(nil)

// concurrentMap wraps the functionality needed by tailSamplingSpanProcessor so the internals of the
// actual implementation are hidden and can be swapped as needed.
type concurrentMap struct {
	sync.RWMutex
	m map[traceKey]*traceValue
}

type traceKey string

type traceValue struct {
	spanCount int64
}

func (tv *traceValue) Add(add int64) int64 {
	return atomic.AddInt64(&tv.spanCount, add)
}

func newConcurrentMap() *concurrentMap {
	return &concurrentMap{
		m: make(map[traceKey]*traceValue),
	}
}

func (cm *concurrentMap) LoadOrStore(key traceKey, valueFactory func() *traceValue) (actual *traceValue, loaded bool) {
	cm.RLock()
	actual, loaded = cm.m[key]
	cm.RUnlock()
	if loaded {
		return
	}

	// Expensive path now.
	cm.Lock()
	actual, loaded = cm.m[key]
	if loaded {
		// Some concurrent LoadOrStore beat us to it, unlock and return what was found.
		cm.Unlock()
		return
	}
	actual = valueFactory()
	cm.m[key] = actual
	cm.Unlock()

	return
}

func (cm *concurrentMap) Delete(key traceKey) {
	cm.Lock()
	delete(cm.m, key)
	cm.Unlock()
}

func (cm *concurrentMap) Load(key traceKey) (value *traceValue, ok bool) {
	cm.RLock()
	value, ok = cm.m[key]
	cm.RUnlock()
	return
}

func (cm *concurrentMap) Len() int {
	cm.RLock()
	len := len(cm.m)
	cm.RUnlock()
	return len
}
