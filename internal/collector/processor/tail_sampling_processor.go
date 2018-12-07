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
	"runtime"
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
	start           sync.Once
	maxNumTraces    uint64
	policies        []*PolicyAndDestinations
	logger          *zap.Logger
	idToTrace       *concurrentMap
	policyTicker    tTicker
	batchQueue      *batcher
	deleteQueue     *batcher
	deleteChan      chan [][]byte
	deleteBatchSize uint64
	deleteStop      chan bool
	numTracesOnMap  uint64
}

var _ SpanProcessor = (*tailSamplingSpanProcessor)(nil)

const (
	deletionQueueBatches uint64 = 100
)

// NewTailSamplingSpanProcessor creates a TailSamplingSpanProcessor from the variadic
// list of passed SpanProcessors.
func NewTailSamplingSpanProcessor(policies []*PolicyAndDestinations, maxNumTraces uint64, logger *zap.Logger) SpanProcessor {
	tsp := &tailSamplingSpanProcessor{
		maxNumTraces: maxNumTraces,
		policies:     policies,
		logger:       logger,
		idToTrace:    newConcurrentMap(),
		batchQueue:   newBatchQueue(30, 50, uint64(2*runtime.NumCPU())), // TODO: (@tail) constants or from config, same below
		deleteChan:   make(chan [][]byte, deletionQueueBatches),
		deleteStop:   make(chan bool),
	}

	tsp.policyTicker = newPolicyTicker(tsp.samplingPolicyOnTick)

	switch {
	case maxNumTraces < deletionQueueBatches:
		tsp.deleteBatchSize = 1 // For tests
	case maxNumTraces%deletionQueueBatches == 0:
		tsp.deleteBatchSize = maxNumTraces / deletionQueueBatches
	default:
		tsp.deleteBatchSize = (maxNumTraces + deletionQueueBatches) / deletionQueueBatches
	}

	actualDeletionBatches := maxNumTraces / tsp.deleteBatchSize
	tsp.deleteQueue = newBatchQueue(actualDeletionBatches, tsp.deleteBatchSize/4, uint64(runtime.NumCPU()))
	tsp.deleteChan = make(chan [][]byte, actualDeletionBatches)

	go func() {
		for deleteBatch := range tsp.deleteChan {
			deleteCount := len(deleteBatch)
			if deleteCount == 0 {
				continue
			}
			tsp.idToTrace.BatchDeletion(deleteBatch)
			atomic.AddUint64(&tsp.numTracesOnMap, ^uint64(deleteCount-1))
		}
		tsp.deleteStop <- true
	}()
	return tsp
}

func (tsp *tailSamplingSpanProcessor) samplingPolicyOnTick() {
	var idNotFoundOnMapCount, evaluateErrorCount, decisionSampled, decisionNotSampled int
	batch, _ := tsp.batchQueue.getAndSwitchBatch() // TODO: (@tail) special treatment when there is no more data
	batchLen := len(batch)
	tsp.logger.Debug("PoliceTicker: ticked")
	for _, id := range batch {
		for _, policy := range tsp.policies {
			trace, ok := tsp.idToTrace.Load(traceKey(id))
			if !ok {
				idNotFoundOnMapCount++
				continue
			}

			decision, err := policy.Evaluator.Evaluate(id, trace)
			if err != nil {
				evaluateErrorCount++
				tsp.logger.Error("Sampling policy error", zap.Error(err))
				continue
			}

			trace.Decision = decision
			trace.DecisionTime = time.Now()

			switch decision {
			case sampling.Sampled:
				decisionSampled++
			case sampling.NotSampled:
				decisionNotSampled++
			}
		}
	}

	if idNotFoundOnMapCount > 0 {
		tsp.logger.Error("Trace ids not present on map", zap.Int("count", idNotFoundOnMapCount))
	}
	if idNotFoundOnMapCount > 0 {
		tsp.logger.Error("Policy evaluation errors", zap.Int("count", evaluateErrorCount))
	}
	tsp.logger.Debug("Sampling policy evaluation completed",
		zap.Int("batch.len", batchLen),
		zap.Int("sampled", decisionSampled),
		zap.Int("notSampled", decisionNotSampled))
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
		actualData, loaded := tsp.idToTrace.LoadOrStore(id, func() *sampling.TraceData {
			return &sampling.TraceData{
				ArrivalTime: time.Now(),
				SpanCount:   lenSpans,
			}
		})
		if loaded {
			// TODO: (@tail) anything related to traceData needs to be protected
			atomic.AddInt64(&actualData.SpanCount, lenSpans)
		} else {
			tsp.batchQueue.addToBatchQueue([]byte(id)) // TODO: (@tail) check casting
			tsp.deleteQueue.addToBatchQueue([]byte(id))
			currMapSize := atomic.AddUint64(&tsp.numTracesOnMap, 1)
			if currMapSize%tsp.deleteBatchSize == 0 {
				deleteBatch, _ := tsp.deleteQueue.getAndSwitchBatch()
				tsp.deleteChan <- deleteBatch
			}
			// TODO: (@tail) set other fields on demand for the first time.
		}
	}

	return 0, nil
}

type batcher struct {
	pendingIds chan []byte
	batches    chan [][]byte
	// cbMutex protects the currentBatch storing ids.
	cbMutex                   sync.Mutex
	currentBatch              [][]byte
	numBatches                uint64
	newBatchesInitialCapacity uint64
	stopchan                  chan bool
	stopped                   bool
}

func newBatchQueue(numBatches, newBatchesInitialCapacity, channelSize uint64) *batcher {
	batches := make(chan [][]byte, numBatches)
	// First numBatches batches will be empty
	for i := uint64(0); i < numBatches; i++ {
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
	m map[traceKey]*sampling.TraceData
}

type traceKey string

func newConcurrentMap() *concurrentMap {
	return &concurrentMap{
		m: make(map[traceKey]*sampling.TraceData),
	}
}

func (cm *concurrentMap) LoadOrStore(key traceKey, valueFactory func() *sampling.TraceData) (actual *sampling.TraceData, loaded bool) {
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

func (cm *concurrentMap) BatchDeletion(keys [][]byte) {
	cm.Lock()
	for _, key := range keys {
		delete(cm.m, traceKey(key))
	}
	cm.Unlock()
}
func (cm *concurrentMap) Load(key traceKey) (value *sampling.TraceData, ok bool) {
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
