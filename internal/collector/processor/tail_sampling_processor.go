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

// Policy combines a sampling policy evaluator with the destinations to be
// used for that policy.
type Policy struct {
	Name         string
	Evaluator    sampling.PolicyEvaluator
	Destinations []SpanProcessor
}

// tailSamplingSpanProcessor handles the incoming trace data and uses the given sampling
// policies to sample traces.
type tailSamplingSpanProcessor struct {
	start           sync.Once
	maxNumTraces    uint64
	policies        []*Policy
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
func NewTailSamplingSpanProcessor(policies []*Policy, maxNumTraces uint64, decisionWait time.Duration, logger *zap.Logger) SpanProcessor {
	numDecisionBatches := uint64(decisionWait.Seconds())
	tsp := &tailSamplingSpanProcessor{
		maxNumTraces: maxNumTraces,
		policies:     policies,
		logger:       logger,
		idToTrace:    newConcurrentMap(),
		batchQueue:   newBatchQueue(numDecisionBatches, 64, uint64(2*runtime.NumCPU())), // TODO: (@tail) constants or from config, same below
		deleteStop:   make(chan bool),
	}

	tsp.policyTicker = newPolicyTicker(tsp.samplingPolicyOnTick)
	tsp.setupTraceDeletion(maxNumTraces, deletionQueueBatches)
	return tsp
}

// setupTraceDeletion calculates the number of batches in the deletion queue and creates the queue,
// setting up the tailSamplingSpanProcessor to keep the map of traces size bounded.
func (tsp *tailSamplingSpanProcessor) setupTraceDeletion(maxNumTraces, deletionQueueSize uint64) {
	switch {
	case maxNumTraces < deletionQueueSize:
		tsp.deleteBatchSize = 1 // For tests
	case maxNumTraces%deletionQueueSize == 0:
		tsp.deleteBatchSize = maxNumTraces / deletionQueueSize
	default:
		tsp.deleteBatchSize = (maxNumTraces + deletionQueueSize) / deletionQueueSize
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
			traces := tsp.idToTrace.BatchDeletion(deleteBatch)
			// Second parameter to AddUint64 is the proper way (per golang docs) to subtract the value.
			atomic.AddUint64(&tsp.numTracesOnMap, ^uint64(deleteCount-1))

			policiesLen := len(tsp.policies)
			for i, trace := range traces {
				for j := 0; j < policiesLen; j++ {
					if trace.Decision[j] == sampling.Pending {
						policy := tsp.policies[j]
						if decision, err := policy.Evaluator.OnDroppedSpans(deleteBatch[i], trace); err != nil {
							tsp.logger.Warn("OnDroppedSpans",
								zap.String("policy", policy.Name),
								zap.Int("decision", int(decision)),
								zap.Error(err))
						}
					}
				}
			}
		}
		tsp.deleteStop <- true
	}()
}

func (tsp *tailSamplingSpanProcessor) samplingPolicyOnTick() {
	var idNotFoundOnMapCount, evaluateErrorCount, decisionSampled, decisionNotSampled int
	batch, _ := tsp.batchQueue.getAndSwitchBatch() // TODO: (@tail) special treatment when there is no more data
	batchLen := len(batch)
	tsp.logger.Debug("Sampling Policy Evaluation ticked")
	for _, id := range batch {
		trace, ok := tsp.idToTrace.Load(traceKey(id))
		if !ok {
			idNotFoundOnMapCount++
			continue
		}
		trace.DecisionTime = time.Now()
		for i, policy := range tsp.policies {
			decision, err := policy.Evaluator.Evaluate(id, trace)
			if err != nil {
				trace.Decision[i] = sampling.NotSampled
				evaluateErrorCount++
				tsp.logger.Error("Sampling policy error", zap.Error(err))
				continue
			}

			trace.Decision[i] = decision

			switch decision {
			case sampling.Sampled:
				decisionSampled++
				trace.Lock()
				traceBatches := trace.ReceivedBatches
				trace.Unlock()
				for j := 0; j < len(policy.Destinations); j++ {
					for k := 0; k < len(traceBatches); k++ {
						policy.Destinations[j].ProcessSpans(traceBatches[k], "tail-sampling")
					}
				}
			case sampling.NotSampled:
				decisionNotSampled++
			}
		}

		// Sampled or not, remove the batches
		trace.Lock()
		trace.ReceivedBatches = nil
		trace.Unlock()
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

	singleTrace := len(idToSpans) == 1
	for id, spans := range idToSpans {
		lenSpans := int64(len(spans))
		lenPolicies := len(tsp.policies)
		actualData, loaded := tsp.idToTrace.LoadOrStore(id, func() *sampling.TraceData {
			initialDecisions := make([]sampling.Decision, lenPolicies, lenPolicies)
			for i := 0; i < lenPolicies; i++ {
				initialDecisions[i] = sampling.Pending
			}
			return &sampling.TraceData{
				Decision:    initialDecisions,
				ArrivalTime: time.Now(),
				SpanCount:   lenSpans,
			}
		})

		if loaded {
			atomic.AddInt64(&actualData.SpanCount, lenSpans)
		} else {
			// TODO: (@tail) check casting
			traceID := []byte(id)
			tsp.batchQueue.addToBatchQueue(traceID)
			tsp.deleteQueue.addToBatchQueue(traceID)
			currMapSize := atomic.AddUint64(&tsp.numTracesOnMap, 1)
			if currMapSize%tsp.deleteBatchSize == 0 {
				// Time to cut another deletion batch, and take the first one to
				// the deletion channel
				deleteBatch, _ := tsp.deleteQueue.getAndSwitchBatch()
				tsp.deleteChan <- deleteBatch
			}
		}

		for i, policyAndDests := range tsp.policies {
			actualData.Lock()
			actualDecision := actualData.Decision[i]
			// If decision is pending, we want to add the new spans still under the lock, so the decision doesn't happen
			// in between the transition from pending.
			if actualDecision == sampling.Pending {
				// Add the spans to the trace, but only once
				traceBatch := prepareTraceBatch(spans, singleTrace, batch)
				actualData.ReceivedBatches = append(actualData.ReceivedBatches, traceBatch)
				actualData.Unlock()
				break
			}
			actualData.Unlock()

			switch actualDecision {
			case sampling.Pending:
				// All process for pending done above, keep the case so it doesn't go to default.
			case sampling.Sampled:
				// Forward the spans to the policy destinations
				traceBatch := prepareTraceBatch(spans, singleTrace, batch)
				for _, dest := range policyAndDests.Destinations {
					if failCount, err := dest.ProcessSpans(traceBatch, spanFormat); err != nil {
						tsp.logger.Warn("Error sending late arrived spans to destination",
							zap.String("policy", policyAndDests.Name),
							zap.Uint64("failCount", failCount),
							zap.Error(err))
					}
				}
				continue // so OnLateArrivingSpans is also called for decision Sampled.
			case sampling.NotSampled:
				policyAndDests.Evaluator.OnLateArrivingSpans(actualDecision, spans)

			default:
				tsp.logger.Warn("Encountered unexpected sampling decision",
					zap.String("policy", policyAndDests.Name),
					zap.Int("decision", int(actualDecision)))
			}
		}
	}

	return 0, nil
}

func prepareTraceBatch(spans []*tracepb.Span, singleTrace bool, batch *agenttracepb.ExportTraceServiceRequest) *agenttracepb.ExportTraceServiceRequest {
	var traceBatch *agenttracepb.ExportTraceServiceRequest
	if singleTrace {
		// Special case no need to prepare a batch
		traceBatch = batch
	} else {
		traceBatch = &agenttracepb.ExportTraceServiceRequest{
			Node:     batch.Node,
			Resource: batch.Resource,
			Spans:    spans,
		}
	}
	return traceBatch
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
		newBatchesInitialCapacity: newBatchesInitialCapacity,
		stopchan:                  make(chan bool),
	}

	// Single goroutine that keeps filling the current batch, contention is expected only
	// when the current batch is being switched.
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

// getAndSwitchBatch takes the batch at the front of the queue, and moves the current batch to
// the end of the queue. It returns that batch that was in front of the queue and a boolean
// that if true indicates that there are more batches to be retrieved.
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

func (cm *concurrentMap) BatchDeletion(keys [][]byte) []*sampling.TraceData {
	keysLen := len(keys)
	traces := make([]*sampling.TraceData, keysLen, keysLen)
	cm.Lock()
	for i := 0; i < keysLen; i++ {
		key := traceKey(keys[i])
		traces[i] = cm.m[key]
		delete(cm.m, key)
	}
	cm.Unlock()
	return traces
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
