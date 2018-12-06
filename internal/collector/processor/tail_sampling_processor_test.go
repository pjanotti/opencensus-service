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
	"testing"
	"time"

	agenttracepb "github.com/census-instrumentation/opencensus-proto/gen-go/agent/trace/v1"
	tracepb "github.com/census-instrumentation/opencensus-proto/gen-go/trace/v1"
	"go.uber.org/zap"

	"github.com/census-instrumentation/opencensus-service/internal/collector/sampling"
)

const (
	defaultTestDecisionWait = 30 * time.Second
)

func TestSequentialTraceArrival(t *testing.T) {
	traceIds, batches := generateIdsAndBatches(128)
	// First do it linearly
	tsp := NewTailSamplingSpanProcessor(newTestPolicy(), uint64(2*len(traceIds)), defaultTestDecisionWait, zap.NewNop()).(*tailSamplingSpanProcessor)
	for _, batch := range batches {
		tsp.ProcessSpans(batch, "test")
	}

	for i := range traceIds {
		d, ok := tsp.idToTrace.Load(traceKey(traceIds[i]))
		v := d.(*sampling.TraceData)
		if !ok {
			t.Fatal("Missing expected traceId")
		} else if v.SpanCount != int64(i+1) {
			t.Fatalf("Incorrect number of spans for entry %d, got %d, want %d", i, v.SpanCount, i+1)
		}
	}
}

func TestConcurrentTraceArrival(t *testing.T) {
	traceIds, batches := generateIdsAndBatches(128)

	var wg sync.WaitGroup
	tsp := NewTailSamplingSpanProcessor(newTestPolicy(), uint64(2*len(traceIds)), defaultTestDecisionWait, zap.NewNop()).(*tailSamplingSpanProcessor)
	for _, batch := range batches {
		// Add the same traceId twice.
		wg.Add(2)
		go func(b *agenttracepb.ExportTraceServiceRequest, sf string) {
			tsp.ProcessSpans(b, sf)
			wg.Done()
		}(batch, "test-0")
		go func(b *agenttracepb.ExportTraceServiceRequest, sf string) {
			tsp.ProcessSpans(b, sf)
			wg.Done()
		}(batch, "test-1")
	}

	wg.Wait()

	for i := range traceIds {
		d, ok := tsp.idToTrace.Load(traceKey(traceIds[i]))
		v := d.(*sampling.TraceData)
		if !ok {
			t.Fatal("Missing expected traceId")
		} else if v.SpanCount != int64(i+1)*2 {
			t.Fatalf("Incorrect number of spans for entry %d, got %d, want %d", i, v.SpanCount, i+1)
		}
	}
}

func TestConcurrentTraceMapSize(t *testing.T) {
	traceIds, batches := generateIdsAndBatches(210)
	const maxSize = 100
	var wg sync.WaitGroup
	tsp := NewTailSamplingSpanProcessor(newTestPolicy(), uint64(maxSize), defaultTestDecisionWait, zap.NewNop()).(*tailSamplingSpanProcessor)
	for _, batch := range batches {
		wg.Add(1)
		go func(b *agenttracepb.ExportTraceServiceRequest, sf string) {
			tsp.ProcessSpans(b, sf)
			wg.Done()
		}(batch, "test")
	}

	wg.Wait()

	for i := 0; i < len(traceIds)-maxSize; i++ {
		if _, ok := tsp.idToTrace.Load(traceKey(traceIds[i])); ok {
			t.Fatalf("Found unexpected traceId[%d] still on map (id: %v)", i, traceIds[i])
		}
	}
}

func generateIdsAndBatches(numIds byte) ([][]byte, []*agenttracepb.ExportTraceServiceRequest) {
	traceIds := make([][]byte, numIds, numIds)
	for i := byte(0); i < numIds; i++ {
		traceIds[i] = []byte{i, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}
	}

	batches := []*agenttracepb.ExportTraceServiceRequest{}
	for i := range traceIds {
		spans := make([]*tracepb.Span, i+1)
		for j := range spans {
			spans[j] = &tracepb.Span{
				TraceId: traceIds[i],
			}
		}

		// Send each span in a separate batch
		for _, span := range spans {
			batch := &agenttracepb.ExportTraceServiceRequest{
				Spans: []*tracepb.Span{span},
			}
			batches = append(batches, batch)
		}
	}

	return traceIds, batches
}

func newTestPolicy() []*Policy {
	return []*Policy{
		{
			Name:        "test",
			Evaluator:   sampling.NewAlwaysSample(),
			Destination: &mockSpanProcessor{},
		},
	}
}
