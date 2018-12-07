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

	agenttracepb "github.com/census-instrumentation/opencensus-proto/gen-go/agent/trace/v1"
	tracepb "github.com/census-instrumentation/opencensus-proto/gen-go/trace/v1"
	"go.uber.org/zap"
)

func TestIdBatcher(t *testing.T) {
	const numBatches = 12
	const queueSize = 5
	b := newBatchQueue(queueSize, 4, 8)

	traceIds, _ := generateIdsAndBatches(128)
	nextBatch := 1
	currentBatchSize := 0
	wantIDCount := 0
	got := make([][][]byte, 0, numBatches)
	for _, id := range traceIds {
		b.addToBatchQueue(id)
		currentBatchSize++
		wantIDCount++
		if nextBatch == currentBatchSize {
			nextBatch++
			currentBatchSize = 0
			batch, _ := b.getAndSwitchBatch()
			got = append(got, batch)
			if nextBatch > numBatches {
				break
			}
		}
	}

	b.stop()

	if len(got) != numBatches {
		t.Fatalf("Batcher got %d batches, want %d", len(got), numBatches)
	}

	// Since the first batches were empty, get remaining batches to get all ids added to the batcher
	for {
		batch, ok := b.getAndSwitchBatch()
		got = append(got, batch)
		if !ok {
			break
		}
	}

	gotIDCount := 0
	for i := 0; i < len(got); i++ {
		batch := got[i]
		if i < queueSize {
			if len(batch) != 0 {
				t.Fatalf("Batcher got %d traces on batch %d, want zero batches until queue size (%d) is full", len(batch), i, queueSize)
			}
			continue
		}

		gotIDCount += len(batch)
	}

	if wantIDCount != gotIDCount {
		t.Fatalf("Batcher got %d traces from batches, want %d", gotIDCount, wantIDCount)
	}
}

func TestSequentialTraceArrival(t *testing.T) {
	traceIds, batches := generateIdsAndBatches(128)
	// First do it linearly
	tsp := NewTailSamplingSpanProcessor(nil, uint64(2*len(traceIds)), zap.NewNop()).(*tailSamplingSpanProcessor)
	for _, batch := range batches {
		tsp.ProcessSpans(batch, "test")
	}

	for i := range traceIds {
		if v, ok := tsp.idToTrace.Load(traceKey(traceIds[i])); !ok {
			t.Fatal("Missing expected traceId")
		} else if v.SpanCount != int64(i+1) {
			t.Fatalf("Incorrect number of spans for entry %d, got %d, want %d", i, v.SpanCount, i+1)
		}
	}
}

func TestConcurrentTraceArrival(t *testing.T) {
	traceIds, batches := generateIdsAndBatches(128)

	var wg sync.WaitGroup
	tsp := NewTailSamplingSpanProcessor(nil, uint64(2*len(traceIds)), zap.NewNop()).(*tailSamplingSpanProcessor)
	for _, batch := range batches {
		wg.Add(1)
		go func(b *agenttracepb.ExportTraceServiceRequest, sf string) {
			tsp.ProcessSpans(b, sf)
			wg.Done()
		}(batch, "test")
	}

	wg.Wait()

	for i := range traceIds {
		if v, ok := tsp.idToTrace.Load(traceKey(traceIds[i])); !ok {
			t.Fatal("Missing expected traceId")
		} else if v.SpanCount != int64(i+1) {
			t.Fatalf("Incorrect number of spans for entry %d, got %d, want %d", i, v.SpanCount, i+1)
		}
	}
}

func TestConcurrentTraceMapSize(t *testing.T) {
	traceIds, batches := generateIdsAndBatches(210)
	const maxSize = 100
	var wg sync.WaitGroup
	tsp := NewTailSamplingSpanProcessor(nil, uint64(maxSize), zap.NewNop()).(*tailSamplingSpanProcessor)
	for _, batch := range batches {
		wg.Add(1)
		go func(b *agenttracepb.ExportTraceServiceRequest, sf string) {
			tsp.ProcessSpans(b, sf)
			wg.Done()
		}(batch, "test")
	}

	wg.Wait()
	tsp.deleteQueue.stop()
	for {
		batch, moredata := tsp.deleteQueue.getAndSwitchBatch()
		tsp.deleteChan <- batch
		if !moredata {
			close(tsp.deleteChan)
			break
		}
	}
	<-tsp.deleteStop

	for i := 0; i < maxSize; i++ {
		if _, ok := tsp.idToTrace.Load(traceKey(traceIds[i])); ok {
			t.Fatalf("Found unexpected traceId still on map %v", traceIds[i])
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
