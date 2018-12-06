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

package sampling

import tracepb "github.com/census-instrumentation/opencensus-proto/gen-go/trace/v1"

type numericTagFilter struct {
	tag                string
	minValue, maxValue int64
}

var _ PolicyEvaluator = (*numericTagFilter)(nil)

// NewNumericTagFilter creates a policy evaluator that samples all traces with
// the given tag in the given numeric range.
func NewNumericTagFilter(tag string, minValue, maxValue int64) PolicyEvaluator {
	return &numericTagFilter{
		tag:      tag,
		minValue: minValue,
		maxValue: maxValue,
	}
}

// LateArrivingSpans notifies the evaluator that the given list of spans arrived
// after the sampling decision was already taken for the trace.
// This gives the evaluator a chance to log any message/metrics and/or update any
// related internal state.
func (ntf *numericTagFilter) OnLateArrivingSpans(earlyDecision Decision, spans []*tracepb.Span) error {
	return nil
}

// Evaluate looks at the trace data and returns a corresponding SamplingDecision.
func (ntf *numericTagFilter) Evaluate(traceID []byte, trace *TraceData) (Decision, error) {
	trace.Lock()
	batches := trace.ReceivedBatches
	trace.Unlock()
	for _, batch := range batches {
		for _, span := range batch.Spans {
			if span == nil || span.Attributes == nil {
				continue
			}
			if v, ok := span.Attributes.AttributeMap[ntf.tag]; ok {
				value := v.GetIntValue()
				if value >= ntf.minValue && value <= ntf.maxValue {
					return Sampled, nil
				}
			}
		}
	}

	return NotSampled, nil
}

// OnDroppedSpans is called when the trace needs to be dropped, due to memory
// pressure, before the decision_wait time has been reached.
func (ntf *numericTagFilter) OnDroppedSpans(traceID []byte, trace *TraceData) (Decision, error) {
	return NotSampled, nil
}