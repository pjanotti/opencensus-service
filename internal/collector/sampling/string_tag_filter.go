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

type stringTagFilter struct {
	tag    string
	values map[string]bool
}

var _ PolicyEvaluator = (*stringTagFilter)(nil)

// NewStringTagFilter creates a policy evaluator that samples all traces with
// the given tag in the given numeric range.
func NewStringTagFilter(tag string, values []string) PolicyEvaluator {
	valuesMap := make(map[string]bool)
	for _, value := range values {
		if value != "" {
			valuesMap[value] = true
		}
	}
	return &stringTagFilter{
		tag:    tag,
		values: valuesMap,
	}
}

// LateArrivingSpans notifies the evaluator that the given list of spans arrived
// after the sampling decision was already taken for the trace.
// This gives the evaluator a chance to log any message/metrics and/or update any
// related internal state.
func (stf *stringTagFilter) OnLateArrivingSpans(earlyDecision Decision, spans []*tracepb.Span) error {
	return nil
}

// Evaluate looks at the trace data and returns a corresponding SamplingDecision.
func (stf *stringTagFilter) Evaluate(traceID []byte, trace *TraceData) (Decision, error) {
	trace.Lock()
	batches := trace.ReceivedBatches
	trace.Unlock()
	for _, batch := range batches {
		node := batch.Node
		if node != nil && node.Attributes != nil {
			if v, ok := node.Attributes[stf.tag]; ok {
				if _, ok := stf.values[v]; ok {
					return Sampled, nil
				}
			}
		}
		for _, span := range batch.Spans {
			if span == nil || span.Attributes == nil {
				continue
			}
			if v, ok := span.Attributes.AttributeMap[stf.tag]; ok {
				truncableStr := v.GetStringValue()
				if truncableStr != nil {
					if _, ok := stf.values[truncableStr.Value]; ok {
						return Sampled, nil
					}
				}
			}
		}
	}

	return NotSampled, nil
}

// OnDroppedSpans is called when the trace needs to be dropped, due to memory
// pressure, before the decision_wait time has been reached.
func (stf *stringTagFilter) OnDroppedSpans(traceID []byte, trace *TraceData) (Decision, error) {
	return NotSampled, nil
}
