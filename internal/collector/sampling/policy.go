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

import "time"

// TraceData stores the trace related data.
type TraceData struct {
	// Decision gives the current status of the sampling decision.
	Decision Decision
	// Arrival time the first span for the trace was received.
	ArrivalTime time.Time
	// Decisiontime time when sampling decision was taken.
	DecisionTime time.Time
	// SpanCount track the number of spans on the trace.
	SpanCount int64
}

// Spans TODO: (@tail) for now just a place holder
type Spans int

// Decision gives the status of sampling decision.
type Decision int

const (
	// Pending indicates that the policy was not evaluated yet.
	Pending Decision = iota
	// Sampled is used to indicate that the decision was already taken
	// to sample the data.
	Sampled
	// NotSampled is used to indicate that the decision was already taken
	// to not sample the data.
	NotSampled
	// Dropped is used when data needs to be purged before the sampling policy
	// had a chance to evaluate it.
	Dropped
)

// PolicyEvaluator implements a tail-based sampling policy evaluator,
// which makes a sampling decision for a given trace when requested.
type PolicyEvaluator interface {
	// LateArrivingSpans notifies the evaluator that the given list of spans arrived
	// after the sampling decision was already taken for the trace.
	// This gives the evaluator a chance to log any message/metrics and/or update any
	// related internal state.
	OnLateArrivingSpans(earlyDecision Decision, spans []*Spans) error

	// Evaluate looks at the trace data and returns a corresponding SamplingDecision.
	Evaluate(traceID []byte, trace *TraceData) (Decision, error)

	// OnDroppedSpans is called when the trace needs to be dropped, due to memory
	// pressure, before the decision_wait time has been reached.
	OnDroppedSpans(traceID []byte, trace *TraceData) (Decision, error)
}
