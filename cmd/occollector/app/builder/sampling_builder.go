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

package builder

import (
	"time"

	"github.com/spf13/viper"
)

const (
	modeTag     string = "mode"
	policiesTag        = "policies"
	samplingTag        = "sampling"
)

// Mode indicates the sampling mode
type Mode string

const (
	// NoSampling mode is the default and means that all data arriving at the collector
	// is passed ahead.
	NoSampling Mode = "no-sampling"
	// TailSampling is the mode in which trace data is temporarily retained until an evaluation
	// if the trace should be sampled is performed.
	TailSampling = "tail"
)

// PolicyType indicates the type of sampling policy.
type PolicyType string

const (
	// AlwaysSamplePolicy samples all traces, typically used for debugging.
	AlwaysSamplePolicy PolicyType = "always-sample"
	// ProbabilisticPolicy sample traces according to a given probability.
	ProbabilisticPolicy = "probabilistic"
	// OpenTracingHTTPErrorPolicy sample traces that report http.status_code > 399.
	OpenTracingHTTPErrorPolicy = "attribute-filter"
)

// PolicyCfg holds the common configuration to all policies.
type PolicyCfg struct {
	// Type of the policy this will be used to match the proper configuration of the policy.
	Type PolicyType
	// Exporter holds the exporter that the policy evaluator is going to be used to
	// make decisions about sending, or not, the traces.
	Exporter string `mapstructure:"exporter"`
	// Configuration holds the settings specific to the policy.
	Configuration interface{}
}

// SamplingCfg holds the sampling configuration.
type SamplingCfg struct {
	Mode     Mode         `mapstructure:"mode"`
	Policies []*PolicyCfg `mapstructure:"policies"`
}

// NewDefaultSamplingCfg creates a SamplingCfg with the default values.
func NewDefaultSamplingCfg() *SamplingCfg {
	return &SamplingCfg{
		Mode: NoSampling,
	}
}

// InitFromViper initializes SamplingCfg with properties from viper.
func (sCfg *SamplingCfg) InitFromViper(v *viper.Viper) *SamplingCfg {
	sv := v.Sub(samplingTag)
	if sv == nil {
		return sCfg
	}

	sCfg.Mode = Mode(sv.GetString(modeTag))

	pv := sv.Sub(policiesTag)
	if pv == nil {
		return sCfg
	}

	for policyType := range sv.GetStringMap(policiesTag) {
		polSub := pv.Sub(policyType)
		polCfg := &PolicyCfg{}
		polCfg.Type = PolicyType(policyType)
		polCfg.Exporter = polSub.GetString("exporter")
		sCfg.Policies = append(sCfg.Policies, polCfg)
	}
	return sCfg
}

// TailBasedCfg holds the configuration for tail-based sampling.
type TailBasedCfg struct {
	DecisionWait time.Duration `mapstructure:"decision_wait"`
	NumTraces    uint64        `mapstructure:"num_traces"`
}

// NewDefaultTailBasedCfg creates a TailBasedCfg with the default values.
func NewDefaultTailBasedCfg() *TailBasedCfg {
	return &TailBasedCfg{
		DecisionWait: 30 * time.Second,
		NumTraces:    5000,
	}
}

// InitFromViper initializes TailBasedCfg with properties from viper.
func (tCfg *TailBasedCfg) InitFromViper(v *viper.Viper) *TailBasedCfg {
	tv := v.Sub(samplingTag)
	if tv == nil {
		return tCfg
	}
	if tv == nil || tv.GetString(modeTag) != TailSampling {
		return tCfg
	}

	tv.Unmarshal(tCfg)
	return tCfg
}
