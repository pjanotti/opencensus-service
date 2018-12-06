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
	// NumericTagFilter sample traces that have a given numberic tag in a specified
	// range, e.g.: tag "http.status_code" >= 399 and <= 999.
	NumericTagFilter = "numeric-tag-filter"
	// StringTagFilter sample traces that a tag, of type string, matching
	// one of the listed values.
	StringTagFilter = "string-tag-filter"
	// RateLimiting allows all traces until the specified limits are satisfied.
	RateLimiting = "rate-limiting"
)

// PolicyCfg holds the common configuration to all policies.
type PolicyCfg struct {
	// Name given to the instance of the policy to make easy to identify it in metrics and logs.
	Name string
	// Type of the policy this will be used to match the proper configuration of the policy.
	Type PolicyType
	// Exporters holds the name of exporters that the policy evaluator is going to be used to
	// make decisions about sending, or not, the traces.
	Exporters []string
	// Configuration holds the settings specific to the policy.
	Configuration interface{}
}

// NumericTagFilterCfg holds the configurable settings to create a numeric tag filter
// sampling policy evaluator.
type NumericTagFilterCfg struct {
	// Tag that the filter is going to be matching against.
	Tag string `mapstructure:"tag"`
	// MinValue is the minimum value of the tag to be considered a match.
	MinValue int64 `mapstructure:"min-value"`
	// MaxValue is the maximum value of the tag to be considered a match.
	MaxValue int64 `mapstructure:"max-value"`
}

// StringTagFilterCfg holds the configurable settings to create a string tag filter
// sampling policy evaluator.
type StringTagFilterCfg struct {
	// Tag that the filter is going to be matching against.
	Tag string `mapstructure:"tag"`
	// Values is the set of tag values that if any is equal to the actual tag valueto be considered a match.
	Values []string `mapstructure:"values"`
}

// RateLimitingCfg holds the configurable settings to create a string tag filter
// sampling policy evaluator.
type RateLimitingCfg struct {
	// SpansPerSecond limit to the number of spans per second
	SpansPerSecond int64 `mapstructure:"spans-per-second"`
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

	for policyName := range sv.GetStringMap(policiesTag) {
		polSub := pv.Sub(policyName)
		polCfg := &PolicyCfg{}
		polCfg.Name = policyName
		polCfg.Type = PolicyType(polSub.GetString("policy"))
		polCfg.Exporters = polSub.GetStringSlice("exporters")

		cfgSub := polSub.Sub("configuration")
		if cfgSub != nil {
			// As the number of polices grow this likely should be in a map.
			var cfg interface{}
			switch polCfg.Type {
			case NumericTagFilter:
				numTagFilterCfg := &NumericTagFilterCfg{}
				cfg = numTagFilterCfg
			case StringTagFilter:
				strTagFilterCfg := &StringTagFilterCfg{}
				cfg = strTagFilterCfg
			case RateLimiting:
				rateLimitingCfg := &RateLimitingCfg{}
				cfg = rateLimitingCfg
			}
			cfgSub.Unmarshal(cfg)
			polCfg.Configuration = cfg
		}

		sCfg.Policies = append(sCfg.Policies, polCfg)
	}
	return sCfg
}

// TailBasedCfg holds the configuration for tail-based sampling.
type TailBasedCfg struct {
	DecisionWait time.Duration `mapstructure:"decision-wait"`
	NumTraces    uint64        `mapstructure:"num-traces"`
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