// Copyright 2023 TiKV Project Authors.
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

// The MIT License (MIT)
// Copyright (c) 2022 go-kratos Project Authors.

package window

import (
	"time"
)

// Metric is a sample interface.
// Implementations of Metrics in metric package are Counter, Gauge,
// PointGauge, RollingCounter and RollingGauge.
type Metric interface {
	// Add adds the given value to the counter.
	Add(int64)
	// Value gets the current value.
	// If the metric's type is PointGauge, RollingCounter, RollingGauge,
	// it returns the sum value within the window.
	Value() int64
}

// Aggregation contains some common aggregation function.
// Each aggregation can compute summary statistics of window.
type Aggregation interface {
	// Min finds the min value within the window.
	Min() float64
	// Max finds the max value within the window.
	Max() float64
	// Avg computes average value within the window.
	Avg() float64
	// Sum computes sum value within the window.
	Sum() float64
}

// RollingCounter represents a ring window based on time duration.
// e.g. [[1], [3], [5]]
type RollingCounter interface {
	Metric
	Aggregation

	Timespan() int
	// Reduce applies the reduction function to all buckets within the window.
	Reduce(func(Iterator) float64) float64
}

// RollingCounterOpts contains the arguments for creating RollingCounter.
type RollingCounterOpts struct {
	Size           int
	BucketDuration time.Duration
}

type rollingCounter struct {
	policy *RollingPolicy
}

// NewRollingCounter creates a new RollingCounter bases on RollingCounterOpts.
func NewRollingCounter(opts RollingCounterOpts) RollingCounter {
	window := NewWindow(Options{Size: opts.Size})
	policy := NewRollingPolicy(window, RollingPolicyOpts{BucketDuration: opts.BucketDuration})
	return &rollingCounter{
		policy: policy,
	}
}

// Add adds the given value to the counter.
func (r *rollingCounter) Add(val int64) {
	r.policy.Add(float64(val))
}

// Reduce applies the reduction function to all buckets within the window.
func (r *rollingCounter) Reduce(f func(Iterator) float64) float64 {
	return r.policy.Reduce(f)
}

// Avg computes average value within the window.
func (r *rollingCounter) Avg() float64 {
	return r.policy.Reduce(Avg)
}

// Min finds the min value within the window.
func (r *rollingCounter) Min() float64 {
	return r.policy.Reduce(Min)
}

// Max finds the max value within the window.
func (r *rollingCounter) Max() float64 {
	return r.policy.Reduce(Max)
}

// Sum computes sum value within the window.
func (r *rollingCounter) Sum() float64 {
	return r.policy.Reduce(Sum)
}

// Value gets the current value.
func (r *rollingCounter) Value() int64 {
	return int64(r.Sum())
}

// Timespan returns passed bucket number since lastAppendTime,
// if it is one bucket duration earlier than the last recorded
// time, it will return the size.
func (r *rollingCounter) Timespan() int {
	r.policy.mu.RLock()
	defer r.policy.mu.RUnlock()
	return r.policy.timespan()
}
