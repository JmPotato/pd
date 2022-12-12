package tenant

import (
	"context"
	"math"
	"time"

	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

const defaultRefillRate = 10000

const defaultInitialRUs = 10 * 10000

type TenantTokenBucket struct {
	LastUpdate  time.Time        `json:"last_update"`
	Bucket      State            `json:"bucket_state"`
	Initialized bool             `json:"initialized"`
	Consumption pdpb.Consumption `json:"consumption"`
}

func (t *TenantTokenBucket) Update(now time.Time) {
	if !t.Initialized {
		t.Bucket.RURefillRate = defaultRefillRate
		t.Bucket.RUCurrent = defaultInitialRUs
		t.LastUpdate = now
		t.Initialized = true
		return
	}

	delta := now.Sub(t.LastUpdate)
	if delta > 0 {
		t.Bucket.Update(delta)
		t.LastUpdate = now
	}
}

type State struct {
	RUBurstLimit float64 `json:"r_u_burst_limit"`
	RURefillRate float64 `json:"r_u_refill_rate"`
	RUCurrent    float64 `json:"r_u_current"`
}

func (s *State) Update(since time.Duration) {
	if since > 0 {
		s.RUCurrent += s.RURefillRate * since.Seconds()
	}
}

func (s *State) Request(
	ctx context.Context, req *pdpb.TokenBucketRequest,
) pdpb.TokenBucketResponse {
	var res pdpb.TokenBucketResponse

	res.FallbackRate = s.RURefillRate
	log.Info("token bucket request", zap.Uint64("tenant", req.TenantId), zap.Float64("request", req.RequestedRU), zap.Float64("current", s.RUCurrent), zap.Float64("fill-rate", s.RURefillRate))

	needed := req.RequestedRU
	if needed <= 0 {
		return res
	}

	if s.RUCurrent >= needed {
		s.RUCurrent -= needed
		res.GrantedRU = needed
		log.Info("request granted ", zap.Uint64("tenant", req.TenantId), zap.Float64("granted", needed), zap.Float64("remaining", s.RUCurrent), zap.Float64("rate", res.FallbackRate))
		return res
	}

	var grantedTokens float64

	if s.RUCurrent > 0 {
		grantedTokens = s.RUCurrent
		needed -= s.RUCurrent
	}

	availableRate := s.RURefillRate
	if debt := -s.RUCurrent; debt > 0 {
		debt -= float64(req.GetTargetRequestPeriodSeconds()) * s.RURefillRate
		if debt > 0 {
			debtRate := debt / float64(req.GetTargetRequestPeriodSeconds())
			availableRate -= debtRate
			availableRate = math.Max(availableRate, 0.05*s.RURefillRate)
		}
	}

	allowedRate := availableRate
	duration := time.Duration(float64(time.Second) * (needed / allowedRate))
	if duration <= time.Duration(req.GetTargetRequestPeriodSeconds())*time.Second {
		grantedTokens += needed
	} else {
		// We don't want to plan ahead for more than the target period; give out
		// fewer tokens.
		duration = time.Duration(req.GetTargetRequestPeriodSeconds()) * time.Second
		grantedTokens += allowedRate * duration.Seconds()
	}
	s.RUCurrent -= grantedTokens
	res.GrantedRU = grantedTokens
	res.TrickleDurationSeconds = int64(duration.Seconds())
	log.Info("request granted over time ", zap.Uint64("tenant", req.TenantId), zap.Float64("granted", res.GrantedRU), zap.Int64("trickle", res.TrickleDurationSeconds), zap.Float64("rate", res.FallbackRate))
	return res
}

// Add consumption from the given structure.
func Add(self *pdpb.Consumption, other *pdpb.Consumption) {
	self.RU += other.RU
	self.ReadRequests += other.ReadRequests
	self.ReadBytes += other.ReadBytes
	self.WriteRequests += other.WriteRequests
	self.WriteBytes += other.WriteBytes
	self.PodsCpuSeconds += other.PodsCpuSeconds
	self.KvReadCpuMilliseconds += other.KvReadCpuMilliseconds
	self.KvWriteCpuMilliseconds += other.KvWriteCpuMilliseconds
}

// Sub subtracts consumption, making sure no fields become negative.
func Sub(c *pdpb.Consumption, other *pdpb.Consumption) {
	if c.RU < other.RU {
		c.RU = 0
	} else {
		c.RU -= other.RU
	}

	if c.ReadRequests < other.ReadRequests {
		c.ReadRequests = 0
	} else {
		c.ReadRequests -= other.ReadRequests
	}

	if c.ReadBytes < other.ReadBytes {
		c.ReadBytes = 0
	} else {
		c.ReadBytes -= other.ReadBytes
	}

	if c.WriteRequests < other.WriteRequests {
		c.WriteRequests = 0
	} else {
		c.WriteRequests -= other.WriteRequests
	}

	if c.WriteBytes < other.WriteBytes {
		c.WriteBytes = 0
	} else {
		c.WriteBytes -= other.WriteBytes
	}

	if c.PodsCpuSeconds < other.PodsCpuSeconds {
		c.PodsCpuSeconds = 0
	} else {
		c.PodsCpuSeconds -= other.PodsCpuSeconds
	}

	if c.KvReadCpuMilliseconds < other.KvReadCpuMilliseconds {
		c.KvReadCpuMilliseconds = 0
	} else {
		c.KvReadCpuMilliseconds -= other.KvReadCpuMilliseconds
	}

	if c.KvWriteCpuMilliseconds < other.KvWriteCpuMilliseconds {
		c.KvWriteCpuMilliseconds = 0
	} else {
		c.KvWriteCpuMilliseconds -= other.KvWriteCpuMilliseconds
	}
}
