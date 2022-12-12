package server

import (
	"context"
	"fmt"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/tenant"
	"github.com/tikv/pd/server/storage/endpoint"
	"go.uber.org/zap"
)

func (s *Server) TokenBucketRequest(
	ctx context.Context, request *pdpb.TokenBucketRequest) (*pdpb.TokenBucketResponse, error) {
	tenantID := request.GetTenantId()
	if tenantID == 0 {
		return nil, errors.New("token bucket request for system tenant")

	}
	instanceFingerprint := request.GetInstanceFingerprint()
	if len(instanceFingerprint) == 0 {
		return nil, errors.New(fmt.Sprintf("invalid instance fingerprint %s", instanceFingerprint))
	}
	if request.RequestedRU < 0 {
		return nil, errors.Errorf("negative requested RUs")
	}

	result := &pdpb.TokenBucketResponse{}
	var consumption pdpb.Consumption

	*result = pdpb.TokenBucketResponse{}

	tenantConfigStorage := s.GetStorage().(endpoint.TenantStorage)
	// TODO: add instance fingerprint?
	tenantInfo, err := tenantConfigStorage.LoadTenantTokenBucket(tenantID)
	if err != nil {
		return nil, err
	}
	log.Info("token bucket request", zap.Uint64("tenant", tenantID), zap.Any("request", request), zap.Any("tenantInfo", tenantInfo))

	now := time.Now()
	tenantInfo.Update(now)

	tenant.Add(&tenantInfo.Consumption, &request.ConsumptionSinceLastRequest)
	consumption = tenantInfo.Consumption

	*result = tenantInfo.Bucket.Request(ctx, request)

	err1 := tenantConfigStorage.SaveTenantTokenBucket(tenantID, tenantInfo)
	if err1 != nil {
		log.Error("save tenant token bucket failed", zap.Uint64("tenant", tenantID), zap.Error(err1))
	}

	// TODO: add metrics of tenant consumption.
	tenantConsumption.WithLabelValues(fmt.Sprintf("%d", tenantID), "total_ru").Set(consumption.GetRU())
	tenantConsumption.WithLabelValues(fmt.Sprintf("%d", tenantID), "total_read_request").Set(float64(consumption.GetReadRequests()))
	tenantConsumption.WithLabelValues(fmt.Sprintf("%d", tenantID), "total_write_request").Set(float64(consumption.GetWriteRequests()))
	tenantConsumption.WithLabelValues(fmt.Sprintf("%d", tenantID), "total_read_bytes").Set(float64(consumption.GetReadBytes()))
	tenantConsumption.WithLabelValues(fmt.Sprintf("%d", tenantID), "total_write_bytes").Set(float64(consumption.GetWriteBytes()))
	tenantConsumption.WithLabelValues(fmt.Sprintf("%d", tenantID), "total_cpu_seconds").Set(float64(consumption.GetPodsCpuSeconds()))
	tenantConsumption.WithLabelValues(fmt.Sprintf("%d", tenantID), "total_kv_read_cpu_milliseconds").Set(float64(consumption.GetKvReadCpuMilliseconds()))
	tenantConsumption.WithLabelValues(fmt.Sprintf("%d", tenantID), "total_kv_write_cpu_milliseconds").Set(float64(consumption.GetKvWriteCpuMilliseconds()))

	TenantTokenBucketState.WithLabelValues(fmt.Sprintf("%d", tenantID), "ru_token_reamining").Set(float64(tenantInfo.Bucket.RUCurrent))
	TenantTokenBucketState.WithLabelValues(fmt.Sprintf("%d", tenantID), "ru_token_refillRate").Set(float64(tenantInfo.Bucket.RURefillRate))

	return result, nil
}
