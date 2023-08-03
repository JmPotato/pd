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

package endpoint

import (
	"encoding/json"
	"math"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/mcs/utils"
	"go.etcd.io/etcd/clientv3"
	"go.uber.org/zap"
)

// GCSafePointV2 represents the overall safe point for a specific keyspace.
type GCSafePointV2 struct {
	KeyspaceID uint32 `json:"keyspace_id"`
	SafePoint  uint64 `json:"safe_point"`
}

// ServiceSafePointV2 represents a service's safepoint under a specific keyspace.
// Services can post service safe point to prevent gc safe point from incrementing.
type ServiceSafePointV2 struct {
	KeyspaceID uint32 `json:"keyspace_id"`
	ServiceID  string `json:"service_id"`
	ExpiredAt  int64  `json:"expired_at"`
	SafePoint  uint64 `json:"safe_point"`
}

// SafePointV2Storage defines the storage operations on safe point v2.
type SafePointV2Storage interface {
	LoadGCSafePointV2(keyspaceID uint32) (*GCSafePointV2, error)
	SaveGCSafePointV2(gcSafePoint *GCSafePointV2) error

	LoadMinServiceSafePointV2(keyspaceID uint32, now time.Time) (*ServiceSafePointV2, error)
	LoadServiceSafePointV2(keyspaceID uint32, serviceID string) (*ServiceSafePointV2, error)

	SaveServiceSafePointV2(serviceSafePoint *ServiceSafePointV2) error
	RemoveServiceSafePointV2(keyspaceID uint32, serviceID string) error
}

var _ SafePointV2Storage = (*StorageEndpoint)(nil)

// LoadGCSafePointV2 loads gc safe point for the given keyspace.
func (se *StorageEndpoint) LoadGCSafePointV2(keyspaceID uint32) (*GCSafePointV2, error) {
	key := GCSafePointV2Path(keyspaceID)
	value, err := se.Load(key)
	if err != nil {
		return nil, err
	}
	// GC Safe Point does not exist for the given keyspace
	if value == "" {
		return &GCSafePointV2{
			KeyspaceID: keyspaceID,
			SafePoint:  0,
		}, nil
	}
	gcSafePoint := &GCSafePointV2{}
	if err = json.Unmarshal([]byte(value), gcSafePoint); err != nil {
		return nil, errs.ErrJSONUnmarshal.Wrap(err).GenWithStackByCause()
	}
	return gcSafePoint, nil
}

// SaveGCSafePointV2 saves gc safe point for the given keyspace.
func (se *StorageEndpoint) SaveGCSafePointV2(gcSafePoint *GCSafePointV2) error {
	key := GCSafePointV2Path(gcSafePoint.KeyspaceID)
	value, err := json.Marshal(gcSafePoint)
	if err != nil {
		return errs.ErrJSONMarshal.Wrap(err).GenWithStackByCause()
	}
	return se.Save(key, string(value))
}

// LoadMinServiceSafePointV2 returns the minimum safepoint for the given keyspace.
// If no service safe point exist for the given key space or all the service safe points just expired, return nil.
// This also attempt to remove expired service safe point.
func (se *StorageEndpoint) LoadMinServiceSafePointV2(keyspaceID uint32, now time.Time) (*ServiceSafePointV2, error) {
	prefix := ServiceSafePointV2Prefix(keyspaceID)
	prefixEnd := clientv3.GetPrefixRangeEnd(prefix)
	keys, values, err := se.LoadRange(prefix, prefixEnd, 0)
	if err != nil {
		return nil, err
	}
	if len(keys) == 0 {
		return se.initServiceSafePointV2ForGCWorker(keyspaceID, 0)
	}

	hasGCWorker := false
	min := &ServiceSafePointV2{KeyspaceID: keyspaceID, SafePoint: math.MaxUint64}

	// Load global service safe point
	serviceSafePointV1, err := se.loadServiceGCSafePointV1(NativeBRServiceID)
	if serviceSafePointV1 != nil {
		min.KeyspaceID = utils.NullKeyspaceID
		min.SafePoint = serviceSafePointV1.SafePoint
	}

	for i, key := range keys {
		serviceSafePoint := &ServiceSafePointV2{}
		if err = json.Unmarshal([]byte(values[i]), serviceSafePoint); err != nil {
			return nil, err
		}
		if serviceSafePoint.ServiceID == GCWorkerServiceSafePointID {
			hasGCWorker = true
			// If gc_worker's expire time is incorrectly set, fix it.
			if serviceSafePoint.ExpiredAt != math.MaxInt64 {
				serviceSafePoint.ExpiredAt = math.MaxInt64
				err = se.SaveServiceSafePointV2(serviceSafePoint)
				if err != nil {
					return nil, errors.Trace(err)
				}
			}
		}
		if serviceSafePoint.ExpiredAt < now.Unix() {
			if err = se.Remove(key); err != nil {
				log.Warn("failed to remove expired service safe point", zap.Error(err))
			}
			continue
		}
		if serviceSafePoint.SafePoint < min.SafePoint {
			min = serviceSafePoint
		}
	}
	if min.SafePoint == math.MaxUint64 {
		// No service safe point or all of them are expired, set min service safe point to 0 to allow any update
		log.Info("there are no valid service safepoints. init gc_worker's service safepoint to 0")
		return se.initServiceSafePointV2ForGCWorker(keyspaceID, 0)
	}
	if !hasGCWorker {
		// If there exists some service safepoints but gc_worker is missing, init it with the min value among all
		// safepoints (including expired ones)
		return se.initServiceSafePointV2ForGCWorker(keyspaceID, min.SafePoint)
	}
	return min, nil
}

// LoadServiceSafePointV2 returns ServiceSafePointV2 for given keyspaceID and serviceID.
func (se *StorageEndpoint) LoadServiceSafePointV2(keyspaceID uint32, serviceID string) (*ServiceSafePointV2, error) {
	key := ServiceSafePointV2Path(keyspaceID, serviceID)
	value, err := se.Load(key)
	if err != nil {
		return nil, err
	}
	// Service Safe Point does not exist for the given keyspaceID and serviceID
	if value == "" {
		return nil, nil
	}
	serviceSafePoint := &ServiceSafePointV2{}
	if err = json.Unmarshal([]byte(value), serviceSafePoint); err != nil {
		return nil, err
	}
	return serviceSafePoint, nil
}

// LoadGCSafePoint loads current GC safe point from storage.
func (se *StorageEndpoint) loadServiceGCSafePointV1(serviceID string) (*ServiceSafePoint, error) {
	serviceIDPath := GCSafePointServicePrefixPath() + serviceID
	value, err := se.Load(serviceIDPath)
	if err != nil || value == "" {
		return nil, err
	}

	ssp := &ServiceSafePoint{}
	if err := json.Unmarshal([]byte(value), ssp); err != nil {
		return nil, err
	}

	return ssp, nil
}

func (se *StorageEndpoint) initServiceSafePointV2ForGCWorker(keyspaceID uint32, initialValue uint64) (*ServiceSafePointV2, error) {

	// Try to load GC worker Service safe point v2.
	GCWorkerServiceSafePointV2, err := se.LoadServiceSafePointV2(keyspaceID, GCWorkerServiceSafePointID)
	if err != nil {
		return nil, err
	}

	// If gc worker service safe point v2 not exist, load it from gc worker service safe point v1.
	var expectGCWorkerSSP uint64
	if GCWorkerServiceSafePointV2 == nil {
		// Load service safe point from v1.
		gCWorkerSSPV1, err := se.loadServiceGCSafePointV1(GCWorkerServiceSafePointID)
		if err != nil {
			return nil, err
		}
		if gCWorkerSSPV1 != nil {
			expectGCWorkerSSP = gCWorkerSSPV1.SafePoint
			log.Info("Gc worker service safe point v1 exists.", zap.Uint64("expect-gc-worker-ssp", expectGCWorkerSSP))
		} else {
			expectGCWorkerSSP = initialValue
			log.Info("Gc worker service safe point v1 not exists.", zap.Uint64("expect-gc-worker-ssp", expectGCWorkerSSP))
		}
	} else {
		expectGCWorkerSSP = initialValue
		log.Debug("Gc worker service safe point v2 exists.", zap.Uint64("expect-gc-worker-ssp", expectGCWorkerSSP))
	}

	ssp := &ServiceSafePointV2{
		KeyspaceID: keyspaceID,
		ServiceID:  GCWorkerServiceSafePointID,
		SafePoint:  expectGCWorkerSSP,
		ExpiredAt:  math.MaxInt64,
	}
	if err := se.SaveServiceSafePointV2(ssp); err != nil {
		return nil, err
	}
	return ssp, nil
}

// SaveServiceSafePointV2 stores service safe point to etcd.
func (se *StorageEndpoint) SaveServiceSafePointV2(serviceSafePoint *ServiceSafePointV2) error {
	if serviceSafePoint.ServiceID == "" {
		return errors.New("service id of service safepoint cannot be empty")
	}

	if serviceSafePoint.ServiceID == GCWorkerServiceSafePointID && serviceSafePoint.ExpiredAt != math.MaxInt64 {
		return errors.New("TTL of gc_worker's service safe point must be infinity")
	}

	key := ServiceSafePointV2Path(serviceSafePoint.KeyspaceID, serviceSafePoint.ServiceID)
	value, err := json.Marshal(serviceSafePoint)
	if err != nil {
		return errs.ErrJSONMarshal.Wrap(err).GenWithStackByCause()
	}
	return se.Save(key, string(value))
}

// RemoveServiceSafePointV2 removes a service safe point.
func (se *StorageEndpoint) RemoveServiceSafePointV2(keyspaceID uint32, serviceID string) error {
	if serviceID == GCWorkerServiceSafePointID {
		return errors.New("cannot remove service safe point of gc_worker")
	}
	key := ServiceSafePointV2Path(keyspaceID, serviceID)
	return se.Remove(key)
}