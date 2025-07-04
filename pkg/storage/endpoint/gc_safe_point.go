// Copyright 2022 TiKV Project Authors.
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
	"strconv"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"

	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/keyspace/constant"
	"github.com/tikv/pd/pkg/utils/keypath"
)

// WARNING: The content of this file is going to be deprecated and replaced by `gc_states.go`.

// GCSafePointStorage defines the storage operations on the GC safe point.
type GCSafePointStorage interface {
	LoadGCSafePoint() (uint64, error)
	SaveGCSafePoint(safePoint uint64) error
	LoadMinServiceGCSafePoint(now time.Time) (*ServiceSafePoint, error)
	LoadAllServiceGCSafePoints() ([]*ServiceSafePoint, error)
	SaveServiceGCSafePoint(ssp *ServiceSafePoint) error
	RemoveServiceGCSafePoint(serviceID string) error
}

var _ GCSafePointStorage = (*StorageEndpoint)(nil)

// LoadGCSafePoint loads current GC safe point from storage.
func (se *StorageEndpoint) LoadGCSafePoint() (uint64, error) {
	value, err := se.Load(keypath.GCSafePointPath(constant.NullKeyspaceID))
	if err != nil || value == "" {
		return 0, err
	}
	safePoint, err := strconv.ParseUint(value, 16, 64)
	if err != nil {
		return 0, errs.ErrStrconvParseUint.Wrap(err).GenWithStackByArgs()
	}
	return safePoint, nil
}

// SaveGCSafePoint saves new GC safe point to storage.
func (se *StorageEndpoint) SaveGCSafePoint(safePoint uint64) error {
	value := strconv.FormatUint(safePoint, 16)
	return se.Save(keypath.GCSafePointPath(constant.NullKeyspaceID), value)
}

// LoadMinServiceGCSafePoint returns the minimum safepoint across all services
func (se *StorageEndpoint) LoadMinServiceGCSafePoint(now time.Time) (*ServiceSafePoint, error) {
	prefix := keypath.ServiceGCSafePointPrefix()
	prefixEnd := clientv3.GetPrefixRangeEnd(prefix)
	keys, values, err := se.LoadRange(prefix, prefixEnd, 0)
	if err != nil {
		return nil, err
	}
	if len(keys) == 0 {
		// There's no service safepoint. It may be a new cluster, or upgraded from an older version where all service
		// safepoints are missing. For the second case, we have no way to recover it. Store an initial value 0 for
		// gc_worker.
		return se.initServiceGCSafePointForGCWorker()
	}

	hasGCWorker := false
	min := &ServiceSafePoint{SafePoint: math.MaxUint64}
	for i, key := range keys {
		ssp := &ServiceSafePoint{}
		if err := json.Unmarshal([]byte(values[i]), ssp); err != nil {
			return nil, err
		}
		if ssp.ServiceID == keypath.GCWorkerServiceSafePointID {
			hasGCWorker = true
			// If gc_worker's expire time is incorrectly set, fix it.
			if ssp.ExpiredAt != math.MaxInt64 {
				ssp.ExpiredAt = math.MaxInt64
				err = se.SaveServiceGCSafePoint(ssp)
				if err != nil {
					return nil, errors.Trace(err)
				}
			}
		}

		if ssp.ExpiredAt < now.Unix() {
			if err := se.Remove(key); err != nil {
				log.Error("failed to remove expired service safepoint", errs.ZapError(err))
			}
			continue
		}
		if ssp.SafePoint < min.SafePoint {
			min = ssp
		}
	}

	if min.SafePoint == math.MaxUint64 {
		// There's no valid safepoints and we have no way to recover it. Just set gc_worker to 0.
		log.Info("there are no valid service safepoints. init gc_worker's service safepoint to 0")
		return se.initServiceGCSafePointForGCWorker()
	}

	if !hasGCWorker {
		// If there exists some service safepoints but gc_worker is missing, init it with the min value among all
		// safepoints (including expired ones)
		return se.initServiceGCSafePointForGCWorker()
	}

	return min, nil
}

func (se *StorageEndpoint) initServiceGCSafePointForGCWorker() (*ServiceSafePoint, error) {
	// Temporary solution:
	// Use the txn safe point as the initial value of gc_worker.
	txnSafePoint, err := se.GetGCStateProvider().LoadTxnSafePoint(constant.NullKeyspaceID)
	if err != nil {
		return nil, err
	}
	ssp := &ServiceSafePoint{
		ServiceID: keypath.GCWorkerServiceSafePointID,
		SafePoint: txnSafePoint,
		ExpiredAt: math.MaxInt64,
	}
	if err := se.SaveServiceGCSafePoint(ssp); err != nil {
		return nil, err
	}
	return ssp, nil
}

// LoadAllServiceGCSafePoints returns all services GC safepoints
func (se *StorageEndpoint) LoadAllServiceGCSafePoints() ([]*ServiceSafePoint, error) {
	prefix := keypath.ServiceGCSafePointPrefix()
	prefixEnd := clientv3.GetPrefixRangeEnd(prefix)
	keys, values, err := se.LoadRange(prefix, prefixEnd, 0)
	if err != nil {
		return nil, err
	}
	if len(keys) == 0 {
		return []*ServiceSafePoint{}, nil
	}

	ssps := make([]*ServiceSafePoint, 0, len(keys))
	for i := range keys {
		ssp := &ServiceSafePoint{}
		if err := json.Unmarshal([]byte(values[i]), ssp); err != nil {
			return nil, err
		}
		ssps = append(ssps, ssp)
	}

	return ssps, nil
}

// SaveServiceGCSafePoint saves a GC safepoint for the service
func (se *StorageEndpoint) SaveServiceGCSafePoint(ssp *ServiceSafePoint) error {
	if ssp.ServiceID == "" {
		return errors.New("service id of service safepoint cannot be empty")
	}

	if ssp.ServiceID == keypath.GCWorkerServiceSafePointID && ssp.ExpiredAt != math.MaxInt64 {
		return errors.New("TTL of gc_worker's service safe point must be infinity")
	}

	return se.saveJSON(keypath.ServiceGCSafePointPath(ssp.ServiceID), ssp)
}

// RemoveServiceGCSafePoint removes a GC safepoint for the service
func (se *StorageEndpoint) RemoveServiceGCSafePoint(serviceID string) error {
	if serviceID == keypath.GCWorkerServiceSafePointID {
		return errors.New("cannot remove service safe point of gc_worker")
	}
	key := keypath.ServiceGCSafePointPath(serviceID)
	return se.Remove(key)
}
