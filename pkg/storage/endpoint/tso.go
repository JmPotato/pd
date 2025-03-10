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
	"context"
	"strings"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"

	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/keypath"
	"github.com/tikv/pd/pkg/utils/typeutil"
)

// TSOStorage is the interface for timestamp storage.
type TSOStorage interface {
	LoadTimestamp(prefix string) (time.Time, error)
	SaveTimestamp(groupID uint32, ts time.Time) error
	DeleteTimestamp(groupID uint32) error
}

var _ TSOStorage = (*StorageEndpoint)(nil)

// LoadTimestamp will get all time windows of Global TSOs from etcd and return the biggest one.
// TODO: Due to local TSO is deprecated, maybe we do not need to load timestamp
// by prefix, we can just load the timestamp by the key.
func (se *StorageEndpoint) LoadTimestamp(prefix string) (time.Time, error) {
	prefixEnd := clientv3.GetPrefixRangeEnd(prefix)
	keys, values, err := se.LoadRange(prefix, prefixEnd, 0)
	if err != nil {
		return typeutil.ZeroTime, err
	}
	if len(keys) == 0 {
		return typeutil.ZeroTime, nil
	}

	maxTSWindow := typeutil.ZeroTime
	for i, key := range keys {
		key := strings.TrimSpace(key)
		if !strings.HasSuffix(key, "timestamp") {
			continue
		}
		tsWindow, err := typeutil.ParseTimestamp([]byte(values[i]))
		if err != nil {
			log.Error("parse timestamp window that from etcd failed", zap.String("ts-window-key", key), zap.Time("max-ts-window", maxTSWindow), zap.Error(err))
			continue
		}
		if typeutil.SubRealTimeByWallClock(tsWindow, maxTSWindow) > 0 {
			maxTSWindow = tsWindow
		}
	}
	return maxTSWindow, nil
}

// SaveTimestamp saves the timestamp to the storage.
func (se *StorageEndpoint) SaveTimestamp(groupID uint32, ts time.Time) error {
	return se.RunInTxn(context.Background(), func(txn kv.Txn) error {
		value, err := txn.Load(keypath.TimestampPath(groupID))
		if err != nil {
			return err
		}

		previousTS := typeutil.ZeroTime
		if value != "" {
			previousTS, err = typeutil.ParseTimestamp([]byte(value))
			if err != nil {
				log.Error("parse timestamp failed", zap.Uint32("group-id", groupID), zap.String("value", value), zap.Error(err))
				return err
			}
		}
		if previousTS != typeutil.ZeroTime && typeutil.SubRealTimeByWallClock(ts, previousTS) <= 0 {
			return errors.Errorf("saving timestamp %d is less than or equal to the previous one %d", ts.UnixNano(), previousTS.UnixNano())
		}
		data := typeutil.Uint64ToBytes(uint64(ts.UnixNano()))
		return txn.Save(keypath.TimestampPath(groupID), string(data))
	})
}

// DeleteTimestamp deletes the timestamp from the storage.
func (se *StorageEndpoint) DeleteTimestamp(groupID uint32) error {
	return se.RunInTxn(context.Background(), func(txn kv.Txn) error {
		return txn.Remove(keypath.TimestampPath(groupID))
	})
}
