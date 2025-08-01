// Copyright 2025 TiKV Project Authors.
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

package server

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/keyspacepb"
	rmpb "github.com/pingcap/kvproto/pkg/resource_manager"

	bs "github.com/tikv/pd/pkg/basicserver"
	"github.com/tikv/pd/pkg/keyspace/constant"
	"github.com/tikv/pd/pkg/storage"
	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/testutil"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m, testutil.LeakOptions...)
}

type mockConfigProvider struct{ bs.Server }

func (*mockConfigProvider) GetControllerConfig() *ControllerConfig { return &ControllerConfig{} }

func (*mockConfigProvider) AddStartCallback(...func()) {}

func (*mockConfigProvider) AddServiceReadyCallback(...func(context.Context) error) {}

func prepareManager() *Manager {
	storage := storage.NewStorageWithMemoryBackend()
	m := NewManager[*mockConfigProvider](&mockConfigProvider{})
	m.storage = storage
	return m
}

func TestInitManager(t *testing.T) {
	re := require.New(t)
	m := prepareManager()

	re.Empty(m.getKeyspaceResourceGroupManagers())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	err := m.Init(ctx)
	re.NoError(err)
	// There should only be one null keyspace resource group manager.
	keyspaceID := uint32(1)
	krgm := m.getKeyspaceResourceGroupManager(constant.NullKeyspaceID)
	re.NotNil(krgm)
	re.Equal(constant.NullKeyspaceID, krgm.keyspaceID)
	re.Equal(DefaultResourceGroupName, krgm.getMutableResourceGroup(DefaultResourceGroupName).Name)
	// Add a new keyspace resource group manager.
	group := &rmpb.ResourceGroup{
		Name:       "test_group",
		Mode:       rmpb.GroupMode_RUMode,
		Priority:   5,
		KeyspaceId: &rmpb.KeyspaceIDValue{Value: keyspaceID},
	}
	err = m.AddResourceGroup(group)
	re.NoError(err)
	// Adding a new keyspace resource group should create a new keyspace resource group manager.
	krgm = m.getKeyspaceResourceGroupManager(1)
	re.NotNil(krgm)
	re.Equal(group.KeyspaceId.Value, krgm.keyspaceID)
	re.Equal(group.Name, krgm.getMutableResourceGroup(group.Name).Name)
	// A default resource group should be created for the keyspace as well.
	defaultGroup := krgm.getMutableResourceGroup(DefaultResourceGroupName)
	re.Equal(DefaultResourceGroupName, defaultGroup.Name)
	// Modify the default resource group settings.
	defaultGroup.RUSettings.RU.Settings.FillRate = 100
	defaultGroupPb := defaultGroup.IntoProtoResourceGroup(keyspaceID)
	err = m.ModifyResourceGroup(defaultGroupPb)
	re.NoError(err)
	// Rebuild the manager based on the same storage.
	storage := m.storage
	m = NewManager[*mockConfigProvider](&mockConfigProvider{})
	m.storage = storage
	err = m.Init(ctx)
	re.NoError(err)
	re.Len(m.getKeyspaceResourceGroupManagers(), 2)
	// Get the default resource group.
	rg, err := m.GetResourceGroup(1, DefaultResourceGroupName, true)
	re.NoError(err)
	re.NotNil(rg)
	// Verify the default resource group settings are updated. This is to ensure the default resource group
	// can be loaded from the storage correctly rather than created as a new one.
	re.Equal(defaultGroup.getFillRate(), rg.getFillRate())
}

func TestBackgroundMetricsFlush(t *testing.T) {
	re := require.New(t)
	m := prepareManager()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	err := m.Init(ctx)
	re.NoError(err)
	// Test without keyspace ID
	checkBackgroundMetricsFlush(ctx, re, m, nil)
	// Test with keyspace ID
	checkBackgroundMetricsFlush(ctx, re, m, &rmpb.KeyspaceIDValue{Value: 1})
}

func checkBackgroundMetricsFlush(ctx context.Context, re *require.Assertions, manager *Manager, keyspaceIDValue *rmpb.KeyspaceIDValue) {
	// Prepare the keyspace name for later lookup.
	prepareKeyspaceName(ctx, re, manager, keyspaceIDValue, "test_keyspace")
	// Add a resource group.
	group := &rmpb.ResourceGroup{
		Name:     "test_group",
		Mode:     rmpb.GroupMode_RUMode,
		Priority: 5,
		RUSettings: &rmpb.GroupRequestUnitSettings{
			RU: &rmpb.TokenBucket{
				Settings: &rmpb.TokenLimitSettings{
					FillRate:   100,
					BurstLimit: 200,
				},
			},
		},
		KeyspaceId: keyspaceIDValue,
	}
	err := manager.AddResourceGroup(group)
	re.NoError(err)

	// Send consumption to the dispatcher.
	req := &rmpb.TokenBucketRequest{
		ResourceGroupName: group.GetName(),
		ConsumptionSinceLastRequest: &rmpb.Consumption{
			RRU: 10.0,
			WRU: 20.0,
		},
		KeyspaceId: keyspaceIDValue,
	}
	err = manager.dispatchConsumption(req)
	re.NoError(err)

	keyspaceID := ExtractKeyspaceID(req.GetKeyspaceId())
	// Verify consumption was added to the resource group.
	testutil.Eventually(re, func() bool {
		updatedGroup, err := manager.GetResourceGroup(keyspaceID, req.GetResourceGroupName(), true)
		re.NoError(err)
		re.NotNil(updatedGroup)
		return updatedGroup.RUConsumption.RRU == req.ConsumptionSinceLastRequest.RRU &&
			updatedGroup.RUConsumption.WRU == req.ConsumptionSinceLastRequest.WRU
	})
}

// Put a keyspace meta into the storage.
func prepareKeyspaceName(ctx context.Context, re *require.Assertions, manager *Manager, keyspaceIDValue *rmpb.KeyspaceIDValue, keyspaceName string) {
	keyspaceMeta := &keyspacepb.KeyspaceMeta{
		Id:   ExtractKeyspaceID(keyspaceIDValue),
		Name: keyspaceName,
	}
	err := manager.storage.RunInTxn(ctx, func(txn kv.Txn) error {
		err := manager.storage.SaveKeyspaceMeta(txn, keyspaceMeta)
		if err != nil {
			return err
		}
		return manager.storage.SaveKeyspaceID(txn, keyspaceMeta.Id, keyspaceMeta.Name)
	})
	re.NoError(err)
}

func TestAddAndModifyResourceGroup(t *testing.T) {
	re := require.New(t)

	storage := storage.NewStorageWithMemoryBackend()
	m := NewManager[*mockConfigProvider](&mockConfigProvider{})
	m.storage = storage

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	err := m.Init(ctx)
	re.NoError(err)

	// Test without keyspace ID
	checkAddAndModifyResourceGroup(re, m, nil)
	// Test with keyspace ID
	checkAddAndModifyResourceGroup(re, m, &rmpb.KeyspaceIDValue{Value: 1})
}

func checkAddAndModifyResourceGroup(re *require.Assertions, manager *Manager, keyspaceIDValue *rmpb.KeyspaceIDValue) {
	group := &rmpb.ResourceGroup{
		Name:     "test_group",
		Mode:     rmpb.GroupMode_RUMode,
		Priority: 5,
		RUSettings: &rmpb.GroupRequestUnitSettings{
			RU: &rmpb.TokenBucket{
				Settings: &rmpb.TokenLimitSettings{
					FillRate:   100,
					BurstLimit: 200,
				},
			},
		},
		KeyspaceId: keyspaceIDValue,
	}
	err := manager.AddResourceGroup(group)
	re.NoError(err)

	group.Priority = 10
	group.RUSettings.RU.Settings.BurstLimit = 300
	err = manager.ModifyResourceGroup(group)
	re.NoError(err)

	keyspaceID := ExtractKeyspaceID(keyspaceIDValue)
	testutil.Eventually(re, func() bool {
		rg, err := manager.GetResourceGroup(keyspaceID, group.Name, true)
		re.NoError(err)
		re.NotNil(rg)
		return rg.Priority == group.Priority &&
			rg.RUSettings.RU.getBurstLimitSetting() == group.RUSettings.RU.Settings.BurstLimit
	})
}

func TestCleanUpTicker(t *testing.T) {
	re := require.New(t)
	m := prepareManager()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// Put a keyspace meta.
	keyspaceID := uint32(1)
	prepareKeyspaceName(ctx, re, m, &rmpb.KeyspaceIDValue{Value: keyspaceID}, "test_keyspace")
	// Insert two consumption records manually.
	m.metrics.consumptionRecordMap[consumptionRecordKey{
		keyspaceID: keyspaceID,
		groupName:  "test_group_1",
		ruType:     defaultTypeLabel,
	}] = time.Now().Add(-metricsCleanupTimeout * 2)
	m.metrics.consumptionRecordMap[consumptionRecordKey{
		keyspaceID: keyspaceID,
		groupName:  "test_group_2",
		ruType:     defaultTypeLabel,
	}] = time.Now().Add(-metricsCleanupTimeout / 2)
	// Start the background metrics flush loop.
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/mcs/resourcemanager/server/fastCleanupTicker", `return(true)`))
	defer func() {
		re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/mcs/resourcemanager/server/fastCleanupTicker"))
	}()
	err := m.Init(ctx)
	re.NoError(err)
	// Ensure the cleanup ticker is triggered.
	time.Sleep(200 * time.Millisecond)
	// Close the manager to avoid the data race.
	m.close()

	re.Len(m.metrics.consumptionRecordMap, 1)
	re.Contains(m.metrics.consumptionRecordMap, consumptionRecordKey{
		keyspaceID: keyspaceID,
		groupName:  "test_group_2",
		ruType:     defaultTypeLabel,
	})
	keyspaceName, err := m.getKeyspaceNameByID(ctx, keyspaceID)
	re.NoError(err)
	re.Equal("test_keyspace", keyspaceName)
}

func TestKeyspaceServiceLimit(t *testing.T) {
	re := require.New(t)

	storage := storage.NewStorageWithMemoryBackend()
	m := NewManager[*mockConfigProvider](&mockConfigProvider{})
	m.storage = storage

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	err := m.Init(ctx)
	re.NoError(err)
	// Test the default service limit is 0.0.
	limiter := m.GetKeyspaceServiceLimiter(constant.NullKeyspaceID)
	re.NotNil(limiter)
	re.Equal(0.0, limiter.ServiceLimit)
	re.Equal(0.0, limiter.AvailableTokens)
	group := &rmpb.ResourceGroup{
		Name:     "test_group",
		Mode:     rmpb.GroupMode_RUMode,
		Priority: 5,
		RUSettings: &rmpb.GroupRequestUnitSettings{
			RU: &rmpb.TokenBucket{
				Settings: &rmpb.TokenLimitSettings{
					FillRate:   100,
					BurstLimit: 200,
				},
			},
		},
		KeyspaceId: &rmpb.KeyspaceIDValue{Value: 1},
	}
	// Test the limiter of the non-existing keyspace is nil.
	limiter = m.GetKeyspaceServiceLimiter(group.KeyspaceId.Value)
	re.Nil(limiter)
	// Test the limiter of the newly created keyspace is 0.0.
	err = m.AddResourceGroup(group)
	re.NoError(err)
	limiter = m.GetKeyspaceServiceLimiter(1)
	re.Equal(0.0, limiter.ServiceLimit)
	re.Equal(0.0, limiter.AvailableTokens)
	// Test set the service limit of the keyspace.
	m.SetKeyspaceServiceLimit(1, 100.0)
	limiter = m.GetKeyspaceServiceLimiter(1)
	re.Equal(100.0, limiter.ServiceLimit)
	re.Equal(0.0, limiter.AvailableTokens) // When setting from 0 to positive, available tokens remain 0
	// Test set the service limit of the non-existing keyspace.
	limiter = m.GetKeyspaceServiceLimiter(2)
	re.Nil(limiter)
	m.SetKeyspaceServiceLimit(2, 100.0)
	limiter = m.GetKeyspaceServiceLimiter(2)
	re.Equal(100.0, limiter.ServiceLimit)
	re.Equal(0.0, limiter.AvailableTokens)
	// Ensure the keyspace resource group manager is initialized correctly.
	krgm := m.getKeyspaceResourceGroupManager(2)
	re.NotNil(krgm)
	re.Equal(uint32(2), krgm.keyspaceID)
	re.Equal(DefaultResourceGroupName, krgm.getMutableResourceGroup(DefaultResourceGroupName).Name)
}

func TestKeyspaceNameLookup(t *testing.T) {
	re := require.New(t)
	m := prepareManager()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	err := m.Init(ctx)
	re.NoError(err)
	// Get the null keyspace ID by an empty name.
	idValue, err := m.GetKeyspaceIDByName(ctx, "")
	re.NoError(err)
	re.NotNil(idValue)
	re.Equal(constant.NullKeyspaceID, idValue.Value)
	// Get the non-existing keyspace ID by name.
	idValue, err = m.GetKeyspaceIDByName(ctx, "non-existing-keyspace")
	re.Error(err)
	re.Nil(idValue)
	// Get the null keyspace name.
	name, err := m.getKeyspaceNameByID(ctx, constant.NullKeyspaceID)
	re.NoError(err)
	re.Empty(name)
	// Get the non-existing keyspace name.
	name, err = m.getKeyspaceNameByID(ctx, 1)
	re.Error(err)
	re.Empty(name)
	// Get the keyspace ID by name first, then get the keyspace name by ID.
	prepareKeyspaceName(ctx, re, m, &rmpb.KeyspaceIDValue{Value: 1}, "test_keyspace")
	idValue, err = m.GetKeyspaceIDByName(ctx, "test_keyspace")
	re.NoError(err)
	re.NotNil(idValue)
	re.Equal(uint32(1), idValue.Value)
	name, err = m.getKeyspaceNameByID(ctx, 1)
	re.NoError(err)
	re.Equal("test_keyspace", name)
	// Get the keyspace name by ID first, then get the keyspace ID by name.
	prepareKeyspaceName(ctx, re, m, &rmpb.KeyspaceIDValue{Value: 2}, "test_keyspace_2")
	name, err = m.getKeyspaceNameByID(ctx, 2)
	re.NoError(err)
	re.Equal("test_keyspace_2", name)
	idValue, err = m.GetKeyspaceIDByName(ctx, "test_keyspace_2")
	re.NoError(err)
	re.NotNil(idValue)
	re.Equal(uint32(2), idValue.Value)
}

func TestResourceGroupPersistence(t *testing.T) {
	re := require.New(t)
	m := prepareManager()

	// Prepare the resource group and service limit.
	group := &rmpb.ResourceGroup{
		Name:       "test_group",
		Mode:       rmpb.GroupMode_RUMode,
		Priority:   5,
		KeyspaceId: &rmpb.KeyspaceIDValue{Value: 1},
	}
	err := m.AddResourceGroup(group)
	re.NoError(err)
	keyspaceID := ExtractKeyspaceID(group.KeyspaceId)
	m.SetKeyspaceServiceLimit(keyspaceID, 100.0)

	// Use the same storage to rebuild a manager.
	storage := m.storage
	m = NewManager[*mockConfigProvider](&mockConfigProvider{})
	m.storage = storage
	// Initialize the manager.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	err = m.Init(ctx)
	re.NoError(err)
	// Check the resource group is loaded from the storage.
	rg, err := m.GetResourceGroup(keyspaceID, group.Name, true)
	re.NoError(err)
	re.NotNil(rg)
	re.Equal(group.Name, rg.Name)
	re.Equal(group.Mode, rg.Mode)
	re.Equal(group.Priority, rg.Priority)
	// Check the service limit is loaded from the storage.
	limiter := m.GetKeyspaceServiceLimiter(keyspaceID)
	re.NotNil(limiter)
	re.Equal(100.0, limiter.ServiceLimit)
	// Null keyspace ID should have a default zero service limit.
	limiter = m.GetKeyspaceServiceLimiter(constant.NullKeyspaceID)
	re.NotNil(limiter)
	re.Equal(0.0, limiter.ServiceLimit)
	// Non-existing keyspace should have a nil limiter.
	limiter = m.GetKeyspaceServiceLimiter(2)
	re.Nil(limiter)
}
