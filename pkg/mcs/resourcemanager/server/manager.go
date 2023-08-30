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

package server

import (
	"context"
	"encoding/json"
	"math"
	"sort"
	"sync"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/pingcap/failpoint"
	rmpb "github.com/pingcap/kvproto/pkg/resource_manager"
	"github.com/pingcap/log"
	bs "github.com/tikv/pd/pkg/basicserver"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/logutil"
	"go.uber.org/zap"
)

const (
	defaultConsumptionChanSize = 1024
	metricsCleanupInterval     = time.Minute
	metricsCleanupTimeout      = 20 * time.Minute
	metricsAvailableRUInterval = 30 * time.Second

	reservedDefaultGroupName = "default"
	middlePriority           = 8
)

// Manager is the manager of resource group.
type Manager struct {
	sync.RWMutex
	srv              bs.Server
	controllerConfig *ControllerConfig
	groups           map[string]*ResourceGroup
	storage          endpoint.ResourceGroupStorage
	// consumptionChan is used to send the consumption
	// info to the background metrics flusher.
	consumptionDispatcher chan struct {
		resourceGroupName string
		*rmpb.Consumption
		isBackground bool
	}
	// record update time of each resource group
	consumptionRecord map[string]time.Time
}

// ConfigProvider is used to get resource manager config from the given
// `bs.server` without modifying its interface.
type ConfigProvider interface {
	GetControllerConfig() *ControllerConfig
}

// NewManager returns a new manager base on the given server,
// which should implement the `ConfigProvider` interface.
func NewManager[T ConfigProvider](srv bs.Server) *Manager {
	m := &Manager{
		controllerConfig: srv.(T).GetControllerConfig(),
		groups:           make(map[string]*ResourceGroup),
		consumptionDispatcher: make(chan struct {
			resourceGroupName string
			*rmpb.Consumption
			isBackground bool
		}, defaultConsumptionChanSize),
		consumptionRecord: make(map[string]time.Time),
	}
	// The first initialization after the server is started.
	srv.AddStartCallback(func() {
		log.Info("resource group manager starts to initialize", zap.String("name", srv.Name()))
		m.storage = endpoint.NewStorageEndpoint(
			kv.NewEtcdKVBase(srv.GetClient(), "resource_group"),
			nil,
		)
		m.srv = srv
	})
	// The second initialization after becoming serving.
	srv.AddServiceReadyCallback(m.Init)
	return m
}

// GetBasicServer returns the basic server.
func (m *Manager) GetBasicServer() bs.Server {
	return m.srv
}

// Init initializes the resource group manager.
func (m *Manager) Init(ctx context.Context) {
	// Todo: If we can modify following configs in the future, we should reload these configs.
	// Store the controller config into the storage.
	m.storage.SaveControllerConfig(m.controllerConfig)
	// Load resource group meta info from storage.
	m.groups = make(map[string]*ResourceGroup)
	handler := func(k, v string) {
		group := &rmpb.ResourceGroup{}
		if err := proto.Unmarshal([]byte(v), group); err != nil {
			log.Error("err", zap.Error(err), zap.String("k", k), zap.String("v", v))
			panic(err)
		}
		m.groups[group.Name] = FromProtoResourceGroup(group)
	}
	m.storage.LoadResourceGroupSettings(handler)
	// Load resource group states from storage.
	tokenHandler := func(k, v string) {
		tokens := &GroupStates{}
		if err := json.Unmarshal([]byte(v), tokens); err != nil {
			log.Error("err", zap.Error(err), zap.String("k", k), zap.String("v", v))
			panic(err)
		}
		if group, ok := m.groups[k]; ok {
			group.SetStatesIntoResourceGroup(tokens)
		}
	}
	m.storage.LoadResourceGroupStates(tokenHandler)

	// Add default group if it's not inited.
	if _, ok := m.groups[reservedDefaultGroupName]; !ok {
		defaultGroup := &ResourceGroup{
			Name: reservedDefaultGroupName,
			Mode: rmpb.GroupMode_RUMode,
			RUSettings: &RequestUnitSettings{
				RU: &GroupTokenBucket{
					Settings: &rmpb.TokenLimitSettings{
						FillRate:   math.MaxInt32,
						BurstLimit: -1,
					},
				},
			},
			Priority: middlePriority,
		}
		if err := m.AddResourceGroup(defaultGroup.IntoProtoResourceGroup()); err != nil {
			log.Warn("init default group failed", zap.Error(err))
		}
	}

	// Start the background metrics flusher.
	go m.backgroundMetricsFlush(ctx)
	go func() {
		defer logutil.LogPanic()
		m.persistLoop(ctx)
	}()
	log.Info("resource group manager finishes initialization")
}

// AddResourceGroup puts a resource group.
// NOTE: AddResourceGroup should also be idempotent because tidb depends
// on this retry mechanism.
func (m *Manager) AddResourceGroup(grouppb *rmpb.ResourceGroup) error {
	// Check the name.
	if len(grouppb.Name) == 0 || len(grouppb.Name) > 32 {
		return errs.ErrInvalidGroup
	}
	// Check the Priority.
	if grouppb.GetPriority() > 16 {
		return errs.ErrInvalidGroup
	}
	group := FromProtoResourceGroup(grouppb)
	m.Lock()
	defer m.Unlock()
	if err := group.persistSettings(m.storage); err != nil {
		return err
	}
	if err := group.persistStates(m.storage); err != nil {
		return err
	}
	m.groups[group.Name] = group
	return nil
}

// ModifyResourceGroup modifies an existing resource group.
func (m *Manager) ModifyResourceGroup(group *rmpb.ResourceGroup) error {
	if group == nil || group.Name == "" {
		return errs.ErrInvalidGroup
	}
	m.Lock()
	curGroup, ok := m.groups[group.Name]
	m.Unlock()
	if !ok {
		return errs.ErrResourceGroupNotExists.FastGenByArgs(group.Name)
	}

	err := curGroup.PatchSettings(group)
	if err != nil {
		return err
	}
	return curGroup.persistSettings(m.storage)
}

// DeleteResourceGroup deletes a resource group.
func (m *Manager) DeleteResourceGroup(name string) error {
	if name == reservedDefaultGroupName {
		return errs.ErrDeleteReservedGroup
	}
	if err := m.storage.DeleteResourceGroupSetting(name); err != nil {
		return err
	}
	m.Lock()
	delete(m.groups, name)
	m.Unlock()
	return nil
}

// GetResourceGroup returns a copy of a resource group.
func (m *Manager) GetResourceGroup(name string) *ResourceGroup {
	m.RLock()
	defer m.RUnlock()
	if group, ok := m.groups[name]; ok {
		return group.Copy()
	}
	return nil
}

// GetMutableResourceGroup returns a mutable resource group.
func (m *Manager) GetMutableResourceGroup(name string) *ResourceGroup {
	m.RLock()
	defer m.RUnlock()
	if group, ok := m.groups[name]; ok {
		return group
	}
	return nil
}

// GetResourceGroupList returns copies of resource group list.
func (m *Manager) GetResourceGroupList() []*ResourceGroup {
	m.RLock()
	res := make([]*ResourceGroup, 0, len(m.groups))
	for _, group := range m.groups {
		res = append(res, group.Copy())
	}
	m.RUnlock()
	sort.Slice(res, func(i, j int) bool {
		return res[i].Name < res[j].Name
	})
	return res
}

func (m *Manager) persistLoop(ctx context.Context) {
	ticker := time.NewTicker(time.Minute)
	failpoint.Inject("fastPersist", func() {
		ticker.Stop()
		ticker = time.NewTicker(100 * time.Millisecond)
	})
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.persistResourceGroupRunningState()
		}
	}
}

func (m *Manager) persistResourceGroupRunningState() {
	m.RLock()
	keys := make([]string, 0, len(m.groups))
	for k := range m.groups {
		keys = append(keys, k)
	}
	m.RUnlock()
	for idx := 0; idx < len(keys); idx++ {
		m.RLock()
		group, ok := m.groups[keys[idx]]
		m.RUnlock()
		if ok {
			m.Lock()
			group.persistStates(m.storage)
			m.Unlock()
		}
	}
}

// Receive the consumption and flush it to the metrics.
func (m *Manager) backgroundMetricsFlush(ctx context.Context) {
	defer logutil.LogPanic()
	cleanUpTicker := time.NewTicker(metricsCleanupInterval)
	defer cleanUpTicker.Stop()
	availableRUTicker := time.NewTicker(metricsAvailableRUInterval)
	defer availableRUTicker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case consumptionInfo := <-m.consumptionDispatcher:
			consumption := consumptionInfo.Consumption
			if consumption == nil {
				continue
			}
			backgroundType := ""
			if consumptionInfo.isBackground {
				backgroundType = backgroundTypeLabel
			}

			var (
				name                     = consumptionInfo.resourceGroupName
				rruMetrics               = readRequestUnitCost.WithLabelValues(name, backgroundType)
				wruMetrics               = writeRequestUnitCost.WithLabelValues(name, backgroundType)
				sqlLayerRuMetrics        = sqlLayerRequestUnitCost.WithLabelValues(name)
				readByteMetrics          = readByteCost.WithLabelValues(name, backgroundType)
				writeByteMetrics         = writeByteCost.WithLabelValues(name, backgroundType)
				kvCPUMetrics             = kvCPUCost.WithLabelValues(name, backgroundType)
				sqlCPUMetrics            = sqlCPUCost.WithLabelValues(name, backgroundType)
				readRequestCountMetrics  = requestCount.WithLabelValues(name, readTypeLabel)
				writeRequestCountMetrics = requestCount.WithLabelValues(name, writeTypeLabel)
			)
			// RU info.
			if consumption.RRU != 0 {
				rruMetrics.Add(consumption.RRU)
			}
			if consumption.WRU != 0 {
				wruMetrics.Add(consumption.WRU)
			}
			// Byte info.
			if consumption.ReadBytes != 0 {
				readByteMetrics.Add(consumption.ReadBytes)
			}
			if consumption.WriteBytes != 0 {
				writeByteMetrics.Add(consumption.WriteBytes)
			}
			// CPU time info.
			if consumption.TotalCpuTimeMs > 0 {
				if consumption.SqlLayerCpuTimeMs > 0 {
					sqlLayerRuMetrics.Add(consumption.SqlLayerCpuTimeMs * m.controllerConfig.RequestUnit.CPUMsCost)
					sqlCPUMetrics.Add(consumption.SqlLayerCpuTimeMs)
				}
				kvCPUMetrics.Add(consumption.TotalCpuTimeMs - consumption.SqlLayerCpuTimeMs)
			}
			// RPC count info.
			if consumption.KvReadRpcCount != 0 {
				readRequestCountMetrics.Add(consumption.KvReadRpcCount)
			}
			if consumption.KvWriteRpcCount != 0 {
				writeRequestCountMetrics.Add(consumption.KvWriteRpcCount)
			}

			m.consumptionRecord[name] = time.Now()

		case <-cleanUpTicker.C:
			// Clean up the metrics that have not been updated for a long time.
			for name, lastTime := range m.consumptionRecord {
				if time.Since(lastTime) > metricsCleanupTimeout {
					readRequestUnitCost.DeleteLabelValues(name)
					writeRequestUnitCost.DeleteLabelValues(name)
					sqlLayerRequestUnitCost.DeleteLabelValues(name)
					readByteCost.DeleteLabelValues(name)
					writeByteCost.DeleteLabelValues(name)
					kvCPUCost.DeleteLabelValues(name)
					sqlCPUCost.DeleteLabelValues(name)
					requestCount.DeleteLabelValues(name, readTypeLabel)
					requestCount.DeleteLabelValues(name, writeTypeLabel)
					availableRUCounter.DeleteLabelValues(name)
					delete(m.consumptionRecord, name)
				}
			}
		case <-availableRUTicker.C:
			m.RLock()
			for name, group := range m.groups {
				if name == reservedDefaultGroupName {
					continue
				}
				ru := group.getRUToken()
				if ru < 0 {
					ru = 0
				}
				availableRUCounter.WithLabelValues(name).Set(ru)
			}
			m.RUnlock()
		}
	}
}