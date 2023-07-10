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

package schedulers

import (
	"context"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/errs"
	sche "github.com/tikv/pd/pkg/schedule/core"
	"github.com/tikv/pd/pkg/schedule/labeler"
	"github.com/tikv/pd/pkg/schedule/operator"
	"github.com/tikv/pd/pkg/schedule/plan"
	"github.com/tikv/pd/pkg/storage"
	"github.com/tikv/pd/pkg/utils/logutil"
	"github.com/tikv/pd/server/config"
	"go.uber.org/zap"
)

const maxScheduleRetries = 10

var denySchedulersByLabelerCounter = labeler.LabelerEventCounter.WithLabelValues("schedulers", "deny")

// Controller is used to manage all schedulers.
type Controller struct {
	sync.RWMutex
	wg           sync.WaitGroup
	ctx          context.Context
	cluster      sche.ClusterInformer
	schedulers   map[string]*ScheduleController
	opController *operator.Controller
}

// NewController creates a scheduler controller.
func NewController(ctx context.Context, cluster sche.ClusterInformer, opController *operator.Controller) *Controller {
	return &Controller{
		ctx:          ctx,
		cluster:      cluster,
		schedulers:   make(map[string]*ScheduleController),
		opController: opController,
	}
}

// GetWaitGroup returns the waitGroup of the controller.
func (c *Controller) GetWaitGroup() *sync.WaitGroup {
	return &c.wg
}

// GetScheduler returns a schedule controller by name.
func (c *Controller) GetScheduler(name string) *ScheduleController {
	c.RLock()
	defer c.RUnlock()
	return c.schedulers[name]
}

// GetSchedulerNames returns all names of schedulers.
func (c *Controller) GetSchedulerNames() []string {
	c.RLock()
	defer c.RUnlock()
	names := make([]string, 0, len(c.schedulers))
	for name := range c.schedulers {
		names = append(names, name)
	}
	return names
}

// GetSchedulerHandlers returns all handlers of schedulers.
func (c *Controller) GetSchedulerHandlers() map[string]http.Handler {
	c.RLock()
	defer c.RUnlock()
	handlers := make(map[string]http.Handler, len(c.schedulers))
	for name, scheduler := range c.schedulers {
		handlers[name] = scheduler.Scheduler
	}
	return handlers
}

// CollectSchedulerMetrics collects metrics of all schedulers.
func (c *Controller) CollectSchedulerMetrics() {
	c.RLock()
	defer c.RUnlock()
	for _, s := range c.schedulers {
		var allowScheduler float64
		// If the scheduler is not allowed to schedule, it will disappear in Grafana panel.
		// See issue #1341.
		if !s.IsPaused() && !c.isSchedulingHalted() {
			allowScheduler = 1
		}
		schedulerStatusGauge.WithLabelValues(s.Scheduler.GetName(), "allow").Set(allowScheduler)
	}
}

func (c *Controller) isSchedulingHalted() bool {
	return c.cluster.GetPersistOptions().IsSchedulingHalted()
}

// ResetSchedulerMetrics resets metrics of all schedulers.
func (c *Controller) ResetSchedulerMetrics() {
	schedulerStatusGauge.Reset()
}

// AddScheduler adds a scheduler.
func (c *Controller) AddScheduler(scheduler Scheduler, args ...string) error {
	c.Lock()
	defer c.Unlock()

	if _, ok := c.schedulers[scheduler.GetName()]; ok {
		return errs.ErrSchedulerExisted.FastGenByArgs()
	}

	s := NewScheduleController(c.ctx, c.cluster, c.opController, scheduler)
	if err := s.Scheduler.Prepare(c.cluster); err != nil {
		return err
	}

	c.wg.Add(1)
	go c.runScheduler(s)
	c.schedulers[s.Scheduler.GetName()] = s
	c.cluster.GetPersistOptions().AddSchedulerCfg(s.Scheduler.GetType(), args)
	return nil
}

// RemoveScheduler removes a scheduler by name.
func (c *Controller) RemoveScheduler(name string) error {
	c.Lock()
	defer c.Unlock()
	if c.cluster == nil {
		return errs.ErrNotBootstrapped.FastGenByArgs()
	}
	s, ok := c.schedulers[name]
	if !ok {
		return errs.ErrSchedulerNotFound.FastGenByArgs()
	}

	opt := c.cluster.GetPersistOptions()
	if err := c.removeOptScheduler(opt, name); err != nil {
		log.Error("can not remove scheduler", zap.String("scheduler-name", name), errs.ZapError(err))
		return err
	}

	if err := opt.Persist(c.cluster.GetStorage()); err != nil {
		log.Error("the option can not persist scheduler config", errs.ZapError(err))
		return err
	}

	if err := c.cluster.GetStorage().RemoveScheduleConfig(name); err != nil {
		log.Error("can not remove the scheduler config", errs.ZapError(err))
		return err
	}

	s.Stop()
	schedulerStatusGauge.DeleteLabelValues(name, "allow")
	delete(c.schedulers, name)

	return nil
}

func (c *Controller) removeOptScheduler(o *config.PersistOptions, name string) error {
	v := o.GetScheduleConfig().Clone()
	for i, schedulerCfg := range v.Schedulers {
		// To create a temporary scheduler is just used to get scheduler's name
		decoder := ConfigSliceDecoder(schedulerCfg.Type, schedulerCfg.Args)
		tmp, err := CreateScheduler(schedulerCfg.Type, c.opController, storage.NewStorageWithMemoryBackend(), decoder, c.RemoveScheduler)
		if err != nil {
			return err
		}
		if tmp.GetName() == name {
			if config.IsDefaultScheduler(tmp.GetType()) {
				schedulerCfg.Disable = true
				v.Schedulers[i] = schedulerCfg
			} else {
				v.Schedulers = append(v.Schedulers[:i], v.Schedulers[i+1:]...)
			}
			o.SetScheduleConfig(v)
			return nil
		}
	}
	return nil
}

// PauseOrResumeScheduler pauses or resumes a scheduler by name.
func (c *Controller) PauseOrResumeScheduler(name string, t int64) error {
	c.Lock()
	defer c.Unlock()
	if c.cluster == nil {
		return errs.ErrNotBootstrapped.FastGenByArgs()
	}
	var s []*ScheduleController
	if name != "all" {
		sc, ok := c.schedulers[name]
		if !ok {
			return errs.ErrSchedulerNotFound.FastGenByArgs()
		}
		s = append(s, sc)
	} else {
		for _, sc := range c.schedulers {
			s = append(s, sc)
		}
	}
	var err error
	for _, sc := range s {
		var delayAt, delayUntil int64
		if t > 0 {
			delayAt = time.Now().Unix()
			delayUntil = delayAt + t
		}
		sc.SetDelay(delayAt, delayUntil)
	}
	return err
}

// IsSchedulerAllowed returns whether a scheduler is allowed to schedule, a scheduler is not allowed to schedule if it is paused or blocked by unsafe recovery.
func (c *Controller) IsSchedulerAllowed(name string) (bool, error) {
	c.RLock()
	defer c.RUnlock()
	if c.cluster == nil {
		return false, errs.ErrNotBootstrapped.FastGenByArgs()
	}
	s, ok := c.schedulers[name]
	if !ok {
		return false, errs.ErrSchedulerNotFound.FastGenByArgs()
	}
	return s.AllowSchedule(false), nil
}

// IsSchedulerPaused returns whether a scheduler is paused.
func (c *Controller) IsSchedulerPaused(name string) (bool, error) {
	c.RLock()
	defer c.RUnlock()
	if c.cluster == nil {
		return false, errs.ErrNotBootstrapped.FastGenByArgs()
	}
	s, ok := c.schedulers[name]
	if !ok {
		return false, errs.ErrSchedulerNotFound.FastGenByArgs()
	}
	return s.IsPaused(), nil
}

// IsSchedulerDisabled returns whether a scheduler is disabled.
func (c *Controller) IsSchedulerDisabled(name string) (bool, error) {
	c.RLock()
	defer c.RUnlock()
	if c.cluster == nil {
		return false, errs.ErrNotBootstrapped.FastGenByArgs()
	}
	s, ok := c.schedulers[name]
	if !ok {
		return false, errs.ErrSchedulerNotFound.FastGenByArgs()
	}
	t := s.Scheduler.GetType()
	scheduleConfig := c.cluster.GetPersistOptions().GetScheduleConfig()
	for _, s := range scheduleConfig.Schedulers {
		if t == s.Type {
			return s.Disable, nil
		}
	}
	return false, nil
}

// IsSchedulerExisted returns whether a scheduler is existed.
func (c *Controller) IsSchedulerExisted(name string) (bool, error) {
	c.RLock()
	defer c.RUnlock()
	if c.cluster == nil {
		return false, errs.ErrNotBootstrapped.FastGenByArgs()
	}
	_, ok := c.schedulers[name]
	if !ok {
		return false, errs.ErrSchedulerNotFound.FastGenByArgs()
	}
	return true, nil
}

func (c *Controller) runScheduler(s *ScheduleController) {
	defer logutil.LogPanic()
	defer c.wg.Done()
	defer s.Scheduler.Cleanup(c.cluster)

	ticker := time.NewTicker(s.GetInterval())
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			diagnosable := s.IsDiagnosticAllowed()
			if !s.AllowSchedule(diagnosable) {
				continue
			}
			if op := s.Schedule(diagnosable); len(op) > 0 {
				added := c.opController.AddWaitingOperator(op...)
				log.Debug("add operator", zap.Int("added", added), zap.Int("total", len(op)), zap.String("scheduler", s.Scheduler.GetName()))
			}
			// Note: we reset the ticker here to support updating configuration dynamically.
			ticker.Reset(s.GetInterval())
		case <-s.Ctx().Done():
			log.Info("scheduler has been stopped",
				zap.String("scheduler-name", s.Scheduler.GetName()),
				errs.ZapError(s.Ctx().Err()))
			return
		}
	}
}

// GetPausedSchedulerDelayAt returns paused timestamp of a paused scheduler
func (c *Controller) GetPausedSchedulerDelayAt(name string) (int64, error) {
	c.RLock()
	defer c.RUnlock()
	if c.cluster == nil {
		return -1, errs.ErrNotBootstrapped.FastGenByArgs()
	}
	s, ok := c.schedulers[name]
	if !ok {
		return -1, errs.ErrSchedulerNotFound.FastGenByArgs()
	}
	return s.GetDelayAt(), nil
}

// GetPausedSchedulerDelayUntil returns the delay time until the scheduler is paused.
func (c *Controller) GetPausedSchedulerDelayUntil(name string) (int64, error) {
	c.RLock()
	defer c.RUnlock()
	if c.cluster == nil {
		return -1, errs.ErrNotBootstrapped.FastGenByArgs()
	}
	s, ok := c.schedulers[name]
	if !ok {
		return -1, errs.ErrSchedulerNotFound.FastGenByArgs()
	}
	return s.GetDelayUntil(), nil
}

// CheckTransferWitnessLeader determines if transfer leader is required, then sends to the scheduler if needed
func (c *Controller) CheckTransferWitnessLeader(region *core.RegionInfo) {
	if core.NeedTransferWitnessLeader(region) {
		c.RLock()
		s, ok := c.schedulers[TransferWitnessLeaderName]
		c.RUnlock()
		if ok {
			select {
			case RecvRegionInfo(s.Scheduler) <- region:
			default:
				log.Warn("drop transfer witness leader due to recv region channel full", zap.Uint64("region-id", region.GetID()))
			}
		}
	}
}

// ScheduleController is used to manage a scheduler.
type ScheduleController struct {
	Scheduler
	cluster            sche.ScheduleCluster
	opController       *operator.Controller
	nextInterval       time.Duration
	ctx                context.Context
	cancel             context.CancelFunc
	delayAt            int64
	delayUntil         int64
	diagnosticRecorder *DiagnosticRecorder
}

// NewScheduleController creates a new ScheduleController.
func NewScheduleController(ctx context.Context, cluster sche.ScheduleCluster, opController *operator.Controller, s Scheduler) *ScheduleController {
	ctx, cancel := context.WithCancel(ctx)
	return &ScheduleController{
		Scheduler:          s,
		cluster:            cluster,
		opController:       opController,
		nextInterval:       s.GetMinInterval(),
		ctx:                ctx,
		cancel:             cancel,
		diagnosticRecorder: NewDiagnosticRecorder(s.GetName(), cluster.GetOpts()),
	}
}

// Ctx returns the context of ScheduleController
func (s *ScheduleController) Ctx() context.Context {
	return s.ctx
}

// Stop stops the ScheduleController
func (s *ScheduleController) Stop() {
	s.cancel()
}

// Schedule tries to create some operators.
func (s *ScheduleController) Schedule(diagnosable bool) []*operator.Operator {
	for i := 0; i < maxScheduleRetries; i++ {
		// no need to retry if schedule should stop to speed exit
		select {
		case <-s.ctx.Done():
			return nil
		default:
		}
		cacheCluster := newCacheCluster(s.cluster)
		// we need only process diagnostic once in the retry loop
		diagnosable = diagnosable && i == 0
		ops, plans := s.Scheduler.Schedule(cacheCluster, diagnosable)
		if diagnosable {
			s.diagnosticRecorder.SetResultFromPlans(ops, plans)
		}
		foundDisabled := false
		for _, op := range ops {
			if labelMgr := s.cluster.GetRegionLabeler(); labelMgr != nil {
				region := s.cluster.GetRegion(op.RegionID())
				if region == nil {
					continue
				}
				if labelMgr.ScheduleDisabled(region) {
					denySchedulersByLabelerCounter.Inc()
					foundDisabled = true
					break
				}
			}
		}
		if len(ops) > 0 {
			// If we have schedule, reset interval to the minimal interval.
			s.nextInterval = s.Scheduler.GetMinInterval()
			// try regenerating operators
			if foundDisabled {
				continue
			}
			return ops
		}
	}
	s.nextInterval = s.Scheduler.GetNextInterval(s.nextInterval)
	return nil
}

// DiagnoseDryRun returns the operators and plans of a scheduler.
func (s *ScheduleController) DiagnoseDryRun() ([]*operator.Operator, []plan.Plan) {
	cacheCluster := newCacheCluster(s.cluster)
	return s.Scheduler.Schedule(cacheCluster, true)
}

// GetInterval returns the interval of scheduling for a scheduler.
func (s *ScheduleController) GetInterval() time.Duration {
	return s.nextInterval
}

// SetInterval sets the interval of scheduling for a scheduler. for test purpose.
func (s *ScheduleController) SetInterval(interval time.Duration) {
	s.nextInterval = interval
}

// AllowSchedule returns if a scheduler is allowed to
func (s *ScheduleController) AllowSchedule(diagnosable bool) bool {
	if !s.Scheduler.IsScheduleAllowed(s.cluster) {
		if diagnosable {
			s.diagnosticRecorder.SetResultFromStatus(Pending)
		}
		return false
	}
	if s.isSchedulingHalted() {
		if diagnosable {
			s.diagnosticRecorder.SetResultFromStatus(Halted)
		}
		return false
	}
	if s.IsPaused() {
		if diagnosable {
			s.diagnosticRecorder.SetResultFromStatus(Paused)
		}
		return false
	}
	return true
}

func (s *ScheduleController) isSchedulingHalted() bool {
	return s.cluster.GetOpts().IsSchedulingHalted()
}

// IsPaused returns if a scheduler is paused.
func (s *ScheduleController) IsPaused() bool {
	delayUntil := atomic.LoadInt64(&s.delayUntil)
	return time.Now().Unix() < delayUntil
}

// GetDelayAt returns paused timestamp of a paused scheduler
func (s *ScheduleController) GetDelayAt() int64 {
	if s.IsPaused() {
		return atomic.LoadInt64(&s.delayAt)
	}
	return 0
}

// GetDelayUntil returns resume timestamp of a paused scheduler
func (s *ScheduleController) GetDelayUntil() int64 {
	if s.IsPaused() {
		return atomic.LoadInt64(&s.delayUntil)
	}
	return 0
}

// SetDelay sets the delay of a scheduler.
func (s *ScheduleController) SetDelay(delayAt, delayUntil int64) {
	atomic.StoreInt64(&s.delayAt, delayAt)
	atomic.StoreInt64(&s.delayUntil, delayUntil)
}

// GetDiagnosticRecorder returns the diagnostic recorder of a scheduler.
func (s *ScheduleController) GetDiagnosticRecorder() *DiagnosticRecorder {
	return s.diagnosticRecorder
}

// IsDiagnosticAllowed returns if a scheduler is allowed to do diagnostic.
func (s *ScheduleController) IsDiagnosticAllowed() bool {
	return s.diagnosticRecorder.IsAllowed()
}

// cacheCluster include cache info to improve the performance.
type cacheCluster struct {
	sche.ScheduleCluster
	stores []*core.StoreInfo
}

// GetStores returns store infos from cache
func (c *cacheCluster) GetStores() []*core.StoreInfo {
	return c.stores
}

// newCacheCluster constructor for cache
func newCacheCluster(c sche.ScheduleCluster) *cacheCluster {
	return &cacheCluster{
		ScheduleCluster: c,
		stores:          c.GetStores(),
	}
}