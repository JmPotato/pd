// Copyright 2020 TiKV Project Authors.
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

package tso

import (
	"context"
	"runtime/trace"
	"strconv"
	"sync"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/log"

	"github.com/tikv/pd/pkg/election"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/member"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/utils/logutil"
)

const (
	// GlobalDCLocation is the Global TSO Allocator's DC location label.
	GlobalDCLocation            = "global"
	checkStep                   = time.Minute
	patrolStep                  = time.Second
	defaultAllocatorLeaderLease = 3
)

var (
	// PriorityCheck exported is only for test.
	PriorityCheck = time.Minute
)

// ElectionMember defines the interface for the election related logic.
type ElectionMember interface {
	// ID returns the unique ID in the election group. For example, it can be unique
	// server id of a cluster or the unique keyspace group replica id of the election
	// group composed of the replicas of a keyspace group.
	ID() uint64
	// Name returns the unique name in the election group.
	Name() string
	// MemberValue returns the member value.
	MemberValue() string
	// GetMember returns the current member
	GetMember() any
	// Client returns the etcd client.
	Client() *clientv3.Client
	// IsLeader returns whether the participant is the leader or not by checking its
	// leadership's lease and leader info.
	IsLeader() bool
	// IsLeaderElected returns true if the leader exists; otherwise false.
	IsLeaderElected() bool
	// CheckLeader checks if someone else is taking the leadership. If yes, returns the leader;
	// otherwise returns a bool which indicates if it is needed to check later.
	CheckLeader() (leader member.ElectionLeader, checkAgain bool)
	// EnableLeader declares the member itself to be the leader.
	EnableLeader()
	// KeepLeader is used to keep the leader's leadership.
	KeepLeader(ctx context.Context)
	// CampaignLeader is used to campaign the leadership and make it become a leader in an election group.
	CampaignLeader(ctx context.Context, leaseTimeout int64) error
	// ResetLeader is used to reset the member's current leadership.
	// Basically it will reset the leader lease and unset leader info.
	ResetLeader()
	// GetLeaderListenUrls returns current leader's listen urls
	// The first element is the leader/primary url
	GetLeaderListenUrls() []string
	// GetLeaderID returns current leader's member ID.
	GetLeaderID() uint64
	// GetLeaderPath returns the path of the leader.
	GetLeaderPath() string
	// GetLeadership returns the leadership of the election member.
	GetLeadership() *election.Leadership
	// GetLastLeaderUpdatedTime returns the last time when the leader is updated.
	GetLastLeaderUpdatedTime() time.Time
	// PreCheckLeader does some pre-check before checking whether it's the leader.
	PreCheckLeader() error
}

// AllocatorManager is used to manage the TSO Allocators a PD server holds.
// It is in charge of maintaining TSO allocators' leadership, checking election
// priority, and forwarding TSO allocation requests to correct TSO Allocators.
type AllocatorManager struct {
	// Global TSO Allocator, as a global single point to allocate TSO for global transactions.
	allocator *GlobalTSOAllocator
	// for the synchronization purpose of the service loops
	svcLoopWG sync.WaitGroup

	ctx    context.Context
	cancel context.CancelFunc
	// kgID is the keyspace group ID
	kgID uint32
	// member is for election use
	member  ElectionMember
	storage endpoint.TSOStorage
	// TSO config
	cfg Config

	logFields []zap.Field
}

// NewAllocatorManager creates a new TSO Allocator Manager.
func NewAllocatorManager(
	ctx context.Context,
	keyspaceGroupID uint32,
	member ElectionMember,
	storage endpoint.TSOStorage,
	cfg Config,
) *AllocatorManager {
	ctx, cancel := context.WithCancel(ctx)
	am := &AllocatorManager{
		ctx:     ctx,
		cancel:  cancel,
		kgID:    keyspaceGroupID,
		member:  member,
		storage: storage,
		cfg:     cfg,
		logFields: []zap.Field{
			logutil.CondUint32("keyspace-group-id", keyspaceGroupID, keyspaceGroupID > 0),
			zap.String("name", member.Name()),
		},
	}
	am.allocator = NewGlobalTSOAllocator(ctx, am)

	am.svcLoopWG.Add(1)
	go am.tsoAllocatorLoop()

	return am
}

// getGroupID returns the keyspace group ID of the allocator manager.
func (am *AllocatorManager) getGroupID() uint32 {
	if am == nil {
		return 0
	}
	return am.kgID
}

// getGroupIDStr returns the keyspace group ID of the allocator manager in string format.
func (am *AllocatorManager) getGroupIDStr() string {
	if am == nil {
		return "0"
	}
	return strconv.FormatUint(uint64(am.kgID), 10)
}

// tsoAllocatorLoop is used to run the TSO Allocator updating daemon.
func (am *AllocatorManager) tsoAllocatorLoop() {
	defer logutil.LogPanic()
	defer am.svcLoopWG.Done()

	tsTicker := time.NewTicker(am.cfg.GetTSOUpdatePhysicalInterval())
	failpoint.Inject("fastUpdatePhysicalInterval", func() {
		tsTicker.Reset(time.Millisecond)
	})
	defer tsTicker.Stop()

	log.Info("entering into allocator update loop", am.logFields...)
	for {
		select {
		case <-tsTicker.C:
			// Only try to update when the member is leader and the allocator is initialized.
			if !am.member.IsLeader() || !am.allocator.IsInitialize() {
				continue
			}
			if err := am.allocator.UpdateTSO(); err != nil {
				log.Warn("failed to update allocator's timestamp", append(am.logFields, errs.ZapError(err))...)
				am.ResetAllocatorGroup(false)
				return
			}
		case <-am.ctx.Done():
			am.allocator.reset()
			log.Info("exit the allocator update loop", am.logFields...)
			return
		}
	}
}

// close is used to shutdown TSO Allocator updating daemon.
// tso service call this function to shutdown the loop here, but pd manages its own loop.
func (am *AllocatorManager) close() {
	log.Info("closing the allocator manager", am.logFields...)

	am.allocator.close()
	am.cancel()
	am.svcLoopWG.Wait()

	log.Info("closed the allocator manager", am.logFields...)
}

// GetMember returns the ElectionMember of this AllocatorManager.
func (am *AllocatorManager) GetMember() ElectionMember {
	return am.member
}

// HandleRequest forwards TSO allocation requests to correct TSO Allocators.
func (am *AllocatorManager) HandleRequest(ctx context.Context, count uint32) (pdpb.Timestamp, error) {
	defer trace.StartRegion(ctx, "AllocatorManager.HandleRequest").End()
	return am.allocator.generateTSO(ctx, count)
}

// ResetAllocatorGroup will reset the allocator's leadership and TSO initialized in memory.
// It usually should be called before re-triggering an Allocator leader campaign.
func (am *AllocatorManager) ResetAllocatorGroup(skipResetLeader bool) {
	am.allocator.reset()
	// Reset if it still has the leadership. Otherwise the data race may occur because of the re-campaigning.
	if !skipResetLeader && am.member.IsLeader() {
		am.member.ResetLeader()
	}
}

// GetAllocator returns the global TSO allocator.
func (am *AllocatorManager) GetAllocator() *GlobalTSOAllocator {
	return am.allocator
}

func (am *AllocatorManager) isLeader() bool {
	if am == nil || am.member == nil {
		return false
	}
	return am.member.IsLeader()
}

// GetLeaderAddr returns the address of leader in the election group.
func (am *AllocatorManager) GetLeaderAddr() string {
	if am == nil || am.member == nil {
		return ""
	}
	leaderAddrs := am.member.GetLeaderListenUrls()
	if len(leaderAddrs) < 1 {
		return ""
	}
	return leaderAddrs[0]
}

// The PD server will conduct its own leadership election independently of the allocator manager,
// while the TSO service will manage its leadership election within the allocator manager.
// This function is used to manually initiate the allocator leadership election loop.
func (am *AllocatorManager) startGlobalAllocatorLoop() {
	if am.allocator == nil {
		// it should never happen
		log.Error("failed to start global allocator loop, global allocator not found", am.logFields...)
		return
	}
	am.allocator.wg.Add(1)
	go am.allocator.primaryElectionLoop(am.getGroupID())
}
