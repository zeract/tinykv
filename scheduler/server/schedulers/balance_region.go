// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package schedulers

import (
	"sort"

	"github.com/pingcap-incubator/tinykv/scheduler/server/core"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/operator"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/opt"
)

func init() {
	schedule.RegisterSliceDecoderBuilder("balance-region", func(args []string) schedule.ConfigDecoder {
		return func(v interface{}) error {
			return nil
		}
	})
	schedule.RegisterScheduler("balance-region", func(opController *schedule.OperatorController, storage *core.Storage, decoder schedule.ConfigDecoder) (schedule.Scheduler, error) {
		return newBalanceRegionScheduler(opController), nil
	})
}

const (
	// balanceRegionRetryLimit is the limit to retry schedule for selected store.
	balanceRegionRetryLimit = 10
	balanceRegionName       = "balance-region-scheduler"
)

type balanceRegionScheduler struct {
	*baseScheduler
	name         string
	opController *schedule.OperatorController
}

// newBalanceRegionScheduler creates a scheduler that tends to keep regions on
// each store balanced.
func newBalanceRegionScheduler(opController *schedule.OperatorController, opts ...BalanceRegionCreateOption) schedule.Scheduler {
	base := newBaseScheduler(opController)
	s := &balanceRegionScheduler{
		baseScheduler: base,
		opController:  opController,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// BalanceRegionCreateOption is used to create a scheduler with an option.
type BalanceRegionCreateOption func(s *balanceRegionScheduler)

func (s *balanceRegionScheduler) GetName() string {
	if s.name != "" {
		return s.name
	}
	return balanceRegionName
}

func (s *balanceRegionScheduler) GetType() string {
	return "balance-region"
}

func (s *balanceRegionScheduler) IsScheduleAllowed(cluster opt.Cluster) bool {
	return s.opController.OperatorCount(operator.OpRegion) < cluster.GetRegionScheduleLimit()
}

func (s *balanceRegionScheduler) Schedule(cluster opt.Cluster) *operator.Operator {
	// Your Code Here (3C).
	stores := make([]*core.StoreInfo, 0)
	all_stores := cluster.GetStores()
	for _, s := range all_stores {
		if s.IsUp() && s.DownTime() <= cluster.GetMaxStoreDownTime() {
			stores = append(stores, s)
			// s.GetAvailable()
		}
	}
	if len(stores) == 1 || len(stores) == 0 {
		return nil
	}
	// 按 GetAvailable() 返回值排序
	sort.Slice(stores, func(i, j int) bool {
		return uint64(stores[i].GetRegionSize()) > uint64(stores[j].GetRegionSize()) // 从大到小排序
	})
	var source *core.StoreInfo
	var target *core.StoreInfo
	var regionInfo *core.RegionInfo
	for i, _ := range stores {
		cluster.GetPendingRegionsWithLock(stores[i].GetID(),
			func(regions core.RegionsContainer) { regionInfo = regions.RandomRegion(nil, nil) })
		if regionInfo != nil {
			source = stores[i]
			break
		}
		cluster.GetFollowersWithLock(stores[i].GetID(),
			func(regions core.RegionsContainer) { regionInfo = regions.RandomRegion(nil, nil) })
		if regionInfo != nil {
			source = stores[i]
			break
		}
		cluster.GetLeadersWithLock(stores[i].GetID(),
			func(regions core.RegionsContainer) { regionInfo = regions.RandomRegion(nil, nil) })
		if regionInfo != nil {
			source = stores[i]
			break
		}
	}
	if regionInfo == nil {
		return nil
	}
	if len(regionInfo.GetStoreIds()) < cluster.GetMaxReplicas() {
		return nil
	}
	for i := len(stores) - 1; i >= 0; i-- {
		suitStore := stores[i]
		exist := regionInfo.GetStorePeer(suitStore.GetID())
		if exist == nil {
			target = suitStore
			break
		}
	}

	// target = findSmallestStore(stores, cluster.GetRegionStores(regionInfo))
	if target == nil {
		return nil
	}
	difference := source.GetRegionSize() - target.GetRegionSize()
	if difference <= 2*regionInfo.GetApproximateSize() {
		return nil
	}
	// 为target sotre分配一个新的peer
	newPeer, err := cluster.AllocPeer(target.GetID())
	if err != nil {
		panic(err)
	}
	peerOperator, err := operator.CreateMovePeerOperator("balance-region", cluster,
		regionInfo, operator.OpBalance, source.GetID(), target.GetID(), newPeer.GetId())

	return peerOperator
}

// 查找最小但不在 region_stores 中的 store
func findSmallestStore(stores, regionStores []*core.StoreInfo) *core.StoreInfo {
	// 将 region_stores 转为哈希集合
	regionSet := make(map[*core.StoreInfo]struct{})
	for _, store := range regionStores {
		regionSet[store] = struct{}{}
	}

	// 从尾部开始查找第一个不在 region_stores 中的 store
	for i := len(stores) - 1; i >= 0; i-- {
		store := stores[i]
		if _, exists := regionSet[store]; !exists {
			return store // 找到后立即返回
		}
	}

	return nil // 未找到返回 nil
}
