package raftstore

import (
	"github.com/pingcap-incubator/tinykv/kv/raftstore/meta"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/kv/util/worker"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/raft_cmdpb"
)

type ApplyTask struct {
	peermsghandler *peerMsgHandler
	entry          eraftpb.Entry
	request        *raft_cmdpb.RaftCmdRequest
	proposal       *proposal
	Notifier       chan struct{}
}

type ApplyTaskHandler struct {
}

func NewApplyTaskHandler() *ApplyTaskHandler {
	handler := &ApplyTaskHandler{}
	return handler
}

func (r *ApplyTaskHandler) Handle(t worker.Task) {
	task, ok := t.(*ApplyTask)
	if !ok {
		log.Errorf("unsupported worker.Task: %+v", t)
		return
	}
	r.apply(task)
}

func (r *ApplyTaskHandler) apply(task *ApplyTask) {
	// Implement your apply logic here
	if task.Notifier != nil {
		task.Notifier <- struct{}{}
		return
	}
	if task.peermsghandler.stopped {
		return
	}
	// 创建这个RaftCmdRequest对应的WriteBatch
	wb := new(engine_util.WriteBatch)
	d := task.peermsghandler
	// 判断是否需要写盘
	// requests := new(raft_cmdpb.RaftCmdRequest)
	// err := requests.Unmarshal(task.entry.Data)
	// if err != nil {
	// 	panic(err)
	// }
	changed := true
	if task.request.AdminRequest != nil {
		d.applyAdminRequests(task.request, wb, task.proposal)
	} else if len(task.request.Requests) > 0 {
		// 将requests中的数据进行apply
		changed = d.applyNormalRequests(task.request, wb, task.proposal)
	}
	if task.entry.Index >= d.peerStorage.applyState.AppliedIndex {
		// 更新PeerStorage的AppliedIndex
		d.peerStorage.applyState.AppliedIndex = task.entry.Index
	} else {
		log.Errorf("Task apply Error, task apply entry index %d < appliedIndex %d", task.entry.Index, d.peerStorage.applyState.AppliedIndex)
	}
	// 如果没有数据变化，则不需要写入KV DB
	if changed {
		// 将更新的ApplyState写入KV DB
		err := wb.SetMeta(meta.ApplyStateKey(d.regionId), d.peerStorage.applyState)
		if err != nil {
			panic(err)
		}
		wb.WriteToDB(d.ctx.engine.Kv)
	}
}
