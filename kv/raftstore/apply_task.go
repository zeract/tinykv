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
	wb := new(engine_util.WriteBatch)
	d := task.peermsghandler
	changed := true
	// 创建这个RaftCmdRequest对应的WriteBatch
	if task.request.AdminRequest != nil {
		// log.Infof("%v Applying AdminRequest task: [index:%d, term:%d]", task.peermsghandler.Tag, task.entry.Index, task.entry.Term)
		d.applyAdminRequests(task.request, wb, task.proposal)
		// log.Infof("%v After apply Split or Compact", task.peermsghandler.Tag)
	} else if len(task.request.Requests) > 0 {
		// log.Infof("%v Applying NormalRequest task: [index:%d, term:%d]", task.peermsghandler.Tag, task.entry.Index, task.entry.Term)
		// 将requests中的数据进行apply
		changed = d.applyNormalRequests(task.request, wb, task.proposal)
	}

	if task.entry.Index >= d.peerStorage.applyState.AppliedIndex {
		// 更新PeerStorage的AppliedIndex
		d.peerStorage.applyState.AppliedIndex = task.entry.Index
	} else {
		log.Errorf("Task apply Error, task apply entry index %d < appliedIndex %d", task.entry.Index, d.peerStorage.applyState.AppliedIndex)
	}
	// 如果没有数据变化，则不需要写入KV DB，但是如果遍历到最后一个commmited Entry，那么就需要将更新的AppliedIndex写入DB
	if changed {
		// 将更新的ApplyState写入KV DB
		// log.Infof("%v Try Write Entry[index:%d, term:%d] to DB", d.Tag, task.entry.Index, task.entry.Term)
		err := wb.SetMeta(meta.ApplyStateKey(d.regionId), d.peerStorage.applyState)
		if err != nil {
			panic(err)
		}
		wb.WriteToDB(d.ctx.engine.Kv)
		// log.Infof("%v Successful Write Entry[index:%d, term:%d] to DB", d.Tag, task.entry.Index, task.entry.Term)
	}
}
