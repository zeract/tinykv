package raftstore

import (
	"sync"

	"github.com/pingcap-incubator/tinykv/kv/raftstore/meta"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/kv/util/worker"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/raft_cmdpb"
)

type ApplyTask struct {
	peermsghandler *peerMsgHandler
	entry          *eraftpb.Entry
	request        *raft_cmdpb.RaftCmdRequest
	proposal       *proposal
}

type ApplyTaskHandler struct {
	tasks    []ApplyTask // 任务队列
	mutex    sync.Mutex  // 互斥锁，保证线程安全
	cond     *sync.Cond  // 条件变量，用于同步任务执行
	isActive bool        // 标志是否有任务正在执行
}

func NewApplyTaskHandler() *ApplyTaskHandler {
	handler := &ApplyTaskHandler{
		tasks:    make([]ApplyTask, 0), // 初始化空任务队列
		isActive: false,
	}
	handler.cond = sync.NewCond(&handler.mutex) // 条件变量初始化
	return handler
}

func (r *ApplyTaskHandler) Handle(t worker.Task) {
	task, ok := t.(*ApplyTask)
	if !ok {
		log.Errorf("unsupported worker.Task: %+v", t)
		return
	}
	r.mutex.Lock() // 加锁防止并发写入
	defer r.mutex.Unlock()

	// 将任务加入队列
	r.tasks = append(r.tasks, *task)

	// 如果当前没有任务在执行，则开始处理队列
	if !r.isActive {
		r.isActive = true
		go r.processTasks() // 启动任务处理的goroutine
	}
	// r.apply(task)
}

// 处理队列中的任务，确保按顺序执行
func (r *ApplyTaskHandler) processTasks() {
	for {
		r.mutex.Lock()

		// 等待直到有任务可执行
		if len(r.tasks) == 0 {
			r.isActive = false // 没有任务时标记为非活跃状态
			r.mutex.Unlock()
			return // 如果没有任务则退出，不再启动新的处理goroutine
		}

		// 取出队列中的第一个任务
		task := r.tasks[0]
		r.tasks = r.tasks[1:] // 从队列中删除任务

		r.mutex.Unlock()

		// 执行任务
		r.apply(&task)

		// 任务执行完成后，检查是否还有任务
		r.mutex.Lock()
		if len(r.tasks) == 0 {
			r.isActive = false // 如果队列为空，标记任务处理已完成
		}
		r.mutex.Unlock()
	}
}
func (r *ApplyTaskHandler) apply(task *ApplyTask) {
	// Implement your apply logic here
	log.Debugf("Applying task: [index:%d, term:%d]", task.entry.Index, task.entry.Term)
	wb := new(engine_util.WriteBatch)
	d := task.peermsghandler
	changed := true
	// 创建这个RaftCmdRequest对应的WriteBatch
	if task.request.AdminRequest != nil {
		d.applyAdminRequests(task.request, *task.entry, wb, task.proposal)

	} else if len(task.request.Requests) > 0 {
		// 将requests中的数据进行apply
		changed = d.applyNormalRequests(task.request, *task.entry, wb, task.proposal)
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
		err := wb.SetMeta(meta.ApplyStateKey(d.regionId), d.peerStorage.applyState)
		if err != nil {
			panic(err)
		}
		wb.MustWriteToDB(d.ctx.engine.Kv)
	}
}
