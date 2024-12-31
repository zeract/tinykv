// Copyright 2015 The etcd Authors
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

package raft

import (
	"github.com/pingcap-incubator/tinykv/log"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// RaftLog manage the log entries, its struct look like:
//
//	snapshot/first.....applied....committed....stabled.....last
//	--------|------------------------------------------------|
//	                          log entries
//
// for simplify the RaftLog implement should manage all log entries
// that not truncated
type RaftLog struct {
	// storage contains all stable entries since the last snapshot.
	storage Storage

	// committed is the highest log position that is known to be in
	// stable storage on a quorum of nodes.
	committed uint64

	// applied is the highest log position that the application has
	// been instructed to apply to its state machine.
	// Invariant: applied <= committed
	applied uint64

	// log entries with index <= stabled are persisted to storage.
	// It is used to record the logs that are not persisted by storage yet.
	// Everytime handling `Ready`, the unstabled logs will be included.
	stabled uint64

	// all entries that have not yet compact.
	entries []pb.Entry

	// the incoming unstable snapshot, if any.
	// (Used in 2C)
	pendingSnapshot *pb.Snapshot

	// Your Data Here (2A).
	// the last entry before firstindex
	dummyIndex uint64
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).
	firstIndex, err := storage.FirstIndex()
	if err != nil {
		panic(err)
	}

	lastIndex, err := storage.LastIndex()
	if err != nil {
		panic(err)
	}
	// log.Infof("Get Entries[%d:%d] from storage", firstIndex, lastIndex+1)
	entries, err := storage.Entries(firstIndex, lastIndex+1)
	if err != nil {
		panic(err)
	}
	// log.Infof("New RaftLog Entry Length is %d\n", len(entries))
	// log.Infof("The firstIndex is %d, LastIndex is %d\n", firstIndex, lastIndex)
	log := RaftLog{entries: entries, storage: storage, dummyIndex: firstIndex - 1}
	// committed和applied从持久化的第一个index的前一个开始
	log.committed = firstIndex - 1
	log.applied = firstIndex - 1
	log.stabled = lastIndex
	log.dummyIndex = firstIndex - 1
	return &log
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
	truncated, _ := l.storage.FirstIndex()
	if len(l.entries) > 0 {
		index := l.entries[0].Index
		if truncated > index {
			l.entries = l.entries[truncated-index:]
		}
	}
}

// allEntries return all the entries not compacted.
// note, exclude any dummy entries from the return value.
// note, this is one of the test stub functions you need to implement.
func (l *RaftLog) allEntries() []pb.Entry {
	// Your Code Here (2A).
	return l.entries
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	// Your Code Here (2A).
	if len(l.entries) > 0 {
		firstIndex := l.FirstIndex()
		if l.stabled < firstIndex {
			return l.entries
		}
		if l.stabled-firstIndex >= uint64(len(l.entries)-1) {
			return make([]pb.Entry, 0)
		}
		return l.entries[l.stabled-firstIndex+1:]
	}
	return make([]pb.Entry, 0)

}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	firstIndex := l.FirstIndex()
	appliedIndex := l.applied
	commitedIndex := l.committed
	if len(l.entries) > 0 {
		if appliedIndex >= firstIndex-1 && commitedIndex >= firstIndex-1 && appliedIndex < commitedIndex && commitedIndex <= l.LastIndex() {
			return l.entries[appliedIndex-firstIndex+1 : commitedIndex-firstIndex+1]
		}
	}
	return make([]pb.Entry, 0)
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	if len(l.entries) == 0 {
		index, _ := l.storage.LastIndex()
		return index
	}
	return l.entries[len(l.entries)-1].Index
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	if len(l.entries) > 0 {
		firstIndex := l.FirstIndex()
		lastIndex := l.LastIndex()
		if i >= firstIndex && i <= lastIndex {
			return l.entries[i-firstIndex].Term, nil
		}
	}

	term, err := l.storage.Term(i)
	if err == nil {
		return term, nil
	}
	return 0, err
}

// 将raftlog的commited修改为tocommit
func (l *RaftLog) commitTo(tocommit uint64) {
	// never decrease commit
	// 首先需要判断，commit索引绝不能变小
	if l.committed < tocommit {
		if l.LastIndex() < tocommit {
			// 传入的值如果比lastIndex大则是非法的
			log.Panicf("tocommit(%d) is out of range [lastIndex(%d)]. Was the raft log corrupted, truncated, or lost?", tocommit, l.LastIndex())
		}
		l.committed = tocommit
		// log.Infof("commit to %d", tocommit)
	}
}

// 将raftlog的applied修改为i
func (l *RaftLog) appliedTo(i uint64) {
	if i == 0 {
		return
	}
	// 判断合法性
	// 新的applied ID既不能比committed大，也不能比当前的applied索引小
	if l.committed < i || i < l.applied {
		log.Panicf("applied(%d) is out of range [prevApplied(%d), committed(%d)]", i, l.applied, l.committed)
	}
	// log.Infof("applied to %d, entries size: %d\n", i, len(l.entries))
	l.applied = i
}

func (l *RaftLog) stableTo(i, t uint64) {
	gt, _ := l.Term(i)
	if i > l.LastIndex() {
		return
	}
	if gt == t && i >= l.stabled {
		// log.Infof("stable to %d, entries size: %d\n", i, len(l.entries))
		l.stabled = i
	}
}

// 尝试去commit
func (l *RaftLog) maybeCommit(maxIndex, term uint64) bool {
	// 只有在传入的index大于当前commit索引，以及maxIndex对应的term与传入的term匹配时，才使用这些数据进行commit
	Term, _ := l.Term(maxIndex)
	// log.Infof("The match index is %d, raft term is %d,  get term is %d", maxIndex, term, Term)
	if maxIndex > l.committed && Term == term {
		l.commitTo(maxIndex)
		return true
	}
	return false
}

// 根据传入的Term，寻找该Term的第一个Entry
func (l *RaftLog) FindFirstTerm(term uint64) uint64 {
	// lst := l.LastIndex()
	// 从committed之后开始查找Term对应的Entry，因为即使entry是stabled的被持久化在storage中
	// 但是如果没有被committed，还是可以被修改进行同步
	if term == 0 {
		return 1
	}
	for _, e := range l.entries {
		if e.Term == term {
			return e.Index
		}
	}
	return l.entries[0].Index
}

// 根据传入的Term，寻找该Term的最后一个Entry
func (l *RaftLog) FindLastTerm(term uint64) uint64 {
	length := l.LastIndex()

	for i, e := range l.entries {
		if e.Term == term {
			if uint64(i) == length {
				return e.Index
			}
			if l.entries[i+1].Term != term {
				return e.Index
			}
		}
		if e.Term > term {
			return 0
		}
	}

	return 0
}

func (l *RaftLog) firstIndex() uint64 {
	// 如果存在snapshot则返回快照中的数据
	if l.pendingSnapshot != nil {
		return l.pendingSnapshot.Metadata.Index + 1
	}
	// 返回持久化数据的firsttIndex
	index, err := l.storage.FirstIndex()
	if err != nil {
		panic(err)
	}
	return index
}

// 获取从i开始的entries返回
func (l *RaftLog) Entries(i uint64) ([]*pb.Entry, error) {
	last := l.LastIndex()
	if i > last {
		return nil, nil
	}
	// log.Infof("Get Entries from[%d:%d]", i, l.LastIndex())
	if i < l.FirstIndex() {
		log.Panicf("The i is %d, FirstIndex is %d", i, l.FirstIndex())
	}
	var entries []*pb.Entry
	for j := i; j <= last; j++ {
		entries = append(entries, &l.entries[j-l.FirstIndex()])
	}
	return entries, nil
}

func (l *RaftLog) snapshot() (pb.Snapshot, error) {
	if l.pendingSnapshot != nil {
		return *l.pendingSnapshot, nil
	}
	return l.storage.Snapshot()
}

// 从Snapshot中恢复数据
func (l *RaftLog) snapRestore(snap pb.Snapshot) {

	// 丢弃之前的所有 entry
	if len(l.entries) > 0 {
		if snap.Metadata.Index >= l.LastIndex() {
			l.entries = nil
		} else {
			l.entries = l.entries[snap.Metadata.Index-l.FirstIndex()+1:]
		}
	}
	l.committed = snap.Metadata.Index
	l.dummyIndex = snap.Metadata.Index
	l.stabled = snap.Metadata.Index
	l.applied = snap.Metadata.Index
	l.pendingSnapshot = &snap
}

func (l *RaftLog) stableSnapTo(i uint64) {
	if l.pendingSnapshot != nil {
		// log.Infof("Ready snap index is %d, PendingSnapshot is %d", i, l.pendingSnapshot.Metadata.Index)
	}
	if l.pendingSnapshot != nil && l.pendingSnapshot.Metadata.Index == i {
		// 传入索引刚好是快照的索引，说明快照已经保存，当前快照可以置空
		// log.Infof("Stable Snap to %d, now Snap is nil", i)
		l.pendingSnapshot = nil
	}
}

// FirstIndex return the first index of the log entries
func (l *RaftLog) FirstIndex() uint64 {
	if len(l.entries) == 0 {
		index, _ := l.storage.FirstIndex()
		return index
	}
	return l.entries[0].Index
}
