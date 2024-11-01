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
	if len(l.entries) == 0 {
		return nil
	}
	// // 不能直接使用l.stabled，还要判断其与entries的长度
	// if l.stabled > uint64(len(l.entries)) {
	// 	return l.entries
	// 	// panic("unstableEntries Error!")
	// }

	return l.entries[l.stabled-l.dummyIndex:]
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	// log.Printf("The Applied index is %d, Commited index is %d\n", l.applied, l.committed)
	start := max(l.applied+1, l.firstIndex())
	if l.committed+1 > start {
		ents, err := l.slice(start, l.committed+1)
		if err != nil {
			log.Panicf("unexpected error when getting unapplied entries (%v)", err)
		}
		return ents
	}
	return nil
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	if len(l.unstableEntries()) != 0 {
		return l.stabled + uint64(len(l.unstableEntries()))
	}

	i, err := l.storage.LastIndex()
	if err != nil {
		panic(err)
	}
	return i
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	dummyIndex := l.firstIndex() - 1
	lastIndex := l.LastIndex()
	// 先判断范围是否在[dummyIndex, last index]
	if i < dummyIndex || i > lastIndex {
		return 0, nil
	}

	// log.Infof("Term index is %d, LastIndex is %d\n", i, lastIndex)
	// 对于snapshot的情况，之后再进行处理
	if t, ok := l.maybeunstableTerm(i); ok {
		// log.Infof("The term in index %d is %d", i, t)
		return t, nil
	}

	// 尝试从storage中查询term
	t, err := l.storage.Term(i)
	if err == nil {
		return t, nil
	}
	// 只有这两种错可以接受
	if err == ErrCompacted || err == ErrUnavailable {
		return 0, err
	}
	panic(err)

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

// 尝试去append entry，返回(新entry的index，true)或者(0,false)
func (l *RaftLog) maybeAppend(index, logTerm, committed uint64, ents ...pb.Entry) (lastnewi uint64, ok bool) {
	term, _ := l.Term(index)
	if term == logTerm {
		lastnewi = index + uint64(len(ents))
		ci := l.findConflict(ents)
		switch {
		case ci == 0:
		case ci <= l.committed:
			log.Panicf("entry %d conflict with committed entry [committed(%d)]", ci, l.committed)
		default:
			offset := index + 1
			l.append(ents[ci-offset:]...)
		}
		l.commitTo(min(committed, lastnewi))
		return lastnewi, true
	}
	return 0, false
}

func (l *RaftLog) append(ents ...pb.Entry) uint64 {
	if len(ents) == 0 {
		return l.LastIndex()
	}
	if after := ents[0].Index - 1; after < l.committed {
		log.Panicf("after(%d) is out of range [committed(%d)]", after, l.committed)
	}
	// l.unstable.truncateAndAppend(ents)
	after := ents[0].Index
	offset := l.stabled + 1
	unstable := l.unstableEntries()
	if after == offset+uint64(len(unstable)) {
		l.entries = append(l.entries, ents...)
	} else if after <= offset {
		l.stabled = after - 1
		l.entries = append([]pb.Entry{}, l.entries[:l.stabled-l.dummyIndex]...)
		l.entries = append(l.entries, ents...)
	} else {
		l.entries = append([]pb.Entry{}, l.entries[:after-1]...)
		l.entries = append(l.entries, ents...)
		l.stabled = after - 1
	}
	return l.LastIndex()
}

func (l *RaftLog) findConflict(ents []pb.Entry) uint64 {
	for _, ne := range ents {
		term, _ := l.Term(ne.Index)
		if term != ne.Term {
			if ne.Index <= l.LastIndex() {
				// log.Infof("found conflict at index %d [existing term: %d, conflicting term: %d]",
				// 	ne.Index, term, ne.Term)
			}
			return ne.Index
		}
	}
	return 0
}

func (l *RaftLog) firstIndex() uint64 {

	// 返回持久化数据的firsttIndex
	index, err := l.storage.FirstIndex()
	if err != nil {
		panic(err)
	}
	return index
}

func (l *RaftLog) hasNextEnts() bool {
	off := max(l.applied+1, l.firstIndex())
	return l.committed+1 > off
}

func (l *RaftLog) maybeunstableTerm(i uint64) (uint64, bool) {
	// log.Infof("Try to get unstable Term, the index is %d, stable index is %d, lastindex is %d", i, l.stabled, l.LastIndex())
	if i <= l.stabled {
		return 0, false
	}

	last := l.LastIndex()
	if i > last || i == 0 {
		return 0, false
	}
	// 使用unstable entry来获取term
	unstable := l.unstableEntries()
	return unstable[i-l.stabled-1].Term, true
}

// 获取从i开始的entries返回
func (l *RaftLog) Entries(i uint64) ([]pb.Entry, error) {
	if i > l.LastIndex() {
		return nil, nil
	}
	// log.Infof("Get Entries from[%d:%d]", i, l.LastIndex())
	return l.slice(i, l.LastIndex()+1)
}

func (l *RaftLog) slice(lo, hi uint64) ([]pb.Entry, error) {
	err := l.mustCheckOutOfBounds(lo, hi)
	if err != nil {
		return nil, err
	}
	if lo == hi {
		return nil, nil
	}
	var ents []pb.Entry
	offset := l.stabled + 1
	if lo < offset {
		// lo 小于unstable的offset，说明前半部分在持久化的storage中

		// 传入storage.Entries的hi参数取hi和unstable offset的较小值
		storedEnts, err := l.storage.Entries(lo, min(hi, offset))
		if err == ErrCompacted {
			return nil, err
		} else if err == ErrUnavailable {
			log.Panicf("entries[%d:%d) is unavailable from storage", lo, min(hi, offset))
		} else if err != nil {
			panic(err)
		}

		// check if ents has reached the size limitation
		if uint64(len(storedEnts)) < min(hi, offset)-lo {
			return storedEnts, nil
		}

		ents = storedEnts
	}

	if hi > offset {
		// hi大于unstable offset，说明后半部分在unstable中取得
		lo = max(lo, offset)
		entries := l.unstableEntries()
		unstable := entries[lo-offset : hi-offset]
		if len(ents) > 0 {
			ents = append([]pb.Entry{}, ents...)
			ents = append(ents, unstable...)
		} else {
			ents = unstable
		}
	}
	return ents, nil
}

// l.firstIndex <= lo <= hi <= l.firstIndex + len(l.entries)
func (l *RaftLog) mustCheckOutOfBounds(lo, hi uint64) error {
	if lo > hi {
		log.Panicf("invalid slice %d > %d", lo, hi)
	}
	fi := l.firstIndex()
	if lo < fi {
		return ErrCompacted
	}

	length := l.LastIndex() + 1 - fi
	if lo < fi || hi > fi+length {
		log.Panicf("slice[%d,%d) out of bound [%d,%d]", lo, hi, fi, l.LastIndex())
	}
	return nil
}
