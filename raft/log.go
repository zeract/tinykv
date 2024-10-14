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
	"log"

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
	log.Printf("New RaftLog Entry Length is %d\n", len(entries))
	log.Printf("The firstIndex is %d, LastIndex is %d\n", firstIndex, lastIndex)
	log := RaftLog{entries: entries, storage: storage, dummyIndex: firstIndex - 1}
	// committed和applied从持久化的第一个index的前一个开始
	log.committed = firstIndex - 1
	log.applied = firstIndex - 1
	log.stabled = lastIndex
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
	return l.entries[l.stabled:]
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	// log.Printf("The Applied index is %d, Commited index is %d\n", l.applied, l.committed)
	return l.entries[l.applied:l.committed]
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	if len(l.entries) == 0 {
		return 0
	}
	return uint64(len(l.entries))
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).

	lastIndex := l.LastIndex()
	log.Printf("Term index is %d, LastIndex is %d\n", i, lastIndex)
	if i > lastIndex {
		return 0, nil
	}
	if i == 0 {
		return 0, nil
	}
	entry := l.entries[i-1]
	return entry.Term, nil
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
		log.Printf("commit to %d", tocommit)
	}
}

// 尝试去commit
func (l *RaftLog) maybeCommit(maxIndex, term uint64) bool {
	// 只有在传入的index大于当前commit索引，以及maxIndex对应的term与传入的term匹配时，才使用这些数据进行commit
	Term, _ := l.Term(maxIndex)
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
	if after == l.LastIndex()+1 {
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
				log.Printf("found conflict at index %d [existing term: %d, conflicting term: %d]",
					ne.Index, term, ne.Term)
			}
			return ne.Index
		}
	}
	return 0
}
