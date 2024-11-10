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
	"errors"
	"math/rand"
	"sort"

	"github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	Match, Next uint64
}

type Raft struct {
	id uint64

	Term uint64
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes map[uint64]bool

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout int
	// random electionTimeout
	randomizedElectionTimeout int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// Ticks since it reached last electionTimeout when it is leader or candidate.
	// Number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	electionElapsed int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	hs, cs, err := c.Storage.InitialState()
	if err != nil {
		panic(err) // TODO(bdarnell)
	}
	// Your Code Here (2A).
	votes := make(map[uint64]bool)
	Prs := make(map[uint64]*Progress)
	peers := c.peers
	if len(cs.Nodes) > 0 {
		if len(peers) > 0 {
			panic("cannot specify both newRaft(peers) and ConfState.Nodes)")
		}
		peers = cs.Nodes
	}
	for _, i := range peers {
		// votes[i] = false
		// log.Printf("Initial progress [%d]\n", i)
		Prs[i] = &Progress{Next: 1, Match: 0}
	}
	log := newLog(c.Storage)
	raft := Raft{
		id:                        c.ID,
		electionTimeout:           c.ElectionTick,
		heartbeatTimeout:          c.HeartbeatTick,
		votes:                     votes,
		Prs:                       Prs,
		randomizedElectionTimeout: c.ElectionTick,
		RaftLog:                   log,
	}
	if hs.Vote != 0 || hs.Term != 0 || hs.Commit != 0 {
		raft.loadState(hs)
	}
	if c.Applied > 0 {
		log.appliedTo(c.Applied)
	}
	return &raft
}

// 加载Storage中的HardState
func (r *Raft) loadState(state pb.HardState) {
	if state.Commit < r.RaftLog.committed || state.Commit > r.RaftLog.LastIndex() {
		log.Panicf("%x state.commit %d is out of range [%d, %d]", r.id, state.Commit, r.RaftLog.committed, r.RaftLog.LastIndex())
	}
	r.RaftLog.committed = state.Commit
	r.Term = state.Term
	r.Vote = state.Vote
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	entry := []*pb.Entry{}
	msg := pb.Message{From: r.id, To: to, Term: r.Term}
	// 将entry[Next:]发送给peer
	pr := r.Prs[to]
	term, errt := r.RaftLog.Term(pr.Next - 1)
	// log.Infof("[%d] Send Append RPC Entry[%d:%d] to [%d]", r.id, pr.Next, r.RaftLog.LastIndex(), to)
	ents, erre := r.RaftLog.Entries(pr.Next)
	if errt != nil || erre != nil {
		msg.MsgType = pb.MessageType_MsgSnapshot
		snap, err := r.RaftLog.snapshot()
		if err != nil {
			if err == ErrSnapshotTemporarilyUnavailable {
				log.Infof("%x failed to send snapshot to %x because snapshot is temporarily unavailable", r.id, to)
				return false
			}
			log.Panic(err)
		}
		// 判断发送快照是否为空
		if IsEmptySnap(&snap) {
			log.Panicf("Need non-empty snapshot")
		}
		msg.Snapshot = &snap
		log.Infof("Send Snapshot[%d,%d] from %d to %d", snap.Metadata.Index, snap.Metadata.Term, r.id, to)

	} else {
		// 可以正确取到term和entries
		// log.Infof("Send append entries is %v", ents)
		for _, e := range ents {
			entry = append(entry, &pb.Entry{
				Index: e.Index,
				Term:  e.Term,
				Data:  e.Data,
			})
		}
		msg.MsgType = pb.MessageType_MsgAppend
		msg.Entries = entry
		msg.Index = r.Prs[to].Next - 1
		msg.LogTerm = term
		msg.Commit = r.RaftLog.committed
		// log.Printf("The Append entry is %v\n", entry[0])
	}

	r.msgs = append(r.msgs, msg)
	return true
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	// 发送一个空的Heartbeat RPC
	commit := min(r.Prs[to].Match, r.RaftLog.committed)
	msg := pb.Message{From: r.id, To: to, Term: r.Term, MsgType: pb.MessageType_MsgHeartbeat, Commit: commit}
	r.msgs = append(r.msgs, msg)
}

func (r *Raft) sendNoopEntry(to uint64) bool {

	entry := []*pb.Entry{}
	msg := pb.Message{From: r.id, To: to, Term: r.Term, MsgType: pb.MessageType_MsgAppend}
	pr := r.Prs[to]
	// log.Infof("[%d] Want send NoopEntry [%d:%d]", r.id, pr.Next, r.RaftLog.LastIndex())
	ents, _ := r.RaftLog.Entries(pr.Next)
	for _, e := range ents {
		entry = append(entry, &pb.Entry{
			Index: e.Index,
			Term:  e.Term,
			Data:  e.Data,
		})
	}
	// entry = append(entry, &pb.Entry{Data: nil, Term: r.Term, Index: r.RaftLog.LastIndex()})
	msg.Entries = entry
	msg.Index = pr.Next - 1
	msg.LogTerm, _ = r.RaftLog.Term(msg.Index)
	// log.Infof("Leader [%d] send noop entry[logterm: %d, index: %d] to [%d]", r.id, msg.LogTerm, msg.Index, to)
	msg.Commit = r.RaftLog.committed
	r.msgs = append(r.msgs, msg)
	return true
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	r.electionElapsed++
	if r.electionElapsed >= r.randomizedElectionTimeout {
		msg := pb.Message{From: r.id, To: r.id, MsgType: pb.MessageType_MsgHup}
		//r.msgs = append(r.msgs, msg)
		r.electionElapsed = 0
		r.randomizedElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
		r.Step(msg)

	}
	if r.State == StateLeader {
		r.heartbeatElapsed++
		// Leader对每个peer发送一个heartbeat请求
		if r.heartbeatElapsed >= r.heartbeatTimeout {
			r.heartbeatElapsed = 0
			for key := range r.Prs {
				if key != r.id {
					r.sendHeartbeat(key)
				}
			}
		}
	}

}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	// change the state of raft node
	r.State = StateFollower
	// change the leader
	r.Lead = lead
	// increment the term
	r.electionElapsed = 0
	r.heartbeatElapsed = 0
	if r.Term != term {
		r.Term = term
		r.Vote = None
	}
	for id := range r.Prs {
		r.Prs[id] = &Progress{Next: r.RaftLog.LastIndex() + 1}
		if id == r.id {
			r.Prs[id].Match = r.RaftLog.LastIndex()
		}
	}

}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	// change the state tot Candidate and increment the term
	r.State = StateCandidate
	r.votes = make(map[uint64]bool)
	r.Vote = r.id
	r.Lead = None
	r.electionElapsed = 0
	r.heartbeatElapsed = 0
	for id := range r.Prs {
		r.Prs[id] = &Progress{Next: r.RaftLog.LastIndex() + 1}
		if id == r.id {
			r.Prs[id].Match = r.RaftLog.LastIndex()
		}
	}
	r.Term++
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	r.State = StateLeader
	r.Lead = r.id
	for id := range r.Prs {
		r.Prs[id] = &Progress{Next: r.RaftLog.LastIndex() + 1}
		if id == r.id {
			r.Prs[id].Match = r.RaftLog.LastIndex()
		}
	}
	// Leader propose a noop entry
	// log.Infof("[%d] become Leader\n", r.id)
	entry := pb.Entry{Data: nil, Index: r.RaftLog.LastIndex() + 1, Term: r.Term}
	r.RaftLog.append(entry)
	// log.Infof("Raft append entry with index %d, after append last index is %d", entry.Index, r.RaftLog.LastIndex())
	if r.Prs[r.id] != nil {
		r.Prs[r.id].maybeUpdate(r.RaftLog.LastIndex())
	}
	r.maybeCommit()

}

func (r *Raft) isUpToDate(e pb.Entry) bool {
	index := r.RaftLog.LastIndex()
	term, _ := r.RaftLog.Term(index)
	if term < e.Term {
		return true
	} else if term == e.Term {
		if index <= e.Index {
			return true
		}
	}

	return false
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	if m.Term > r.Term {
		lead := m.From
		// log.Infof("%x [term: %d] received a %s message with higher term from %x [term: %d]\n",
		// 	r.id, r.Term, m.MsgType, m.From, m.Term)
		// 如果是MessageType_MsgRequestVote请求，则将lead置为None
		if m.MsgType == pb.MessageType_MsgRequestVote {
			lead = None
		}
		r.becomeFollower(m.Term, lead)
	}
	if m.MsgType == pb.MessageType_MsgRequestVote {
		entry := pb.Entry{Term: m.LogTerm, Index: m.Index}
		CanVote := m.Term > r.Term || r.Vote == None || r.Vote == m.From
		if CanVote && r.isUpToDate(entry) {
			msg := pb.Message{From: r.id, To: m.From, MsgType: pb.MessageType_MsgRequestVoteResponse, Term: r.Term}
			r.msgs = append(r.msgs, msg)
			r.electionElapsed = 0
			r.Vote = m.From
			// r.becomeFollower(m.Term, None)
		} else {
			msg := pb.Message{From: r.id, To: m.From, MsgType: pb.MessageType_MsgRequestVoteResponse, Reject: true, Term: r.Term}
			r.msgs = append(r.msgs, msg)
		}
		return nil
	}
	switch r.State {
	case StateFollower:
		switch m.MsgType {
		// start new election
		case pb.MessageType_MsgHup:
			r.becomeCandidate()
			// Candidate vote for self
			r.votes[r.id] = true
			if r.hasMajority() {
				r.becomeLeader()
				for id := range r.Prs {
					if id != r.id {
						// r.sendNoopEntry(id)
						r.sendAppend(id)
					}
				}
			} else {
				for key := range r.Prs {
					if key != r.id {
						index := r.RaftLog.LastIndex()
						term, _ := r.RaftLog.Term(index)
						// log.Infof("Candidate [%d] Send [logterm %d, index %d] to Vote\n", r.id, term, index)
						msg := pb.Message{From: r.id, To: key, MsgType: pb.MessageType_MsgRequestVote, Term: r.Term, Index: index, LogTerm: term}
						r.msgs = append(r.msgs, msg)
					}

				}
			}

		case pb.MessageType_MsgAppend:
			// 当前的Term小于Append
			// log.Infof("[%d] Received Append RPC from [%d]", m.To, m.From)
			r.electionElapsed = 0
			r.Lead = m.From
			r.handleAppendEntries(m)
		case pb.MessageType_MsgHeartbeat:
			r.electionElapsed = 0
			r.Lead = m.From
			r.handleHeartbeat(m)
		case pb.MessageType_MsgSnapshot:
			// 处理snapshot消息
			r.Lead = m.From
			r.electionElapsed = 0
			r.handleSnapshot(m)
		}

	case StateCandidate:
		switch m.MsgType {
		case pb.MessageType_MsgHup:
			r.becomeCandidate()
			// Candidate vote for self
			r.votes[r.id] = true
			if r.hasMajority() {
				r.becomeLeader()
				for id := range r.Prs {
					if id != r.id {
						// r.sendNoopEntry(id)
						r.sendAppend(id)
					}
				}
			} else {
				for key := range r.Prs {
					if key != r.id {
						index := r.RaftLog.LastIndex()
						term, _ := r.RaftLog.Term(index)
						// log.Infof("Candidate [%d] Send [logterm %d, index %d] to Vote\n", r.id, term, index)
						msg := pb.Message{From: r.id, To: key, MsgType: pb.MessageType_MsgRequestVote, Term: r.Term, Index: index, LogTerm: term}
						r.msgs = append(r.msgs, msg)
					}
				}
			}

		case pb.MessageType_MsgRequestVoteResponse:
			if !m.Reject {
				r.votes[m.From] = true
				trueCount := 0
				// 统计投票的数量
				for _, vote := range r.votes {
					if vote {
						trueCount++
					}
				}

				if trueCount >= r.quorum() {
					// 得到超过一半的票，成为leader
					r.becomeLeader()
					for id := range r.Prs {
						if id != r.id {
							// r.sendNoopEntry(id)
							r.sendAppend(id)
						}
					}
				}
			} else {
				r.votes[m.From] = false
				falseCount := 0
				// 统计拒绝数量
				for _, vote := range r.votes {
					if !vote {
						falseCount++
					}
				}
				if falseCount >= r.quorum() {
					r.becomeFollower(m.Term, None)
				}
			}
		case pb.MessageType_MsgAppend:
			if m.Term >= r.Term {
				r.becomeFollower(m.Term, m.From)
			}
		case pb.MessageType_MsgHeartbeat:
			r.becomeFollower(m.Term, m.From)
			r.handleHeartbeat(m)
		case pb.MessageType_MsgSnapshot:
			// 处理snapshot消息
			r.becomeFollower(m.Term, m.From)
			r.handleSnapshot(m)
		}

	case StateLeader:
		switch m.MsgType {
		case pb.MessageType_MsgAppend:
			{
				if m.Term >= r.Term {
					// log.Printf("Leader [%d] become Follower\n", r.id)
					r.becomeFollower(m.Term, m.From)
				}
			}
		case pb.MessageType_MsgBeat:
			for id := range r.Prs {
				if r.id != id {
					r.sendHeartbeat(id)
				}
			}

		case pb.MessageType_MsgPropose:
			// log.Infof("Receive Propose signal!\n")
			entry := pb.Entry{Data: m.Entries[0].Data, Term: r.Term, Index: r.RaftLog.LastIndex() + 1}
			r.RaftLog.append(entry)
			// log.Infof("Raft append entry with index %d, after append last index is %d", entry.Index, r.RaftLog.LastIndex())
			r.Prs[r.id].maybeUpdate(r.RaftLog.LastIndex())
			r.maybeCommit()
			// log.Infof("After Propose, Raftlog Entry Length is %d\n", len(r.RaftLog.entries))
			for id := range r.Prs {
				if r.id != id {
					r.sendAppend(id)
				}
			}

		case pb.MessageType_MsgAppendResponse:
			if !m.Reject {
				pr := r.Prs[m.From]
				pr.maybeUpdate(m.Index)
				if r.maybeCommit() {
					// 更新follower的commited
					for id := range r.Prs {
						if id != r.id {
							r.sendAppend(id)
						}
					}
				}
			} else {
				// append失败的index,减1继续发送append请求
				pr := r.Prs[m.From]
				if m.Index != pr.Next-1 {
					return nil
				}
				pr.Next = m.Index
				if pr.Next < 1 {
					pr.Next = 1
					log.Panicf("Retry Append Error!\n")
				}
				entry := []*pb.Entry{}
				msg := pb.Message{From: r.id, To: m.From, Term: r.Term, MsgType: pb.MessageType_MsgAppend}
				ents, err := r.RaftLog.Entries(pr.Next)
				if err != nil {
					log.Panicf("Append fail: Get Entries fail!")
				}
				for _, e := range ents {
					entry = append(entry, &pb.Entry{
						Index: e.Index,
						Term:  e.Term,
						Data:  e.Data,
					})
				}
				msg.Entries = entry
				msg.Index = pr.Next - 1
				msg.LogTerm, _ = r.RaftLog.Term(msg.Index)
				msg.Commit = r.RaftLog.committed
				r.msgs = append(r.msgs, msg)
				// log.Infof("[%d] retry to append [logterm: %d, index: %d] to [%d]\n", r.id, msg.LogTerm, msg.Index, m.From)
			}
		case pb.MessageType_MsgHeartbeatResponse:
			pr := r.Prs[m.From]
			// 如果follower的Match index小于Leader的lastindex，就向follower发送append请求
			if pr.Match < r.RaftLog.LastIndex() {
				r.sendAppend(m.From)
			}
		}
	}
	return nil
}

// 超过半数的节点数量
func (r *Raft) quorum() int { return len(r.Prs)/2 + 1 }

func (r *Raft) hasMajority() bool {
	trueCount := 0
	// 统计投票的数量
	for _, vote := range r.votes {
		if vote {
			trueCount++
		}
	}

	if trueCount >= r.quorum() {
		// 得到超过一半的票，成为leader
		return true
	}
	return false
}

// 尝试commit当前的日志，如果commit日志索引发生变化了就返回true
func (r *Raft) maybeCommit() bool {
	mis := make(uint64Slice, 0, len(r.Prs))
	// 拿到当前所有节点的Match到数组中
	for id := range r.Prs {
		mis = append(mis, r.Prs[id].Match)
	}
	// 逆序排列
	sort.Sort(sort.Reverse(mis))
	// 排列之后拿到中位数的Match，因为如果这个位置的Match对应的Term也等于当前的Term
	// 说明有过半的节点至少comit了mci这个索引的数据，这样leader就可以以这个索引进行commit了
	mci := mis[r.quorum()-1]
	// raft日志尝试commit
	return r.RaftLog.maybeCommit(mci, r.Term)
}

// 收到appresp的成功应答之后，leader更新节点的索引数据
// 如果传入的n小于等于当前的match索引，则索引就不会更新，返回false；否则更新索引返回true
func (pr *Progress) maybeUpdate(n uint64) bool {
	var updated bool
	if pr.Match < n {
		pr.Match = n
		updated = true
	}
	if pr.Next < n+1 {
		// log.Infof("Update Next index from %d to %d", pr.Next, n+1)
		pr.Next = n + 1
	}

	return updated
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	// 如果发过来的消息索引小于当前commit索引，就返回当前commit索引
	if m.Term >= r.Term {
		r.Term = m.Term
	} else {
		msg := pb.Message{From: r.id, To: m.From, MsgType: pb.MessageType_MsgAppendResponse, Term: r.Term, Reject: true}
		r.msgs = append(r.msgs, msg)
		return
	}
	if m.Index < r.RaftLog.committed {
		msg := pb.Message{From: r.id, To: m.From, MsgType: pb.MessageType_MsgAppendResponse, Index: r.RaftLog.committed, Term: r.Term}
		r.msgs = append(r.msgs, msg)
		return
	}

	entry := []pb.Entry{}
	if len(m.Entries) != 0 {
		for _, ent := range m.Entries {
			entry = append(entry, *ent)
		}
	}
	if mlastIndex, ok := r.RaftLog.maybeAppend(m.Index, m.LogTerm, m.Commit, entry...); ok {
		// 添加日志成功，应答当前的最后索引回去
		msg := pb.Message{From: r.id, To: m.From, MsgType: pb.MessageType_MsgAppendResponse, Index: mlastIndex, Term: r.Term}
		r.msgs = append(r.msgs, msg)
		// log.Infof("[%d] after Append commited is %d, LastIndex is %d\n", r.id, r.RaftLog.committed, mlastIndex)
	} else {
		// 添加日志失败
		// term, _ := r.RaftLog.Term(m.Index)
		// log.Infof("%x [logterm: %d, index: %d] rejected msgApp [logterm: %d, index: %d] from %x\n",
		// 	r.id, term, m.Index, m.LogTerm, m.Index, m.From)
		msg := pb.Message{From: r.id, To: m.From, MsgType: pb.MessageType_MsgAppendResponse, Index: m.Index, Term: r.Term, Reject: true}
		r.msgs = append(r.msgs, msg)
	}
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	// log.Infof("[%d] receive heartbeat from [%d]\n", r.id, m.From)
	r.RaftLog.commitTo(m.Commit)
	msg := pb.Message{From: r.id, To: m.From, MsgType: pb.MessageType_MsgHeartbeatResponse}
	r.msgs = append(r.msgs, msg)
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
	sindex, sterm := m.Snapshot.Metadata.Index, m.Snapshot.Metadata.Term
	if r.snapRestore(*m.Snapshot) {
		log.Infof("%x [commit: %d] restored snapshot [index: %d, term: %d]",
			r.id, r.RaftLog.committed, sindex, sterm)
		msg := pb.Message{From: r.id, To: m.From, MsgType: pb.MessageType_MsgAppendResponse, Index: r.RaftLog.LastIndex()}
		r.msgs = append(r.msgs, msg)

	} else {
		log.Infof("%x [commit: %d] ignored snapshot [index: %d, term: %d]",
			r.id, r.RaftLog.committed, sindex, sterm)
		msg := pb.Message{From: r.id, To: m.From, MsgType: pb.MessageType_MsgAppendResponse, Index: r.RaftLog.committed}
		r.msgs = append(r.msgs, msg)
	}
}

func (r *Raft) snapRestore(snap pb.Snapshot) bool {
	if snap.Metadata.Index <= r.RaftLog.committed {
		return false
	}
	term, _ := r.RaftLog.Term(snap.Metadata.Index)
	// Term匹配说明raftlog中已经存在对应的日志
	if term == snap.Metadata.Term {
		r.RaftLog.commitTo(snap.Metadata.Index)
		return false
	}
	r.RaftLog.snapRestore(snap)
	r.Prs = make(map[uint64]*Progress)
	// 对集群中其他节点的状态也使用快照中的状态数据进行恢复
	for _, n := range snap.Metadata.ConfState.Nodes {
		match, next := uint64(0), r.RaftLog.LastIndex()+1
		if n == r.id {
			match = next - 1
		}
		r.Prs[n] = &Progress{
			Match: match,
			Next:  next,
		}
		// log.Infof("%x restored progress of %x [%s]", r.id, n, r.Prs[n])
	}
	return true
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}

func (r *Raft) softState() *SoftState { return &SoftState{Lead: r.Lead, RaftState: r.State} }

func (r *Raft) hardState() pb.HardState {
	return pb.HardState{
		Term:   r.Term,
		Vote:   r.Vote,
		Commit: r.RaftLog.committed,
	}
}
