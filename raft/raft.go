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

	"github.com/pingcap-incubator/tinykv/kv/raftstore/util"
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
	StatePreCandidate
)

type SnapShotStateType uint64

const (
	StateNormal SnapShotStateType = iota
	StateSending
)

// CampaignType represents the type of campaigning
// the reason we use the type of string instead of uint64
// is because it's simpler to compare and fill in raft entries
type CampaignType string

// Possible values for CampaignType
const (
	// campaignPreElection represents the first phase of a normal election when
	// Config.PreVote is true.
	campaignPreElection CampaignType = "CampaignPreElection"
	// campaignElection represents a normal (time-based) election (the second phase
	// of the election when Config.PreVote is true).
	campaignElection CampaignType = "CampaignElection"
	// campaignTransfer represents the type of leader transfer
	// 由于leader转让发起的竞选
	campaignTransfer CampaignType = "CampaignTransfer"
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

	// PreVote Flag
	preVote bool
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

	// the last communication ts
	lastCommunicteTs int64
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

	// PreVote Config
	preVote bool

	// Debug Config
	debug bool

	// SnapShot check
	haveSendedSnapShot map[uint64]int

	// SnapShotTimeout
	pendingSnapShotTimeout int

	// 在某一轮心跳中，每个 follower 是否给了 heartbeat 回应，用于应对网络分区
	// 每一次 electionTimeout ，就重置
	heartbeatResp map[uint64]bool

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
	haveSendedSnapShot := make(map[uint64]int)
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
		Prs[i] = &Progress{Next: 1, Match: 0, lastCommunicteTs: 0}
	}
	log := newLog(c.Storage)
	raft := Raft{
		id:                        c.ID,
		electionTimeout:           c.ElectionTick,
		heartbeatTimeout:          c.HeartbeatTick,
		votes:                     votes,
		Prs:                       Prs,
		randomizedElectionTimeout: c.ElectionTick + rand.Intn(c.ElectionTick),
		RaftLog:                   log,
		preVote:                   c.preVote,
		debug:                     false,
		pendingSnapShotTimeout:    5 * c.ElectionTick / 10,
		haveSendedSnapShot:        haveSendedSnapShot,
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
	if _, ok := r.Prs[to]; !ok {
		return false
	}
	// Your Code Here (2A).
	entry := []*pb.Entry{}
	msg := pb.Message{From: r.id, To: to, Term: r.Term}
	// 将entry[Next:]发送给peer
	pr := r.Prs[to]
	term, errt := r.RaftLog.Term(pr.Next - 1)
	if r.debug {
		log.Infof("[%d] Send Append Entry[%d:%d] to [%d]", r.id, pr.Next, r.RaftLog.LastIndex(), to)
	}
	ents, erre := r.RaftLog.Entries(pr.Next)
	if errt != nil || erre != nil {

		if _, ok := r.haveSendedSnapShot[to]; ok {
			// 如果已经发送了snapshot，那么就不再发送append消息
			return false
		}
		msg.MsgType = pb.MessageType_MsgSnapshot
		snap, err := r.RaftLog.snapshot()
		if err != nil {
			if err == ErrSnapshotTemporarilyUnavailable {
				if r.debug {
					log.Infof("%x failed to send snapshot to %x because snapshot is temporarily unavailable", r.id, to)
				}
				return false
			}
			log.Panic(err)
		}
		// 判断发送快照是否为空
		if IsEmptySnap(&snap) {
			log.Panicf("Need non-empty snapshot")
		}
		msg.Snapshot = &snap
		// 发送snapshot，添加记录
		r.haveSendedSnapShot[to] = 1
		if r.debug {
			log.Infof("Send Snapshot[%d,%d] from %d to %d", snap.Metadata.Index, snap.Metadata.Term, r.id, to)
		}

	} else {
		// 可以正确取到term和entries
		// log.Infof("Send append entries is %v", ents)
		for _, e := range ents {
			entry = append(entry, &pb.Entry{
				EntryType: e.EntryType,
				Index:     e.Index,
				Term:      e.Term,
				Data:      e.Data,
			})
			// entry = append(entry, &e)
		}
		msg.MsgType = pb.MessageType_MsgAppend
		msg.Entries = entry
		msg.Index = r.Prs[to].Next - 1
		msg.LogTerm = term
		msg.Commit = r.RaftLog.committed
		// Pipeline 优化？
		// r.Prs[to].Next = msg.Index + uint64(len(ents)) + 1
		// log.Printf("The Append entry is %v\n", entry[0])
	}

	r.msgs = append(r.msgs, msg)
	return true
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	// 发送一个空的Heartbeat RPC
	// commit := min(r.Prs[to].Match, r.RaftLog.committed)
	if _, ok := r.Prs[to]; !ok {
		return
	}
	msg := pb.Message{From: r.id, To: to, Term: r.Term, MsgType: pb.MessageType_MsgHeartbeat, Commit: util.RaftInvalidIndex}
	r.msgs = append(r.msgs, msg)
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	if _, ok := r.Prs[r.id]; !ok {
		return
	}
	r.electionElapsed++
	switch r.State {
	case StateFollower, StateCandidate:
		if r.electionElapsed >= r.randomizedElectionTimeout {
			if r.debug {
				log.Infof("Timeout Send MsgHup to [%d]", r.id)
			}
			r.electionElapsed = 0
			r.randomizedElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
			r.Step(pb.Message{From: r.id, To: r.id, MsgType: pb.MessageType_MsgHup})

		}
	case StateLeader:
		r.heartbeatElapsed++
		hbrNum := len(r.heartbeatResp)
		total := len(r.Prs)
		if r.electionElapsed >= r.randomizedElectionTimeout {
			if r.debug {
				log.Infof("Timeout Send MsgHup to [%d]", r.id)
			}
			r.electionElapsed = 0
			r.randomizedElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
			r.heartbeatResp = make(map[uint64]bool)
			r.heartbeatResp[r.id] = true
			// 心跳回应数不超过一半，说明成为孤岛，重新开始选举
			if hbrNum*2 <= total {
				// if r.preVote {
				// 	r.becomeFollower(r.Term, None)
				// 	r.campaign(campaignPreElection)
				// } else {
				// 	r.campaign(campaignElection)
				// }
				r.becomeFollower(r.Term, None)
			}
			if r.leadTransferee != None {
				r.leadTransferee = None
			}
		}
		// 遍历r.haveSendedSnapShot，对每个项加1
		for k, v := range r.haveSendedSnapShot {
			r.haveSendedSnapShot[k] = v + 1
			if r.haveSendedSnapShot[k] > r.pendingSnapShotTimeout {
				delete(r.haveSendedSnapShot, k)
			}
		}
		// Leader对每个peer发送一个heartbeat请求
		if r.heartbeatElapsed >= r.heartbeatTimeout {
			r.heartbeatElapsed = 0
			for id := range r.Prs {
				if id != r.id {
					r.sendHeartbeat(id)
				}
			}
		}
	}
	// r.tickLeaderLeaseCheck()
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	if _, ok := r.Prs[r.id]; !ok {
		return
	}
	// change the state of raft node
	r.State = StateFollower
	// change the leader
	r.Lead = lead
	// increment the term
	r.electionElapsed = 0
	r.heartbeatElapsed = 0
	r.heartbeatResp = make(map[uint64]bool)
	r.heartbeatResp[r.id] = true
	// r.PendingConfIndex = 0
	r.leadTransferee = None
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

// becomePreCandidate transform this peer's state to Precandidate
func (r *Raft) becomePreCandidate() {
	if _, ok := r.Prs[r.id]; !ok {
		return
	}
	if r.State == StateLeader {
		log.Panicf("Invalid transition [Leader -> preCandidate]")
	}
	// change State to PreCandidate
	r.State = StatePreCandidate
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	// change the state tot Candidate and increment the term
	if _, ok := r.Prs[r.id]; !ok {
		return
	}
	r.State = StateCandidate
	r.votes = make(map[uint64]bool)
	r.Vote = r.id
	r.Lead = None
	r.electionElapsed = 0
	r.heartbeatElapsed = 0
	r.heartbeatResp = make(map[uint64]bool)
	r.heartbeatResp[r.id] = true
	// r.PendingConfIndex = 0
	r.leadTransferee = None
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
	if _, ok := r.Prs[r.id]; !ok {
		return
	}
	r.State = StateLeader
	r.Lead = r.id
	r.heartbeatResp = make(map[uint64]bool)
	r.heartbeatResp[r.id] = true
	// r.PendingConfIndex = 0
	r.leadTransferee = None
	for id := range r.Prs {
		r.Prs[id] = &Progress{Next: r.RaftLog.LastIndex() + 1}
		if id == r.id {
			r.Prs[id].Match = r.RaftLog.LastIndex()
		}
	}
	ents, err := r.RaftLog.Entries(r.RaftLog.committed + 1)
	if err != nil {
		log.Panic(err)
	}
	nconf := numOfPendingConf(ents)
	if nconf > 1 {
		panic("unexpected multiple uncommitted config entry")
	}
	if nconf == 1 {
		r.PendingConfIndex = r.RaftLog.LastIndex()
	}
	// Leader propose a noop entry
	if r.debug {
		log.Infof("[%d] become Leader", r.id)
	}
	entry := pb.Entry{Data: nil, Index: r.RaftLog.LastIndex() + 1, Term: r.Term}
	r.RaftLog.append(entry)
	// log.Infof("Raft append entry with index %d, after append last index is %d", entry.Index, r.RaftLog.LastIndex())
	if r.Prs[r.id] != nil {
		r.Prs[r.id].maybeUpdate(r.RaftLog.LastIndex())
	}
	r.maybeCommit()

}

func (r *Raft) campaign(t CampaignType) {
	var term uint64
	var voteMsg pb.MessageType
	if t == campaignPreElection {
		r.becomePreCandidate()
		voteMsg = pb.MessageType_MsgPreRequestVote
		// PreVote RPCs are sent for the next term before we've incremented r.Term.
		term = r.Term + 1
	} else {
		if r.debug {
			log.Infof("[%d] start campaign", r.id)
		}
		r.becomeCandidate()
		voteMsg = pb.MessageType_MsgRequestVote
		term = r.Term
	}

	// Candidate vote for self
	r.votes[r.id] = true
	if r.hasMajority() {
		if t == campaignPreElection {
			r.campaign(campaignElection)
		} else {
			// 如果给自己投票之后，刚好超过半数的通过，那么就成为新的leader
			r.becomeLeader()
			for id := range r.Prs {
				if id != r.id {
					// r.sendNoopEntry(id)
					r.sendAppend(id)
				}
			}
		}
	} else {
		for id := range r.Prs {
			if id != r.id {
				index := r.RaftLog.LastIndex()
				logTerm, _ := r.RaftLog.Term(index)
				if r.debug {
					log.Infof("Candidate [%d] Send [logterm %d, index %d] to %d\n", r.id, term, index, id)
				}
				msg := pb.Message{From: r.id, To: id, MsgType: voteMsg, Term: term, Index: index, LogTerm: logTerm}
				r.msgs = append(r.msgs, msg)
			}

		}
	}

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

// voteResponseType maps vote and prevote message types to their corresponding responses.
func voteRespMsgType(msgt pb.MessageType) pb.MessageType {
	switch msgt {
	case pb.MessageType_MsgRequestVote:
		return pb.MessageType_MsgRequestVoteResponse
	case pb.MessageType_MsgPreRequestVote:
		return pb.MessageType_MsgPreRequestVoteResponse
	default:
		log.Panicf("not a vote message: %s", msgt)
	}
	panic("VoteResp Err")
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// if _, ok := r.Prs[m.From]; !ok {
	// 	return nil
	// }
	if r.debug {
		log.Infof("[%d] Receive %s Message from %d", r.id, m.MsgType, m.From)
	}

	// Your Code Here (2A).
	if m.Term > r.Term {
		lead := m.From
		// log.Infof("%x [term: %d] received a %s message with higher term from %x [term: %d]\n",
		// 	r.id, r.Term, m.MsgType, m.From, m.Term)
		// 如果是MessageType_MsgRequestVote请求，则将lead置为None
		if m.MsgType == pb.MessageType_MsgRequestVote {
			lead = None
		}
		switch {
		case m.MsgType == pb.MessageType_MsgPreRequestVote:
		case m.MsgType == pb.MessageType_MsgPreRequestVoteResponse && !m.Reject:
		default:
			if r.State != StateFollower {
				r.becomeFollower(r.Term, lead)
			}
		}

	}
	if m.MsgType == pb.MessageType_MsgRequestVote || m.MsgType == pb.MessageType_MsgPreRequestVote {
		if r.Term < m.Term {
			r.Vote = None
			r.Term = m.Term
			if r.State != StateFollower {
				r.becomeFollower(r.Term, None)
			}
		}
		entry := pb.Entry{Term: m.LogTerm, Index: m.Index}
		CanVote := m.Term > r.Term || r.Vote == None || r.Vote == m.From
		if CanVote && r.isUpToDate(entry) {
			msg := pb.Message{From: r.id, To: m.From, MsgType: voteRespMsgType(m.MsgType), Term: r.Term}
			r.msgs = append(r.msgs, msg)
			r.electionElapsed = 0
			r.Vote = m.From
			if r.debug {
				log.Infof("[%d] Votes for [%d]", r.id, m.From)
			}
			// r.becomeFollower(m.Term, None)
		} else {
			if r.debug {
				log.Infof("[%d] Reject Vote for [%d]", r.id, m.From)
			}
			msg := pb.Message{From: r.id, To: m.From, MsgType: voteRespMsgType(m.MsgType), Reject: true, Term: r.Term}
			r.msgs = append(r.msgs, msg)
		}
		return nil
	}
	switch r.State {
	case StateFollower:
		switch m.MsgType {
		// start new election
		case pb.MessageType_MsgHup:
			if _, ok := r.Prs[r.id]; !ok {
				return nil
			}
			if r.preVote {
				r.campaign(campaignPreElection)
			} else {
				r.campaign(campaignElection)
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
		case pb.MessageType_MsgTimeoutNow:
			// 当前节点必须在集群中
			if _, ok := r.Prs[r.id]; ok {
				// // 向自己发送msgHup请求
				// msg := pb.Message{From: r.id, To: r.id, MsgType: pb.MessageType_MsgHup}
				// // MsgHup是一个local message，不能添加到r.msgs中
				// r.Step(msg)
				// 发起选举
				r.campaign(campaignElection)
			}

		case pb.MessageType_MsgTransferLeader:
			if r.Lead == None {
				// 还没有选出leader,直接返回
				return nil
			}
			m.To = r.Lead
			r.msgs = append(r.msgs, m)
		}

	case StateCandidate, StatePreCandidate:
		var myVoteRespType pb.MessageType
		if r.State == StatePreCandidate {
			// log.Infof("[%d] is PreCandidate,So Receive MsgPreRequestVoteResponse", r.id)
			myVoteRespType = pb.MessageType_MsgPreRequestVoteResponse
		} else {
			// log.Infof("[%d] is Candidate,So Receive MsgRequestVoteResponse", r.id)
			myVoteRespType = pb.MessageType_MsgRequestVoteResponse
		}
		switch m.MsgType {
		case pb.MessageType_MsgHup:
			if _, ok := r.Prs[r.id]; !ok {
				return nil
			}
			// ents, err := r.RaftLog.slice(r.RaftLog.applied+1, r.RaftLog.committed+1)
			// if err != nil {
			// 	log.Panic(err)
			// }
			// if n := numOfPendingConf(ents); n != 0 && r.RaftLog.committed > r.RaftLog.applied {
			// 	// log.Infof("%x cannot campaign at term %d since there are still %d pending configuration changes to apply", r.id, r.Term, n)
			// 	return nil
			// }
			if r.preVote {
				r.campaign(campaignPreElection)
			} else {
				r.campaign(campaignElection)
			}

		case myVoteRespType:
			if !m.Reject {
				//log.Infof("[%d] Receive VoteResponse Msg from [%d]", r.id, m.From)
				r.votes[m.From] = true
				trueCount := 0
				// 统计投票的数量
				for _, vote := range r.votes {
					if vote {
						trueCount++
					}
				}
				if trueCount >= r.quorum() {
					if r.State == StatePreCandidate {
						// PreVotes结束，可以正常进行选举
						r.campaign(campaignElection)
					} else {
						// 得到超过一半的票，成为leader
						r.becomeLeader()
						for id := range r.Prs {
							if id != r.id {
								// r.sendNoopEntry(id)
								r.sendAppend(id)
							}
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
			r.becomeFollower(m.Term, m.From)
			r.handleAppendEntries(m)
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
					// log.Infof("Leader [%d] become Follower\n", r.id)
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
			if r.debug {
				log.Infof("Receive Propose signal!\n")
			}
			if _, ok := r.Prs[r.id]; !ok {
				// 节点被移除集群
				return nil
			}
			if r.leadTransferee != None {
				// 在转换leader的过程中，不能提交
				return ErrProposalDropped
			}
			for i, e := range m.Entries {
				if e.EntryType == pb.EntryType_EntryConfChange {
					cc := new(pb.ConfChange)
					err := cc.Unmarshal(e.Data)
					if err != nil {
						panic(err)
					}
					if cc.ChangeType == pb.ConfChangeType_RemoveNode && cc.NodeId == r.id {
						var transferee uint64
						for index, _ := range r.Prs {
							if index != r.id {
								transferee = index
								break
							}
						}
						// 如果要删除leader，那么要先进行TransferLeader，再进行ConfChange
						msg := pb.Message{MsgType: pb.MessageType_MsgTransferLeader, From: transferee}
						// r.msgs = append(r.msgs, m)
						r.Step(msg)
						return nil
					}
					if r.PendingConfIndex > r.RaftLog.applied {
						m.Entries[i] = &pb.Entry{EntryType: pb.EntryType_EntryNormal}
						// return nil
					}
					r.PendingConfIndex = r.RaftLog.LastIndex() + uint64(i) + 1
					// log.Infof("Propose ConfChange Entry")
				}
			}
			var entries []pb.Entry
			last := r.RaftLog.LastIndex()
			for i := range m.Entries {
				m.Entries[i].Term = r.Term
				m.Entries[i].Index = last + 1 + uint64(i)
				entries = append(entries, *m.Entries[i])
			}
			// entry := pb.Entry{Data: m.Entries[0].Data, Term: r.Term, Index: r.RaftLog.LastIndex() + 1}
			r.RaftLog.append(entries...)
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
				// 更新leader与follower的通信间隔
				r.heartbeatResp[m.From] = true
				// 删除haveSendedSnapShot记录
				delete(r.haveSendedSnapShot, m.From)
				if pr.maybeUpdate(m.Index) {
					if r.maybeCommit() {
						// 更新follower的commited
						for id := range r.Prs {
							if id != r.id {
								r.sendAppend(id)
							}
						}
					}
					if m.From == r.leadTransferee && pr.Match == r.RaftLog.LastIndex() {
						// 发送TimeoutNow消息
						if r.debug {
							log.Infof("[%d] Send TimeoutNow Message to [%d]", r.id, m.From)
						}
						msg := pb.Message{To: m.From, MsgType: pb.MessageType_MsgTimeoutNow, From: r.id}
						r.msgs = append(r.msgs, msg)
					}
				}

			} else {
				// Follower的log length太短, nextIndex = Xlen
				// if m.Xlen < m.Index {
				// 	// log.Infof("Next Changed: %d -> %d", r.Prs[m.From].Next, m.Xlen+1)
				// 	r.Prs[m.From].Next = m.Xlen + 1
				// } else {
				// 	index := r.RaftLog.FindLastTerm(m.Xterm)
				// 	if index == 0 {
				// 		// Leader没有XTerm，nextIndex = Xindex
				// 		// log.Infof("Next Changed: %d -> %d", r.Prs[m.From].Next, m.Xindex)
				// 		r.Prs[m.From].Next = m.Xindex
				// 	} else {
				// 		// Leader有Xterm, nextIndex = leader's last entry for XTerm
				// 		// log.Infof("Next Changed: %d -> %d", r.Prs[m.From].Next, index)
				// 		r.Prs[m.From].Next = index
				// 	}
				// }
				// append失败的index,减1继续发送append请求
				r.Prs[m.From].Next = min(m.Index+1, r.Prs[m.From].Next-1)
				r.sendAppend(m.From)

			}
		case pb.MessageType_MsgHeartbeatResponse:
			pr := r.Prs[m.From]
			// 更新leader与follower的通信间隔
			r.heartbeatResp[m.From] = true
			if r.Term < m.Term {
				r.Term = m.Term
				if r.State != StateFollower {
					r.becomeFollower(r.Term, None)
				}
			}
			// 如果follower的Match index小于Leader的lastindex，就向follower发送append请求
			if pr.Match < r.RaftLog.LastIndex() {
				r.sendAppend(m.From)
			}
		case pb.MessageType_MsgTransferLeader:
			if _, ok := r.Prs[m.From]; !ok {
				return nil
			}
			leadTransferee := m.From
			lastLeadTransferee := r.leadTransferee
			if lastLeadTransferee != None {
				if lastLeadTransferee == leadTransferee {
					// 已经有相同节点的leader转让流程在进行中
					return nil
				}
				// 中断之前的转让流程
				r.leadTransferee = None
			}
			if leadTransferee == r.id {
				return nil
			}

			r.electionElapsed = 0
			r.leadTransferee = leadTransferee
			pr, ok := r.Prs[m.From]
			if !ok {
				// log.Infof("%x no progress available for %x", r.id, m.From)
				return nil
			}
			if pr.Match == r.RaftLog.LastIndex() {
				// 发送TimeoutNow消息
				if r.debug {
					log.Infof("[%d] Raft Send TimeoutNow Message to [%d]", r.id, leadTransferee)
				}
				msg := pb.Message{To: leadTransferee, MsgType: pb.MessageType_MsgTimeoutNow, From: r.id}
				r.msgs = append(r.msgs, msg)
			} else {
				if r.debug {
					log.Infof("Transfer Leader is not Match, [%d] Send Append Message to [%d]", r.id, leadTransferee)
				}
				r.sendAppend(leadTransferee)
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
	// log.Infof("The votes count is %d, the Prs is %d", trueCount, len(r.Prs))
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
		if r.State != StateFollower {
			r.becomeFollower(r.Term, None)
		}
	} else {
		msg := pb.Message{From: r.id, To: m.From, MsgType: pb.MessageType_MsgAppendResponse, Index: r.RaftLog.LastIndex(), Term: r.Term, Reject: true}
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
		if r.debug {
			log.Infof("[%d] Receive Append from [%d], after Append commited is %d, LastIndex is %d\n", r.id, m.From, r.RaftLog.committed, mlastIndex)
		}
		// log.Infof("[%d] Raftlog Entries is %v", r.id, r.RaftLog.unstableEntries())
	} else {
		// 添加日志失败
		term, _ := r.RaftLog.Term(m.Index)
		if r.debug {
			log.Infof("%x [logterm: %d, index: %d] rejected msgApp [logterm: %d, index: %d] from %x\n",
				r.id, term, m.Index, m.LogTerm, m.Index, m.From)
		}
		// xlen := r.RaftLog.LastIndex()
		msg := pb.Message{From: r.id, To: m.From, MsgType: pb.MessageType_MsgAppendResponse, Index: m.Index, Term: r.Term, Reject: true}
		// 如果Follower的log length小于nextIndex,那么说明follower's log太短,这里没有冲突的Term
		// if xlen < m.Index {
		// 	// log.Infof("Follower too short, xlen is %d", xlen)
		// 	msg.Xlen = xlen
		// } else {
		// 	// 寻找冲突的Term和该Term在Follower's log中的第一个entry index
		// 	xindex := r.RaftLog.FindFirstTerm(term)
		// 	msg.Xindex = xindex
		// 	msg.Xterm = term
		// }

		r.msgs = append(r.msgs, msg)
	}
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	// log.Infof("[%d] receive heartbeat from [%d]\n", r.id, m.From)
	if r.Term <= m.Term {
		r.Term = m.Term
		if r.State != StateFollower {
			r.becomeFollower(r.Term, None)
		}
	}
	// r.RaftLog.commitTo(m.Commit)
	msg := pb.Message{From: r.id, To: m.From, MsgType: pb.MessageType_MsgHeartbeatResponse}
	r.msgs = append(r.msgs, msg)
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
	if r.Term < m.Term {
		r.Term = m.Term
		if r.State != StateFollower {
			r.becomeFollower(r.Term, None)
		}
	}
	// sindex, sterm := m.Snapshot.Metadata.Index, m.Snapshot.Metadata.Term
	if r.snapRestore(*m.Snapshot) {
		// log.Infof("%x [commit: %d] restored snapshot [index: %d, term: %d]",
		// 	r.id, r.RaftLog.committed, sindex, sterm)
		msg := pb.Message{From: r.id, To: m.From, MsgType: pb.MessageType_MsgAppendResponse, Index: r.RaftLog.LastIndex()}
		r.msgs = append(r.msgs, msg)
		// 发送SnapShot完成
	} else {
		// log.Infof("%x [commit: %d] ignored snapshot [index: %d, term: %d]",
		// 	r.id, r.RaftLog.committed, sindex, sterm)
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
	// r.PendingConfIndex = 0
	if _, ok := r.Prs[id]; ok {
		if r.debug {
			log.Infof("[%d] Already have [%d] in Prs", r.id, id)
		}
		// 已经在节点列表中
		return
	}
	if r.debug {
		log.Infof("[%d] Add Noede [%d]", r.id, id)
	}
	r.Prs[id] = &Progress{Next: 1, Match: 0}
	if r.State == StateLeader {
		r.sendHeartbeat(id)
	}
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
	delete(r.Prs, id)
	// 重置PendingConfIndex
	// r.PendingConfIndex = 0

	if len(r.Prs) == 0 {
		return
	}
	if r.debug {
		log.Infof("[%d] Delete Noede [%d]", r.id, id)
	}
	// 删除节点后，可能可以进行commit操作
	if r.State == StateLeader {
		if len(r.Prs) != 0 {
			if r.maybeCommit() {
				for id := range r.Prs {
					if id != r.id {
						r.sendAppend(id)
					}
				}
			}
		}
	}

	// 如果在leader迁移过程中发生删除节点操作，就中断迁移流程
	if r.State == StateLeader && r.leadTransferee == id {
		r.leadTransferee = None
	}
}

func (r *Raft) softState() *SoftState { return &SoftState{Lead: r.Lead, RaftState: r.State} }

func (r *Raft) hardState() pb.HardState {
	return pb.HardState{
		Term:   r.Term,
		Vote:   r.Vote,
		Commit: r.RaftLog.committed,
	}
}
func numOfPendingConf(ents []pb.Entry) int {
	n := 0
	for i := range ents {
		if ents[i].EntryType == pb.EntryType_EntryConfChange {
			n++
		}
	}
	return n
}
