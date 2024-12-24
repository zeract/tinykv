package raftstore

import (
	"fmt"
	"reflect"
	"time"

	"github.com/Connor1996/badger/y"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/message"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/meta"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/runner"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/snap"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/util"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/raft_cmdpb"
	rspb "github.com/pingcap-incubator/tinykv/proto/pkg/raft_serverpb"
	"github.com/pingcap-incubator/tinykv/scheduler/pkg/btree"
	"github.com/pingcap/errors"
)

type PeerTick int

const (
	PeerTickRaft               PeerTick = 0
	PeerTickRaftLogGC          PeerTick = 1
	PeerTickSplitRegionCheck   PeerTick = 2
	PeerTickSchedulerHeartbeat PeerTick = 3
)

type peerMsgHandler struct {
	*peer
	ctx *GlobalContext
}

func newPeerMsgHandler(peer *peer, ctx *GlobalContext) *peerMsgHandler {
	return &peerMsgHandler{
		peer: peer,
		ctx:  ctx,
	}
}

func (d *peerMsgHandler) HandleRaftReady() {
	if d.stopped {
		return
	}
	// Your Code Here (2B).
	if d.RaftGroup.HasReady() {
		// log.Infof("Call HandleRaftReady")
		ready := d.RaftGroup.Ready()
		applySnap, err := d.peerStorage.SaveReadyState(&ready)
		if err != nil {
			return
		}
		if applySnap != nil {
			if !reflect.DeepEqual(applySnap.PrevRegion, applySnap.Region) {
				d.peerStorage.SetRegion(applySnap.Region)
				d.ctx.storeMeta.Lock()
				d.ctx.storeMeta.regions[applySnap.Region.Id] = applySnap.Region
				d.ctx.storeMeta.regionRanges.Delete(&regionItem{region: applySnap.PrevRegion})
				d.ctx.storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region: applySnap.Region})
				d.ctx.storeMeta.Unlock()
			}
		}
		if len(ready.Messages) != 0 {
			d.Send(d.ctx.trans, ready.Messages)
		}
		if len(ready.CommittedEntries) > 0 {
			// 对所有Cmmitted的Entries进行处理
			for _, entry := range ready.CommittedEntries {
				if entry.Data == nil {
					continue
				}
				wb := new(engine_util.WriteBatch)
				changed := true
				// 遍历所有的committed Entries，如果是Normal request则继续进行处理
				if entry.EntryType == eraftpb.EntryType_EntryNormal {
					// 从Entry中取出对应的RaftCmdRequest，其中包含多个Requests
					requests := new(raft_cmdpb.RaftCmdRequest)
					err := requests.Unmarshal(entry.Data)
					if err != nil {
						panic(err)
					}
					// 创建这个RaftCmdRequest对应的WriteBatch
					if requests.AdminRequest != nil {
						d.applyAdminRequests(requests, entry, wb)

					} else if len(requests.Requests) > 0 {
						// 将requests中的数据进行apply
						changed = d.applyNormalRequests(requests, entry, wb)
					}
				} else {
					d.applyConfChangeRequest(&entry, wb)
				}
				if d.stopped {
					WB := &engine_util.WriteBatch{}
					WB.DeleteMeta(meta.ApplyStateKey(d.regionId))
					err = WB.WriteToDB(d.peerStorage.Engines.Kv)
					if err != nil {
						panic(err)
					}
					return
				}
				// 更新PeerStorage的AppliedIndex
				d.peerStorage.applyState.AppliedIndex = entry.Index
				// 如果没有数据变化，则不需要写入KV DB，但是如果遍历到最后一个commmited Entry，那么就需要将更新的AppliedIndex写入DB
				if changed {
					// 将更新的ApplyState写入KV DB
					err = wb.SetMeta(meta.ApplyStateKey(d.regionId), d.peerStorage.applyState)
					if err != nil {
						panic(err)
					}
					wb.MustWriteToDB(d.peerStorage.Engines.Kv)
				}
				engines := d.peerStorage.Engines
				txn := engines.Kv.NewTransaction(false)
				regionId := d.peerStorage.Region().GetId()
				regionState := new(rspb.RegionLocalState)
				err = engine_util.GetMetaFromTxn(txn, meta.RegionStateKey(regionId), regionState)
				if err != nil {
					log.Panic(err)
				}
			}
		}
		d.RaftGroup.Advance(ready)
	}

}

func (d *peerMsgHandler) execSplit(entry *eraftpb.Entry, msg *raft_cmdpb.RaftCmdRequest, req *raft_cmdpb.AdminRequest, kvWB *engine_util.WriteBatch) {
	p := d.FindProposal(entry.Index, entry.Term)
	if msg.Header.RegionId != d.regionId {
		resp := ErrResp(&util.ErrRegionNotFound{RegionId: msg.Header.RegionId})
		if p != nil {
			p.cb.Done(resp)
		}
		return
	}
	err := util.CheckRegionEpoch(msg, d.Region(), true)
	if err != nil {
		resp := ErrResp(err)
		if p != nil {
			p.cb.Done(resp)
		}
		return
	}
	err = util.CheckKeyInRegion(req.Split.SplitKey, d.Region())
	if err != nil {
		resp := ErrResp(err)
		if p != nil {
			p.cb.Done(resp)
		}
		return
	}
	if len(req.Split.NewPeerIds) != len(d.Region().Peers) {
		resp := ErrResp(errors.Errorf("length of NewPeerIds != length of Peers"))
		if p != nil {
			p.cb.Done(resp)
		}
		return
	}

	log.Infof("region %d peer %d begin to split", d.regionId, d.PeerId())

	// copy peers
	cpPeers := make([]*metapb.Peer, 0)
	for i, pr := range d.Region().Peers {
		cpPeers = append(cpPeers, &metapb.Peer{
			Id:      req.Split.NewPeerIds[i],
			StoreId: pr.StoreId,
		})
	}

	newRegion := &metapb.Region{
		Id:       req.Split.NewRegionId,
		StartKey: req.Split.SplitKey,
		EndKey:   d.Region().EndKey,
		RegionEpoch: &metapb.RegionEpoch{
			ConfVer: 0,
			Version: 0,
		},
		Peers: cpPeers,
	}

	// 修改 regionState
	d.ctx.storeMeta.Lock()
	d.Region().RegionEpoch.Version++
	newRegion.RegionEpoch.Version++
	d.Region().EndKey = req.Split.SplitKey
	d.ctx.storeMeta.regions[req.Split.NewRegionId] = newRegion
	d.ctx.storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region: d.Region()})
	d.ctx.storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region: newRegion})
	meta.WriteRegionState(kvWB, newRegion, rspb.PeerState_Normal)
	meta.WriteRegionState(kvWB, d.Region(), rspb.PeerState_Normal)
	d.ctx.storeMeta.Unlock()

	// 创建并注册新的 peer
	newPeer, err := createPeer(d.storeID(), d.ctx.cfg, d.ctx.regionTaskSender, d.ctx.engine, newRegion)
	if err != nil {
		log.Panic(err)
	}
	newPeer.peerStorage.SetRegion(newRegion)
	d.ctx.router.register(newPeer)
	startMsg := message.Msg{
		RegionID: req.Split.NewRegionId,
		Type:     message.MsgTypeStart,
	}
	err = d.ctx.router.send(req.Split.NewRegionId, startMsg)
	if err != nil {
		log.Panic(err)
	}
	// 回应
	resp := &raft_cmdpb.RaftCmdResponse{
		Header: &raft_cmdpb.RaftResponseHeader{},
		AdminResponse: &raft_cmdpb.AdminResponse{
			CmdType: raft_cmdpb.AdminCmdType_Split,
			Split: &raft_cmdpb.SplitResponse{
				Regions: []*metapb.Region{newRegion, d.Region()},
			},
		},
	}
	if p != nil {
		p.cb.Done(resp)
	}
	// 刷新 scheduler 的 region 缓存
	d.notifyHeartbeatScheduler(d.Region(), d.peer)
	d.notifyHeartbeatScheduler(newRegion, newPeer)
	// PendingVotes优化, 使新peer能够及时选举
	// d.PendingVotes(newPeer, newRegion)

	return
}

func IsPeerCreate(region *metapb.Region, id uint64) bool {
	for _, p := range region.Peers {
		if p.Id == id {
			return true
		}
	}
	return false
}

func (d *peerMsgHandler) notifyHeartbeatScheduler(region *metapb.Region, peer *peer) {
	clonedRegion := new(metapb.Region)
	err := util.CloneMsg(region, clonedRegion)
	if err != nil {
		return
	}
	d.ctx.schedulerTaskSender <- &runner.SchedulerRegionHeartbeatTask{
		Region:          clonedRegion,
		Peer:            peer.Meta,
		PendingPeers:    peer.CollectPendingPeers(),
		ApproximateSize: peer.ApproximateSize,
	}
}

func (d *peerMsgHandler) applyConfChangeRequest(entry *eraftpb.Entry, wb *engine_util.WriteBatch) {
	// ConfChange请求
	region := d.Region()
	conf := &eraftpb.ConfChange{}
	conf.Unmarshal(entry.Data)
	msg := &raft_cmdpb.RaftCmdRequest{}
	err := msg.Unmarshal(conf.Context)
	if err != nil {
		log.Panic(err)
	}
	p := d.FindProposal(entry.Index, entry.Term)
	// 判断 RegionEpoch
	if msg.Header != nil {
		fromEpoch := msg.GetHeader().GetRegionEpoch()
		if fromEpoch != nil {
			if util.IsEpochStale(fromEpoch, d.Region().RegionEpoch) {
				resp := ErrResp(&util.ErrEpochNotMatch{})
				if p != nil {
					p.cb.Done(resp)
				}
				return
			}
		}
	}

	d.RaftGroup.ApplyConfChange(*conf)
	// peer := region.GetPeers()
	if conf.ChangeType == eraftpb.ConfChangeType_AddNode {
		if !IsPeerCreate(region, conf.NodeId) {
			// log.Infof("[%d] Add Peer [%d]", d.PeerId(), conf.NodeId)
			d.ctx.storeMeta.Lock()
			region.RegionEpoch.ConfVer++
			peer := &metapb.Peer{
				Id:      conf.NodeId,
				StoreId: msg.AdminRequest.ChangePeer.Peer.StoreId,
			}
			region.Peers = append(region.Peers, peer)
			// 持久化region
			meta.WriteRegionState(wb, region, rspb.PeerState_Normal)
			d.ctx.storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region: d.Region()})
			// 更新缓存
			d.insertPeerCache(peer)
			d.ctx.storeMeta.Unlock()
		}
	} else {
		if conf.NodeId == d.PeerId() {
			wb.DeleteMeta(meta.ApplyStateKey(d.regionId))
			d.destroyPeer()
		} else if IsPeerCreate(region, conf.NodeId) {
			d.ctx.storeMeta.Lock()
			// log.Infof("[%d] Remove Peer [%d]", d.PeerId(), conf.NodeId)
			region.RegionEpoch.ConfVer++
			util.RemovePeer(region, msg.AdminRequest.ChangePeer.Peer.StoreId)
			meta.WriteRegionState(wb, region, rspb.PeerState_Normal)
			// 更新缓存
			d.removePeerCache(conf.NodeId)
			d.ctx.storeMeta.Unlock()
		}

	}
	resp := &raft_cmdpb.RaftCmdResponse{
		Header: &raft_cmdpb.RaftResponseHeader{},
		AdminResponse: &raft_cmdpb.AdminResponse{
			CmdType:    raft_cmdpb.AdminCmdType_ChangePeer,
			ChangePeer: &raft_cmdpb.ChangePeerResponse{},
		},
	}
	if p != nil {
		p.cb.Done(resp)
	}
	// 更新region缓存
	d.notifyHeartbeatScheduler(region, d.peer)
}

func (d *peerMsgHandler) applyNormalRequests(requests *raft_cmdpb.RaftCmdRequest, entry eraftpb.Entry, wb *engine_util.WriteBatch) bool {
	p := d.FindProposal(entry.Index, entry.Term)
	err := util.CheckRegionEpoch(requests, d.Region(), true)
	if err != nil {
		if p != nil {
			p.cb.Done(ErrResp(err))
		}
		return false
	}
	// 创建这个RaftCmdRequest对应的WriteBatch
	resp := &raft_cmdpb.RaftCmdResponse{
		Header:    &raft_cmdpb.RaftResponseHeader{},
		Responses: []*raft_cmdpb.Response{},
	}
	changed := false
	for _, request := range requests.Requests {
		if request.GetCmdType() != raft_cmdpb.CmdType_Invalid {
			t := request.GetCmdType()
			// 判断是读请求还是写请求
			if t == raft_cmdpb.CmdType_Delete || t == raft_cmdpb.CmdType_Put {
				changed = true || changed
				if t == raft_cmdpb.CmdType_Delete {
					// Delete Request，需要执行Delete操作
					cf := request.GetDelete().GetCf()
					key := request.GetDelete().GetKey()
					wb.DeleteCF(cf, key)
					// 构造一个空的Delete Response请求
					resp.Responses = append(resp.Responses, &raft_cmdpb.Response{CmdType: raft_cmdpb.CmdType_Delete,
						Delete: &raft_cmdpb.DeleteResponse{}})
				} else {
					// Put Request，需要执行Put操作
					cf := request.GetPut().GetCf()
					key := request.GetPut().GetKey()
					wb.SetCF(cf, key, request.Put.Value)
					// 构造一个空的Put Response请求
					resp.Responses = append(resp.Responses, &raft_cmdpb.Response{CmdType: raft_cmdpb.CmdType_Put,
						Put: &raft_cmdpb.PutResponse{}})
				}

			} else {
				changed = false || changed
				// 读取请求
				if t == raft_cmdpb.CmdType_Get {
					// Get Request, 需要执行Get操作
					cf := request.GetGet().GetCf()
					key := request.GetGet().GetKey()
					val, err := engine_util.GetCF(d.ctx.engine.Kv, cf, key)
					if err != nil {
						if p != nil {
							p.cb.Done(ErrResp(err))
						}
						return changed
					}
					resp.Responses = append(resp.Responses, &raft_cmdpb.Response{CmdType: raft_cmdpb.CmdType_Get,
						Get: &raft_cmdpb.GetResponse{Value: val}})
				} else {
					// Snap Request, 需要执行Snap操作
					// 对region进行拷贝
					region := new(metapb.Region)
					util.CloneMsg(d.Region(), region)
					resp.Responses = append(resp.Responses, &raft_cmdpb.Response{CmdType: raft_cmdpb.CmdType_Snap,
						Snap: &raft_cmdpb.SnapResponse{Region: region}})
					if p != nil {
						// 设置一个新的Transaction供来读
						p.cb.Txn = d.ctx.engine.Kv.NewTransaction(false)
					}
				}
			}

		} else {
			log.Panicf("The Request is Nil!")
		}
	}
	if p != nil {
		p.cb.Done(resp)
	}
	return changed
}

func (d *peerMsgHandler) execCompactLog(entry *eraftpb.Entry, req *raft_cmdpb.AdminRequest, kvWB *engine_util.WriteBatch) {
	p := d.FindProposal(entry.Index, entry.Term)
	compactLog := req.GetCompactLog()
	compactIndex := compactLog.CompactIndex
	compactTerm := compactLog.CompactTerm
	if compactIndex >= d.peerStorage.applyState.TruncatedState.Index {
		d.peerStorage.applyState.TruncatedState.Index = compactIndex
		d.peerStorage.applyState.TruncatedState.Term = compactTerm
		err := kvWB.SetMeta(meta.ApplyStateKey(d.Region().GetId()), d.peerStorage.applyState)
		if err != nil {
			log.Panic(err)
			return
		}
		d.ScheduleCompactLog(compactIndex)
	}

	// 回应
	adminResp := &raft_cmdpb.AdminResponse{
		CmdType:    raft_cmdpb.AdminCmdType_CompactLog,
		CompactLog: &raft_cmdpb.CompactLogResponse{},
	}
	cmdResp := &raft_cmdpb.RaftCmdResponse{
		Header:        &raft_cmdpb.RaftResponseHeader{},
		AdminResponse: adminResp,
	}
	if p != nil {
		p.cb.Done(cmdResp)
	}
}

func (d *peerMsgHandler) applyAdminRequests(requests *raft_cmdpb.RaftCmdRequest, entry eraftpb.Entry, wb *engine_util.WriteBatch) {
	// 判断 RegionEpoch
	if requests.Header != nil {
		fromEpoch := requests.GetHeader().GetRegionEpoch()
		if fromEpoch != nil {
			if util.IsEpochStale(fromEpoch, d.Region().RegionEpoch) {
				resp := ErrResp(&util.ErrEpochNotMatch{})
				p := d.FindProposal(entry.Index, entry.Term)
				if p != nil {
					p.cb.Done(resp)
				}
				return
			}
		}
	}

	switch requests.AdminRequest.CmdType {
	case raft_cmdpb.AdminCmdType_CompactLog:
		// 执行Compactlog操作
		d.execCompactLog(&entry, requests.AdminRequest, wb)
	case raft_cmdpb.AdminCmdType_Split:
		// 执行Split操作
		d.execSplit(&entry, requests, requests.AdminRequest, wb)
	}
	return
}

func (d *peerMsgHandler) PendingVotes(newPeer *peer, newRegion *metapb.Region) {
	if d.IsLeader() {
		// 触发新peer的选举
		// peer.RaftGroup.Campaign()
		msgs := make([]eraftpb.Message, 0)
		index := newPeer.RaftGroup.Raft.RaftLog.LastIndex()
		if index != 5 {
			return
		}
		term, _ := newPeer.RaftGroup.Raft.RaftLog.Term(index)
		msg := eraftpb.Message{
			MsgType: eraftpb.MessageType_MsgRequestVote,
			From:    newPeer.Meta.Id,
			Term:    newPeer.Term() + 1,
			Index:   index,
			LogTerm: term,
		}
		for _, p := range newRegion.Peers {
			if p.Id == newPeer.Meta.Id {
				continue
			}
			msg.To = p.Id
			// 向其他store中的peer发送RequestVote请求
			msgs = append(msgs, msg)
		}
		// 使用新的peer来进行发送，此时的region也是新建的
		newPeerMsgHandler(newPeer, d.ctx).Send(d.ctx.trans, msgs)
		newPeer.RaftGroup.RaftCandidate()
	} else {
		if len(d.ctx.storeMeta.pendingVotes) != 0 {
			err := newPeer.RaftGroup.Step(*d.ctx.storeMeta.pendingVotes[0].Message)
			if err != nil {
				panic(err)
			}
			d.ctx.storeMeta.pendingVotes = d.ctx.storeMeta.pendingVotes[:0]

		}
	}
}

// 在peerMsgHandler中寻找对应的proposal请求
func (d *peerMsgHandler) FindProposal(index, term uint64) *proposal {
	for len(d.proposals) > 0 {
		// 获取第一个proposal
		p := d.proposals[0]
		// 判断按序获取的proposal是否与需要的index和term匹配，如果不匹配则标记为stale
		if p.index < index {
			NotifyStaleReq(d.Term(), p.cb)
			// 将获取的proposal从列表中去除
			d.proposals = d.proposals[1:]
		} else if p.index == index {
			// 将获取的proposal从列表中去除
			d.proposals = d.proposals[1:]
			if p.term != term {
				NotifyStaleReq(d.Term(), p.cb)
			} else {
				return p
			}
		} else {
			break
		}
	}
	return nil
}

func (d *peerMsgHandler) HandleMsg(msg message.Msg) {
	switch msg.Type {
	case message.MsgTypeRaftMessage:
		raftMsg := msg.Data.(*rspb.RaftMessage)
		if err := d.onRaftMsg(raftMsg); err != nil {
			log.Errorf("%s handle raft message error %v", d.Tag, err)
		}
	case message.MsgTypeRaftCmd:
		raftCMD := msg.Data.(*message.MsgRaftCmd)
		d.proposeRaftCommand(raftCMD.Request, raftCMD.Callback)
	case message.MsgTypeTick:
		d.onTick()
	case message.MsgTypeSplitRegion:
		split := msg.Data.(*message.MsgSplitRegion)
		log.Infof("%s on split with %v", d.Tag, split.SplitKey)
		d.onPrepareSplitRegion(split.RegionEpoch, split.SplitKey, split.Callback)
	case message.MsgTypeRegionApproximateSize:
		d.onApproximateRegionSize(msg.Data.(uint64))
	case message.MsgTypeGcSnap:
		gcSnap := msg.Data.(*message.MsgGCSnap)
		d.onGCSnap(gcSnap.Snaps)
	case message.MsgTypeStart:
		d.startTicker()
	}
}

func (d *peerMsgHandler) preProposeRaftCommand(req *raft_cmdpb.RaftCmdRequest) error {
	// Check store_id, make sure that the msg is dispatched to the right place.
	if err := util.CheckStoreID(req, d.storeID()); err != nil {
		return err
	}

	// Check whether the store has the right peer to handle the request.
	regionID := d.regionId
	leaderID := d.LeaderId()
	if !d.IsLeader() {
		leader := d.getPeerFromCache(leaderID)
		return &util.ErrNotLeader{RegionId: regionID, Leader: leader}
	}
	// peer_id must be the same as peer's.
	if err := util.CheckPeerID(req, d.PeerId()); err != nil {
		return err
	}
	// Check whether the term is stale.
	if err := util.CheckTerm(req, d.Term()); err != nil {
		return err
	}
	err := util.CheckRegionEpoch(req, d.Region(), true)
	if errEpochNotMatching, ok := err.(*util.ErrEpochNotMatch); ok {
		// Attach the region which might be split from the current region. But it doesn't
		// matter if the region is not split from the current region. If the region meta
		// received by the TiKV driver is newer than the meta cached in the driver, the meta is
		// updated.
		siblingRegion := d.findSiblingRegion()
		if siblingRegion != nil {
			errEpochNotMatching.Regions = append(errEpochNotMatching.Regions, siblingRegion)
		}
		return errEpochNotMatching
	}
	return err
}

func (d *peerMsgHandler) proposeRaftCommand(msg *raft_cmdpb.RaftCmdRequest, cb *message.Callback) {
	err := d.preProposeRaftCommand(msg)
	if err != nil {
		cb.Done(ErrResp(err))
		return
	}
	// Your Code Here (2B).
	if len(msg.Requests) != 0 {
		cmd_msg := new(raft_cmdpb.RaftCmdRequest)
		cmd_msg.Header = msg.Header
		for len(msg.Requests) > 0 {
			req := msg.Requests[0]
			var key []byte
			switch req.CmdType {
			case raft_cmdpb.CmdType_Get:
				key = req.Get.Key
			case raft_cmdpb.CmdType_Put:
				key = req.Put.Key
			case raft_cmdpb.CmdType_Delete:
				key = req.Delete.Key
			case raft_cmdpb.CmdType_Snap:
			}
			err = util.CheckKeyInRegion(key, d.Region())
			if err != nil && req.CmdType != raft_cmdpb.CmdType_Snap {
				cb.Done(ErrResp(err))
				msg.Requests = msg.Requests[1:]
				continue
			}
			// cmd := new(raft_cmdpb.RaftCmdRequest)
			// cmd.Header = msg.Header
			// cmd.Requests = append(cmd.Requests, req)
			// data, err1 := cmd.Marshal()
			// if err1 != nil && data != nil {
			// 	cb.Done(ErrResp(err1))
			// 	return
			// }
			// d.proposals = append(d.proposals, &proposal{
			// 	index: d.nextProposalIndex(),
			// 	term:  d.Term(),
			// 	cb:    cb,
			// })
			cmd_msg.Requests = append(cmd_msg.Requests, req)
			// _ = d.RaftGroup.Propose(data)
			msg.Requests = msg.Requests[1:]
		}
		d.proposals = append(d.proposals, &proposal{
			index: d.nextProposalIndex(),
			term:  d.Term(),
			cb:    cb,
		})
		data, err := cmd_msg.Marshal()
		if err != nil && data != nil {
			cb.Done(ErrResp(err))
			return
		}
		_ = d.RaftGroup.Propose(data)

	} else if msg.AdminRequest != nil {
		c_type := msg.AdminRequest.CmdType
		switch c_type {
		case raft_cmdpb.AdminCmdType_TransferLeader:
			d.RaftGroup.TransferLeader(msg.GetAdminRequest().TransferLeader.Peer.Id)
			resp := &raft_cmdpb.RaftCmdResponse{
				Header: &raft_cmdpb.RaftResponseHeader{},
				AdminResponse: &raft_cmdpb.AdminResponse{
					CmdType:        raft_cmdpb.AdminCmdType_TransferLeader,
					TransferLeader: &raft_cmdpb.TransferLeaderResponse{},
				},
			}
			cb.Done(resp)
			return
		case raft_cmdpb.AdminCmdType_ChangePeer:
			// 使用Context来存储peer，在Apply时使用
			// peer_data, err := msg.AdminRequest.ChangePeer.Peer.Marshal()
			context, err := msg.Marshal()
			if err != nil {
				cb.Done(ErrResp(err))
				return
			}
			cc := eraftpb.ConfChange{
				ChangeType: msg.AdminRequest.ChangePeer.ChangeType,
				NodeId:     msg.AdminRequest.ChangePeer.Peer.Id,
				Context:    context,
			}
			d.proposals = append(d.proposals, &proposal{
				index: d.nextProposalIndex(),
				term:  d.Term(),
				cb:    cb,
			})
			// log.Infof("Propose ConfChange!")
			d.RaftGroup.ProposeConfChange(cc)
			return
		case raft_cmdpb.AdminCmdType_CompactLog:
			data, err := msg.Marshal()
			if err != nil {
				cb.Done(ErrResp(err))
				return
			}
			d.proposals = append(d.proposals, &proposal{
				index: d.nextProposalIndex(),
				term:  d.Term(),
				cb:    cb,
			})
			d.RaftGroup.Propose(data)
		case raft_cmdpb.AdminCmdType_Split:
			key := msg.AdminRequest.Split.SplitKey
			// 检查split key是否在region中
			err := util.CheckKeyInRegion(key, d.Region())
			if err != nil {
				cb.Done(ErrResp(err))
				return
			}
			err = util.CheckRegionEpoch(msg, d.Region(), true)
			if err != nil {
				cb.Done(ErrResp(err))
				return
			}
			d.proposals = append(d.proposals, &proposal{
				index: d.nextProposalIndex(),
				term:  d.Term(),
				cb:    cb,
			})
			data, err := msg.Marshal()
			if err != nil {
				cb.Done(ErrResp(err))
				return
			}
			d.RaftGroup.Propose(data)
		}

	}
	// data, _ := msg.Marshal()
	// d.RaftGroup.Propose(data)
}

func (d *peerMsgHandler) onTick() {
	if d.stopped {
		return
	}
	d.ticker.tickClock()
	if d.ticker.isOnTick(PeerTickRaft) {
		d.onRaftBaseTick()
	}
	if d.ticker.isOnTick(PeerTickRaftLogGC) {
		d.onRaftGCLogTick()
	}
	if d.ticker.isOnTick(PeerTickSchedulerHeartbeat) {
		d.onSchedulerHeartbeatTick()
	}
	if d.ticker.isOnTick(PeerTickSplitRegionCheck) {
		d.onSplitRegionCheckTick()
	}
	d.ctx.tickDriverSender <- d.regionId
}

func (d *peerMsgHandler) startTicker() {
	d.ticker = newTicker(d.regionId, d.ctx.cfg)
	d.ctx.tickDriverSender <- d.regionId
	d.ticker.schedule(PeerTickRaft)
	d.ticker.schedule(PeerTickRaftLogGC)
	d.ticker.schedule(PeerTickSplitRegionCheck)
	d.ticker.schedule(PeerTickSchedulerHeartbeat)
}

func (d *peerMsgHandler) onRaftBaseTick() {
	d.RaftGroup.Tick()
	d.ticker.schedule(PeerTickRaft)
}

func (d *peerMsgHandler) ScheduleCompactLog(truncatedIndex uint64) {
	raftLogGCTask := &runner.RaftLogGCTask{
		RaftEngine: d.ctx.engine.Raft,
		RegionID:   d.regionId,
		StartIdx:   d.LastCompactedIdx,
		EndIdx:     truncatedIndex + 1,
	}
	d.LastCompactedIdx = raftLogGCTask.EndIdx
	d.ctx.raftLogGCTaskSender <- raftLogGCTask
}

func (d *peerMsgHandler) onRaftMsg(msg *rspb.RaftMessage) error {
	log.Debugf("%s handle raft message %s from %d to %d",
		d.Tag, msg.GetMessage().GetMsgType(), msg.GetFromPeer().GetId(), msg.GetToPeer().GetId())
	if !d.validateRaftMessage(msg) {
		return nil
	}
	if d.stopped {
		return nil
	}
	if msg.GetIsTombstone() {
		// we receive a message tells us to remove self.
		d.handleGCPeerMsg(msg)
		return nil
	}
	if d.checkMessage(msg) {
		return nil
	}
	key, err := d.checkSnapshot(msg)
	if err != nil {
		return err
	}
	if key != nil {
		// If the snapshot file is not used again, then it's OK to
		// delete them here. If the snapshot file will be reused when
		// receiving, then it will fail to pass the check again, so
		// missing snapshot files should not be noticed.
		s, err1 := d.ctx.snapMgr.GetSnapshotForApplying(*key)
		if err1 != nil {
			return err1
		}
		d.ctx.snapMgr.DeleteSnapshot(*key, s, false)
		return nil
	}
	d.insertPeerCache(msg.GetFromPeer())
	err = d.RaftGroup.Step(*msg.GetMessage())
	if err != nil {
		return err
	}
	if d.AnyNewPeerCatchUp(msg.FromPeer.Id) {
		d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
	}
	return nil
}

// return false means the message is invalid, and can be ignored.
func (d *peerMsgHandler) validateRaftMessage(msg *rspb.RaftMessage) bool {
	regionID := msg.GetRegionId()
	from := msg.GetFromPeer()
	to := msg.GetToPeer()
	log.Debugf("[region %d] handle raft message %s from %d to %d", regionID, msg, from.GetId(), to.GetId())
	if to.GetStoreId() != d.storeID() {
		log.Warnf("[region %d] store not match, to store id %d, mine %d, ignore it",
			regionID, to.GetStoreId(), d.storeID())
		return false
	}
	if msg.RegionEpoch == nil {
		log.Errorf("[region %d] missing epoch in raft message, ignore it", regionID)
		return false
	}
	return true
}

// / Checks if the message is sent to the correct peer.
// /
// / Returns true means that the message can be dropped silently.
func (d *peerMsgHandler) checkMessage(msg *rspb.RaftMessage) bool {
	fromEpoch := msg.GetRegionEpoch()
	isVoteMsg := util.IsVoteMessage(msg.Message)
	fromStoreID := msg.FromPeer.GetStoreId()

	// Let's consider following cases with three nodes [1, 2, 3] and 1 is leader:
	// a. 1 removes 2, 2 may still send MsgAppendResponse to 1.
	//  We should ignore this stale message and let 2 remove itself after
	//  applying the ConfChange log.
	// b. 2 is isolated, 1 removes 2. When 2 rejoins the cluster, 2 will
	//  send stale MsgRequestVote to 1 and 3, at this time, we should tell 2 to gc itself.
	// c. 2 is isolated but can communicate with 3. 1 removes 3.
	//  2 will send stale MsgRequestVote to 3, 3 should ignore this message.
	// d. 2 is isolated but can communicate with 3. 1 removes 2, then adds 4, remove 3.
	//  2 will send stale MsgRequestVote to 3, 3 should tell 2 to gc itself.
	// e. 2 is isolated. 1 adds 4, 5, 6, removes 3, 1. Now assume 4 is leader.
	//  After 2 rejoins the cluster, 2 may send stale MsgRequestVote to 1 and 3,
	//  1 and 3 will ignore this message. Later 4 will send messages to 2 and 2 will
	//  rejoin the raft group again.
	// f. 2 is isolated. 1 adds 4, 5, 6, removes 3, 1. Now assume 4 is leader, and 4 removes 2.
	//  unlike case e, 2 will be stale forever.
	// TODO: for case f, if 2 is stale for a long time, 2 will communicate with scheduler and scheduler will
	// tell 2 is stale, so 2 can remove itself.
	region := d.Region()
	if util.IsEpochStale(fromEpoch, region.RegionEpoch) && util.FindPeer(region, fromStoreID) == nil {
		// The message is stale and not in current region.
		handleStaleMsg(d.ctx.trans, msg, region.RegionEpoch, isVoteMsg)
		return true
	}
	target := msg.GetToPeer()
	if target.Id < d.PeerId() {
		log.Infof("%s target peer ID %d is less than %d, msg maybe stale", d.Tag, target.Id, d.PeerId())
		return true
	} else if target.Id > d.PeerId() {
		if d.MaybeDestroy() {
			log.Infof("%s is stale as received a larger peer %s, destroying", d.Tag, target)
			d.destroyPeer()
			d.ctx.router.sendStore(message.NewMsg(message.MsgTypeStoreRaftMessage, msg))
		}
		return true
	}
	return false
}

func handleStaleMsg(trans Transport, msg *rspb.RaftMessage, curEpoch *metapb.RegionEpoch,
	needGC bool) {
	regionID := msg.RegionId
	fromPeer := msg.FromPeer
	toPeer := msg.ToPeer
	msgType := msg.Message.GetMsgType()

	if !needGC {
		log.Infof("[region %d] raft message %s is stale, current %v ignore it",
			regionID, msgType, curEpoch)
		return
	}
	gcMsg := &rspb.RaftMessage{
		RegionId:    regionID,
		FromPeer:    toPeer,
		ToPeer:      fromPeer,
		RegionEpoch: curEpoch,
		IsTombstone: true,
	}
	if err := trans.Send(gcMsg); err != nil {
		log.Errorf("[region %d] send message failed %v", regionID, err)
	}
}

func (d *peerMsgHandler) handleGCPeerMsg(msg *rspb.RaftMessage) {
	fromEpoch := msg.RegionEpoch
	if !util.IsEpochStale(d.Region().RegionEpoch, fromEpoch) {
		return
	}
	if !util.PeerEqual(d.Meta, msg.ToPeer) {
		log.Infof("%s receive stale gc msg, ignore", d.Tag)
		return
	}
	log.Infof("%s peer %s receives gc message, trying to remove", d.Tag, msg.ToPeer)
	if d.MaybeDestroy() {
		d.destroyPeer()
	}
}

// Returns `None` if the `msg` doesn't contain a snapshot or it contains a snapshot which
// doesn't conflict with any other snapshots or regions. Otherwise a `snap.SnapKey` is returned.
func (d *peerMsgHandler) checkSnapshot(msg *rspb.RaftMessage) (*snap.SnapKey, error) {
	if msg.Message.Snapshot == nil {
		return nil, nil
	}
	regionID := msg.RegionId
	snapshot := msg.Message.Snapshot
	key := snap.SnapKeyFromRegionSnap(regionID, snapshot)
	snapData := new(rspb.RaftSnapshotData)
	err := snapData.Unmarshal(snapshot.Data)
	if err != nil {
		return nil, err
	}
	snapRegion := snapData.Region
	peerID := msg.ToPeer.Id
	var contains bool
	for _, peer := range snapRegion.Peers {
		if peer.Id == peerID {
			contains = true
			break
		}
	}
	if !contains {
		log.Infof("%s %s doesn't contains peer %d, skip", d.Tag, snapRegion, peerID)
		return &key, nil
	}
	meta := d.ctx.storeMeta
	meta.Lock()
	defer meta.Unlock()
	if !util.RegionEqual(meta.regions[d.regionId], d.Region()) {
		if !d.isInitialized() {
			log.Infof("%s stale delegate detected, skip", d.Tag)
			return &key, nil
		} else {
			panic(fmt.Sprintf("%s meta corrupted %s != %s", d.Tag, meta.regions[d.regionId], d.Region()))
		}
	}

	existRegions := meta.getOverlapRegions(snapRegion)
	for _, existRegion := range existRegions {
		if existRegion.GetId() == snapRegion.GetId() {
			continue
		}
		log.Infof("%s region overlapped %s %s", d.Tag, existRegion, snapRegion)
		return &key, nil
	}

	// check if snapshot file exists.
	_, err = d.ctx.snapMgr.GetSnapshotForApplying(key)
	if err != nil {
		return nil, err
	}
	return nil, nil
}

func (d *peerMsgHandler) destroyPeer() {
	log.Infof("%s starts destroy", d.Tag)
	regionID := d.regionId
	// We can't destroy a peer which is applying snapshot.
	meta := d.ctx.storeMeta
	meta.Lock()
	defer meta.Unlock()
	isInitialized := d.isInitialized()
	if err := d.Destroy(d.ctx.engine, false); err != nil {
		// If not panic here, the peer will be recreated in the next restart,
		// then it will be gc again. But if some overlap region is created
		// before restarting, the gc action will delete the overlap region's
		// data too.
		panic(fmt.Sprintf("%s destroy peer %v", d.Tag, err))
	}
	d.ctx.router.close(regionID)
	d.stopped = true
	if isInitialized && meta.regionRanges.Delete(&regionItem{region: d.Region()}) == nil {
		panic(d.Tag + " meta corruption detected")
	}
	if _, ok := meta.regions[regionID]; !ok {
		panic(d.Tag + " meta corruption detected")
	}
	delete(meta.regions, regionID)
}

func (d *peerMsgHandler) findSiblingRegion() (result *metapb.Region) {
	meta := d.ctx.storeMeta
	meta.RLock()
	defer meta.RUnlock()
	item := &regionItem{region: d.Region()}
	meta.regionRanges.AscendGreaterOrEqual(item, func(i btree.Item) bool {
		result = i.(*regionItem).region
		return true
	})
	return
}

func (d *peerMsgHandler) onRaftGCLogTick() {
	d.ticker.schedule(PeerTickRaftLogGC)
	if !d.IsLeader() {
		return
	}

	appliedIdx := d.peerStorage.AppliedIndex()
	firstIdx, _ := d.peerStorage.FirstIndex()
	var compactIdx uint64
	if appliedIdx > firstIdx && appliedIdx-firstIdx >= d.ctx.cfg.RaftLogGcCountLimit {
		compactIdx = appliedIdx
	} else {
		return
	}

	y.Assert(compactIdx > 0)
	compactIdx -= 1
	if compactIdx < firstIdx {
		// In case compact_idx == first_idx before subtraction.
		return
	}

	term, err := d.RaftGroup.Raft.RaftLog.Term(compactIdx)
	if err != nil {
		log.Fatalf("appliedIdx: %d, firstIdx: %d, compactIdx: %d", appliedIdx, firstIdx, compactIdx)
		panic(err)
	}

	// Create a compact log request and notify directly.
	regionID := d.regionId
	request := newCompactLogRequest(regionID, d.Meta, compactIdx, term)
	d.proposeRaftCommand(request, nil)
}

func (d *peerMsgHandler) onSplitRegionCheckTick() {
	d.ticker.schedule(PeerTickSplitRegionCheck)
	// To avoid frequent scan, we only add new scan tasks if all previous tasks
	// have finished.
	if len(d.ctx.splitCheckTaskSender) > 0 {
		return
	}

	if !d.IsLeader() {
		return
	}
	if d.ApproximateSize != nil && d.SizeDiffHint < d.ctx.cfg.RegionSplitSize/8 {
		return
	}
	d.ctx.splitCheckTaskSender <- &runner.SplitCheckTask{
		Region: d.Region(),
	}
	d.SizeDiffHint = 0
}

func (d *peerMsgHandler) onPrepareSplitRegion(regionEpoch *metapb.RegionEpoch, splitKey []byte, cb *message.Callback) {
	if err := d.validateSplitRegion(regionEpoch, splitKey); err != nil {
		cb.Done(ErrResp(err))
		return
	}
	region := d.Region()
	d.ctx.schedulerTaskSender <- &runner.SchedulerAskSplitTask{
		Region:   region,
		SplitKey: splitKey,
		Peer:     d.Meta,
		Callback: cb,
	}
}

func (d *peerMsgHandler) validateSplitRegion(epoch *metapb.RegionEpoch, splitKey []byte) error {
	if len(splitKey) == 0 {
		err := errors.Errorf("%s split key should not be empty", d.Tag)
		log.Error(err)
		return err
	}

	if !d.IsLeader() {
		// region on this store is no longer leader, skipped.
		log.Infof("%s not leader, skip", d.Tag)
		return &util.ErrNotLeader{
			RegionId: d.regionId,
			Leader:   d.getPeerFromCache(d.LeaderId()),
		}
	}

	region := d.Region()
	latestEpoch := region.GetRegionEpoch()

	// This is a little difference for `check_region_epoch` in region split case.
	// Here we just need to check `version` because `conf_ver` will be update
	// to the latest value of the peer, and then send to Scheduler.
	if latestEpoch.Version != epoch.Version {
		log.Infof("%s epoch changed, retry later, prev_epoch: %s, epoch %s",
			d.Tag, latestEpoch, epoch)
		return &util.ErrEpochNotMatch{
			Message: fmt.Sprintf("%s epoch changed %s != %s, retry later", d.Tag, latestEpoch, epoch),
			Regions: []*metapb.Region{region},
		}
	}
	return nil
}

func (d *peerMsgHandler) onApproximateRegionSize(size uint64) {
	d.ApproximateSize = &size
}

func (d *peerMsgHandler) onSchedulerHeartbeatTick() {
	d.ticker.schedule(PeerTickSchedulerHeartbeat)

	if !d.IsLeader() {
		return
	}
	d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
}

func (d *peerMsgHandler) onGCSnap(snaps []snap.SnapKeyWithSending) {
	compactedIdx := d.peerStorage.truncatedIndex()
	compactedTerm := d.peerStorage.truncatedTerm()
	for _, snapKeyWithSending := range snaps {
		key := snapKeyWithSending.SnapKey
		if snapKeyWithSending.IsSending {
			snap, err := d.ctx.snapMgr.GetSnapshotForSending(key)
			if err != nil {
				log.Errorf("%s failed to load snapshot for %s %v", d.Tag, key, err)
				continue
			}
			if key.Term < compactedTerm || key.Index < compactedIdx {
				log.Infof("%s snap file %s has been compacted, delete", d.Tag, key)
				d.ctx.snapMgr.DeleteSnapshot(key, snap, false)
			} else if fi, err1 := snap.Meta(); err1 == nil {
				modTime := fi.ModTime()
				if time.Since(modTime) > 4*time.Hour {
					log.Infof("%s snap file %s has been expired, delete", d.Tag, key)
					d.ctx.snapMgr.DeleteSnapshot(key, snap, false)
				}
			}
		} else if key.Term <= compactedTerm &&
			(key.Index < compactedIdx || key.Index == compactedIdx) {
			log.Infof("%s snap file %s has been applied, delete", d.Tag, key)
			a, err := d.ctx.snapMgr.GetSnapshotForApplying(key)
			if err != nil {
				log.Errorf("%s failed to load snapshot for %s %v", d.Tag, key, err)
				continue
			}
			d.ctx.snapMgr.DeleteSnapshot(key, a, false)
		}
	}
}

func newAdminRequest(regionID uint64, peer *metapb.Peer) *raft_cmdpb.RaftCmdRequest {
	return &raft_cmdpb.RaftCmdRequest{
		Header: &raft_cmdpb.RaftRequestHeader{
			RegionId: regionID,
			Peer:     peer,
		},
	}
}

func newCompactLogRequest(regionID uint64, peer *metapb.Peer, compactIndex, compactTerm uint64) *raft_cmdpb.RaftCmdRequest {
	req := newAdminRequest(regionID, peer)
	req.AdminRequest = &raft_cmdpb.AdminRequest{
		CmdType: raft_cmdpb.AdminCmdType_CompactLog,
		CompactLog: &raft_cmdpb.CompactLogRequest{
			CompactIndex: compactIndex,
			CompactTerm:  compactTerm,
		},
	}
	return req
}
