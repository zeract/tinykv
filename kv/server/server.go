package server

import (
	"context"

	"github.com/pingcap-incubator/tinykv/kv/coprocessor"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/storage/raft_storage"
	"github.com/pingcap-incubator/tinykv/kv/transaction/latches"
	"github.com/pingcap-incubator/tinykv/kv/transaction/mvcc"
	coppb "github.com/pingcap-incubator/tinykv/proto/pkg/coprocessor"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/tinykvpb"
	"github.com/pingcap/tidb/kv"
)

var _ tinykvpb.TinyKvServer = new(Server)

// Server is a TinyKV server, it 'faces outwards', sending and receiving messages from clients such as TinySQL.
type Server struct {
	storage storage.Storage

	// (Used in 4B)
	Latches *latches.Latches

	// coprocessor API handler, out of course scope
	copHandler *coprocessor.CopHandler
}

func NewServer(storage storage.Storage) *Server {
	return &Server{
		storage: storage,
		Latches: latches.NewLatches(),
	}
}

// The below functions are Server's gRPC API (implements TinyKvServer).

// Raft commands (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Raft(stream tinykvpb.TinyKv_RaftServer) error {
	return server.storage.(*raft_storage.RaftStorage).Raft(stream)
}

// Snapshot stream (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Snapshot(stream tinykvpb.TinyKv_SnapshotServer) error {
	return server.storage.(*raft_storage.RaftStorage).Snapshot(stream)
}

// Transactional API.
func (server *Server) KvGet(_ context.Context, req *kvrpcpb.GetRequest) (*kvrpcpb.GetResponse, error) {
	// Your Code Here (4B).
	key := req.Key
	wait := server.Latches.AcquireLatches([][]byte{key})
	//释放锁
	defer server.Latches.ReleaseLatches([][]byte{key})
	if wait != nil {
		return nil, nil
	}
	response := &kvrpcpb.GetResponse{}
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			response.RegionError = regionErr.RequestErr
		}
		return response, err
	}
	defer reader.Close()
	txn := mvcc.NewMvccTxn(reader, req.Version)
	// 获取Lock
	lock, err := txn.GetLock(key)
	if err != nil {
		panic(err)
	}
	if lock != nil && lock.Ts <= txn.StartTS {
		response.Error = &kvrpcpb.KeyError{
			Locked: &kvrpcpb.LockInfo{
				PrimaryLock: lock.Primary,
				LockVersion: lock.Ts,
				Key:         req.Key,
				LockTtl:     lock.Ttl,
			}}
		return response, nil
	}
	value, err := txn.GetValue(key)
	if err != nil {
		panic(err)
	}
	if value == nil {
		response.NotFound = true
	}
	response.Value = value

	return response, nil
}

func (server *Server) KvPrewrite(_ context.Context, req *kvrpcpb.PrewriteRequest) (*kvrpcpb.PrewriteResponse, error) {
	// Your Code Here (4B).
	var keys [][]byte
	for _, m := range req.Mutations {
		keys = append(keys, m.Key)
	}
	wait := server.Latches.AcquireLatches(keys)
	// 释放锁
	defer server.Latches.ReleaseLatches(keys)
	// 当前锁被占用
	if wait != nil {
		return nil, nil
	}
	response := &kvrpcpb.PrewriteResponse{}
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			response.RegionError = regionErr.RequestErr
		}
		return response, err
	}
	defer reader.Close()
	txn := mvcc.NewMvccTxn(reader, req.StartVersion)

	var keyErrs []*kvrpcpb.KeyError
	for _, m := range req.Mutations {
		write, ts, err := txn.MostRecentWrite(m.Key)
		if err != nil {
			panic(err)
		}
		if write != nil && req.StartVersion <= ts {
			keyErrs = append(keyErrs, &kvrpcpb.KeyError{
				Conflict: &kvrpcpb.WriteConflict{
					StartTs:    req.StartVersion,
					ConflictTs: ts, Key: m.Key,
					Primary: req.PrimaryLock,
				}})
			continue
		}
		lock, err := txn.GetLock(m.Key)
		if err != nil {
			panic(err)
		}
		if lock != nil && lock.Ts <= txn.StartTS {
			keyErrs = append(keyErrs, &kvrpcpb.KeyError{
				Locked: &kvrpcpb.LockInfo{
					PrimaryLock: lock.Primary,
					LockVersion: lock.Ts,
					Key:         m.Key,
					LockTtl:     lock.Ttl,
				}})
			continue
		}
		var kind mvcc.WriteKind
		switch m.Op {
		case kvrpcpb.Op_Put:
			kind = mvcc.WriteKindPut
			txn.PutValue(m.Key, m.Value)
		case kvrpcpb.Op_Del:
			kind = mvcc.WriteKindDelete
			txn.DeleteValue(m.Key)
		default:
			return nil, nil
		}
		txn.PutLock(m.Key, &mvcc.Lock{
			Primary: req.PrimaryLock,
			Ts:      req.StartVersion,
			Ttl:     req.LockTtl,
			Kind:    kind,
		})

	}
	if len(keyErrs) > 0 {
		response.Errors = keyErrs
		return response, nil
	}
	err = server.storage.Write(req.Context, txn.Writes())
	if err != nil {
		panic(err)
	}
	return response, nil
}

func (server *Server) KvCommit(_ context.Context, req *kvrpcpb.CommitRequest) (*kvrpcpb.CommitResponse, error) {
	// Your Code Here (4B).
	keys := req.Keys
	server.Latches.WaitForLatches(keys)
	//释放锁
	defer server.Latches.ReleaseLatches(keys)

	response := &kvrpcpb.CommitResponse{}
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			response.RegionError = regionErr.RequestErr
		}
		return response, err
	}
	defer reader.Close()
	txn := mvcc.NewMvccTxn(reader, req.StartVersion)
	for _, k := range req.Keys {
		lock, err := txn.GetLock(k)
		if err != nil {
			panic(err)
		}
		if lock == nil {
			write, _, err := txn.CurrentWrite(k)
			if err != nil {
				panic(err)
			}
			if write != nil {
				if write.Kind == mvcc.WriteKindRollback {
					response.Error = &kvrpcpb.KeyError{Retryable: "false"}
					return response, nil
				}
			}
			continue
		}

		if lock.Ts != req.StartVersion {
			response.Error = &kvrpcpb.KeyError{Retryable: "true"}
			write, _, err := txn.CurrentWrite(k)
			if err != nil {
				panic(err)
			}
			if write != nil {
				if write.Kind == mvcc.WriteKindRollback {
					response.Error = &kvrpcpb.KeyError{Retryable: "false"}
				}
			}
			return response, nil
		}
		txn.PutWrite(k, req.CommitVersion, &mvcc.Write{
			StartTS: req.StartVersion,
			Kind:    lock.Kind,
		})
		txn.DeleteLock(k)
	}
	err = server.storage.Write(req.Context, txn.Writes())
	if err != nil {
		panic(err)
	}
	return response, nil
}

func (server *Server) KvScan(_ context.Context, req *kvrpcpb.ScanRequest) (*kvrpcpb.ScanResponse, error) {
	// Your Code Here (4C).
	return nil, nil
}

func (server *Server) KvCheckTxnStatus(_ context.Context, req *kvrpcpb.CheckTxnStatusRequest) (*kvrpcpb.CheckTxnStatusResponse, error) {
	// Your Code Here (4C).
	return nil, nil
}

func (server *Server) KvBatchRollback(_ context.Context, req *kvrpcpb.BatchRollbackRequest) (*kvrpcpb.BatchRollbackResponse, error) {
	// Your Code Here (4C).
	return nil, nil
}

func (server *Server) KvResolveLock(_ context.Context, req *kvrpcpb.ResolveLockRequest) (*kvrpcpb.ResolveLockResponse, error) {
	// Your Code Here (4C).
	return nil, nil
}

// SQL push down commands.
func (server *Server) Coprocessor(_ context.Context, req *coppb.Request) (*coppb.Response, error) {
	resp := new(coppb.Response)
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	switch req.Tp {
	case kv.ReqTypeDAG:
		return server.copHandler.HandleCopDAGRequest(reader, req), nil
	case kv.ReqTypeAnalyze:
		return server.copHandler.HandleCopAnalyzeRequest(reader, req), nil
	}
	return nil, nil
}
