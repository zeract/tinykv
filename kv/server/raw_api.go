package server

import (
	"context"

	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	// Your Code Here (1).
	reader, _ := server.storage.Reader(nil)
	value, err := reader.GetCF(req.Cf, req.Key)
	if value == nil {
		reply := &kvrpcpb.RawGetResponse{NotFound: true}
		return reply, nil
	}
	reply := &kvrpcpb.RawGetResponse{Value: value}
	return reply, err
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified
	put := storage.Put{
		Key:   req.Key,
		Value: req.Value,
		Cf:    req.Cf,
	}
	modify := storage.Modify{Data: put}
	server.storage.Write(nil, []storage.Modify{modify})
	return nil, nil
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	delete := storage.Delete{
		Key: req.Key,
		Cf:  req.Cf,
	}
	modify := storage.Modify{Data: delete}
	server.storage.Write(nil, []storage.Modify{modify})
	return nil, nil
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	reader, _ := server.storage.Reader(nil)
	iter := reader.IterCF(req.Cf)
	nums := 1
	kvpairs := []*kvrpcpb.KvPair{}
	for iter.Seek(req.StartKey); iter.Valid(); iter.Next() {
		if nums > int(req.GetLimit()) {
			break
		}
		item := iter.Item()
		key := item.Key()
		value, _ := item.Value()
		pair := &kvrpcpb.KvPair{
			Key:   key,
			Value: value,
		}
		kvpairs = append(kvpairs, pair)
		nums++
	}
	iter.Close()
	reply := &kvrpcpb.RawScanResponse{Kvs: kvpairs}
	return reply, nil
}
