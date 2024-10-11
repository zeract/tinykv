package standalone_storage

import (
	"log"

	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	config *config.Config
	db     *badger.DB
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).

	return &StandAloneStorage{config: conf}
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	opts := badger.DefaultOptions
	opts.Dir = s.config.DBPath
	opts.ValueDir = s.config.DBPath
	db, err := badger.Open(opts)
	if err != nil {
		log.Fatal(err)
	}
	s.db = db
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	s.db.Close()
	return nil
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	// 自己构造一个reader

	return &StandAloneReader{txn: s.db.NewTransaction(true)}, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	// iterate the data batch
	for _, m := range batch {
		if m.Value() != nil {
			err := engine_util.PutCF(s.db, m.Cf(), m.Key(), m.Value())
			if err != nil {
				return err
			}
		} else {
			err := engine_util.DeleteCF(s.db, m.Cf(), m.Key())
			if err != nil {
				return err
			}
		}

	}
	return nil
}
