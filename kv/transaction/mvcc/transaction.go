package mvcc

import (
	"bytes"
	"encoding/binary"

	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/codec"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/scheduler/pkg/tsoutil"
)

// KeyError is a wrapper type so we can implement the `error` interface.
type KeyError struct {
	kvrpcpb.KeyError
}

func (ke *KeyError) Error() string {
	return ke.String()
}

// MvccTxn groups together writes as part of a single transaction. It also provides an abstraction over low-level
// storage, lowering the concepts of timestamps, writes, and locks into plain keys and values.
type MvccTxn struct {
	StartTS uint64
	Reader  storage.StorageReader
	writes  []storage.Modify
}

func NewMvccTxn(reader storage.StorageReader, startTs uint64) *MvccTxn {
	return &MvccTxn{
		Reader:  reader,
		StartTS: startTs,
	}
}

// Writes returns all changes added to this transaction.
func (txn *MvccTxn) Writes() []storage.Modify {
	return txn.writes
}

// PutWrite records a write at key and ts.
func (txn *MvccTxn) PutWrite(key []byte, ts uint64, write *Write) {
	// Your Code Here (4A).
	encode := EncodeKey(key, ts)
	put := storage.Put{
		Key:   encode,
		Value: write.ToBytes(),
		Cf:    engine_util.CfWrite,
	}
	modify := storage.Modify{Data: put}
	txn.writes = append(txn.writes, modify)
}

// GetLock returns a lock if key is locked. It will return (nil, nil) if there is no lock on key, and (nil, err)
// if an error occurs during lookup.
func (txn *MvccTxn) GetLock(key []byte) (*Lock, error) {
	// Your Code Here (4A).
	value, err := txn.Reader.GetCF(engine_util.CfLock, key)
	if err != nil {
		return nil, err
	}
	if value == nil {
		return nil, nil
	}
	lock, locke := ParseLock(value)
	if locke != nil {
		return nil, locke
	}
	return lock, nil
}

// PutLock adds a key/lock to this transaction.
func (txn *MvccTxn) PutLock(key []byte, lock *Lock) {
	// Your Code Here (4A).
	put := storage.Put{
		Key:   key,
		Value: lock.ToBytes(),
		Cf:    engine_util.CfLock,
	}
	modify := storage.Modify{Data: put}
	txn.writes = append(txn.writes, modify)
}

// DeleteLock adds a delete lock to this transaction.
func (txn *MvccTxn) DeleteLock(key []byte) {
	// Your Code Here (4A).
	delete := storage.Delete{
		Key: key,
		Cf:  engine_util.CfLock,
	}
	modify := storage.Modify{Data: delete}
	txn.writes = append(txn.writes, modify)
}

// GetValue finds the value for key, valid at the start timestamp of this transaction.
// I.e., the most recent value committed before the start of this transaction.
func (txn *MvccTxn) GetValue(key []byte) ([]byte, error) {
	// Your Code Here (4A).
	iter := txn.Reader.IterCF(engine_util.CfWrite)
	iter.Seek(EncodeKey(key, txn.StartTS))
	k := iter.Item().Key()
	defer iter.Close()
	// 如果找到的key为nil，直接返回
	if k == nil {
		return nil, nil
	}
	// 获取其中的User key
	user := DecodeUserKey(k)
	// Write的key与查找的key不同，直接返回
	if !bytes.Equal(user, key) {
		return nil, nil
	}
	value, _ := iter.Item().Value()
	write, err := ParseWrite(value)
	if err != nil {
		return nil, err
	}
	// 判断write是否是Put
	if write.Kind != WriteKindPut {
		return nil, nil
	}
	v, err := txn.Reader.GetCF(engine_util.CfDefault, EncodeKey(key, write.StartTS))
	if err != nil {
		panic(err)
	}
	return v, nil
}

// PutValue adds a key/value write to this transaction.
func (txn *MvccTxn) PutValue(key []byte, value []byte) {
	// Your Code Here (4A).
	encode := EncodeKey(key, txn.StartTS)
	put := storage.Put{
		Key:   encode,
		Value: value,
		Cf:    engine_util.CfDefault,
	}
	modify := storage.Modify{Data: put}
	txn.writes = append(txn.writes, modify)
}

// DeleteValue removes a key/value pair in this transaction.
func (txn *MvccTxn) DeleteValue(key []byte) {
	// Your Code Here (4A).
	encode := EncodeKey(key, txn.StartTS)
	delete := storage.Delete{
		Key: encode,
		Cf:  engine_util.CfDefault,
	}
	modify := storage.Modify{Data: delete}
	txn.writes = append(txn.writes, modify)
}

// CurrentWrite searches for a write with this transaction's start timestamp. It returns a Write from the DB and that
// write's commit timestamp, or an error.
func (txn *MvccTxn) CurrentWrite(key []byte) (*Write, uint64, error) {
	// Your Code Here (4A).
	iter := txn.Reader.IterCF(engine_util.CfWrite)
	iter.Seek(EncodeKey(key, TsMax))
	value, _ := iter.Item().Value()
	defer iter.Close()
	// 如果没有write记录，直接返回
	if value == nil {
		return nil, 0, nil
	}
	write, err := ParseWrite(value)
	if err != nil {
		panic(err)
	}
	for write.StartTS > txn.StartTS {
		iter.Next()
		if !iter.Valid() {
			return nil, 0, nil
		}
		value, _ := iter.Item().Value()
		if value == nil {
			return nil, 0, nil
		}
		write, err = ParseWrite(value)
		if err != nil {
			panic(err)
		}
	}
	if write.StartTS == txn.StartTS {
		// 返回commited timestamp
		k := iter.Item().Key()
		return write, decodeTimestamp(k), nil
	}
	return nil, 0, nil
}

// MostRecentWrite finds the most recent write with the given key. It returns a Write from the DB and that
// write's commit timestamp, or an error.
func (txn *MvccTxn) MostRecentWrite(key []byte) (*Write, uint64, error) {
	// Your Code Here (4A).
	iter := txn.Reader.IterCF(engine_util.CfWrite)
	iter.Seek(EncodeKey(key, TsMax))
	k := iter.Item().Key()
	defer iter.Close()
	if k == nil {
		return nil, 0, nil
	}
	user := DecodeUserKey(k)
	if bytes.Equal(key, user) {
		ts := decodeTimestamp(k)
		value, _ := iter.Item().Value()
		if value == nil {
			return nil, 0, nil
		}
		write, err := ParseWrite(value)
		if err != nil {
			panic(err)
		}
		return write, ts, nil
	}
	return nil, 0, nil
}

// EncodeKey encodes a user key and appends an encoded timestamp to a key. Keys and timestamps are encoded so that
// timestamped keys are sorted first by key (ascending), then by timestamp (descending). The encoding is based on
// https://github.com/facebook/mysql-5.6/wiki/MyRocks-record-format#memcomparable-format.
func EncodeKey(key []byte, ts uint64) []byte {
	encodedKey := codec.EncodeBytes(key)
	newKey := append(encodedKey, make([]byte, 8)...)
	binary.BigEndian.PutUint64(newKey[len(encodedKey):], ^ts)
	return newKey
}

// DecodeUserKey takes a key + timestamp and returns the key part.
func DecodeUserKey(key []byte) []byte {
	_, userKey, err := codec.DecodeBytes(key)
	if err != nil {
		panic(err)
	}
	return userKey
}

// decodeTimestamp takes a key + timestamp and returns the timestamp part.
func decodeTimestamp(key []byte) uint64 {
	left, _, err := codec.DecodeBytes(key)
	if err != nil {
		panic(err)
	}
	return ^binary.BigEndian.Uint64(left)
}

// PhysicalTime returns the physical time part of the timestamp.
func PhysicalTime(ts uint64) uint64 {
	return ts >> tsoutil.PhysicalShiftBits
}
