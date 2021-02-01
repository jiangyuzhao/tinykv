package standalone_storage

import (
	"fmt"
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	CfDefault *badger.DB
	CfLock *badger.DB
	CfWrite *badger.DB
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	return &StandAloneStorage{
		CfDefault: engine_util.CreateDB("default", conf),
		CfLock: engine_util.CreateDB("lock", conf),
		CfWrite: engine_util.CreateDB("write", conf),
	}
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	log.Info("StandAloneStorage started")
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	if err := s.CfDefault.Close(); err != nil {
		log.Errorf("stop StandAloneStorage default db failed for %v", err)
	}
	if err := s.CfLock.Close(); err != nil {
		log.Errorf("stop StandAloneStorage lock db failed for %v", err)
	}
	if err := s.CfWrite.Close(); err != nil {
		log.Errorf("stop StandAloneStorage write db failed for %v", err)
	}
	log.Info("StandAloneStorage closed")
	return nil
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	return NewStandaloneStorageReader(s), nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	for _, m := range batch {
		switch data := m.Data.(type) {
		case storage.Put:
			switch data.Cf {
			case engine_util.CfDefault:
				return engine_util.PutCF(s.CfDefault, data.Cf, data.Key, data.Value)
			case engine_util.CfLock:
				return engine_util.PutCF(s.CfLock, data.Cf, data.Key, data.Value)
			case engine_util.CfWrite:
				return engine_util.PutCF(s.CfWrite, data.Cf, data.Key, data.Value)
			}
		case storage.Delete:
			switch data.Cf {
			case engine_util.CfDefault:
				return engine_util.DeleteCF(s.CfDefault, data.Cf, data.Key)
			case engine_util.CfLock:
				return engine_util.DeleteCF(s.CfLock, data.Cf, data.Key)
			case engine_util.CfWrite:
				return engine_util.DeleteCF(s.CfWrite, data.Cf, data.Key)
			}
		}
	}
	return nil
}

type StandaloneStorageReader struct {
	storage *StandAloneStorage
	activeTnx []*badger.Txn
}

func NewStandaloneStorageReader(storage *StandAloneStorage) *StandaloneStorageReader {
	return &StandaloneStorageReader{
		storage: storage,
		activeTnx: make([]*badger.Txn, 0),
	}
}
/**
GetCF(cf string, key []byte) ([]byte, error)
IterCF(cf string) engine_util.DBIterator
Close()
 */
func (sareader *StandaloneStorageReader) GetCF(cf string, key []byte) (result []byte, err error) {
	switch cf {
	case engine_util.CfDefault:
		result, err = engine_util.GetCF(sareader.storage.CfDefault, cf, key)
	case engine_util.CfLock:
		result, err = engine_util.GetCF(sareader.storage.CfLock, cf, key)
	case engine_util.CfWrite:
		result, err = engine_util.GetCF(sareader.storage.CfWrite, cf, key)
	default:
		return nil, fmt.Errorf("mem-server: bad CF %s", cf)
	}
	// 这真是太脏了, 为了过test TestRawDelete1导致的, 这个是设计问题, GetCF本来应该返回的err不能返回
	if err != nil && err.Error() == "Key not found" {
		return nil, nil
	}
	return
}

func (sareader *StandaloneStorageReader) IterCF(cf string) engine_util.DBIterator {
	var txn *badger.Txn
	switch cf {
	case engine_util.CfDefault:
		txn = sareader.storage.CfDefault.NewTransaction(false)
	case engine_util.CfLock:
		txn = sareader.storage.CfLock.NewTransaction(false)
	case engine_util.CfWrite:
		txn = sareader.storage.CfWrite.NewTransaction(false)
	default:
		log.Fatalf("cf is invalid: %s", cf)
		panic("cf invalid")
	}
	sareader.activeTnx = append(sareader.activeTnx, txn)
	return engine_util.NewCFIterator(cf, txn)
}

func (sareader *StandaloneStorageReader)Close()  {
	for i := range sareader.activeTnx {
		// BadgerIterator的Close方法会调用iter.Close, 把对应tx的引用计数-1
		// 如果有没有关闭的iter, 则panic
		sareader.activeTnx[i].Discard()
	}
}