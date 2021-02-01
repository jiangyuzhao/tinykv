package server

import (
	"context"
	"github.com/pingcap-incubator/tinykv/kv/util"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/log"

	"github.com/pingcap-incubator/tinykv/kv/coprocessor"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/storage/raft_storage"
	"github.com/pingcap-incubator/tinykv/kv/transaction/latches"
	coppb "github.com/pingcap-incubator/tinykv/proto/pkg/coprocessor"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/tinykvpb"
	"github.com/pingcap/tidb/kv"
)

var _ tinykvpb.TinyKvServer = new(Server)

// Server is a TinyKV server, it 'faces outwards', sending and receiving messages from clients such as TinySQL.
type Server struct {
	storage storage.Storage

	// (Used in 4A/4B)
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

// Raw API.
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	// Your Code Here (1).
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		log.Fatalf("RawGet failed for storage.Reader failed. req: %s, err: %v\n", util.JsonExpr(req), err)
		return nil, err
	}
	value, err := reader.GetCF(req.Cf, req.Key)
	resp := &kvrpcpb.RawGetResponse{}
	// 这真是太脏了, 为了过test TestRawDelete1导致的, 这个是设计问题, GetCF本来应该返回的err不能返回, 这里没办法判断err
	// 为了后续的语义一致性, 这里就写得脏了点, 否则应该判断的
	if value == nil {
		resp.NotFound = true
		if err != nil {
			resp.Error = err.Error()
		}
		log.Errorf("RawGet failed for GetCF failed, req: %s, err: %v\n",util.JsonExpr(req), err)
	} else {
		resp.Value = value
	}
	return resp, nil
}

func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	data := storage.Put{
		Key: req.Key,
		Value: req.Value,
		Cf: req.Cf,
	}
	modify := storage.Modify{Data: data}
	err := server.storage.Write(req.Context, []storage.Modify{modify})
	if err != nil {
		log.Errorf("RawPut err for storage.Write err, req: %s, err: %v\n", util.JsonExpr(req), err)
		return &kvrpcpb.RawPutResponse{Error: err.Error()}, err
	}
	return &kvrpcpb.RawPutResponse{}, nil
}

func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	data := storage.Delete{
		Key: req.Key,
		Cf: req.Cf,
	}
	modify := storage.Modify{Data: data}
	err := server.storage.Write(req.Context, []storage.Modify{modify})
	if err != nil {
		log.Errorf("RawDelete err for storage.Write err, req: %s, err: %v\n", util.JsonExpr(req), err)
		return &kvrpcpb.RawDeleteResponse{Error: err.Error()}, err
	}
	return &kvrpcpb.RawDeleteResponse{}, nil
}

func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		log.Fatalf("RawScan failed for storage.Reader failed. req: %s, err: %v\n", util.JsonExpr(req), err)
		return nil, err
	}
	dbIter := reader.IterCF(req.Cf)
	dbIter.Seek(req.StartKey)
	kvs := make([]*kvrpcpb.KvPair, 0)
	var k, v []byte
	var item engine_util.DBItem
	resp := &kvrpcpb.RawScanResponse{}
	for i := uint32(0); i < req.Limit; i++ {
		if dbIter.Valid() {
			item = dbIter.Item()
			k = item.Key()
			v, err = item.Value()
			if err != nil {
				log.Errorf("RawScan err when get item key %s. req: %s, offset: %d, err: %v\n", string(k), util.JsonExpr(req), i, err)
				resp.Error = err.Error()
				break
			}
			kvs = append(kvs, &kvrpcpb.KvPair{Key: k, Value: v})
			dbIter.Next()
		} else {
			break
		}
	}
	return &kvrpcpb.RawScanResponse{Kvs: kvs}, err
}

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
	return nil, nil
}

func (server *Server) KvPrewrite(_ context.Context, req *kvrpcpb.PrewriteRequest) (*kvrpcpb.PrewriteResponse, error) {
	// Your Code Here (4B).
	return nil, nil
}

func (server *Server) KvCommit(_ context.Context, req *kvrpcpb.CommitRequest) (*kvrpcpb.CommitResponse, error) {
	// Your Code Here (4B).
	return nil, nil
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
