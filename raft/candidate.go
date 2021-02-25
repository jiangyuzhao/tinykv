package raft

import (
	"github.com/pingcap-incubator/tinykv/kv/util"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

type Candidate struct {
	r *Raft
}

func (c *Candidate) HandleMsg(m eraftpb.Message) (err error) {
	switch m.MsgType {
	case eraftpb.MessageType_MsgHup:
		// 再次发起选举
		c.r.becomeCandidate()
	case eraftpb.MessageType_MsgBeat:
		// 暂时不需要处理
		log.Warnf("candidate %d receive message %s", c.r.id, util.JsonExpr(m))
	case eraftpb.MessageType_MsgPropose:
		// 暂时不需要处理
		log.Warnf("candidate %d receive message %s", c.r.id, util.JsonExpr(m))
	case eraftpb.MessageType_MsgAppend:
		c.r.handleAppendEntries(m)
	case eraftpb.MessageType_MsgAppendResponse:
		// 暂时不需要处理
		log.Warnf("candidate %d receive message %s", c.r.id, util.JsonExpr(m))
	case eraftpb.MessageType_MsgRequestVote:
		c.r.handleVote(m)
	case eraftpb.MessageType_MsgRequestVoteResponse:
		c.r.handleVoteResponse(m)
	case eraftpb.MessageType_MsgSnapshot:
		// 暂时不需要处理
		log.Warnf("candidate %d receive message %s", c.r.id, util.JsonExpr(m))
	case eraftpb.MessageType_MsgHeartbeat:
		c.r.handleHeartbeat(m)
	case eraftpb.MessageType_MsgHeartbeatResponse, eraftpb.MessageType_MsgTransferLeader, eraftpb.MessageType_MsgTimeoutNow:
		// 暂时不需要处理
		log.Warnf("candidate %d receive message %s", c.r.id, util.JsonExpr(m))
	default:
		log.Panicf("candidate %d receive message %s", c.r.id, util.JsonExpr(m))
	}
	return
}
