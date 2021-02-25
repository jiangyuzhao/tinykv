package raft

import (
	"github.com/pingcap-incubator/tinykv/kv/util"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"time"
)

type Leader struct {
	r *Raft
}

func (l *Leader) HandleMsg(m eraftpb.Message) (err error) {
	switch m.MsgType {
	case eraftpb.MessageType_MsgHup:
		// 暂时不需要处理
		log.Warnf("leader %d receive message %s", l.r.id, util.JsonExpr(m))
	case eraftpb.MessageType_MsgBeat:
		for _, p := range l.r.peers {
			if p != l.r.id {
				l.r.sendHeartbeat(p)
			}
		}
	case eraftpb.MessageType_MsgPropose:
		l.r.handlePropose(m)
	case eraftpb.MessageType_MsgAppend:
		l.r.handleAppendEntries(m)
	case eraftpb.MessageType_MsgAppendResponse:
		l.r.handleAppendResponse(m)
	case eraftpb.MessageType_MsgRequestVote:
		l.r.handleVote(m)
	case eraftpb.MessageType_MsgRequestVoteResponse, eraftpb.MessageType_MsgSnapshot:
		// 暂时不需要处理
		log.Warnf("leader %d receive message %s", l.r.id, util.JsonExpr(m))
	case eraftpb.MessageType_MsgHeartbeat:
		l.r.handleHeartbeat(m)
	case eraftpb.MessageType_MsgHeartbeatResponse:
		if m.Reject {
			l.r.becomeFollower(m.Term, m.Lead)
		}
	case eraftpb.MessageType_MsgTransferLeader, eraftpb.MessageType_MsgTimeoutNow:
		// 暂时不需要处理
		log.Warnf("leader %d receive message %s", l.r.id, util.JsonExpr(m))
	default:
		log.Panicf("leader %d receive message %s", l.r.id, util.JsonExpr(m))
	}
	return
}

type LeaderMockClock struct {
	// heartbeat interval, should send
	heartbeatTimeout int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int

	c chan time.Time
	r *Raft
}

func (c *LeaderMockClock) Reset() bool {
	c.heartbeatElapsed = 0
	return true
}

func (c *LeaderMockClock) Timeout() <-chan time.Time {
	return c.c
}

func (c *LeaderMockClock) Tick() {
	c.heartbeatElapsed++
	if c.heartbeatElapsed >= c.heartbeatTimeout {
		c.c <- time.Time{}
	}
}

func (c *LeaderMockClock) DoTimeoutFunc() {
	err := c.r.Step(eraftpb.Message{
		MsgType:              eraftpb.MessageType_MsgBeat,
	})
	if err != nil {
		log.Errorf("leader %d send msg beat error: %v", c.r.id, err)
	}
	c.Reset()
}
