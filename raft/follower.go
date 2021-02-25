package raft

import (
	"github.com/pingcap-incubator/tinykv/kv/util"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"math/rand"
	"time"
)

type Follower struct {
	r *Raft
}

func (f *Follower) HandleMsg(m eraftpb.Message) (err error) {
	switch m.MsgType {
	case eraftpb.MessageType_MsgHup:
		// 发起一次选举
		f.r.becomeCandidate()
	case eraftpb.MessageType_MsgBeat, eraftpb.MessageType_MsgPropose:
		// 暂时不需要处理
		log.Warnf("follower %d handler msg: %s", f.r.id, util.JsonExpr(m))
	case eraftpb.MessageType_MsgAppend:
		f.r.handleAppendEntries(m)
	case eraftpb.MessageType_MsgAppendResponse:
		// 暂时不需要处理
		log.Warnf("follower %d handler msg: %s", f.r.id, util.JsonExpr(m))
	case eraftpb.MessageType_MsgRequestVote:
		f.r.handleVote(m)
	case eraftpb.MessageType_MsgRequestVoteResponse:
		// 暂时不需要处理
		log.Warnf("follower %d handler msg: %s", f.r.id, util.JsonExpr(m))
	case eraftpb.MessageType_MsgSnapshot:
		panic("not support now")
	case eraftpb.MessageType_MsgHeartbeat:
		f.r.handleHeartbeat(m)
	case eraftpb.MessageType_MsgHeartbeatResponse, eraftpb.MessageType_MsgTransferLeader, eraftpb.MessageType_MsgTimeoutNow:
		// 暂时不需要处理
		log.Warnf("follower %d handler msg: %s", f.r.id, util.JsonExpr(m))
	default:
		log.Panicf("follower %d handler msg: %s", f.r.id, util.JsonExpr(m))
	}
	return
}

type NotLeaderMockClock struct {
	// baseline of election interval
	electionTimeout int
	// Ticks since it reached last electionTimeout when it is leader or candidate.
	// Number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	electionElapsed    int
	electionTimeoutNow int // 超时边界, 一次性

	c chan time.Time
	r *Raft
}

func (c *NotLeaderMockClock) Reset() bool {
	c.electionElapsed = 0
	c.electionTimeoutNow = c.electionTimeout + rand.Intn(c.electionTimeout)
	return true
}

func (c *NotLeaderMockClock) Timeout() <-chan time.Time {
	return c.c
}

func (c *NotLeaderMockClock) Tick() {
	c.electionElapsed++
	if c.electionElapsed >= c.electionTimeoutNow {
		c.c <- time.Time{}
	}
}

// TODO: check it if necessary
func (c *NotLeaderMockClock) DoTimeoutFunc() {
	c.r.becomeCandidate()
}
