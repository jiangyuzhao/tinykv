package raft

import (
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"math/rand"
	"time"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

type RoleHandler interface {
	HandleMsg(m pb.Message) error
}

type Timer interface {
	Reset() bool
	Timeout() <-chan time.Time
}

type MockTimer interface {
	Timer
	Tick()
	DoTimeoutFunc()
}

func newRoleHandler(role StateType, r *Raft) RoleHandler {
	switch role {
	case StateFollower:
		return &Follower{r: r}
	case StateCandidate:
		return &Candidate{r: r}
	case StateLeader:
		return &Leader{r: r}
	default:
		panic("not support role handler")
	}
}

func newClock(role StateType, r *Raft, timeout int) Timer {
	switch role {
	case StateFollower, StateCandidate:
		return &NotLeaderMockClock{
			electionTimeout:    timeout,
			electionElapsed:    0,
			c:                  make(chan time.Time, 1),
			r:                  r,
			electionTimeoutNow: timeout + rand.Intn(timeout),
		}
	case StateLeader:
		return &LeaderMockClock{
			heartbeatTimeout: timeout,
			heartbeatElapsed: 0,
			c:                make(chan time.Time, 1),
			r:                r,
		}
	default:
		panic("not support role in new clock")
	}
}
