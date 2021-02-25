// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"errors"
	"github.com/pingcap-incubator/tinykv/kv/util"
	"github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"sort"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	Match, Next uint64
}

// 设计上让index 0, term 0是一条空记录, 作为一个共识, 不存储, 这样的话选举消息就不用特殊处理了.
type Raft struct {
	id uint64

	Term uint64
	Vote uint64 // not zero only when follower vote, reset when becomeXXX or receive heartbeat

	// the log
	RaftLog *RaftLog

	// this peer's role
	State StateType

	// log replication progress of each peers, just used in leader
	Prs map[uint64]*Progress

	// votes records, just used in candidate
	votes map[uint64]bool

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64

	// my code for raft
	clock  Timer
	peers  []uint64
	voting bool
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).
	r := &Raft{
		id:               c.ID,
		Term:             0,
		Vote:             0,
		RaftLog:          newLog(c.Storage),
		Prs:              nil,
		State:            StateFollower,
		votes:            nil,
		msgs:             nil,
		Lead:             0,
		heartbeatTimeout: c.HeartbeatTick,
		electionTimeout:  c.ElectionTick,
		leadTransferee:   0,
		PendingConfIndex: 0,
		peers:            c.peers,
	}
	r.clock = newClock(StateFollower, r, c.ElectionTick)
	return r
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	if to == r.id {
		return true
	}
	sendIndex := r.Prs[to].Next
	if sendIndex > r.RaftLog.LastIndex() {
		return false
	}
	logIndex := sendIndex - 1
	logTerm, err := r.RaftLog.Term(logIndex)
	if err != nil {
		log.Errorf("raft %d send append msg error: %v, logIndex: %d", r.id, err, logIndex)
		return false
	}
	e := r.RaftLog.GetEntriesFromIndex(sendIndex, 0)
	if e == nil {
		log.Warnf("raft %d raft log get entries for %d in index %d get nil", r.id, to, sendIndex)
		return false
	}
	var eSend []*pb.Entry
	// use index to avoid copy
	for i := range e {
		eSend = append(eSend, &e[i])
	}
	r.send(pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		LogTerm: logTerm,
		Index:   logIndex,
		Entries: eSend,
		Commit:  r.RaftLog.committed,
	})
	return true
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	r.send(pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		To:      to,
		From:    r.id,
		Term:    r.Term,
	})
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	// only mock timer will call this function
	t := r.clock.(MockTimer)
	t.Tick()
	if isTimeout(r.clock) {
		t.DoTimeoutFunc()
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	if term == 0 || lead == 0 {
		log.Warnf("raft %d become follower with term %d, lead %d", r.id, term, lead)
	}
	r.State = StateFollower
	r.Term = term
	r.Lead = lead
	r.voteOff()
	r.clock = newClock(StateFollower, r, r.electionTimeout)
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	r.State = StateCandidate
	r.Lead = r.id
	r.Vote = r.id
	r.voting = true
	r.Term++
	r.clock = newClock(StateCandidate, r, r.electionTimeout)
	// 特判, 如果只有一个节点, 不会收到任何回复
	if len(r.peers) == 1 {
		r.becomeLeader()
		return
	}
	// initial for votes
	r.votes = map[uint64]bool{r.id: true}
	for _, p := range r.peers {
		if p != r.id {
			r.send(pb.Message{
				MsgType: pb.MessageType_MsgRequestVote,
				To:      p,
				From:    r.id,
				Term:    r.Term,
				LogTerm: r.RaftLog.LastTerm(),
				Index:   r.RaftLog.LastIndex(),
			})
		}
	}
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	r.State = StateLeader
	r.Lead = r.id
	r.voteOff()
	r.clock = newClock(StateLeader, r, r.heartbeatTimeout)
	// send heartbeat first
	err := r.Step(pb.Message{
		MsgType: pb.MessageType_MsgBeat,
	})
	if err != nil {
		log.Errorf("raft %d step msg beat error when become leader, error: %v", r.id, err)
		// just panic now
		panic("step msg beat error when become leader")
		//r.becomeCandidate()
	}
	// initial Prs
	lastIndex := r.RaftLog.LastIndex()
	r.Prs = map[uint64]*Progress{}
	for _, p := range r.peers {
		r.Prs[p] = &Progress{
			Match: 0,
			Next:  lastIndex + 1,
		}
	}
	// append empty log
	r.Step(pb.Message{
		MsgType: pb.MessageType_MsgPropose,
		Entries: []*pb.Entry{{
			EntryType: pb.EntryType_EntryNormal,
		}},
	})
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	return newRoleHandler(r.State, r).HandleMsg(m)
}

// my code.
func (r *Raft) send(m pb.Message) {
	r.msgs = append(r.msgs, m)
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	resp := pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		To:      m.From,
		From:    m.To,
		Commit:  r.RaftLog.committed,
	}
	// 处理term
	if r.Term > m.Term {
		log.Warnf("raft %d term: %d is larger than msg term: %d from %d", r.id, r.Term, m.Term, m.From)
		r.responseMsgTermExpire(&resp)
		return
	}
	if r.Term < m.Term || r.isVoting() {
		r.becomeFollower(m.Term, m.From)
	}
	// 处理log
	if r.Lead != m.From {
		// 同一个任期两个Leader
		log.Panicf("multi-leader in term: %d, r.id: %d, r.Lead: %d, m.From: %d", r.Term, r.id, r.Lead, m.From)
	}
	r.clock.Reset()
	resp.Term = r.Term
	selfLogTerm, err := r.RaftLog.Term(m.Index)
	if err != nil {
		log.Warnf("raft %d get log term with index %d error: %v", r.id, m.Index, err)
		resp.Reject = true
		r.send(resp)
		return
	}
	if selfLogTerm != m.LogTerm {
		log.Warnf("raft %d log term not match, msg log term: %d, self log term: %d", r.id, m.LogTerm, selfLogTerm)
		resp.Reject = true
		r.send(resp)
		return
	}
	resp.Reject = false
	entriesLastIndex := m.Index // 可更新的index, 走到这里至少是可以commit到这里
	for _, e := range m.Entries {
		entriesLastIndex = max(entriesLastIndex, e.Index)
		r.RaftLog.AppendLogWithIndex(*e)
	}
	// 先更新storage, 再更新stable, 再更新commit
	// 只更新commit到要么是这次请求所能触达的最大Index, 要么是commit
	r.RaftLog.updateCommit(min(m.Commit, entriesLastIndex))
	resp.Index = r.RaftLog.LastIndex()
	r.send(resp)
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	msg := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		To:      m.From,
		From:    m.To,
	}
	if r.Term > m.Term {
		r.responseMsgTermExpire(&msg)
		return
	}
	// 如果r还在投票中, 收到term不小于当前term的心跳那就成为follower
	if r.Term < m.Term || r.isVoting() {
		r.becomeFollower(m.Term, m.From)
	}
	if r.Lead != m.From {
		log.Panicf("double leader! raft %d has leader %d, but receive %s", r.id, r.Lead, util.JsonExpr(m))
	}
	r.clock.Reset()
	msg.Term = r.Term
	msg.Reject = false
	r.send(msg)
}

// my code
func (r *Raft) handleVote(m pb.Message) {
	msg := pb.Message{
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		To:      m.From,
		From:    m.To,
	}
	if r.Term > m.Term {
		r.responseMsgTermExpire(&msg)
		return
	}
	if r.Term == m.Term && r.Vote != m.From && r.Vote != 0 {
		r.responseMsgTermExpire(&msg)
		return
	}
	// 判断日志是不是足够新
	if selfLastTerm := r.RaftLog.LastTerm(); selfLastTerm > m.LogTerm {
		r.rejectVote(&msg, m.Term)
		return
	} else if selfLastTerm == m.LogTerm && r.RaftLog.LastIndex() > m.Index {
		r.rejectVote(&msg, m.Term)
		return
	}
	// become follower will reset clock
	r.becomeFollower(m.Term, m.From)
	r.Vote = m.From
	r.voting = true
	msg.Term = r.Term
	msg.Reject = false
	r.send(msg)
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}

// my code
func (r *Raft) rejectVote(resp *pb.Message, respTerm uint64) {
	// 这里回复的消息的term不是raft的term, 这是为了让接受者明确这个消息是什么时候的回复, 并且不设置leader
	resp.Term = respTerm
	resp.Reject = true
	r.send(*resp)
}

func (r *Raft) responseMsgTermExpire(resp *pb.Message) {
	resp.Term = r.Term
	resp.Lead = r.Lead
	resp.Reject = true
	r.send(*resp)
}

type matchIndexArray []uint64

func (a matchIndexArray) Len() int           { return len(a) }
func (a matchIndexArray) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a matchIndexArray) Less(i, j int) bool { return a[i] < a[j] }

func (r *Raft) updateCommit() {
	matchArray := matchIndexArray([]uint64{r.RaftLog.LastIndex()})
	for k, v := range r.Prs {
		if k == r.id {
			continue
		}
		matchArray = append(matchArray, v.Match)
	}
	sort.Sort(matchArray)
	targetArrayIndex := len(r.peers) / 2
	if len(r.peers)%2 == 0 {
		targetArrayIndex-- // 4-->1, 4/2 - 1
	}
	targetLogIndex := matchArray[targetArrayIndex]

	if term, err := r.RaftLog.Term(targetLogIndex); err != nil {
		log.Errorf("raft %d update commit get term error: %v, index: %d", r.id, err, targetLogIndex)
		return
	} else if term != r.Term {
		log.Infof("raft %d cannot commit term %d, index %d with its term %d", r.id, term, targetLogIndex, r.Term)
		return
	}
	if targetLogIndex > r.RaftLog.committed {
		r.RaftLog.updateCommit(targetLogIndex)
	}
}

func (r *Raft) handlePropose(m pb.Message) {
	// propose是内部消息, 不需要判断term
	logIndex := r.RaftLog.LastIndex() + 1
	logTerm := r.Term
	for _, e := range m.Entries {
		e.Term = logTerm
		e.Index = logIndex
		logIndex++
		r.RaftLog.entries = append(r.RaftLog.entries, *e)
	}
	r.Prs[r.id].Next = logIndex
	r.Prs[r.id].Match = logIndex - 1
	// 发送新消息
	if len(r.peers) == 1 {
		r.updateCommit()
		return
	}
	for _, p := range r.peers {
		if p == r.id {
			continue
		}
		r.sendAppend(p)
	}
}

func (r *Raft) handleAppendResponse(m pb.Message) {
	if !m.Reject {
		if m.Term != r.Term {
			log.Warnf("raft %d receive append response in term %d with raft term: %d", r.id, m.Term, r.Term)
			return
		}
		// if not reject, index is valid
		r.Prs[m.From].Next = m.Index + 1
		r.Prs[m.From].Match = m.Index
		r.updateCommit()
		return
	}
	// reject
	// 处理term
	if m.Term > r.Term {
		if m.Lead != 0 {
			r.becomeFollower(m.Term, m.Lead)
			return
		}
		log.Panicf("raft %d get append response: %s", r.id, util.JsonExpr(m))
	}
	// 如果不是term的问题, 则需要修改Next
	r.Prs[m.From].Next--
	if r.Prs[m.From].Next < 0 {
		log.Panicf("raft %d send node %d next index %d smaller than 0", r.id, m.From, r.Prs[m.From].Next)
	}
	// TODO: ugly! 为了过测试, 需要再次触发sendAppend直到成功
	r.sendAppend(m.From)
}

func (r *Raft) handleVoteResponse(m pb.Message) {
	if r.Term < m.Term {
		r.becomeFollower(m.Term, m.Lead)
		return
	}
	if r.Term > m.Term {
		return
	}
	// 消息是该任期的消息, 那么更新一下是不是被投票
	r.votes[m.From] = !m.Reject
	if len(r.votes) <= len(r.peers)/2 {
		// 未超过半数, 不用统计
		return
	}
	voteCnt := 0
	for _, voteResult := range r.votes {
		if voteResult {
			voteCnt++
		}
	}
	if voteCnt > len(r.peers)/2 {
		r.becomeLeader()
	}
	// 不用对其他情况特殊处理, 如果所有的投票都收集了依然没有成为leader, 那就继续等待下一次超时
}

func (r *Raft) isVoting() bool {
	return r.voting || r.Lead == 0
}

func (r *Raft) voteOff() {
	r.voting = false
}
