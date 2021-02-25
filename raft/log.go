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
	"fmt"
	"github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// RaftLog manage the log entries, its struct look like:
//
//  snapshot/first.....applied....committed....stabled.....last
//  --------|------------------------------------------------|
//                            log entries
//
// for simplify the RaftLog implement should manage all log entries
// that not truncated
type RaftLog struct {
	// storage contains all stable entries since the last snapshot.
	storage Storage

	// committed is the highest log position that is known to be in
	// stable storage on a quorum of nodes.
	committed uint64

	// applied is the highest log position that the application has
	// been instructed to apply to its state machine.
	// Invariant: applied <= committed
	applied uint64

	// log entries with index <= stabled are persisted to storage.
	// It is used to record the logs that are not persisted by storage yet.
	// Everytime handling `Ready`, the unstabled logs will be included.
	stabled uint64

	// all entries that have not yet compact.
	entries []pb.Entry

	// the incoming unstable snapshot, if any.
	// (Used in 2C)
	pendingSnapshot *pb.Snapshot

	// Your Data Here (2A).
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).
	stabled, err := storage.LastIndex()
	if err != nil {
		panic("new log error")
	}
	return &RaftLog{
		storage:   storage,
		committed: 0,
		applied:   0,
		stabled:   stabled,
		entries:   make([]pb.Entry, 0),
	}
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	// Your Code Here (2A).
	return l.GetEntriesFromIndex(l.stabled+1, 0)
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	if l.committed-l.applied == 0 {
		return nil
	}
	ents = l.GetEntriesFromIndex(l.applied+1, l.committed-l.applied)
	return
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	length := len(l.entries)
	if length == 0 {
		return l.stabled
	}
	return l.entries[len(l.entries)-1].Index
}

// my code
// if limit is 0, return all!
func (l *RaftLog) GetEntriesFromIndex(from uint64, limit uint64) []pb.Entry {
	from = max(1, from)
	var to uint64
	if limit == 0 {
		to = l.LastIndex() + 1
	} else {
		to = min(from+limit, l.LastIndex()+1)
	}
	if to <= from {
		log.Warnf("from %d, to: %d", from, to)
		return []pb.Entry{}
	}
	// TODO: use lock when concurrent, 或者封装一个结构体来CAS
	entries := l.entries
	stabled := l.stabled

	if len(entries) == 0 || to < entries[0].Index {
		es, err := l.storage.Entries(from, to)
		if err != nil {
			log.Errorf("get entries from storage in [%d, %d) error: %v", from, to, err)
			return []pb.Entry{}
		}
		return es
	}
	if from > stabled {
		fromIndex := findEntriesArrayIndexByLogIndex(from, entries)
		toIndex := findEntriesArrayIndexByLogIndex(to-1, entries)
		if fromIndex < 0 || toIndex < 0 {
			log.Warnf("fromIndex: %d, toIndex: %d", fromIndex, toIndex)
			return []pb.Entry{}
		}
		return entries[fromIndex : toIndex+1]
	}
	ret, err := l.storage.Entries(from, stabled+1)
	if err != nil {
		log.Errorf("get entries from storage in [%d, %d) error: %v", from, stabled+1, err)
		return []pb.Entry{}
	}
	if stabled == l.LastIndex() {
		return ret
	}
	// 否则stabled < lastIndex, stabled + 1 <= lastIndex, 一定能找到fromIndex
	fromIndex := findEntriesArrayIndexByLogIndex(stabled+1, entries)
	toIndex := findEntriesArrayIndexByLogIndex(to-1, entries)
	if fromIndex < 0 || toIndex < 0 {
		log.Warnf("fromIndex: %d, toIndex: %d", fromIndex, toIndex)
		return []pb.Entry{}
	}
	ret = append(ret, entries[fromIndex:toIndex+1]...)
	return ret
}

// Term return the term of the entry in the given index
// 设计上让index 0, term 0是一条空记录, 作为一个共识, 不存储, 这样的话选举消息就不用特殊处理了.
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	if i == 0 {
		return 0, nil
	}
	arrayIndex := findEntriesArrayIndexByLogIndex(i, l.entries)
	switch arrayIndex {
	case int64(cannotFind):
		return 0, fmt.Errorf("cannot find log with index %d", i)
	case int64(mayInStorage):
		// 根据logIndex找term
		return l.storage.Term(i)
	default:
		return l.entries[arrayIndex].Term, nil
	}
}

func (l *RaftLog) AppendLogWithIndex(e pb.Entry) {
	logIndex := e.Index
	if logIndex == 0 {
		log.Warnf("shouldn't append log with index 0")
		return
	}
	// 根据测试, load所有非commit的数据到entries
	l.loadUncommittedEntries()
	// 特判边界情况
	if logIndex == 1 && len(l.entries) == 0 {
		l.entries = append(l.entries, e)
		return
	}
	// 找到分歧点
	index := findEntriesArrayIndexByLogIndex(logIndex, l.entries)
	switch index {
	case int64(cannotFind), int64(mayInStorage):
		// 必须是最新的log
		if l.LastIndex()+1 != logIndex {
			log.Panicf("last index: %d, append entry log index: %d", l.LastIndex(), logIndex)
		}
		l.entries = append(l.entries, e)
	default:
		if entryEqual(l.entries[index], e) {
			return
		}
		l.entries = l.entries[0:index]
		l.entries = append(l.entries, e)
		// 截断stabled, logIndex是新的Index, logIndex-1是原本就存在的Index, 如果这个index小于stabled, 那么stabled只能到这里了
		if logIndex - 1 < l.stabled {
			l.stabled = logIndex - 1
		}
	}
}

func (l *RaftLog) LastTerm() uint64 {
	lastIndex := l.LastIndex()
	logTerm, err := l.Term(lastIndex)
	if err != nil {
		log.Errorf("get index %d error: %v", lastIndex, err)
		return 0
	}
	return logTerm
}

func (l *RaftLog) loadUncommittedEntries() {
	var err error
	if len(l.entries) == 0 {
		if l.committed >= l.stabled {
			return
		}
		l.entries, err = l.storage.Entries(l.committed+1, l.stabled+1)
		if err != nil {
			log.Errorf("storage get entries error: %v, li: %d, hi: %d", err, l.committed+1, l.stabled+1)
			return
		}
	} else {
		if l.committed + 1 >= l.entries[0].Index {
			return
		}
		entries, err := l.storage.Entries(l.committed+1, l.entries[0].Index)
		if err != nil {
			log.Errorf("storage get entries error: %v, li: %d, hi: %d", err, l.committed, l.stabled+1)
			return
		}
		l.entries = append(entries, l.entries...)
	}
}

func (l *RaftLog) StoreUntil(logIndex uint64) {
	arrayIndex := findEntriesArrayIndexByLogIndex(logIndex, l.entries)
	switch s := l.storage.(type) {
	case *MemoryStorage:
		err := s.Append(l.entries[:arrayIndex+1])
		if err != nil {
			log.Panicf("store entries into memory error: %v", err)
		}
	default:
		log.Panicf("not support for store in type: %T", l.storage)
	}
	l.stabled = logIndex
}

func (l *RaftLog) updateCommit(commit uint64) {
	if commit <= l.committed {
		return
	}
	if commit > l.stabled {
		l.StoreUntil(commit)
	}
	l.committed = commit
}

// TODO: ugly! 只在测试使用! 垃圾代码
func (l *RaftLog) loadAllEntriesFromStorage() {
	_ = l.storage.(*MemoryStorage).Append(l.entries)
	l.entries = l.storage.(*MemoryStorage).ents
	if l.entries[0].Term == 0 {
		l.entries = l.entries[1:]
	}
}

type FindResult int64

const (
	mayInStorage FindResult = -1
	cannotFind   FindResult = -2
	indexZero    FindResult = -3
)

func findEntriesArrayIndexByLogIndex(index uint64, entries []pb.Entry) int64 {
	if index == 0 {
		return int64(indexZero)
	}
	if len(entries) == 0 || entries[0].Index > index {
		return int64(mayInStorage)
	}
	for i, e := range entries {
		if e.Index == index {
			return int64(i)
		}
	}
	return int64(cannotFind)
}

func entryEqual(e1, e2 pb.Entry) bool {
	// 优先检查容易的
	if !(len(e1.Data) == len(e2.Data) && e1.Index == e2.Index && e1.Term == e2.Term) {
		return false
	}
	for i := range e1.Data {
		if e1.Data[i] != e2.Data[i] {
			return false
		}
	}
	return true
}
