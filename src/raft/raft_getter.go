package raft

import "time"

func (rf *Raft) getCurrentTerm() int {
	var currentTerm int
	rf.mu.Lock()
	currentTerm = rf.currentTerm
	rf.mu.Unlock()
	return currentTerm
}

func (rf *Raft) getStatus() string {
	var status string
	rf.mu.Lock()
	status = rf.status
	rf.mu.Unlock()
	return status
}

func (rf *Raft) getLastRpcReceivedTime() time.Time {
	var lastRpcReceivedTime time.Time
	rf.mu.Lock()
	lastRpcReceivedTime = rf.lastRpcReceivedTime
	rf.mu.Unlock()
	return lastRpcReceivedTime
}

func (rf *Raft) getLastEntryWithoutLock() *LogEntry {
	var entry LogEntry

	if len(rf.log) == 0 {
		entry = LogEntry{}
	} else {
		entry = rf.log[len(rf.log)-1]
	}

	return &entry
}

func (rf *Raft) getEntryByIndexWithoutLock(index int) (*LogEntry, bool) {
	if index == 0 {
		return &LogEntry{}, true
	} else if len(rf.log) == 0 {
		return &LogEntry{}, false
	} else if index < rf.log[0].Index || index > rf.log[len(rf.log)-1].Index {
		return &LogEntry{}, false
	} else {
		index -= rf.log[0].Index
		if index >= len(rf.log) {
			DPrintf("[%v] log %v, index %v, rf.log[0] %v", rf.me, rf.log, index, rf.log[0].Index)
		}
		return &rf.log[index], true
	}
}

func (rf *Raft) get1stEntryByTermWithoutLock(term int) *LogEntry {
	var entry LogEntry
	var ok bool

	for i := len(rf.log) - 1; i >= 0; i-- {
		if rf.log[i].Term == term {
			ok = true
		}

		if rf.log[i].Term != term && ok {
			entry = rf.log[i+1]
			break
		}
	}

	return &entry
}

func (rf *Raft) getLastEntryByTermWithoutLock(term int) (*LogEntry, bool) {
	var entry LogEntry
	var ok bool

	for i := len(rf.log) - 1; i >= 0; i-- {
		if rf.log[i].Term == term {
			entry = rf.log[i]
			ok = true
			break
		}

		if rf.log[i].Term < term {
			ok = false
			break
		}
	}

	return &entry, ok
}

func (rf *Raft) GetRaftStateSize() int {
	return rf.persister.RaftStateSize()
}
