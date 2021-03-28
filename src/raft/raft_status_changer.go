package raft

import "time"

func (rf *Raft) becomesCandidate() {
	rf.mu.Lock()
	rf.status = CANDIDATE
	rf.currentTerm++
	rf.voteFor = rf.me
	rf.persist()
	rf.lastRpcReceivedTime = time.Now()
	//DPrintf("[%v] becomes %v at Term %v", rf.me, rf.status, rf.currentTerm)
	rf.mu.Unlock()
}

func (rf *Raft) becomesLeader() {
	rf.mu.Lock()
	rf.status = LEADER
	DPrintf("[%v] becomes %v at Term %v", rf.me, rf.status, rf.currentTerm)

	lastLogIndex := rf.getLastEntryWithoutLock().Index
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = lastLogIndex + 1
		rf.matchIndex[i] = 0
	}

	rf.mu.Unlock()

	go rf.heartBeatTimer()
}

func (rf *Raft) becomesFollower(term int) {
	rf.mu.Lock()
	if term > rf.currentTerm {
		rf.currentTerm = term
		rf.voteFor = -1
		rf.persist()
	}

	if rf.status == LEADER {
		rf.status = FOLLOWER
		go rf.electionTimer()
	} else if rf.status == CANDIDATE {
		rf.status = FOLLOWER
	}
	//DPrintf("[%v] becomes %v at Term %v", rf.me, rf.status, rf.currentTerm)
	rf.mu.Unlock()
}
