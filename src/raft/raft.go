package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(Command interface{}) (Index, Term, isleader)
//   start agreement on a new log entry
// rf.GetState() (Term, isLeader)
//   ask a Raft for its current Term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"math/rand"
	"sync"
	"time"
)
import "sync/atomic"
import "../labrpc"

import "bytes"
import "../labgob"

const (
	FOLLOWER  = "FOLLOWER"
	CANDIDATE = "CANDIDATE"
	LEADER    = "LEADER"
)

type LogEntry struct {
	Index   int
	Term    int
	Command interface{}
}

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//

type ApplyMsg struct {
	CommandValid   bool
	Command        interface{}
	CommandIndex   int
	IsReadSnapshot bool
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's Index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	currentTerm int
	voteFor     int
	log         []LogEntry

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int

	status string

	applyCh   chan ApplyMsg
	applyCond *sync.Cond

	lastRpcReceivedTime time.Time

	lastIncludedIndex int
	lastIncludedTerm  int

	isInstallingSnapshot []bool
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).

	rf.mu.Lock()
	term = rf.currentTerm
	isleader = rf.status == LEADER
	rf.mu.Unlock()

	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// state := w.Bytes()
	// rf.persister.SaveRaftState(state)

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.voteFor)
	e.Encode(rf.log)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	state := w.Bytes()
	rf.persister.SaveRaftState(state)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(state []byte) {
	if state == nil || len(state) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(state)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }

	r := bytes.NewBuffer(state)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log []LogEntry
	var lastIncludedIndex int
	var lastIncludedTerm int

	d.Decode(&currentTerm)
	d.Decode(&votedFor)
	d.Decode(&log)
	d.Decode(&lastIncludedIndex)
	d.Decode(&lastIncludedTerm)

	rf.currentTerm = currentTerm
	rf.voteFor = votedFor
	rf.log = log
	rf.lastIncludedIndex = lastIncludedIndex
	rf.lastIncludedTerm = lastIncludedTerm
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term     int
	LeaderId int

	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool

	XTerm  int
	XIndex int
	XLen   int
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Offset            int
	Data              []byte
	Done              bool
}

type InstallSnapshotReply struct {
	Term int
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	// update current term using term in args
	rf.updateTermWithoutLock(args.Term)

	// term not matched, return immediately
	if args.Term != rf.getCurrentTerm() {
		//DPrintf("[%v] return %v", rf.me, args)
		reply.Term = rf.getCurrentTerm()
		return
	}

	rf.mu.Lock()

	// check if current raft instance has voted
	if rf.voteFor == -1 || rf.voteFor == args.CandidateId {
		entry := rf.getLastEntryWithoutLock()
		//for i := 0; i < len(rf.log); i++ {
		//	DPrintf("[%v] %v", rf.me, rf.log[i])
		//}
		//
		//DPrintf("[%v] receive voteReq from candidate %v :{args.LastLogTerm %v, entry.Term %v, args.LastLogIndex %v, entry.Index %v}", rf.me, args.CandidateId, args.LastLogTerm, entry.Term, args.LastLogIndex, entry.Index)
		// check if current log entry is up to date
		if args.LastLogTerm > entry.Term || (args.LastLogTerm == entry.Term && args.LastLogIndex >= entry.Index) {
			rf.lastRpcReceivedTime = time.Now()
			reply.VoteGranted = true
			rf.voteFor = args.CandidateId
			rf.persist()
		} else {
			reply.VoteGranted = false
		}
	} else {
		reply.VoteGranted = false
	}

	rf.mu.Unlock()
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// update current term using term in args
	rf.updateTermWithoutLock(args.Term)

	//if len(args.Entries) != 0 {
	//	DPrintf("[%v] args %v", rf.me, args)
	//}

	var updated bool

	// term not matched, return immediately
	if args.Term != rf.getCurrentTerm() {
		//DPrintf("[%v] return %v", rf.me, args)
		reply.Term = rf.getCurrentTerm()
		return
	}

	rf.mu.Lock()
	rf.lastRpcReceivedTime = time.Now()
	reply.Term = rf.currentTerm

	if rf.status == CANDIDATE {
		rf.mu.Unlock()
		rf.becomesFollower(rf.getCurrentTerm())
		rf.mu.Lock()
	}

	entry, ok := rf.getEntryByIndexWithoutLock(args.PrevLogIndex)

	// check if Follower has required log entry
	if args.PrevLogIndex == 0 || (ok && entry.Term == args.PrevLogTerm) {
		reply.Success = true
		isConflicted := false
		conflictIndex := 0

		// traverse log entries in args to get ready to add to current raft instance's log
		for i := 0; i < len(args.Entries); i++ {
			entry := args.Entries[i]
			rfEntry, ok := rf.getEntryByIndexWithoutLock(entry.Index)
			if ok {
				if rfEntry.Term != entry.Term {
					isConflicted = true
					conflictIndex = rfEntry.Index - rf.log[0].Index
					break
				}
			} else {
				// log entry from args not found in current raft instance's log, add this log entry immediately
				rf.log = append(rf.log, entry)
			}
		}

		// overwriting conflicted log entries
		if isConflicted {
			rf.log = rf.log[:conflictIndex]
			beginIndex := rf.getLastEntryWithoutLock().Index + 1
			for i := 0; i < len(args.Entries); i++ {
				entry := args.Entries[i]
				if entry.Index >= beginIndex {
					rf.log = append(rf.log, entry)
				}
			}
		}

		rf.persist()

		//if len(args.Entries) != 0 {
		//	DPrintf("[%v] receiving args %v", rf.me, args)
		//}

		// updates current commit index
		if args.LeaderCommit > rf.commitIndex {
			updated = true
			if rf.getLastEntryWithoutLock().Index < args.LeaderCommit {
				rf.commitIndex = rf.getLastEntryWithoutLock().Index
			} else {
				rf.commitIndex = args.LeaderCommit
			}
		}

	} else {
		// try to rematch with Leader
		reply.Success = false
		if !ok {
			reply.XLen = len(rf.log)
			// for snapshot
			reply.XLen += rf.lastIncludedIndex
		} else {
			reply.XTerm = entry.Term
			reply.XIndex = rf.get1stEntryByTermWithoutLock(reply.XTerm).Index
		}
	}

	//if len(args.Entries) != 0 {
	//	DPrintf("[%v] rf.commitIndex: %v, rf.log: ", rf.me, rf.commitIndex)
	//	for i := 0; i < len(rf.log); i++ {
	//		DPrintf("%v", rf.log[i])
	//	}
	//}

	// awakes apply daemon routine
	if updated {
		rf.applyCond.Signal()
	}

	rf.mu.Unlock()
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.updateTermWithoutLock(args.Term)

	if args.Term != rf.getCurrentTerm() {
		//DPrintf("[%v] return %v", rf.me, args)
		reply.Term = rf.getCurrentTerm()
		return
	}

	rf.mu.Lock()
	rf.lastRpcReceivedTime = time.Now()

	_, ok := rf.getEntryByIndexWithoutLock(args.LastIncludedIndex)

	if ok {
		// overwriting old log
		rf.deleteLogsBeforeIndexWithoutLock(args.LastIncludedIndex)
		rf.saveStateAndSnapshotWithoutLock(args)
		rf.mu.Unlock()
		return
	}

	rf.log = []LogEntry{}
	rf.log = append(rf.log, LogEntry{
		Index:   args.LastIncludedIndex,
		Term:    args.LastIncludedTerm,
		Command: nil,
	})
	rf.saveStateAndSnapshotWithoutLock(args)

	rf.commitIndex = args.LastIncludedIndex
	rf.applyCond.Signal()

	rf.mu.Unlock()
}

//
// example code to send a RequestVote RPC to a server.
// server is the Index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next Command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// Command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the Index that the Command will appear at
// if it's ever committed. the second return value is the current
// Term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	rf.mu.Lock()

	// only leader can apply me
	if rf.status == LEADER {
		isLeader = true

		entry := &LogEntry{}

		// create a new log entry then append to log
		entry.Index = rf.getLastEntryWithoutLock().Index + 1

		entry.Term = rf.currentTerm
		entry.Command = command

		index = entry.Index
		term = entry.Term

		rf.log = append(rf.log, *entry)
		rf.persist()

		//DPrintf("START rf.log: ")
		//for i := 0; i < len(rf.log); i++ {
		//	DPrintf("%v", rf.log[i])
		//}

		//DPrintf("[%v] START %v, index %v", rf.me, command, index)

		// send appendEntries RPC to all peers
		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}

			go rf.appendEntries(i, false)
		}
	} else {
		isLeader = false
	}

	rf.mu.Unlock()
	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) electionTimer() {
	for rf.getStatus() != LEADER {
		if rf.killed() {
			return
		}

		interval := randNum(300, 600)
		time.Sleep(time.Millisecond * time.Duration(interval))

		if rf.getStatus() == FOLLOWER {
			if time.Since(rf.getLastRpcReceivedTime()) < time.Millisecond*time.Duration(interval) {
				continue
			} else {
				rf.handleElectionTimeout()
			}
		} else if rf.getStatus() == CANDIDATE {
			rf.handleElectionTimeout()
		}

		return
	}
}

func (rf *Raft) buildRequestVoteArgs() *RequestVoteArgs {
	args := RequestVoteArgs{}

	rf.mu.Lock()
	args.Term = rf.currentTerm
	args.CandidateId = rf.me

	entry := rf.getLastEntryWithoutLock()
	args.LastLogIndex = entry.Index
	args.LastLogTerm = entry.Term
	//DPrintf("[%v] build args %v", rf.me, args)
	rf.mu.Unlock()

	return &args
}

func (rf *Raft) heartBeatTimer() {
	for rf.getStatus() == LEADER {
		if rf.killed() {
			return
		}
		time.Sleep(time.Millisecond * 150)

		rf.handleHeartBeatTimeout()
	}
}

func (rf *Raft) appendEntries(server int, isHeartBeat bool) {
	var entries []LogEntry

	if rf.getStatus() != LEADER {
		return
	}

	rf.mu.Lock()

	if rf.isInstallingSnapshot[server] {
		rf.mu.Unlock()
		return
	}

	for i := rf.nextIndex[server]; i <= rf.getLastEntryWithoutLock().Index; i++ {
		entry, ok := rf.getEntryByIndexWithoutLock(i)

		if !ok {
			entries = []LogEntry{}
			//DPrintf("[%v] can't find index %v, rf.log %v", rf.me, i, rf.log)
			break
		}

		entries = append(entries, *entry)
	}

	if len(entries) == 0 && !isHeartBeat {
		rf.mu.Unlock()
		return
	}

	//DPrintf("[%v] entries", rf.me, entries)
	args := rf.buildAppendEntriesArgsWithoutLock(server, entries)
	reply := &AppendEntriesReply{}

	rf.mu.Unlock()

	ok := rf.sendAppendEntries(server, args, reply)

	var retry bool

	if ok {
		retry = rf.handleAppendEntriesResponse(args, reply, server)
	}

	if retry {
		go rf.appendEntries(server, false)
	}
}

func (rf *Raft) buildAppendEntriesArgsWithoutLock(server int, entries []LogEntry) *AppendEntriesArgs {
	args := &AppendEntriesArgs{}

	args.Term = rf.currentTerm
	args.LeaderId = rf.me

	args.PrevLogIndex = rf.nextIndex[server] - 1
	entry, _ := rf.getEntryByIndexWithoutLock(args.PrevLogIndex)
	args.PrevLogTerm = entry.Term

	args.Entries = entries
	args.LeaderCommit = rf.commitIndex

	return args
}

func (rf *Raft) updateCommitIndexWithoutLock(args *AppendEntriesArgs) bool {
	var updated bool
	var commitIndex int

	//if len(args.Entries) != 0 {
	//	DPrintf("UPDATE_COMMIT, from index %v to %v", rf.commitIndex+1, rf.getLastEntryWithoutLock().Index)
	//}
	for index := rf.commitIndex + 1; index <= rf.getLastEntryWithoutLock().Index; index++ {
		grantedNum := 1

		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}

			if rf.matchIndex[i] >= index {
				grantedNum++
			}
		}

		entry, ok := rf.getEntryByIndexWithoutLock(index)
		if grantedNum > len(rf.peers)/2 && ok && entry.Term == rf.currentTerm {
			commitIndex = index
		}
	}

	if commitIndex > rf.commitIndex {
		rf.commitIndex = commitIndex
		//DPrintf("[%v] updates commitIndex %v", rf.me, rf.commitIndex)
		updated = true
	}

	return updated
}

func (rf *Raft) updateTermWithoutLock(term int) {
	if term > rf.getCurrentTerm() {
		rf.becomesFollower(term)
	}
}

func (rf *Raft) apply() {
	for {
		if rf.killed() {
			break
		}

		rf.mu.Lock()

		// program is blocked when commit index is less or equal than last applied index
		for rf.commitIndex <= rf.lastApplied {
			rf.applyCond.Wait()
			//DPrintf("[%v] wakeup commitIndex %v, lastApplied %v", rf.me, rf.commitIndex, rf.lastApplied)
		}

		// tell the layer(kv layer) above to read snapshot
		if rf.lastApplied < rf.lastIncludedIndex {
			applyMsg := ApplyMsg{}
			applyMsg.CommandValid = true
			applyMsg.Command = nil
			applyMsg.CommandIndex = 0
			applyMsg.IsReadSnapshot = true

			rf.mu.Unlock()

			rf.applyCh <- applyMsg

			rf.mu.Lock()
			rf.lastApplied = rf.lastIncludedIndex
			rf.mu.Unlock()
			continue
		}

		// apply log entries from last applied index plus one to commit index
		for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
			entry, ok := rf.getEntryByIndexWithoutLock(i)

			if !ok || entry.Index == rf.lastIncludedIndex {
				continue
			}

			applyMsg := ApplyMsg{}
			applyMsg.CommandValid = true
			applyMsg.Command = entry.Command
			applyMsg.CommandIndex = i
			applyMsg.IsReadSnapshot = false

			rf.mu.Unlock()
			// apply msg through apply channel
			rf.applyCh <- applyMsg
			//DPrintf("[%v] APPLY %v, lastApplied %v", rf.me, applyMsg, i)

			//for i := 0; i < len(rf.log); i++ {
			//	DPrintf("[%v] %v", rf.me, rf.log[i])
			//}

			rf.mu.Lock()
			rf.lastApplied = i
		}

		rf.mu.Unlock()
	}
}

func (rf *Raft) ReadSnapshot() ([]byte, int) {
	snapshot := rf.persister.ReadSnapshot()
	//DPrintf("[%v] READ_SNAPSHOT %v", rf.me, 0)
	rf.mu.Lock()
	index := rf.lastIncludedIndex
	rf.mu.Unlock()

	return snapshot, index
}

func (rf *Raft) SaveStateAndSnapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	rf.deleteLogsBeforeIndexWithoutLock(index)

	entry, _ := rf.getEntryByIndexWithoutLock(index)

	rf.lastIncludedIndex = index
	rf.lastIncludedTerm = entry.Term

	e.Encode(rf.currentTerm)
	e.Encode(rf.voteFor)
	e.Encode(rf.log)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)

	state := w.Bytes()

	rf.mu.Unlock()
	rf.persister.SaveStateAndSnapshot(state, snapshot)

	//DPrintf("[%v] SAVE_SNAPSHOT ,index %v, rf.log %v, snapshot %v", rf.me, index, 0, 0)
}

func (rf *Raft) deleteLogsBeforeIndexWithoutLock(index int) {
	i := len(rf.log) - 1
	for ; i >= 0; i-- {
		if rf.log[i].Index == index {
			break
		}
	}

	if i == len(rf.log)-1 {
		entry := rf.log[i]
		rf.log = []LogEntry{}
		rf.log = append(rf.log, entry)
	} else if i >= 0 {
		rf.log = rf.log[i:]
	}
}

func (rf *Raft) installSnapshot(server int) {
	rf.mu.Lock()

	if rf.isInstallingSnapshot[server] {
		rf.mu.Unlock()
		return
	}
	rf.isInstallingSnapshot[server] = true

	snapshot, _ := rf.ReadSnapshotWithoutLock()

	args := InstallSnapshotArgs{}
	args.Term = rf.currentTerm
	args.LeaderId = rf.me
	args.LastIncludedIndex = rf.lastIncludedIndex
	args.LastIncludedTerm = rf.lastIncludedTerm
	args.Offset = 0
	args.Data = snapshot
	args.Done = true

	reply := InstallSnapshotReply{}
	rf.mu.Unlock()

	ok := rf.sendInstallSnapshot(server, &args, &reply)
	if ok {
		rf.handleInstallSnapshotResponse(&args, &reply, server)
	} else {
		rf.mu.Lock()
		rf.isInstallingSnapshot[server] = false
		rf.mu.Unlock()
	}
}

func (rf *Raft) saveStateAndSnapshotWithoutLock(args *InstallSnapshotArgs) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm

	e.Encode(rf.currentTerm)
	e.Encode(rf.voteFor)
	e.Encode(rf.log)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)

	state := w.Bytes()

	rf.persister.SaveStateAndSnapshot(state, args.Data)
	//DPrintf("[%v] SAVE_SNAPSHOT_WITHOUT_LOCK %v", rf.me, 0)
}

func (rf *Raft) ReadSnapshotWithoutLock() ([]byte, int) {
	snapshot := rf.persister.ReadSnapshot()
	index := rf.lastIncludedIndex

	return snapshot, index
}

func randNum(low int, high int) int {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	diff := high - low
	return low + r.Intn(diff)
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.voteFor = -1

	rf.commitIndex = 0
	rf.lastApplied = 0

	for i := 0; i < len(peers); i++ {
		rf.nextIndex = append(rf.nextIndex, 1)
		rf.matchIndex = append(rf.matchIndex, 0)
		rf.isInstallingSnapshot = append(rf.isInstallingSnapshot, false)
	}

	rf.status = FOLLOWER

	rf.applyCh = applyCh
	rf.applyCond = sync.NewCond(&rf.mu)

	rf.lastRpcReceivedTime = time.Now()

	rf.lastIncludedIndex = 0
	rf.lastIncludedTerm = 0

	// starts election timer
	go rf.electionTimer()
	// starts apply daemon routine
	go rf.apply()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
