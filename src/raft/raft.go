package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"math/rand"
	"sort"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"bytes"
	"sync"
	"sync/atomic"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	status      int
	currentTerm int
	votedFor    int
	log         []LogEntry
	applyCh     chan<- ApplyMsg

	snapshot                  []byte
	snapshotLastIncludedIndex int
	snapshotLastIncludedTerm  int

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int

	termChanged           EventDispatcher
	cancelElectionTimeout EventDispatcher
	cancelCurrentElection EventDispatcher
	needLogReplication    []EventDispatcher
	needApply             EventDispatcher
}

type LogEntry struct {
	Term int
	Cmd  interface{}
}

const (
	FOLLOWER  = 0
	LEADER    = 1
	CANDIDATE = 2
)

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = rf.status == LEADER

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
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.snapshotLastIncludedIndex)
	e.Encode(rf.snapshotLastIncludedTerm)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log []LogEntry
	var lastIncludeLogIndex int
	var lastIncludeLogTerm int
	if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&log) != nil ||
		d.Decode(&lastIncludeLogIndex) != nil || d.Decode(&lastIncludeLogTerm) != nil {
		panic("decode error")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
		rf.snapshotLastIncludedIndex = lastIncludeLogIndex
		rf.snapshotLastIncludedTerm = lastIncludeLogTerm
	}
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// DPrintf("[%d] cond install snapshot, lastTerm=%d lastIndex=%d myLastTerm=%d myLastIndex=%d\n", rf.me, lastIncludedTerm, lastIncludedIndex, rf.snapshotLastIncludedTerm, rf.snapshotLastIncludedIndex)
	if rf.lastApplied >= lastIncludedIndex {
		// DPrintf("[%d] snapshot too old\n", rf.me)
		return false
	}
	if lastIncludedIndex <= rf.snapshotLastIncludedIndex+len(rf.log) {
		rf.log = rf.log[lastIncludedIndex-rf.snapshotLastIncludedIndex:]
	} else {
		rf.log = nil
	}
	rf.snapshotLastIncludedIndex = lastIncludedIndex
	rf.snapshotLastIncludedTerm = lastIncludedTerm
	rf.snapshot = snapshot
	rf.lastApplied = lastIncludedIndex

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.snapshotLastIncludedIndex)
	e.Encode(rf.snapshotLastIncludedTerm)
	data := w.Bytes()
	rf.persister.SaveStateAndSnapshot(data, snapshot)

	rf.needApply.Notify()

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if index <= rf.snapshotLastIncludedIndex {
		// DPrintf("[%d] snapshot too old (index=%d lastIncludedIndex=%d), skip\n", rf.me, index, rf.snapshotLastIncludedIndex)
		return
	}
	rf.snapshotLastIncludedTerm = rf.log[index-rf.snapshotLastIncludedIndex-1].Term
	rf.log = rf.log[index-rf.snapshotLastIncludedIndex:]
	rf.snapshotLastIncludedIndex = index
	rf.snapshot = snapshot
	rf.lastApplied = rf.snapshotLastIncludedIndex

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.snapshotLastIncludedIndex)
	e.Encode(rf.snapshotLastIncludedTerm)
	data := w.Bytes()
	rf.persister.SaveStateAndSnapshot(data, snapshot)
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

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// DPrintf("[%d] received request vote from [%d]\n", rf.me, args.CandidateId)
	if args.Term > rf.currentTerm {
		rf.newTermDetected(args.Term)
	}
	reply.Term = rf.currentTerm

	var lastLogTerm int
	if len(rf.log) == 0 {
		lastLogTerm = rf.snapshotLastIncludedTerm
	} else {
		lastLogTerm = rf.log[len(rf.log)-1].Term
	}

	reply.VoteGranted = false
	if args.Term >= rf.currentTerm && (rf.votedFor == -1 || rf.votedFor == args.CandidateId) {
		if args.LastLogTerm > lastLogTerm {
			reply.VoteGranted = true
		} else if args.LastLogTerm == lastLogTerm && args.LastLogIndex >= rf.snapshotLastIncludedIndex+len(rf.log) {
			reply.VoteGranted = true
		}
	}

	if reply.VoteGranted {
		// DPrintf("[%d] grant the vote to %d\n", rf.me, args.CandidateId)
		rf.votedFor = args.CandidateId
		rf.persist()
		rf.cancelElectionTimeout.Notify()
	} else {
		// DPrintf("[%d] NOT grant the vote to %d\n", rf.me, args.CandidateId)
		// DPrintf("[%d] my logs: %v\n", rf.me, rf.log)
		// DPrintf("[%d] my snapshots: (%d, %d)\n", rf.me, rf.snapshotLastIncludedIndex, rf.snapshotLastIncludedTerm)
	}
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
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
	if !ok {
		return false
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term != rf.currentTerm {
		return true
	}
	if reply.Term > rf.currentTerm {
		rf.newTermDetected(reply.Term)
	}
	return true
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	// DPrintf("[%d] received append entries from %d\n", rf.me, args.LeaderId)
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	} else if args.Term > rf.currentTerm {
		rf.newTermDetected(args.Term)
	} else if args.Term == rf.currentTerm && rf.status == CANDIDATE {
		rf.currentLeaderDetected()
	}

	reply.Term = rf.currentTerm
	rf.cancelElectionTimeout.Emit(nil)
	// DPrintf("[%d] received log: prevIndex=%d, prevTerm=%d, entries=%v\n", rf.me, args.PrevLogIndex, args.PrevLogTerm, args.Entries)
	// DPrintf("[%d] my logs: %v\n", rf.me, rf.log)
	// DPrintf("[%d] my snapshots: (%d, %d)\n", rf.me, rf.snapshotLastIncludedIndex, rf.snapshotLastIncludedTerm)
	reply.Success = false
	if rf.snapshotLastIncludedIndex > args.PrevLogIndex {
		reply.Success = false
		// DPrintf("[%d] append entries conflict with snapshot\n", rf.me)
	} else if rf.snapshotLastIncludedIndex == args.PrevLogIndex {
		reply.Success = rf.snapshotLastIncludedTerm == args.PrevLogTerm
	} else if rf.snapshotLastIncludedIndex+len(rf.log) >= args.PrevLogIndex {
		reply.Success = rf.log[args.PrevLogIndex-rf.snapshotLastIncludedIndex-1].Term == args.PrevLogTerm
	}

	if reply.Success {
		// DPrintf("[%d] append entries success\n", rf.me)
		// DPrintf("[%d] previous log: %v\n", rf.me, rf.log)
		preserveLogs := false
		if args.PrevLogIndex-rf.snapshotLastIncludedIndex+len(args.Entries) <= len(rf.log) {
			preserveLogs = true
			for i, entry := range args.Entries {
				if rf.log[args.PrevLogIndex-rf.snapshotLastIncludedIndex+i].Term != entry.Term {
					preserveLogs = false
					break
				}
			}
		}
		if !preserveLogs {
			rf.log = rf.log[:args.PrevLogIndex-rf.snapshotLastIncludedIndex]
			rf.log = append(rf.log, args.Entries...)
		}
		// DPrintf("[%d] current log: %v\n", rf.me, rf.log)
		rf.persist()

		if args.LeaderCommit > rf.commitIndex {
			rf.commitIndex = args.LeaderCommit
			if rf.commitIndex > rf.snapshotLastIncludedIndex+len(rf.log) {
				rf.commitIndex = rf.snapshotLastIncludedIndex + len(rf.log)
			}
			rf.needApply.Notify()
		}
	} else {
		// DPrintf("[%d] append entries failed\n", rf.me)
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if !ok {
		return false
	}
	you := server
	startIndex := args.PrevLogIndex + 1
	endIndex := startIndex + len(args.Entries)

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term != rf.currentTerm {
		return true
	}

	if reply.Term > rf.currentTerm {
		rf.newTermDetected(reply.Term)
	} else if reply.Success {
		rf.nextIndex[you] = endIndex
		rf.matchIndex[you] = endIndex - 1
		if len(args.Entries) > 0 {
			// DPrintf("[%d] log appending success for %d, set nextIndex=%d\n", rf.me, you, endIndex)
			rf.needLogReplication[you].Notify()
			rf.checkCommits()
		}
	} else {

		if startIndex > rf.nextIndex[you] {
			startIndex = rf.nextIndex[you]
		}

		rf.nextIndex[you] = startIndex - (endIndex - startIndex)
		if rf.nextIndex[you] >= startIndex {
			rf.nextIndex[you] = startIndex - 1
		}
		if rf.nextIndex[you] <= rf.matchIndex[you] {
			rf.nextIndex[you] = rf.matchIndex[you] + 1
		}

		// DPrintf("[%d] log appending failed for %d, set nextIndex=%d\n", rf.me, you, rf.nextIndex[you])
		// DPrintf("[%d] startIndex=%d endIndex=%d\n", rf.me, startIndex, endIndex)
		rf.needLogReplication[you].Notify()
	}
	return true
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	// DPrintf("[%d] received install snapshot\n", rf.me)
	reply.Term = rf.currentTerm
	rf.mu.Unlock()
	if args.Term < reply.Term {
		return
	}
	msg := ApplyMsg{
		CommandValid:  false,
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  args.LastIncludedTerm,
		SnapshotIndex: args.LastIncludedIndex,
	}
	rf.applyCh <- msg
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if !ok {
		return false
	}
	if reply.Term > rf.currentTerm {
		rf.newTermDetected(reply.Term)
	} else if args.Term == rf.currentTerm {
		// DPrintf("[%d] snapshot installation success for %d, set nextIndex=%d\n", rf.me, server, args.LastIncludedIndex + 1)
		rf.nextIndex[server] = args.LastIncludedIndex + 1
		rf.matchIndex[server] = args.LastIncludedIndex
		rf.needLogReplication[server].Notify()
		rf.checkCommits()
	}
	return true
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.status == LEADER {
		index = rf.snapshotLastIncludedIndex + len(rf.log) + 1
		term = rf.currentTerm
		// DPrintf("[%d] new command %v come in\n", rf.me, command)
		newLog := LogEntry{
			Term: rf.currentTerm,
			Cmd:  command,
		}
		// DPrintf("[%d] previous logs: %v\n", rf.me, rf.log)
		rf.log = append(rf.log, newLog)
		// DPrintf("[%d] current logs: %v\n", rf.me, rf.log)
		rf.persist()
		for i := range rf.peers {
			rf.needLogReplication[i].Notify()
		}
	} else {
		isLeader = false
	}

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
	rf.cancelCurrentElection.Notify()
	rf.cancelElectionTimeout.Notify()
	rf.termChanged.Notify()
	for i := range rf.peers {
		rf.needLogReplication[i].Notify()
	}
	rf.needApply.Notify()
	// close(rf.applyCh)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

const (
	HeartbeatTimeout    = 200
	ElectionTimeoutLow  = 500
	ElectionTimeoutHigh = 1000
)

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) electionTicker() {
	for !rf.killed() {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		rf.mu.Lock()

		for rf.status == LEADER {
			rf.mu.Unlock()
			<-rf.termChanged.Listen()
			if rf.killed() {
				return
			}
			rf.mu.Lock()
		}

		// me := rf.me
		electionTimeout := time.Duration(ElectionTimeoutLow+rand.Intn(ElectionTimeoutHigh-ElectionTimeoutLow)) * time.Millisecond
		// DPrintf("[%d] prepared to raise new election after %v", me, electionTimeout)
		rf.mu.Unlock()

		select {
		case <-rf.cancelElectionTimeout.Listen():
			// DPrintf("[%d] skip this election timeout", me)
		case <-time.After(electionTimeout):
			rf.newElection()
		}

	}
}

func (rf *Raft) newElection() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.status == LEADER {
		// panic("leader should not make election")
		// DPrintf("[%d] already leader but new election\n", rf.me)
	}

	rf.cancelCurrentElection.Emit(nil)
	rf.status = CANDIDATE
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.persist()
	// DPrintf("[%d] new election start for term %d\n", rf.me, rf.currentTerm)
	// DPrintf("[%d] my logs: %v\n", rf.me, rf.log)
	// DPrintf("[%d] my snapshots: (%d, %d)\n", rf.me, rf.snapshotLastIncludedIndex, rf.snapshotLastIncludedTerm)

	var voteCnt uint32
	voteCnt = 0
	peerNum := len(rf.peers)
	thisTerm := rf.currentTerm
	// me := rf.me
	var dispatcher EventDispatcher

	for i := range rf.peers {
		if i != rf.me {
			var lastLogTerm int
			if len(rf.log) == 0 {
				lastLogTerm = rf.snapshotLastIncludedTerm
			} else {
				lastLogTerm = rf.log[len(rf.log)-1].Term
			}

			args := RequestVoteArgs{
				Term:         rf.currentTerm,
				CandidateId:  rf.me,
				LastLogIndex: rf.snapshotLastIncludedIndex + len(rf.log),
				LastLogTerm:  lastLogTerm,
			}
			reply := RequestVoteReply{}
			you := i

			go func() {
				// DPrintf("[%d] send request vote into %d\n", me, you)
				ok := rf.sendRequestVote(you, &args, &reply)
				if ok && reply.VoteGranted {
					// DPrintf("[%d] receive granted vote from %d\n", me, you)
					if atomic.AddUint32(&voteCnt, 1) >= uint32(peerNum/2) {
						dispatcher.Notify()
					}
				}
			}()
		}
	}

	go func() {
		select {
		case <-dispatcher.Listen():
			rf.mu.Lock()
			if rf.currentTerm == thisTerm {
				rf.startLeader()
			}
			rf.mu.Unlock()
		case <-rf.cancelCurrentElection.Listen():
			// DPrintf("[%d] election failed\n", me)
		}
	}()
}

func (rf *Raft) startLeader() {
	// DPrintf("[%d] majority voted, become a leader\n", rf.me)
	rf.status = LEADER
	rf.nextIndex = nil
	rf.matchIndex = nil
	for range rf.peers {
		rf.nextIndex = append(rf.nextIndex, rf.snapshotLastIncludedIndex+len(rf.log)+1)
		rf.matchIndex = append(rf.matchIndex, 0)
	}
	rf.cancelElectionTimeout.Emit(nil)
	rf.heartbeat()
	go rf.heartbeatTicker(rf.currentTerm)
	for i := range rf.peers {
		if i != rf.me {
			go rf.logReplicationRoutine(i, rf.currentTerm, &rf.needLogReplication[i])
		}
	}
}

func (rf *Raft) heartbeatTicker(thisTerm int) {
	for !rf.killed() {
		rf.mu.Lock()
		heartbeatTimeout := HeartbeatTimeout * time.Millisecond
		// DPrintf("[%d] prepared to send heartbeats after %v", rf.me, heartbeatTimeout)
		rf.mu.Unlock()

		select {
		case <-rf.termChanged.Listen():
			return
		case <-time.After(heartbeatTimeout):
			rf.mu.Lock()
			if rf.currentTerm != thisTerm {
				rf.mu.Unlock()
				return
			}
			rf.heartbeat()
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) heartbeat() {
	for i := range rf.peers {
		if i != rf.me {
			var lastLogTerm int
			if len(rf.log) == 0 {
				lastLogTerm = rf.snapshotLastIncludedTerm
			} else {
				lastLogTerm = rf.log[len(rf.log)-1].Term
			}

			args := AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: rf.snapshotLastIncludedIndex + len(rf.log),
				PrevLogTerm:  lastLogTerm,
				Entries:      nil,
				LeaderCommit: rf.commitIndex,
			}
			reply := AppendEntriesReply{}
			// me := rf.me
			you := i
			go func() {
				// DPrintf("[%d] send heartbeats to %d\n", me, you)
				rf.sendAppendEntries(you, &args, &reply)
			}()
		}
	}
}

func (rf *Raft) logReplicationRoutine(you int, thisTerm int, needLogReplication *EventDispatcher) {
	for !rf.killed() {
		select {
		case <-rf.termChanged.Listen():
			return
		case <-needLogReplication.Listen():
		}
		rf.mu.Lock()
		if rf.killed() || rf.currentTerm != thisTerm {
			rf.mu.Unlock()
			return
		}
		if rf.snapshotLastIncludedIndex+len(rf.log) < rf.nextIndex[you] {
			rf.mu.Unlock()
			continue
		}

		// DPrintf("[%d] log non-matching detected, will append logs to %d (nextIndex=%d)\n", rf.me, you, rf.nextIndex[you])
		// DPrintf("[%d] my logs: %v\n", rf.me, rf.log)
		// DPrintf("[%d] my snapshots: (%d, %d)\n", rf.me, rf.snapshotLastIncludedIndex, rf.snapshotLastIncludedTerm)

		if rf.nextIndex[you] <= rf.snapshotLastIncludedIndex {
			// DPrintf("[%d] conflict with snapshot, shall install snapshot\n", rf.me)
			args := InstallSnapshotArgs{
				Term:              rf.currentTerm,
				LeaderId:          rf.me,
				LastIncludedIndex: rf.snapshotLastIncludedIndex,
				LastIncludedTerm:  rf.snapshotLastIncludedTerm,
				Data:              rf.snapshot,
			}
			reply := InstallSnapshotReply{}
			rf.mu.Unlock()
			go rf.sendInstallSnapshot(you, &args, &reply)
		} else {
			var prevLogTerm int
			if rf.nextIndex[you] == rf.snapshotLastIncludedIndex+1 {
				prevLogTerm = rf.snapshotLastIncludedTerm
			} else {
				prevLogTerm = rf.log[rf.nextIndex[you]-rf.snapshotLastIncludedIndex-2].Term
			}

			args := AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: rf.nextIndex[you] - 1,
				PrevLogTerm:  prevLogTerm,
				Entries:      rf.log[rf.nextIndex[you]-rf.snapshotLastIncludedIndex-1:],
				LeaderCommit: rf.commitIndex,
			}
			reply := AppendEntriesReply{}
			rf.mu.Unlock()
			go rf.sendAppendEntries(you, &args, &reply)
		}
	}
}

func (rf *Raft) newTermDetected(term int) {
	rf.currentTerm = term
	rf.status = FOLLOWER
	rf.votedFor = -1
	rf.nextIndex = nil
	rf.matchIndex = nil
	rf.persist()
	// DPrintf("[%d] new term %d detected, turn into a follower\n", rf.me, term)
	rf.termChanged.Notify()
	rf.cancelCurrentElection.Notify()
}

func (rf *Raft) currentLeaderDetected() {
	rf.status = FOLLOWER
	// DPrintf("[%d] leader in current term detected, stop the election", rf.me)
	rf.cancelCurrentElection.Notify()
}

func (rf *Raft) checkCommits() {
	// DPrintf("[%d] check commits %v commitIndex=%d\n", rf.me, rf.matchIndex, rf.commitIndex)
	var matchIndexes []int
	for i := range rf.peers {
		if i != rf.me {
			matchIndexes = append(matchIndexes, rf.matchIndex[i])
		}
	}
	sort.Slice(matchIndexes, func(i, j int) bool { return matchIndexes[i] > matchIndexes[j] })
	nextCommitIndex := matchIndexes[len(rf.peers)/2-1]
	if nextCommitIndex <= rf.commitIndex {
		return
	}

	var nextCommitTerm int
	if nextCommitIndex == rf.snapshotLastIncludedIndex {
		nextCommitTerm = rf.snapshotLastIncludedTerm
	} else if nextCommitIndex > rf.snapshotLastIncludedIndex {
		nextCommitTerm = rf.log[nextCommitIndex-rf.snapshotLastIncludedIndex-1].Term
	} else {
		panic("next commit must be outside snapshot")
	}

	if nextCommitTerm == rf.currentTerm {
		rf.commitIndex = nextCommitIndex
		// DPrintf("[%d] now commitIndex=%d will apply\n", rf.me, rf.commitIndex)
		rf.needApply.Notify()
	}
}

func (rf *Raft) applyRoutine() {
	notDone := true
	for !rf.killed() {
		if !notDone {
			<-rf.needApply.Listen()
		}

		rf.mu.Lock()
		// DPrintf("[%d] applyRoutine commitIndex=%d lastApplied=%d\n", rf.me, rf.commitIndex, rf.lastApplied)
		if rf.lastApplied >= rf.commitIndex {
			rf.mu.Unlock()
			notDone = false
			continue
		}

		index := rf.lastApplied + 1
		// DPrintf("[%d] lastApplied=%d commitIndex=%d index=%d lastIndex=%d\n", rf.me, rf.lastApplied, rf.commitIndex, index, rf.snapshotLastIncludedIndex)
		msg := ApplyMsg{
			CommandValid:  true,
			Command:       rf.log[index-rf.snapshotLastIncludedIndex-1].Cmd,
			CommandIndex:  index,
			SnapshotValid: false,
		}
		// DPrintf("[%d] will apply index %d log %v\n", rf.me, index, rf.log[index - rf.snapshotLastIncludedIndex- 1].Cmd)
		// DPrintf("[%d] my logs: {... %d, %d}, %v\n", rf.me, rf.snapshotLastIncludedIndex, rf.snapshotLastIncludedTerm, rf.log)

		rf.mu.Unlock()
		// if rf.killed() {
		// 	close(rf.applyCh)
		// 	return
		// }
		rf.applyCh <- msg
		rf.mu.Lock()
		if rf.lastApplied < index-1 {
			// DPrintf("inconsistent, skip")
			rf.mu.Unlock()
			notDone = true
			continue
		}

		if rf.lastApplied == index-1 {
			rf.lastApplied = index
		}
		notDone = rf.lastApplied < rf.commitIndex
		rf.mu.Unlock()
	}
	// close(rf.applyCh)
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
	rf.status = FOLLOWER
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = nil
	rf.snapshotLastIncludedIndex = 0
	rf.snapshotLastIncludedTerm = 0
	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.applyCh = applyCh
	rf.needLogReplication = make([]EventDispatcher, len(rf.peers))

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.lastApplied = rf.snapshotLastIncludedIndex
	rf.snapshot = persister.ReadSnapshot()

	// start ticker goroutine to start elections
	go rf.electionTicker()
	go rf.applyRoutine()

	return rf
}
