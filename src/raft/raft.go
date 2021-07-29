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

	//	"bytes"
	"sync"
	"sync/atomic"
	//	"6.824/labgob"
	"6.824/labrpc"
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

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int

	termChanged           EventDispatcher
	cancelElectionTimeout EventDispatcher
	cancelCurrentElection EventDispatcher
	newLogEntries         EventDispatcher
	//cancelHeartbeat EventDispatcher
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
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
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
	// r := bytes.NewBuffer(data)
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
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

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

	reply.VoteGranted = false
	if args.Term >= rf.currentTerm && (rf.votedFor == -1 || rf.votedFor == args.CandidateId) {
		if args.LastLogTerm > rf.log[len(rf.log)-1].Term {
			reply.VoteGranted = true
		} else if args.LastLogTerm == rf.log[len(rf.log)-1].Term && args.LastLogIndex >= len(rf.log)-1 {
			reply.VoteGranted = true
		}
	}

	if reply.VoteGranted {
		// DPrintf("[%d] grant the vote to %d\n", rf.me, args.CandidateId)
		rf.votedFor = args.CandidateId
		rf.cancelElectionTimeout.Emit(nil)
	} else {
		// DPrintf("[%d] NOT grant the vote to %d\n", rf.me, args.CandidateId)
		// DPrintf("[%d] my logs: %v\n", rf.me, rf.log)
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
	return ok
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
	// DPrintf("[%d] received log: prev=%d, entries=%v\n", rf.me, args.PrevLogIndex, args.Entries)
	if len(rf.log) <= args.PrevLogIndex || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
		// DPrintf("[%d] append entries failed\n", rf.me)
		// DPrintf("[%d] my logs: %v\n", rf.me, rf.log)
	} else {
		reply.Success = true

		// DPrintf("[%d] previous log: %v\n", rf.me, rf.log)
		rf.log = rf.log[:args.PrevLogIndex+1]
		rf.log = append(rf.log, args.Entries...)
		// DPrintf("[%d] current log: %v\n", rf.me, rf.log)

		if args.LeaderCommit > rf.commitIndex {
			rf.commitIndex = args.LeaderCommit
			if rf.commitIndex >= len(rf.log) {
				rf.commitIndex = len(rf.log) - 1
			}
			rf.apply()
		}
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
	if reply.Term > rf.currentTerm {
		rf.newTermDetected(reply.Term)
	} else if reply.Success {
		if len(args.Entries) > 0 {
			// DPrintf("[%d] log appending success for %d, set nextIndex=%d\n", rf.me, you, endIndex)
			rf.nextIndex[you] = endIndex
			rf.matchIndex[you] = endIndex - 1
			rf.checkCommits()
		}
	} else {
		if startIndex > rf.nextIndex[you] {
			startIndex = rf.nextIndex[you]
		}
		rf.nextIndex[you] = startIndex - (endIndex - startIndex)
		if rf.nextIndex[you] >= startIndex {
			rf.nextIndex[you] = startIndex - 1
		} else if rf.nextIndex[you] <= 0 {
			rf.nextIndex[you] = 1
		}
		// DPrintf("[%d] log appending failed for %d, set nextIndex=%d\n", rf.me, you, rf.nextIndex[you])
	}
	rf.mu.Unlock()
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
		index = len(rf.log)
		term = rf.currentTerm
		// DPrintf("[%d] new log %v come in\n", rf.me, command)
		newlog := LogEntry{
			Term: rf.currentTerm,
			Cmd:  command,
		}
		// DPrintf("[%d] previous logs: %v\n", rf.me, rf.log)
		rf.log = append(rf.log, newlog)
		// DPrintf("[%d] current logs: %v\n", rf.me, rf.log)
		rf.newLogEntries.Emit(nil)
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
	rf.cancelCurrentElection.Emit(nil)
	rf.cancelElectionTimeout.Emit(nil)
	rf.termChanged.Emit(nil)
	rf.newLogEntries.Emit(nil)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

const (
	HEARTBEAT_TIMEOUT     = 200
	ELECTION_TIMEOUT_LOW  = 500
	ELECTION_TIMEOUT_HIGH = 1000
)

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) electionTicker() {
	for rf.killed() == false {

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
		electionTimeout := time.Duration(ELECTION_TIMEOUT_LOW+rand.Intn(ELECTION_TIMEOUT_HIGH-ELECTION_TIMEOUT_LOW)) * time.Millisecond
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
		panic("leader should not make election")
	}

	rf.cancelCurrentElection.Emit(nil)
	rf.status = CANDIDATE
	rf.currentTerm++
	rf.votedFor = rf.me
	// DPrintf("[%d] new election start for term %d\n", rf.me, rf.currentTerm)
	// DPrintf("[%d] my logs: %v\n", rf.me, rf.log)

	var voteCnt uint32
	voteCnt = 0
	peerNum := len(rf.peers)
	// me := rf.me
	var dispatcher EventDispatcher

	for i := range rf.peers {
		if i != rf.me {
			args := RequestVoteArgs{
				Term:         rf.currentTerm,
				CandidateId:  rf.me,
				LastLogIndex: len(rf.log) - 1,
				LastLogTerm:  rf.log[len(rf.log)-1].Term,
			}
			reply := RequestVoteReply{}
			you := i

			go func() {
				// DPrintf("[%d] send request vote into %d\n", me, you)
				rf.sendRequestVote(you, &args, &reply)
				if reply.VoteGranted {
					// DPrintf("[%d] receive granted vote from %d\n", me, you)
					if atomic.AddUint32(&voteCnt, 1) >= uint32(peerNum/2) {
						dispatcher.Emit(nil)
					}
				}
			}()
		}
	}

	go func() {
		select {
		case <-dispatcher.Listen():
			rf.startLeader()
		case <-rf.cancelCurrentElection.Listen():
			// DPrintf("[%d] election failed\n", me)
		}
	}()
}

func (rf *Raft) startLeader() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// DPrintf("[%d] majority voted, become a leader\n", rf.me)
	rf.status = LEADER
	rf.nextIndex = nil
	rf.matchIndex = nil
	for range rf.peers {
		rf.nextIndex = append(rf.nextIndex, len(rf.log))
		rf.matchIndex = append(rf.matchIndex, 0)
	}
	rf.cancelElectionTimeout.Emit(nil)
	rf.heartbeat()
	go rf.heartbeatTicker()
	for i := range rf.peers {
		if i != rf.me {
			go rf.logMatchingDetection(i)
		}
	}
}

func (rf *Raft) heartbeatTicker() {
	for !rf.killed() {
		rf.mu.Lock()
		heartbeatTimeout := HEARTBEAT_TIMEOUT * time.Millisecond
		// DPrintf("[%d] prepared to send heartbeats after %v", rf.me, heartbeatTimeout)
		rf.mu.Unlock()

		select {
		case <-rf.termChanged.Listen():
			// rf.mu.Lock()
			// DPrintf("[%d] heartbeat ended", rf.me)
			// rf.mu.Unlock()
			return
		case <-time.After(heartbeatTimeout):
			rf.mu.Lock()
			rf.heartbeat()
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) heartbeat() {
	for i := range rf.peers {
		if i != rf.me {
			args := AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: len(rf.log) - 1,
				PrevLogTerm:  rf.log[len(rf.log)-1].Term,
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

func (rf *Raft) logMatchingDetection(you int) {
	retryTimer := time.After(0)
	rf.mu.Lock()
	thisTerm := rf.currentTerm
	rf.mu.Unlock()
	for !rf.killed() {
		for !rf.killed() {
			select {
			case <-rf.termChanged.Listen():
				return
			case <-retryTimer:
				retryTimer = time.After(100 * time.Millisecond)
			}
			rf.mu.Lock()
			// DPrintf("[%d] log matching check: log-len=%d nextIndex[%d]=%d\n", rf.me, len(rf.log), you, rf.nextIndex[you])
			if len(rf.log) <= rf.nextIndex[you] {
				rf.mu.Unlock()
			} else if rf.currentTerm > thisTerm {
				rf.mu.Unlock()
				return
			} else {
				break
			}
		}

		// DPrintf("[%d] log non-matching detected, will append logs to %d (nextIndex=%d)\n", rf.me, you, rf.nextIndex[you])
		// DPrintf("[%d] my logs: %v\n", rf.me, rf.log)
		startIndex := rf.nextIndex[you]
		endIndex := len(rf.log)
		args := AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: startIndex - 1,
			PrevLogTerm:  rf.log[startIndex-1].Term,
			Entries:      rf.log[startIndex:endIndex],
			LeaderCommit: rf.commitIndex,
		}
		reply := AppendEntriesReply{}
		// DPrintf("[%d] prev=%d entries=%v\n", rf.me, args.PrevLogIndex, args.Entries)
		rf.mu.Unlock()
		rf.sendAppendEntries(you, &args, &reply)
	}
}

func (rf *Raft) newTermDetected(term int) {
	rf.currentTerm = term
	rf.status = FOLLOWER
	rf.votedFor = -1
	// DPrintf("[%d] new term %d detected, upgrade myself\n", rf.me, term)
	rf.termChanged.Emit(nil)
	rf.cancelCurrentElection.Emit(nil)
}

func (rf *Raft) currentLeaderDetected() {
	rf.status = FOLLOWER
	// DPrintf("[%d] leader in current term detected, stop the election", rf.me)
	rf.cancelCurrentElection.Emit(nil)
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
	if nextCommitIndex > rf.commitIndex && rf.log[nextCommitIndex].Term == rf.currentTerm {
		rf.commitIndex = nextCommitIndex
		rf.apply()
	}
}

func (rf *Raft) apply() {
	// DPrintf("[%d] lastApplied=%d commitIndex=%d\n", rf.me, rf.lastApplied, rf.commitIndex)
	for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
		msg := ApplyMsg{
			CommandValid:  true,
			Command:       rf.log[i].Cmd,
			CommandIndex:  i,
			SnapshotValid: false,
			Snapshot:      nil,
			SnapshotTerm:  0,
			SnapshotIndex: 0,
		}

		// DPrintf("[%d] will apply index %d log %v\n", rf.me, i, rf.log[i].Cmd)
		rf.applyCh <- msg
		rf.lastApplied = i
	}
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
	rf.log = []LogEntry{{Term: 0, Cmd: nil}}
	rf.applyCh = applyCh

	rf.commitIndex = 0
	rf.lastApplied = 0

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.electionTicker()

	return rf
}
