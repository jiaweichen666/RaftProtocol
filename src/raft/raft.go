package raft

// NOTICE: log index of raft begins from 1

import (
	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//"fmt"

	"6.5840/labgob"
	"6.5840/labrpc"
)

// election timeout of every serve ranges from 500 to 1000 millionseconds
const MinElectionTimeout = 500
const MaxElectionTimeout = 1000

func randTimeout() time.Duration {
	randTimeout := MinElectionTimeout + rand.Intn(MaxElectionTimeout-MinElectionTimeout)
	return time.Duration(randTimeout) * time.Millisecond
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// getLastIndex returns logs last index
func (rf *Raft) getLastIndex() int {
	return rf.log[len(rf.log)-1].Index
}

// server status, one raft group only contains one leader
const (
	Follower = iota
	Candidate
	Leader
)

// structure for raft log
type LogEntry struct {
	Command interface{}
	Index   int
	Term    int
}

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
type ApplyMsg struct {
	CommandValid  bool
	Command       interface{}
	CommandIndex  int
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type InstallSnapshotArgs struct {
	Term     int
	LeaderId int
	// last log index included in snapshot
	LastIndex int
	// last log term included in snapshot
	LastTerm int
	// snapshot bytes
	Snapshot []byte
}

type InstallSnapshotReply struct {
	Term int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu                  sync.Mutex          // Lock to protect shared access to this peer's state
	peers               []*labrpc.ClientEnd // RPC end points of all peers
	persister           *Persister          // Object to hold this peer's persisted state
	me                  int                 // this peer's index into peers[]
	dead                int32               // set by Kill()
	status              int                 // serve status of raft group
	applyMsg            chan ApplyMsg       // applied log channel
	currentTerm         int                 // term of current server, need persist
	votedFor            int                 // leader id of current server, need persist
	log                 []LogEntry          // log Entries buffer of logs,need persist
	commitIndex         int                 // volatile states of last committed log index
	lastApplied         int                 // volatile states of last applied log index
	nextIndex           []int               // next index tracker for all slaves
	matchIndex          []int               // matched index tracker for all slaves
	lastAccessed        time.Time           // last time raft object accessed from the leader
	lastIndexOfSnapshot int                 // last log index of newest snapshot, persist when snapshotting
	lastTermOfSnapshot  int                 // last log term of newest snapshot, persist when snapshotting
}

// truncate logs from begin to lastIndex
// lastIndex log is included
// all these logs are included in snapshot
// used for slave when leader handling snapshot sent by leader
func (rf *Raft) TruncateLogs(lastIndex int, lastTerm int) {
	index := -1
	// find target log index in log buffer
	for i := len(rf.log) - 1; i >= 0; i-- {
		if rf.log[i].Index == rf.lastIndexOfSnapshot && rf.log[i].Term == rf.lastTermOfSnapshot {
			index = i
			break
		}
	}
	if index <= -1 {
		// not found, clean all logs in log buffer
		rf.log = []LogEntry{}
	} else {
		// reserve logs after index, log of index not included
		rf.log = append([]LogEntry{}, rf.log[index+1:]...)
	}
	// TODO: update meta, is this necessary?
	rf.lastIndexOfSnapshot = lastIndex
	rf.lastTermOfSnapshot = lastTerm
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	rf.mu.Lock()
	defer rf.mu.Unlock()
	isleader = rf.status == Leader
	term = rf.currentTerm
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.Save(data, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.log)
}

// read snapshot from persistant storage
// lastIndex and lastTerm are included in snapshot buffer
func (rf *Raft) readSnapshot(data []byte) {
	if len(data) == 0 {
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	// use intermidiate var to reduce the scope of the lock
	// aim to get better performance
	lastIndexOfSnapshot, lastTermOfSnapshot := 0, 0
	d.Decode(&lastIndexOfSnapshot)
	d.Decode(&lastTermOfSnapshot)
	rf.mu.Lock()
	rf.lastIndexOfSnapshot = lastIndexOfSnapshot
	rf.lastTermOfSnapshot = lastTermOfSnapshot
	rf.commitIndex = lastIndexOfSnapshot
	rf.lastApplied = lastIndexOfSnapshot
	rf.TruncateLogs(lastIndexOfSnapshot, lastTermOfSnapshot)
	rf.mu.Unlock()
	// we need to update apply index
	go func() {
		rf.applyMsg <- ApplyMsg{
			SnapshotValid: true,
			Snapshot:      data,
			SnapshotTerm:  lastTermOfSnapshot,
			SnapshotIndex: lastIndexOfSnapshot,
		}
	}()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	firstIndex := rf.lastIndexOfSnapshot
	lastIndex := rf.getLastIndex()
	if index <= firstIndex || index > lastIndex {
		return
	}
	// get index and term of target 'Index' in log buffer
	newSnapLastLogIndex := rf.log[index-firstIndex-1].Index
	newSnapLastLogTerm := rf.log[index-firstIndex-1].Term
	// truncate the log from log buffer to index, log of index is included
	rf.log = append([]LogEntry{}, rf.log[index-firstIndex:]...)
	// update meta
	rf.lastIndexOfSnapshot = newSnapLastLogIndex
	rf.lastTermOfSnapshot = newSnapLastLogTerm
	rf.persist()
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(newSnapLastLogIndex)
	e.Encode(newSnapLastLogTerm)
	data := w.Bytes()
	data = append(data, snapshot...)
	// FIXME:Save interface need to pass raft state as first argument
	// for convinience, we read from persister and pass to function
	// performance may suck
	rf.persister.Save(rf.persister.ReadRaftState(), data)
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entry        []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
	Xterm   int
	XIndex  int
	XLen    int
}

// RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// already voted for candidate, return directly
	if rf.currentTerm == args.Term && rf.votedFor == args.CandidateId {
		reply.VoteGranted, reply.Term = true, rf.currentTerm
		return
	}
	// current term is bigger than requester's, reject vote request
	// current term is equal to requesters's, but vote for some server else, reject vote request
	if rf.currentTerm > args.Term || (rf.currentTerm == args.Term && rf.votedFor != -1) {
		reply.Term, reply.VoteGranted = rf.currentTerm, false
		return
	}
	// requester's term is bigger than current term
	if args.Term > rf.currentTerm {
		rf.currentTerm, rf.votedFor = args.Term, -1
		rf.status = Follower
	}
	// current term <= request's term
	reply.Term = args.Term
	// requester's term is same as current server's
	// but current server has more logs of current term
	// reject requester's vote request
	if rf.getLastIndex()-rf.lastIndexOfSnapshot-1 >= 0 {
		lastLogTerm := rf.log[rf.getLastIndex()-rf.lastIndexOfSnapshot-1].Term
		if lastLogTerm > args.LastLogTerm ||
			(lastLogTerm == args.LastLogTerm && rf.getLastIndex() > args.LastLogIndex) {
			reply.VoteGranted = false
			return
		}
	}
	// requester's log is newer, vote for requester
	rf.status = Follower
	rf.lastAccessed = time.Now()
	reply.VoteGranted = true
	rf.votedFor = args.CandidateId
	rf.persist()
}

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// appendEntries rpc handler
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// XTerm:  term in the conflicting entry (if any)
	// XIndex: index of first entry with that term (if any)
	// XLen:   log length
	reply.Xterm = -1
	reply.XIndex = -1
	//FIXME: reply.Xlen = len(rf.log)
	reply.XLen = rf.getLastIndex() + 1
	reply.Success = false
	reply.Term = rf.currentTerm
	// request from older leader, reject
	if args.Term < rf.currentTerm {
		// fmt.Printf("leader's term: %v smaller than me:%v, reject\n", args.Term, rf.currentTerm)
		return
	}
	// receive heartbeat from leader, update access time to prevent vote
	rf.lastAccessed = time.Now()
	// not continous, return
	// we expect prevlogIndex <= rf.lastIndex
	// if prevlogIndex == lastIndex, log is continous
	// if prevlogIndex < lastIndex, logs in args maybe can be partially used
	if rf.getLastIndex() < args.PrevLogIndex {
		// fmt.Printf("Prev log index:%v not continous with mine lastIndex:%v, reject\n", args.PrevLogIndex, rf.getLastIndex())
		return
	}
	/*
	* According to raft thesis, confilct log replication can be handled more quickly by machnism below:
	* If desired, the protocol can be optimized to reduce the number of rejected AppendEntries RPCs.
	* For example, when rejecting an AppendEntries request,
	* the follower can include the term of the conflicting entry and the first index it stores for that term.
	* With this information, the leader can decrement nextIndex to bypass all of the con- flicting entries in that term;
	* one AppendEntries RPC will be required for each term with conflicting entries,
	* rather than one RPC per entry.
	* In practice, we doubt this opti- mization is necessary,
	* since failures happen infrequently and it is unlikely that there will be many inconsistent en- tries.
	 */
	// current server's log conflict with leader's, find the first index of conflict term
	if rf.log[args.PrevLogIndex-rf.lastIndexOfSnapshot-1].Term != args.PrevLogTerm {
		// set Xterm as conflict term
		reply.Xterm = rf.log[args.PrevLogIndex-rf.lastIndexOfSnapshot-1].Term
		for i, v := range rf.log {
			if v.Term == reply.Xterm {
				// find first log index of conflict term
				reply.XIndex = rf.log[i].Index
				break
			}
		}
		return
	}

	index := 0
	// current server
	for ; index < len(args.Entry); index++ {
		// caculate current log index
		currentIndex := args.PrevLogIndex + 1 + index
		if currentIndex > rf.getLastIndex() {
			// find the continous place, append log from this index to
			// current servers log batch
			break
		}

		if rf.log[currentIndex-rf.lastIndexOfSnapshot-1].Term != args.Entry[index].Term {
			// if current server's log conflict with leader's at currentIndex
			// cut off log of range [currentIndex, end] in current server's
			// log batch
			rf.log = rf.log[:currentIndex-rf.lastIndexOfSnapshot-1]
			rf.persist()
			break
		}
	}

	reply.Success = true
	// append logs from leader to current server's log batch tail
	if len(args.Entry) > 0 {
		rf.log = append(rf.log, args.Entry[index:]...)
		rf.persist()
	}

	// commit log by commit index transfered from leader

	if args.LeaderCommit > rf.commitIndex {
		// commit log index should smaller than max log index of current server
		min := min(args.LeaderCommit, rf.getLastIndex())
		for i := rf.commitIndex + 1; i <= min; i++ {
			rf.commitIndex = i
			rf.applyMsg <- ApplyMsg{
				CommandValid: true,
				Command:      rf.log[i-rf.lastIndexOfSnapshot-1].Command,
				CommandIndex: i,
			}
		}
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// rpc handler for installing snapshot
func (rf *Raft) InstallSnapshot(args InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return
	}

	// save snapshot to local
	// FIXME: read from persister of raft state first
	// performance may suck
	rf.persister.Save(rf.persister.ReadRaftState(), args.Snapshot)
	// clean unneed log in log buffer
	rf.TruncateLogs(args.LastIndex, args.LastTerm)
	// update meta
	rf.lastApplied = args.LastIndex
	rf.commitIndex = args.LastIndex
	rf.lastIndexOfSnapshot = args.LastIndex
	rf.lastTermOfSnapshot = args.LastTerm
	// update heartbeat
	rf.lastAccessed = time.Now()
	rf.persist()
	// we need to update apply info
	rf.applyMsg <- ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Snapshot,
		SnapshotTerm:  args.LastTerm,
		SnapshotIndex: args.LastIndex,
	}
}

// sendSnapshot RPC to a server
func (rf *Raft) sendSnapshot(server int, args InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

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
// This is the propose function
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	rf.mu.Lock()
	defer rf.mu.Unlock()
	isLeader = rf.status == Leader
	if !isLeader {
		return 0, 0, false
	}
	term = rf.currentTerm
	// monotonically increasing index
	index = rf.getLastIndex() + 1
	// propose a new log entry
	rf.log = append(rf.log, LogEntry{
		Command: command,
		Index:   index,
		Term:    term,
	})
	rf.persist()
	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for !rf.killed() {
		// Check if a leader election should be started.
		rf.mu.Lock()
		status := rf.status
		rf.mu.Unlock()
		if status == Follower {
			rf.manageFollower()
		} else if status == Candidate {
			rf.manageCandidate()
		} else if status == Leader {
			rf.manageLeader()
		}
		// pause for a random amount of time between 50 and 350
		// milliseconds.
		//ms := 50 + (rand.Int63() % 300)
		ms := 100
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	// server starts as Follower
	rf.status = Follower
	rf.votedFor = -1
	// acording to raft paper, current term starts with 0
	rf.currentTerm = 0
	// acording to raft paper, commitIndex and lastApplied initialized with 0
	rf.commitIndex = 0
	rf.lastApplied = 0
	// initiate log with a dummy logentry
	rf.log = append(rf.log, LogEntry{
		Index: 0,
		Term:  0,
	})
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	// maybe a little hack
	// lastIndexOfSnapshot initial value need to be -1
	// to prevent index out of log buffer range
	rf.lastIndexOfSnapshot = -1
	rf.lastTermOfSnapshot = -1
	rf.applyMsg = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

// Check whether candidate haven't receive heartbeat from leader
// if time elapsed over election timeout interval
// if true, change current server status to candidate and start voting for itself
func (rf *Raft) manageFollower() {
	duration := randTimeout()
	time.Sleep(duration)
	rf.mu.Lock()
	lastAccessed := rf.lastAccessed
	rf.mu.Unlock()
	if time.Since(lastAccessed).Milliseconds() >= duration.Milliseconds() {
		// heartbeat timeout, start voting
		// fmt.Printf("server %v heartbeat timeout, start voting...\n", rf.me)
		rf.mu.Lock()
		rf.status = Candidate
		rf.currentTerm++
		rf.votedFor = -1
		rf.persist()
		rf.mu.Unlock()
	}
}

func (rf *Raft) manageCandidate() {
	timeOut := randTimeout()
	start := time.Now()
	rf.mu.Lock()
	peers := rf.peers
	me := rf.me
	term := rf.currentTerm
	lastLogTerm := 0
	lastLogTerm = rf.log[rf.getLastIndex()-rf.lastIndexOfSnapshot-1].Term
	rf.mu.Unlock()
	count := 0
	total := len(peers)
	finished := 0
	majority := (total / 2) + 1
	for peer := range peers {
		if me == peer {
			rf.mu.Lock()
			count++
			finished++
			rf.mu.Unlock()
			continue
		}

		go func(peer int) {
			args := RequestVoteArgs{}
			args.Term = term
			args.CandidateId = me
			args.LastLogIndex = rf.getLastIndex()
			args.LastLogTerm = lastLogTerm
			reply := RequestVoteReply{}
			ok := rf.sendRequestVote(peer, &args, &reply)
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if !ok {
				finished++
				return
			}
			if reply.VoteGranted {
				finished++
				count++
			} else {
				finished++
				if args.Term < reply.Term {
					rf.status = Follower
					rf.persist()
				}
			}
		}(peer)
	}
	for {
		rf.mu.Lock()
		if count >= majority || finished == total || time.Since(start).Milliseconds() >= timeOut.Milliseconds() {
			break
		}
		rf.mu.Unlock()
		time.Sleep(50 * time.Millisecond)
	}

	if time.Since(start).Milliseconds() >= timeOut.Milliseconds() {
		rf.status = Follower
		rf.mu.Unlock()
		return
	}
	if rf.status == Candidate && count >= majority {
		rf.status = Leader
		// fmt.Printf("server %v become leader of term %v\n", rf.me, rf.currentTerm)
		for peer := range peers {
			rf.nextIndex[peer] = rf.getLastIndex() + 1
		}
	} else {
		rf.status = Follower
	}
	rf.persist()
	rf.mu.Unlock()
}

func (rf *Raft) manageLeader() {
	rf.mu.Lock()
	me := rf.me
	term := rf.currentTerm
	commitIndex := rf.commitIndex
	peers := rf.peers
	nextIndex := rf.nextIndex

	lastLogIndex := rf.getLastIndex()
	matchIndex := rf.matchIndex
	nextIndex[me] = lastLogIndex + 1
	matchIndex[me] = lastLogIndex
	log := rf.log
	rf.mu.Unlock()
	// check log replicated to most peers and acked
	// if acked by most peers, this log can be committed to state machine and reply
	for n := commitIndex + 1; n <= lastLogIndex; n++ {
		count := 0
		total := len(peers)
		majority := (total / 2) + 1
		for peer := range peers {
			if matchIndex[peer] >= n && log[n-rf.lastIndexOfSnapshot-1].Term == term {
				count++
			}
		}
		if count >= majority {
			rf.mu.Lock()
			i := rf.commitIndex + 1
			for ; i <= n; i++ {
				rf.applyMsg <- ApplyMsg{
					CommandValid: true,
					Command:      log[i-rf.lastIndexOfSnapshot-1].Command,
					CommandIndex: i,
				}
				rf.commitIndex = rf.commitIndex + 1
			}
			rf.mu.Unlock()
		}
	}
	// do log replication here
	for peer := range peers {
		if peer == me {
			continue
		}

		args := AppendEntriesArgs{}
		reply := AppendEntriesReply{}
		rf.mu.Lock()
		args.Term = rf.currentTerm
		prevLogIndex := nextIndex[peer] - 1
		args.PrevLogIndex = prevLogIndex
		args.PrevLogTerm = rf.log[prevLogIndex-rf.lastIndexOfSnapshot-1].Term
		args.LeaderCommit = rf.commitIndex
		args.LeaderId = rf.me
		if nextIndex[peer] <= lastLogIndex {
			args.Entry = rf.log[prevLogIndex-rf.lastIndexOfSnapshot : lastLogIndex-rf.lastIndexOfSnapshot]
		}
		rf.mu.Unlock()

		// execute appending entries
		go func(peer int) {
			ok := rf.sendAppendEntries(peer, &args, &reply)
			if !ok {
				return
			}

			rf.mu.Lock()
			if reply.Success {
				rf.nextIndex[peer] = min(rf.nextIndex[peer]+len(args.Entry), rf.getLastIndex()+1)
				rf.matchIndex[peer] = prevLogIndex + len(args.Entry)
			} else {
				// Case 1: leader doesn't have XTerm: nextIndex = XIndex
				// Case 2: leader has XTerm: nextIndex = leader's last entry for XTerm
				// Case 3: follower's log is too short: nextIndex = XLen
				if reply.Term > args.Term {
					rf.status = Follower
					rf.mu.Unlock()
					return
				}
				// no conflict term
				if reply.Xterm == -1 {
					rf.nextIndex[peer] = reply.XLen
					rf.mu.Unlock()
					return
				}
				index := -1
				// if leader has the conflict term, set peers next
				// to the last log index of this term
				for i, v := range rf.log {
					if v.Term == reply.Xterm {
						index = i
					}
				}
				if index == -1 {
					// if leader cannot find log of Xterm
					// set peers next to XIndex to improve efficiency
					rf.nextIndex[peer] = reply.XIndex
				} else {
					rf.nextIndex[peer] = rf.log[index].Index
				}
			}
			rf.mu.Unlock()
		}(peer)
	}
}
