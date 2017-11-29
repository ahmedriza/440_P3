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
	"sync"
	"time"

	"github.com/cmu440/labrpc"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

// State of raft sever
type State int

const (
	follower State = iota + 1
	candidate
	leader
)

// Entry log structure
type Entry struct {
	term     int
	index    int
	commited bool
	command  interface{}
	foo      int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu    sync.Mutex          // Lock to protect shared access to this peer's state
	peers []*labrpc.ClientEnd // RPC end points of all peers
	me    int                 // this peer's index into peers[]

	// Your data here (3A, 3B).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// Presistent states
	state       State
	currentTerm int
	votedFor    int
	log         map[int]*Entry

	// Volatile states
	commitIndex int
	lastApplied int

	// states for leader
	nextIndex  []int
	matchIndex []int

	// channels
	applyCh           chan ApplyMsg
	electionTimeoutCh chan bool
	elected           chan bool
	heartbeatCh       chan *AppendEntriesArgs

	// timeouts
	// electionTimeout time.Duration
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool

	term = rf.currentTerm
	isleader = rf.state == leader
	// Your code here (3A).
	return term, isleader
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
	} else if args.Term == rf.currentTerm {
		if rf.votedFor != -1 && rf.votedFor != args.CandidateID {
			reply.VoteGranted = false
			reply.Term = rf.currentTerm
		} else {
			reply.VoteGranted = true
			rf.votedFor = args.CandidateID
			reply.Term = rf.currentTerm
		}
	} else {
		rf.currentTerm = args.Term
		rf.votedFor = args.CandidateID
		reply.VoteGranted = true
		reply.Term = rf.currentTerm
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
	DPrintf("Server %d requested vote to server %d", rf.me, server)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// AppendEntriesArgs RPC arguments structure
type AppendEntriesArgs struct {
	Term     int
	LeaderID int
	Entries  string
}

// AppendEntriesReply RPC reply structure
type AppendEntriesReply struct {
	Term    int
	Success bool
}

// AppendEntries RPC function
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// heartbeat
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if len(args.Entries) == 0 {
		rf.heartbeatCh <- args
		reply.Term = rf.currentTerm
		reply.Success = true
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
	}

}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, &reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
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

	// Your code here (3B).

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int, applyCh chan ApplyMsg) *Raft {
	DPrintf("make function call")
	rf := &Raft{}
	rf.peers = peers
	rf.me = me

	// Your initialization code here (3A, 3B).
	rf.applyCh = applyCh

	rf.state = follower
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = make(map[int]*Entry)

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	rf.electionTimeoutCh = make(chan bool)
	rf.elected = make(chan bool)
	rf.heartbeatCh = make(chan *AppendEntriesArgs)

	go rf.mainHandler()

	return rf
}

func genRand(min int, max int) int {
	return rand.Intn(max-min) + min
}

func electionTimeout() *time.Timer {
	return time.NewTimer(time.Duration(genRand(400, 600)) * time.Millisecond)
}

func (rf *Raft) mainHandler() {
	electionTimer := electionTimeout()
	for {
		select {
		case beat := <-rf.heartbeatCh: // appendentries RPC that carry no log entries
			DPrintf("Server %d received heartbeat", rf.me)
			electionTimer.Stop()
			rf.mu.Lock()
			if rf.state == candidate && beat.Term >= rf.currentTerm {
				rf.electionTimeoutCh <- true
				rf.state = follower
			}
			if beat.Term > rf.currentTerm {
				rf.currentTerm = beat.Term
			}
			rf.mu.Unlock()
		case <-rf.elected:
			DPrintf("Server %d elected as leader", rf.me)
			electionTimer.Stop()
			rf.mu.Lock()
			rf.state = leader
			rf.mu.Unlock()
			rf.leaderHandler() // blocks until it steps down from leader
		case <-electionTimer.C:
			DPrintf("Server %d starts election", rf.me)
			electionTimer.Stop()
			rf.mu.Lock()
			if rf.state == candidate && rf.currentTerm != 0 {
				// stop previous election goroutine
				rf.electionTimeoutCh <- true
			}
			rf.currentTerm++
			rf.state = candidate
			rf.mu.Unlock()
			go rf.electionHandler()
		}

		electionTimer = electionTimeout()
	}
}

func (rf *Raft) electionHandler() {
	voteCount := 1 // self voting
	args := &RequestVoteArgs{
		Term:        rf.currentTerm,
		CandidateID: rf.me,
	}

	replyCh := make(chan *RequestVoteReply, len(rf.peers))

	DPrintf("asdf")
	// send async request vote message to other nodes
	for peer := 0; peer < len(rf.peers); peer++ {
		if peer == rf.me {
			continue
		}

		var reply RequestVoteReply
		go func(peer int, args *RequestVoteArgs, reply *RequestVoteReply) {
			rf.sendRequestVote(peer, args, reply) // wrapper function for RequestVote RPC
			replyCh <- reply
		}(peer, args, &reply)
	}

	// wait for majority votes to come in or timeout
	for {
		select {
		case <-rf.electionTimeoutCh:
			return
		case reply := <-replyCh:
			DPrintf("Server %d received vote", rf.me, reply)
			if reply.Term > rf.currentTerm {
				rf.mu.Lock()
				rf.currentTerm = reply.Term
				rf.state = follower
				rf.mu.Unlock()
				return
			}
			if reply.VoteGranted {
				voteCount++
			}
			if voteCount > len(rf.peers)/2 {
				// become leader
				rf.elected <- true
				return
			}
		}
	}
}

func (rf *Raft) leaderHandler() {
	// send periodic heartbeat signal
	args := &AppendEntriesArgs{
		Term:     rf.currentTerm,
		LeaderID: rf.me,
		Entries:  "",
	}

	for {
		for peer := 0; peer < len(rf.peers); peer++ {
			if peer == rf.me {
				continue
			}

			var reply AppendEntriesReply

			go func(peer int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
				rf.sendAppendEntries(peer, args, reply)
			}(peer, args, &reply)
		}
		time.Sleep(100 * time.Millisecond)
	}
}
