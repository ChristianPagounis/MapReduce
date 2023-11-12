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
	"cs350/labrpc"
	"fmt"
	"log"
	"math/rand"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"
)

// import "bytes"
// import "cs350/labgob"

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.

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

type printer interface {
	print(data ...interface{})
}

type myLock struct {
	mu sync.Mutex
	p  printer
}

func (ml *myLock) Lock() {
	ml.p.print("before lock")
	ml.mu.Lock()
	ml.p.print("after lock", debug.Stack())
}

func (ml *myLock) Unlock() {
	ml.p.print("before unlok")
	ml.mu.Unlock()
	ml.p.print("after unlock")
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu            sync.Locker         // Lock to protect shared access to this peer's state
	peers         []*labrpc.ClientEnd // RPC end points of all peers
	persister     *Persister          // Object to hold this peer's persisted state
	me            int                 // this peer's index into peers[]
	dead          int32               // set by Kill()
	log           []Entry             //log in raft structure
	currentTerm   int
	state         int
	votedFor      int
	votesReceived int
	lastHeartBeat time.Time
	applyCh       chan ApplyMsg
	nextIndex     []int
	matchIndex    []int
	commitIndex   int
	lastApplied   int

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

//STATE STRUCTS HERE!!!!

const (
	HEARTBEAT_INTERVAL = time.Duration(120) * time.Millisecond
	VOTE_BOUND_MIN_MS  = 250 //ELECTION TIME UPPERBOUND, LOWER BOUNDS. UNSURE IF I NEED THIS
	VOTE_BOUND_MAX_MS  = 400 //UNSURE IF I NEED THESE ANYMORE
	VOTE_NULL          = -1
)

const (
	STATE_FOLLOWER  = 0 // Alternately use iota instead of 1,2
	STATE_CANDIDATE = 1
	STATE_LEADER    = 2
	STATE_DEAD      = 3
)

//func inc(x *int) { //increment *x
//	*x += 1
//
//}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isLeader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.currentTerm
	if rf.state == 2 {
		isLeader = true
	} else {
		isLeader = false
	}
	rf.mu.Unlock()
	return term, isLeader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
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

// restore previously persisted state.
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

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
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

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	fmt.Print("RequestVote")
	// Your code here (2A, 2B).

	rf.mu.Lock()
	defer rf.mu.Unlock()
	//remember seeing to check if dead????
	rf.print(args.Term, "BEFORE TESTREQUEST")
	if args.Term > rf.currentTerm {
		rf.print("Candidate to follower in Request vote")
		rf.currentTerm = reply.Term
		rf.state = STATE_FOLLOWER
		rf.votedFor = -1
		rf.votesReceived = -1
	}
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		rf.print("RV RPC check#1: Term is less than currentTerm")
		reply.Term = rf.currentTerm
		return 
	}
	

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateID) && (args.Term >= rf.currentTerm)  { //this is when they have not yet voted, so we proceed with receiver implementation
		//for if statement unsure if check candidate Id as we dont do log in 2a
		LocallastlogIndex = len(rf.log) - 1
		LocallastlogTerm = rf.log[LocallastlogTerm].Term
		if ((args.LastLogIndex == LocallastlogIndex) && (LocallastlogTerm == LocallastlogTerm)) {
			rf.print("RV RPC Check#2: VoteGranted")
			rf.votedFor = args.CandidateID
			reply.VoteGranted = true
			reply.Term = rf.currentTerm
		}
		rf.print("RV RPC Check#2: Log is NOT up to date ")
		

	} else { // case where they have voted,I thought votegranted would be false
		rf.print("Case where they voted")
		reply.VoteGranted = false
		reply.Term = rf.currentTerm

	}
	//reply.Term = rf.currentTerm
	//TESTING TO SEE IF THIS CAUSED PROBLEMS

	//thinking this should work for setting term after the if checks

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

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)

	return ok
}

func (rf *Raft) requestAndProcessVote(server int) {
	rf.mu.Lock()
	rf.print("Beginning of request and process vote")
	args := &RequestVoteArgs{rf.currentTerm, rf.me, 0, 0}
	reply := &RequestVoteReply{0, false}
	rf.print(rf.currentTerm, reply.Term, "BEFORE")
	rf.mu.Unlock()

	ok := rf.sendRequestVote(server, args, reply)
	rf.mu.Lock()
	rf.print(reply.Term, "AFTER")
	defer rf.mu.Unlock()
	if ok {
		if reply.Term > rf.currentTerm {
			rf.print("Candidate to follower in sendRequestVote")
			rf.currentTerm = reply.Term
			rf.state = STATE_FOLLOWER
			rf.votedFor = -1
			rf.votesReceived = -1
		}
		if reply.VoteGranted {
			rf.print("Vote granted true")
			rf.votesReceived++                          //increment votesReceived
			if rf.votesReceived > (len(rf.peers) / 2) { //i have no idea if this comparision to the length makes sense
				rf.state = STATE_LEADER
				rf.print("New leader elected")
				for zcheck := 0; ycheck < len(rf.peers); zcheck++ { 
					if zcheck != rf.me {
						rf.nextIndex[zcheck] = len(rf.log) // + 1 //SET TO CURRENT LOG LENGTH MAYBE +!
						rf.matchIndex[zcheck] = 0 //initialzed to 0
				go rf.AppendEntryOut()
				rf.print("Inside current test")
				// rf.currentTerm = reply.Term TEST

			}
			// rf.currentTerm = reply.Term
			//set the term of the server to what it was in reply
		}
		//rf.print("Outside current test")
		//rf.currentTerm = reply.Term
	} else {
		rf.print("NOT OKAY IN PROCESS")
	}

}

func (rf *Raft) print(data ...interface{}) {

	// rf.mu.Lock()
	fmt.Printf("id=%d, term=%d, %s\n", rf.me, rf.currentTerm, data)
	// rf.mu.Unlock()
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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	//index := -1
	//term := -1
	//isLeader := true

	// Your code here (2B).
	rf.mu.Lock()
	if rf.state != STATE_LEADER {
		rf.mu.Unlock()
		return -1, -1, false
	} else {
		
		entry := Entry{Term: rf.currentTerm, Command: command}
		rf.log = append(rf.log, entry)

		for ycheck := 0; ycheck < len(rf.peers); ycheck++ {
			if ycheck != rf.me {
				args := &AppendEntriesArgs{rf.currentTerm, 0, 0, 0, nil, 0} //populate these fields with leader info and should be individaul for each term
				reply := &AppendEntriesReply{0, false}
				go rf.sendAppendEntries(ycheck, args, reply)
			}
		}
		rf.mu.Unlock()
		return len(rf.log) - 1, rf.currentTerm, true
	}
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
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {

	rf.mu.Lock()
	if rf.state == STATE_LEADER {
		log.Panic("Ticker started by leader\n")
	}
	rf.mu.Unlock()

	for !rf.killed() {

		election := rand.Intn(500) + 500
		time.Sleep(time.Duration(election) * time.Millisecond)
		rf.mu.Lock()

		if rf.state != STATE_LEADER {
			rf.print("Not leader %+v\n", time.Since(rf.lastHeartBeat))

			if time.Since(rf.lastHeartBeat) < time.Duration(election)*time.Millisecond { // compare start time to heartbeat time, if greater than election time out {
				rf.print("Heartbeat recieved \n")
				rf.mu.Unlock()

			} else {
				rf.print("New candidate \n")
				rf.currentTerm++
				rf.state = STATE_CANDIDATE
				rf.votesReceived = 1

				for xcheck := 0; xcheck < len(rf.peers); xcheck++ {
					if xcheck != rf.me {
						rf.print("GOING INTO VOTE", rf.me, "rf.me", rf.currentTerm, "rf.currentTerm")
						go rf.requestAndProcessVote(xcheck)

						// count up the votes given to this server in request and process vote
					} else { //this should be when the server votes for itself, dont need to vote for self because i lready set it to 1
						rf.votedFor = rf.me
					}
				}
				rf.mu.Unlock()

			}
		} else {
			rf.print("Leader unlock\n") //POTENTIAL ERROR??????
			rf.mu.Unlock()
		}

	}
}

//structs for append entries I added

type Entry struct {
	Term    int
	Command interface{}
}

type AppendEntriesArgs struct { //only leader send out append entries
	Term         int     //leader term
	LeaderId     int     //so follower can redirect client
	PrevLogIndex int     // index of log entry immediately preceding new ones
	PrevLogTerm  int     // term of prevLogIndex entry
	Entries      []Entry // log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int     //leader’s commitIndex

}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

// New function I will be implementing is AppendEntries, created from Figure 2
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		reply.Success = false
		return

	} else if args.Term > rf.currentTerm {
		// rf.currentTerm = args.Term
		rf.state = STATE_FOLLOWER
		rf.votedFor = -1
		rf.votesReceived = -1
		//reply.Success = true
		//rf.lastHeartBeat = time.Now()
	} // else {
	if len(args.Entries) > 0 { //when theres something in args

		//if log doesnt contain entry at prevLogIndex = prevLogTerm
		// 	find position correspding to prevlogindex and prevlogterm, remove everything after this new parameter, then insert this into the entries
		//		reply reply.Success = false
		if rf.log[args.PrevLogIndex] == args.PrevLogTerm {
			rf.log = append(rf.log, args.Entries)
		} else {
			reply.Success = false
			//MAYBE RETURN???????
		}
		// if existing entry conflicts with new one ? it says same index but different terms, idk
		// 		delete existing entry and all that follow it?
		
		// if/for new entry not in log?
		// 		use append function to append to log

		//parts 2-5 in paper rpc
		//applying is the apend func

		if args.LeaderCommit > rf.commitIndex {
			rf.commitIndex == min(args.LeaderCommit, args.PrevLogIndex)
		}
		//		commitIndex == min(leaderCommit, index of last new entry)
	}
	reply.Success = true
	rf.currentTerm = args.Term
	rf.lastHeartBeat = time.Now()
	//}

	
	// ???

}

// MAYBE HELPER FUNCTION: LEADER BROADCASTS HEARTBEATS TO OTHER PPERS, HEARTBEATS ARE EMPTY APPEND REQUESTS
func (rf *Raft) AppendEntryOut() {
	for {
		rf.mu.Lock()
		if rf.state != STATE_LEADER {
			rf.mu.Unlock()
			return //break out of for loop if it isnt a leader
		}
		//FOR LOOP CHECK IF IT IS EQUAL TO I
		for ycheck := 0; ycheck < len(rf.peers); ycheck++ {
			if ycheck != rf.me {
				// IF STATEMENT BEFORE GO FUNC
				
				//IF FOLLOWER BEHIND in its index, INCLUDE MISSING ENTIRES. example IF YOU ARE AT 10, THEIR NEXT NDEX AT 3, SEND THEM



				//HEARTBEAT
				//This is the case where heartbeat are sent
				go func(idx int) {
					rf.mu.Lock()
					rf.print("idx func")
					args := &AppendEntriesArgs{rf.currentTerm, 0, 0, 0, nil, 0}
					reply := &AppendEntriesReply{0, false}
					rf.mu.Unlock()
					ok := rf.sendAppendEntries(idx, args, reply) //sends an empty append entry
					rf.mu.Lock()
					rf.print(reply.Term, "APPEND ENTRIE REPLY")
					if !ok || rf.state != STATE_LEADER {
						rf.print("rf.SendAppendEntries OK IS FALSE")
						rf.mu.Unlock()
						return
					}

					rf.print("Heartbeat rf.currentTerm", rf.currentTerm, reply.Term)
					// check the term of reply

					if reply.Term > rf.currentTerm {
						rf.print("Become follower in Heartbeat")
						rf.currentTerm = reply.Term
						rf.state = STATE_FOLLOWER
						rf.votedFor = -1
						rf.votesReceived = -1
					} else if reply.Success == false{
						nextIndex[idx]-- //DECREMENT NEXT INDEX OF IDX??? TA SAID TO DO THIS BUT IDK WHY OR IF THIS FORMAT MAKES SENSE
					}
					
					if reply.Success {
						rf.print("Heartbeat Success " + "id=" + fmt.Sprintf("%d", idx))
						rf.lastHeartBeat = time.Now()
					}
					rf.mu.Unlock()
				}(ycheck)
			}
		}

		rf.mu.Unlock()
		time.Sleep(HEARTBEAT_INTERVAL)
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
	fmt.Println("Make Start ", me)
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	// rf.mu = &myLock{
	// 	p:  rf,
	// 	mu: sync.Mutex{},
	// }
	rf.mu = &sync.Mutex{}
	//maybe initialize candidate id idk that doesnt make sense for rf

	// Your initialization code here (2A, 2B, 2C).
	rf.votedFor = -1

	rf.state = STATE_FOLLOWER
	//was told that current Term being 0 makes sense
	rf.currentTerm = 0

	rf.votesReceived = -1

	rf.applyCh = applyCh

	rf.commitIndex = 0
	rf.lastApplied = 0

	//election timer, idk if it sould be in make?
	rf.lastHeartBeat = time.Now()

	rf.log = append(rf.log, Entry{Term: 0, Command: 0})
	msg := ApplyMsg{
		CommandValid: true,
		Command:      rf.log[0].Command,
		CommandIndex: 0,
	}
	rf.applyCh <- msg

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
