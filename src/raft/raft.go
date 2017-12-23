package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new Log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the Log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import "sync"
import "labrpc"
import "bytes"
import "encoding/gob"
import "time"
import "math/rand"
import "fmt"

//
// as each Raft peer becomes aware that successive Log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Persistent state on all servers
	CurrentTerm int
	VotedFor    int
	Log         []Log

	// Volatile state on all servers
	commitIndex int
	lastApplied int

	// Volatile state on leaders
	nextIndex  []int
	matchIndex []int

	// Additional state added by me to assist with implementation.
	electionTimeout    *time.Timer
	isLeader           bool
	electionTimeoutVal int
	killAllGoRoutines  bool
}

type Log struct {
	Command interface{}
	Term    int
}

// return CurrentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	rf.mu.Lock()
	term = rf.CurrentTerm
	isleader = rf.isLeader
	rf.mu.Unlock()
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	rf.mu.Lock()
	e.Encode(rf.CurrentTerm)
	e.Encode(rf.VotedFor)
	e.Encode(rf.Log)
	rf.mu.Unlock()
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	rf.mu.Lock()
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.CurrentTerm)
	d.Decode(&rf.VotedFor)
	d.Decode(&rf.Log)
	if data == nil || len(data) < 1 { // bootstrap without any state?
		rf.CurrentTerm = 0
		rf.VotedFor = -1
		rf.mu.Unlock()
		return
	}
	rf.mu.Unlock()
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
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
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	if args.Term < rf.CurrentTerm {
		reply.Term = rf.CurrentTerm
		reply.VoteGranted = false
		rf.mu.Unlock()
		rf.persist()
		return
	}

	if args.Term > rf.CurrentTerm {
		rf.CurrentTerm = args.Term
		rf.VotedFor = -1
	}

	if rf.VotedFor == -1 || rf.VotedFor == args.CandidateId {
		// Check to see if the requester's Log is atleast as upto date as me.
		requesterLogcheck := false
		myLastLogIndex := len(rf.Log) - 1
		myLastLogTerm := -1
		if myLastLogIndex >= 0 {
			myLastLogTerm = rf.Log[myLastLogIndex].Term
		}
		if args.LastLogTerm > myLastLogTerm {
			requesterLogcheck = true
		}
		if args.LastLogTerm == myLastLogTerm {
			if args.LastLogIndex >= myLastLogIndex {
				requesterLogcheck = true
			}
		}

		if requesterLogcheck == true {
			reply.Term = rf.CurrentTerm
			reply.VoteGranted = true
			rf.VotedFor = args.CandidateId
			//fmt.Println("Voted for: ", rf.VotedFor, " me:", rf.me)
			rf.electionTimeout.Reset(time.Millisecond *
				time.Duration(rf.electionTimeoutVal))
		} else {
			reply.Term = rf.CurrentTerm
			reply.VoteGranted = false
		}
	} else {
		reply.Term = rf.CurrentTerm
		reply.VoteGranted = false
	}
	rf.mu.Unlock()
	rf.persist()
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs,
	reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) electLeader() {
	for {
		<-rf.electionTimeout.C
	retry:
		rf.mu.Lock()
		isLeader := rf.isLeader
		killAllGoRoutines := rf.killAllGoRoutines
		rf.mu.Unlock()
		if killAllGoRoutines {
			return
		}
		// As the Leader doesn't send heartbeat to itself, if it is leader,
		// we can ignore this
		if !isLeader {
			// Will now become candidate and requestvotes.
			rf.mu.Lock()
			rf.CurrentTerm++
			rf.VotedFor = rf.me

			rf.electionTimeout.Reset(time.Millisecond *
				time.Duration(rf.electionTimeoutVal))

			myLastLogIndex := len(rf.Log) - 1
			myLastLogTerm := -1
			if myLastLogIndex >= 0 {
				myLastLogTerm = rf.Log[myLastLogIndex].Term
			}
			arg := &RequestVoteArgs{Term: rf.CurrentTerm, CandidateId: rf.me,
				LastLogIndex: myLastLogIndex, LastLogTerm: myLastLogTerm}
			rf.mu.Unlock()

			//fmt.Printf("In peer: %d, CurrentTerm = %d  at:%v\n", rf.me,
			// rf.CurrentTerm,time.Now())
			votesRecvd := 1
			out := make(chan bool, len(rf.peers))
			var shouldBreak bool
			for i := 0; i < len(rf.peers); i++ {
				if i == rf.me {
					continue
				}
				go func(i int) {
					reply := &RequestVoteReply{}

					ok := rf.sendRequestVote(i, arg, reply)
					if !ok {
						//fmt.Println("failed requestvote at ",rf.me," for:",i,
						//"term:",rf.CurrentTerm, " at:",time.Now())
						out <- ok
						return
					}

					// To prevent Term Confusion (because of stale rpcs)
					rf.mu.Lock()
					if arg.Term != rf.CurrentTerm {
						//fmt.Println("TERM CONFUSION at ",rf.me," for:",i)
						shouldBreak = true
						rf.mu.Unlock()
						out <- ok
						return
					}

					if rf.VotedFor != rf.me {
						// This means that we got an AppendEntries RPC,
						// thus should convert to follower
						shouldBreak = true
						rf.mu.Unlock()
						out <- ok
						return
					}

					if reply.VoteGranted == false {
						if reply.Term > rf.CurrentTerm {
							rf.CurrentTerm = reply.Term
							rf.VotedFor = -1
							shouldBreak = true
						}
					} else {
						//fmt.Println("Vote recvd at ",rf.me," from:",i," term:",
						// rf.CurrentTerm)
						votesRecvd++
					}
					rf.mu.Unlock()
					out <- ok
				}(i)
			}
		loop:
			// If the timeout occurs, start the election process again.
			select {
			case <-rf.electionTimeout.C:
				rf.electionTimeout.Reset(time.Millisecond *
					time.Duration(rf.electionTimeoutVal))
				goto retry
			case <-out:
				rf.mu.Lock()
				if shouldBreak {
					rf.mu.Unlock()
					break
				}
				// Check if got majority votes, if yes, become leader
				if votesRecvd >= ((len(rf.peers) / 2) + 1) {
					// Received majority votes, can declare myself as leader and
					// start sending heartbeats to others
					rf.isLeader = true
					//fmt.Println("Leader is ",rf.me," term: ",rf.CurrentTerm,
					//" at:",time.Now())//, "log: ",rf.Log)
					for i := 0; i < len(rf.peers); i++ {
						rf.nextIndex[i] = len(rf.Log)
						rf.matchIndex[i] = 0
					}
					rf.mu.Unlock()
					break
				}
				rf.mu.Unlock()
				goto loop
			}
		} else {
			rf.electionTimeout.Reset(time.Millisecond *
				time.Duration(rf.electionTimeoutVal))
		}
	}
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Log
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term                      int
	Success                   bool
	ConflictingTerm           int
	FirstIndexConflictingTerm int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	//If current term greater than the one at leader, reply false & return
	if args.Term < rf.CurrentTerm {
		reply.Term = rf.CurrentTerm
		reply.Success = false
		rf.mu.Unlock()
		rf.persist()
		return
	}

	// Update the state based on the args received
	rf.isLeader = false
	if args.Term > rf.CurrentTerm {
		rf.CurrentTerm = args.Term
		rf.VotedFor = -1
	}
	//fmt.Println("Got Heartbeat from ",args.LeaderId," for", rf.me," term:",
	// rf.CurrentTerm, " at ",time.Now())
	rf.electionTimeout.Reset(time.Millisecond * time.Duration(rf.electionTimeoutVal))

	// check if the entry at previous index is correct.
	if args.PrevLogIndex > len(rf.Log)-1 {
		reply.ConflictingTerm = -1
		reply.FirstIndexConflictingTerm = len(rf.Log)
		reply.Term = rf.CurrentTerm
		reply.Success = false
		rf.mu.Unlock()
		rf.persist()
		return
	}

	if args.PrevLogIndex >= 0 {
		if rf.Log[args.PrevLogIndex].Term != args.PrevLogTerm {
			reply.ConflictingTerm = rf.Log[args.PrevLogIndex].Term
			var i int
			for i = args.PrevLogIndex; i >= -1; i-- {
				if i < 0 {
					break
				}
				if rf.Log[i].Term != reply.ConflictingTerm {
					break
				}
			}
			reply.FirstIndexConflictingTerm = i + 1
			reply.Term = rf.CurrentTerm
			reply.Success = false
			rf.mu.Unlock()
			rf.persist()
			return
		}
	}

	// check if an existing entry conflicts with the new one and append new
	// entries not already in the Log.
	for i := 0; i < len(args.Entries); i++ {
		if len(rf.Log) > args.PrevLogIndex+1+i {
			if rf.Log[args.PrevLogIndex+1+i].Term != args.Entries[i].Term {
				rf.Log = rf.Log[:args.PrevLogIndex+1+i+1]
			}
			rf.Log[args.PrevLogIndex+1+i] = args.Entries[i]
		} else {
			rf.Log = append(rf.Log, args.Entries[i])
		}
	}

	// set commitIndex.
	if args.LeaderCommit > rf.commitIndex {
		if args.LeaderCommit > (args.PrevLogIndex + len(args.Entries)) {
			rf.commitIndex = args.PrevLogIndex + len(args.Entries)
		} else {
			rf.commitIndex = args.LeaderCommit
		}
		//fmt.Println("setting commitIndex: ",rf.commitIndex, "at: ", rf.me)
	}

	reply.Success = true
	reply.Term = rf.CurrentTerm
	rf.mu.Unlock()
	rf.persist()
	return
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs,
	reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's Log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft Log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	index := len(rf.Log) + 1
	term := rf.CurrentTerm
	isLeader := rf.isLeader
	rf.mu.Unlock()
	if isLeader {
		// Start the agreement now for this entry.
		rf.mu.Lock()
		//fmt.Println("start: ", command,"me: ", rf.me, "Log len: ", len(rf.Log))
		rf.Log = append(rf.Log, Log{Command: command, Term: rf.CurrentTerm})
		rf.mu.Unlock()
		rf.persist()
		go rf.startAgreement()
	}
	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	rf.mu.Lock()
	fmt.Println("Kill at: ", rf.me, "log length:", len(rf.Log)) //, "log: ", rf.Log)
	rf.killAllGoRoutines = true
	rf.mu.Unlock()
}

func (rf *Raft) startAgreement() {
	for i := 0; i < len(rf.peers); i++ {
		rf.mu.Lock()
		if i == rf.me || rf.nextIndex[i] >= (len(rf.Log)) {
			rf.mu.Unlock()
			continue
		}
		rf.mu.Unlock()
		go func(i int) {
			rf.makeAppendEntriesCall(i)
		}(i)
	}
}

func (rf *Raft) sendHeartbeat() {
	ticker := time.NewTicker(time.Millisecond * 150)
	for _ = range ticker.C {
		rf.mu.Lock()
		isLeader := rf.isLeader
		killAllGoRoutines := rf.killAllGoRoutines
		rf.mu.Unlock()
		if killAllGoRoutines {
			return
		}
		if isLeader {
			for i := 0; i < len(rf.peers); i++ {
				if i == rf.me {
					continue
				}
				go func(i int) {
					rf.makeAppendEntriesCall(i)
				}(i)
			}
		}
	}
}

func (rf *Raft) makeAppendEntriesCall(i int) {
	rf.mu.Lock()
	if !rf.isLeader {
		rf.mu.Unlock()
		return
	}

	prevLogIndex := rf.nextIndex[i] - 1
	prevLogTerm := 0
	if prevLogIndex >= 0 {
		prevLogTerm = rf.Log[prevLogIndex].Term
	}
	entries := make([]Log, (len(rf.Log)-1)-prevLogIndex)
	copy(entries, rf.Log[rf.nextIndex[i]:])
	arg := &AppendEntriesArgs{Term: rf.CurrentTerm, LeaderId: rf.me,
		PrevLogIndex: prevLogIndex, PrevLogTerm: prevLogTerm,
		Entries: entries, LeaderCommit: rf.commitIndex}
	rf.mu.Unlock()

	reply := &AppendEntriesReply{}
	ok := rf.sendAppendEntries(i, arg, reply)
	if !ok {
		// Node unreachable, continue indefinitely till it is up
		return
	}

	// Handling stale rpcs
	rf.mu.Lock()
	if arg.Term != rf.CurrentTerm {
		rf.mu.Unlock()
		return
	}

	if reply.Success {
		rf.nextIndex[i] = arg.PrevLogIndex + len(arg.Entries) + 1
		rf.matchIndex[i] = arg.PrevLogIndex + len(arg.Entries)
		rf.mu.Unlock()
		return
	}
	// Handling the false case.
	if reply.Term > rf.CurrentTerm {
		rf.CurrentTerm = reply.Term
		rf.VotedFor = -1
		rf.isLeader = false
		rf.mu.Unlock()
		return
	}

	// If the previous entry is not correct, then
	// 	- If the receiver log was shorter, set nextIndex to the
	//	  length of the last receiver log entry.
	//	- If there was a mismatch, try to rollback to as many
	//    entries as possible as an optimization.
	if reply.ConflictingTerm == -1 {
		if reply.FirstIndexConflictingTerm < 0 {
			rf.nextIndex[i] = 0
		} else {
			rf.nextIndex[i] = reply.FirstIndexConflictingTerm
		}
	} else {
		var j int
		for j = rf.nextIndex[i] - 1; j >= reply.FirstIndexConflictingTerm-1; j-- {
			if j < 0 {
				break
			}
			if rf.Log[j].Term == reply.ConflictingTerm {
				break
			}
		}
		rf.nextIndex[i] = j + 1
	}
	rf.mu.Unlock()
	return
}

func (rf *Raft) applyMsg(applyCh chan ApplyMsg) {
	ticker := time.NewTicker(time.Millisecond * 100)
	for _ = range ticker.C {
		rf.mu.Lock()
		if rf.killAllGoRoutines {
			rf.mu.Unlock()
			return
		}
		// Logic to increment commitIndex at the Leader.
		if rf.isLeader {
			for i := len(rf.Log) - 1; i > rf.commitIndex; i-- {
				if rf.Log[i].Term != rf.CurrentTerm {
					continue
				}
				majority := 1
				for j := 0; j < len(rf.peers); j++ {
					if j == rf.me {
						continue
					}
					if rf.matchIndex[j] >= i {
						majority++
					}
				}
				if majority > len(rf.peers)/2 {
					rf.commitIndex = i
					break
				}
			}
		}

		for rf.commitIndex > rf.lastApplied {
			rf.lastApplied++
			//fmt.Println("MANAV test", "rf.me: ",rf.me, " ", "rf.commit: ",
			// rf.commitIndex, "rf.lastap: ", rf.lastApplied)
			arg := ApplyMsg{Index: rf.lastApplied + 1,
				Command: rf.Log[rf.lastApplied].Command}
			rf.mu.Unlock()
			applyCh <- arg
			rf.mu.Lock()
		}
		rf.mu.Unlock()
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

	// Initialize from state persisted before a crash.
	rf.readPersist(persister.ReadRaftState())
	fmt.Println("ME: ", rf.me, " CURRENT TERM IS ", rf.CurrentTerm,
		" Voted for is:", rf.VotedFor)

	// Initialize the other state values.
	rf.commitIndex = -1
	rf.lastApplied = -1
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	rf.isLeader = false

	// Heartbeat to be sent every 150 ms (since no more than 10 per second)
	// Thus, eleaction timout should be randomly chosen each time between
	// (200-900 ms); need to elect a leader within 5 secs.
	rand.Seed(int64(rf.me*100000 + 10000))
	rf.electionTimeoutVal = 200 + rand.Intn(700)
	fmt.Println("ELECTION VALUE at ", rf.me, " is:", rf.electionTimeoutVal)
	rf.electionTimeout = time.NewTimer(time.Millisecond *
		time.Duration(rf.electionTimeoutVal))

	rf.killAllGoRoutines = false
	go rf.sendHeartbeat()
	go rf.electLeader()
	go rf.applyMsg(applyCh)
	return rf
}
