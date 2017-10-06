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

import "sync"
import "labrpc"
import "bytes"
import "encoding/gob"
import "time"
import "math/rand"
import "fmt"

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

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// Persistent state on all servers
	currentTerm int
	votedFor    int
	log         []Log

	// Volatile state on all servers
	commitIndex int
	lastApplied int

	// Volatile state on leaders
	nextIndex  []int
	matchIndex []int

	// state added by me to assist with implementation.
	// TODO: check if really need all or can get away with a few of them ?
	electionTimeout *time.Timer
	isLeader        bool
	hasLeader       bool
}

type Log struct {
	Command interface{}
	Term    int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.currentTerm
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
	// Your code here (2C).
	// Example:
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.log)
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
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
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		rf.mu.Unlock()
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}

	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		// Check to see if the requester's log is atleast as upto date as me.
		requesterLogcheck := false
		myLastLogIndex := len(rf.log) - 1
		myLastLogTerm := -1
		if myLastLogIndex >= 0 {
			myLastLogTerm = rf.log[myLastLogIndex].Term
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
			reply.Term = rf.currentTerm
			reply.VoteGranted = true
			rf.votedFor = args.CandidateId
			rf.electionTimeout.Reset(time.Millisecond * time.Duration((800 + rf.me*rand.Intn(800))))
		} else {
			reply.Term = rf.currentTerm
			reply.VoteGranted = false
		}
	} else {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
	}
	rf.mu.Unlock()
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

type temp struct {
	Command int
	Term    int
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
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// TODO: Handle the race condition when this node is requesting vote itself
	rf.mu.Lock()
	//If current term greater than the one at leader, reply false & return
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		rf.mu.Unlock()
		return
	}

	// Update the state based on the args received
	rf.isLeader = false
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}
	rf.hasLeader = true
	// TODO: check the best position for this reset statement.
	rf.electionTimeout.Reset(time.Millisecond * time.Duration((700 + rand.Intn(800))))

	// Handling the case when there are no entries in the leader log and this is just a heartbeat.
	if args.PrevLogIndex == -1 {
		reply.Success = true
		reply.Term = rf.currentTerm
		rf.mu.Unlock()
		return
	}

	// check if the entry at previous index is correct.
	if args.PrevLogIndex > len(rf.log)-1 {
		reply.Term = rf.currentTerm
		reply.Success = false
		rf.mu.Unlock()
		return
	}
	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		rf.mu.Unlock()
		return
	}

	// check if an existing entry conflicts with the new one.
	// TODO: check if this & next section can be refactored... ideally might not need the top section.
	if len(args.Entries) != 0 {
		if len(rf.log) > args.PrevLogIndex+1 {
			if rf.log[args.PrevLogIndex+1].Term != args.Entries[0].Term {
				//TODO: check if implemented correctly.
				rf.log = rf.log[:args.PrevLogIndex+1]
			}
		}
	}

	// append new entries not already in the log.
	for i := 0; i < len(args.Entries); i++ {
		if len(rf.log) > args.PrevLogIndex+1+i {
			rf.log[args.PrevLogIndex+1+i] = args.Entries[i]
		} else {
			rf.log = append(rf.log, args.Entries[i])
		}
	}

	// set commitIndex.
	if args.LeaderCommit > rf.commitIndex {
		if args.LeaderCommit > len(rf.log)-1 {
			rf.commitIndex = len(rf.log) - 1
		} else {
			rf.commitIndex = args.LeaderCommit
		}
	}

	reply.Success = true
	reply.Term = rf.currentTerm
	rf.mu.Unlock()
	return
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
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
	rf.mu.Lock()
	index := len(rf.log)
	term := rf.currentTerm
	isLeader := rf.isLeader
	rf.mu.Unlock()
	if !isLeader {
		return index, term, isLeader
	} else {
		// Start the agreement now for this entry.
		rf.mu.Lock()
		rf.log = append(rf.log, Log{Command: command, Term: rf.currentTerm})
		rf.mu.Unlock()
		go rf.startAgreement()
		return index, term, isLeader
	}
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

func (rf *Raft) startAgreement() {
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go func(i int) {
			for {
				rf.mu.Lock()
				if !rf.isLeader {
					rf.mu.Unlock()
					return
				}

				if rf.nextIndex[i] > (len(rf.log) - 1) {
					//Entries already upto date at the follower, move on
					return
				}
				prevLogIndex := rf.nextIndex[i] - 1
				prevLogTerm := rf.log[prevLogIndex].Term
				//TODO: check the entries logic; should we just do one entry at a time ?
				entries := make([]Log, (len(rf.log)-1)-prevLogIndex)
				copy(entries, rf.log[rf.nextIndex[i]:])
				arg := &AppendEntriesArgs{Term: rf.currentTerm, LeaderId: rf.me,
					PrevLogIndex: prevLogIndex, PrevLogTerm: prevLogTerm,
					Entries: entries, LeaderCommit: rf.commitIndex}
				rf.mu.Unlock()

				reply := &AppendEntriesReply{}
				ok := rf.sendAppendEntries(i, arg, reply)
				if !ok {
					// Node unreachable, continue indefinitely till it is up
					continue
				}

				// TODO: Handle stale rpcs
				if reply.Success {
					//TODO: check for race condition when while this go routine we accept another command and append it to leader log
					//TODO: make sure there is only one startAgreement goroutine at a time ?
					rf.mu.Lock()
					rf.nextIndex[i] = len(rf.log)
					rf.matchIndex[i] = len(rf.log)
					rf.mu.Unlock()
					return
				}
				// Handling the false case.
				rf.mu.Lock()
				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.votedFor = -1
					rf.isLeader = false
					rf.hasLeader = false
					//TODO: check for commitIndex, lastApplied and log initialization ?
					//TODO: should we just return at this point ?
					rf.mu.Unlock()
					return
				} else {
					// In this case, the prev entry is not correct, try again by decrementing the nextIndex
					rf.nextIndex[i]--
				}
				rf.mu.Unlock()
			}
		}(i)
	}
}

func (rf *Raft) checkHeartbeat() {
	for {
		<-rf.electionTimeout.C
		// TODO : THIS CAN STILL RACE WITH APPEND ENTRIES (if append entries set
		// hasLeader to true between the execution after the last and the before
		// the next), BUT THIS CAN STILL BE OKAY (this is equivalent to saying that
		// the next election timeout happened immediately somehow, does not impact
		// the correctness really. Although frequently hitting this case might
		// cause unneeded leader elections.
		rf.hasLeader = false

		// As the Leader doesn't send heartbeat to itself, if it is leader,
		// we can ignore this
		if !rf.isLeader {
		restart:
			// Will now become candidate and requestvotes.
			// TODO: can possible check for hasLeader again here..
			rf.mu.Lock()
			rf.currentTerm++
			rf.votedFor = rf.me

			rf.electionTimeout.Reset(time.Millisecond * time.Duration((700 +
				rand.Intn(800))))
			// TODO: check for the race when the lastlogIndex & lastLogTerm changes
			// in between due to an appendEntries call. Is this a problem ?
			myLastLogIndex := len(rf.log) - 1
			myLastLogTerm := -1
			if myLastLogIndex >= 0 {
				myLastLogTerm = rf.log[myLastLogIndex].Term
			}
			arg := &RequestVoteArgs{Term: rf.currentTerm, CandidateId: rf.me,
				LastLogIndex: myLastLogIndex, LastLogTerm: myLastLogTerm}
			rf.mu.Unlock()

			votesRecvd := 1
			//fmt.Printf("In peer: %d, currentTerm = %d\n", rf.me, rf.currentTerm)

			// Randomly pick nodes to send requestVote RPC.
			perm := rand.Perm(len(rf.peers))
			for _, i := range perm {
				if i == rf.me {
					continue
				}
				reply := &RequestVoteReply{}

				// As the rpc implement timeouts, call should eventually return
				ok := rf.sendRequestVote(i, arg, reply)
				if !ok {
					continue
				}

				// If the timeout occurs, start the election process again.
				select {
				case <-rf.electionTimeout.C:
					goto restart
				default:
				}

				// To prevent Term Confusion (because of stale rpcs)
				if arg.Term != rf.currentTerm {
					goto endf
				}

				rf.mu.Lock()
				if rf.hasLeader {
					// This means that we got an AppendEntries RPC,
					// thus should convert to follower
					rf.mu.Unlock()
					goto endf
				}
				rf.mu.Unlock()

				if reply.VoteGranted == false {
					rf.mu.Lock()
					if reply.Term > rf.currentTerm {
						rf.currentTerm = reply.Term
						rf.votedFor = -1
					}
					rf.mu.Unlock()
					goto endf
				} else {
					votesRecvd++
				}
			}
			// Check if got mjority votes, if yes, become leader
			if votesRecvd >= ((len(rf.peers) / 2) + 1) {
				// Received majority votes, can declare myself as leader and
				// start sending heartbeats to others
				rf.mu.Lock()
				rf.isLeader = true
				for i := 0; i < len(rf.peers); i++ {
					rf.nextIndex[i] = len(rf.log)
					rf.matchIndex[i] = 0
				}
				// TODO: What should be hasLeader if it becomes leader. Can the
				// old value of hasLeader have an impact in the future.
				rf.mu.Unlock()
			}
		}
	endf:
	}
	fmt.Printf("Exiting checkheartbeat in peer %d\n", rf.me)
}

func (rf *Raft) sendHeartbeat() {
	// TODO: should the leader send heartbeat to itself ? Ideally, should not
	ticker := time.NewTicker(time.Millisecond * 150)
	for _ = range ticker.C {
		if rf.isLeader {
			for i := 0; i < len(rf.peers); i++ {
				if i == rf.me {
					continue
				}

				rf.mu.Lock()

				prevLogIndex := rf.nextIndex[i] - 1
				prevLogTerm := 0
				if prevLogIndex != -1 {
					prevLogTerm = rf.log[prevLogIndex].Term
				}
				entries := make([]Log, 0)
				arg := &AppendEntriesArgs{Term: rf.currentTerm, LeaderId: rf.me,
					PrevLogIndex: prevLogIndex, PrevLogTerm: prevLogTerm,
					Entries: entries, LeaderCommit: rf.commitIndex}

				rf.mu.Unlock()

				reply := &AppendEntriesReply{}
				ok := rf.sendAppendEntries(i, arg, reply)
				if !ok {
					// Node unreachable, move on to the next
					continue
				}
				// TODO: Handle stale rpcs
				// TODO: Handle reply correctly
				rf.mu.Lock()
				if !reply.Success {
					if reply.Term > rf.currentTerm {
						rf.currentTerm = reply.Term
						rf.votedFor = -1
						rf.isLeader = false
						rf.hasLeader = false
						rf.mu.Unlock()
						break
					} else {
						// This means that prevLog Index or term didn't match.
						// Thus decrement the nextIndex. TODO: should we check
						// recursively also ? Might not be reqd as this is just
						// a heartbeat.
						rf.nextIndex[i]--
					}
				}
				rf.mu.Unlock()
			}

			// Logic to increment commitIndex at the Leader
			// TODO: check if this is the right place to do this
			if rf.isLeader {
				rf.mu.Lock()
				for i := len(rf.log) - 1; i > rf.commitIndex; i-- {
					// TODO: check this
					if rf.log[i].Term != rf.currentTerm {
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
				rf.mu.Unlock()
			}
		}
	}
}

func (rf *Raft) applyMsg(applyCh chan ApplyMsg) {
	// TODO: check if the channel should be passed by value ?
	ticker := time.NewTicker(time.Millisecond * 250)
	for _ = range ticker.C {
		if rf.commitIndex > rf.lastApplied {
			rf.mu.Lock()
			rf.lastApplied++
			arg := ApplyMsg{Index: rf.log[rf.lastApplied].Term,
				Command: rf.log[rf.lastApplied].Command}
			rf.mu.Unlock()
			applyCh <- arg
		}
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

	//TODO: mmove this back later
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.votedFor = -1
	//TODO: do something for log
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	// TODO: check for timeout and isleader
	// TODO check this:
	// Heartbeat to be sent every 150 ms (since no more than 10 per second)
	// Thus, eleaction timout should be randomly chosen each time between
	// (350-800 ms); need to elect a leader within 5 secs
	rf.electionTimeout = time.NewTimer(time.Millisecond * time.Duration((700 +
		rand.Intn(800))))
	rf.isLeader = false
	rf.hasLeader = false

	go rf.sendHeartbeat()
	go rf.checkHeartbeat()
	// TODO: check if we need a seperate function for this or can this be merge with any of the above two
	go rf.applyMsg(applyCh)
	return rf
}

//TODO: FIXES TO DO:

// 2(A)
// Try to eliminate the hasLeader variable all together
// Add more printfs for debugging
// Read the TA blog again...
// Understand the rpc code again.
// Try testing and checking the behaviour of timer again ?? replace this with something else

// 2(B)
// sendHeartbeat should ideally not be invoked when startAgreement has made rpcs... check a way to handle this
// Make sure that all the state gets correctly initialized on leader changes or if become a follower
// see how to send stuff to the applyCh
// should the leader also notify followers again once the entry is commited so that they too can send it on their applyCh ?
// see how to apply things to state machine and increment lastApplied
// should leader periodically keep sending next entries to followers (in the heartbeat ??)
// add logic to increment commitIndex and matchIndex
