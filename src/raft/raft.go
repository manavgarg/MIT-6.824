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

	// additional state added by me to assist with implementation.
	// TODO: check if really need all or can get away with a few of them ?
	electionTimeout *time.Timer
	isLeader        bool
	// hasLeader is only required for the edge case: when a node is requesting
	// votes and it receives an appendEntry with the same term. In such a case,
	// just checking rf.votedFor would not be sufficient, as it would have voted
	// for itself and not the leader.
	hasLeader          bool
	electionTimeoutVal int
}

type Log struct {
	Command interface{}
	Term    int
}

var killAllGoRoutines bool

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
		//fmt.Println("At: ",rf.me, " candi:", args.CandidateId," scene 1")
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
			rf.electionTimeout.Reset(time.Millisecond * time.Duration(rf.electionTimeoutVal))
			//fmt.Println("At: ",rf.me, " candi:", args.CandidateId," scene 2")
		} else {
			reply.Term = rf.currentTerm
			reply.VoteGranted = false
			//fmt.Println("At: ",rf.me, " candi:", args.CandidateId," scene 3")
		}
	} else {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		//fmt.Println("At: ",rf.me, " candi:", args.CandidateId,"voted for: ", rf.me," scene 4")
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
	//fmt.Println("Got Heartbeat from ",args.LeaderId," for", rf.me," term:", rf.currentTerm, " at ",time.Now())
	rf.electionTimeout.Reset(time.Millisecond * time.Duration(rf.electionTimeoutVal))
	rf.hasLeader = true

	// check if the entry at previous index is correct.
	if args.PrevLogIndex > len(rf.log)-1 {
		reply.Term = rf.currentTerm
		reply.Success = false
		rf.mu.Unlock()
		return
	}

	if args.PrevLogIndex >= 0 {
		if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
			reply.Term = rf.currentTerm
			reply.Success = false
			rf.mu.Unlock()
			return
		}
	}

	// check if an existing entry conflicts with the new one and append new
	// entries not already in the log.
	for i := 0; i < len(args.Entries); i++ {
		if len(rf.log) > args.PrevLogIndex+1+i {
			if rf.log[args.PrevLogIndex+1+i].Term != args.Entries[i].Term {
				rf.log = rf.log[:args.PrevLogIndex+1+i+1]
			}
			rf.log[args.PrevLogIndex+1+i] = args.Entries[i]
		} else {
			rf.log = append(rf.log, args.Entries[i])
		}
	}

	// set commitIndex.
	if args.LeaderCommit > rf.commitIndex {
		if args.LeaderCommit > (args.PrevLogIndex + len(args.Entries)) {
			rf.commitIndex = args.PrevLogIndex + len(args.Entries)
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
	index := len(rf.log) + 1
	term := rf.currentTerm
	isLeader := rf.isLeader
	rf.mu.Unlock()
	if isLeader {
		// Start the agreement now for this entry.
		rf.mu.Lock()
		//fmt.Println("start: ", command,"me: ", rf.me, "log len: ", len(rf.log))
		rf.log = append(rf.log, Log{Command: command, Term: rf.currentTerm})
		rf.mu.Unlock()
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
	// Your code here, if desired.
	fmt.Println(rf.log)
	killAllGoRoutines = true
	time.Sleep(5 * time.Second)
}

func (rf *Raft) startAgreement() {
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go func(i int) {
			// Keep on trying indefinitely till the log entry is replicated on
			// the node.
			for {
				rf.mu.Lock()
				if !rf.isLeader {
					rf.mu.Unlock()
					return
				}

				if rf.nextIndex[i] > (len(rf.log)) {
					//Entries already upto date at the follower, move on
					rf.mu.Unlock()
					return
				}

				prevLogIndex := rf.nextIndex[i] - 1
				prevLogTerm := 0

				if prevLogIndex >= 0 {
					prevLogTerm = rf.log[prevLogIndex].Term
				}
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

				// Handling stale rpcs
				if arg.Term != rf.currentTerm {
					return
				}

				if reply.Success {
					//TODO: check for race condition when while this go routine we accept another command and append it to leader log
					//TODO: make sure there is only one startAgreement goroutine at a time ?
					rf.mu.Lock()
					rf.nextIndex[i] = arg.PrevLogIndex + len(arg.Entries) + 1
					rf.matchIndex[i] = arg.PrevLogIndex + len(arg.Entries)
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

func (rf *Raft) electLeader() {
	for {
		if killAllGoRoutines {
			return
		}
		<-rf.electionTimeout.C

		// As the Leader doesn't send heartbeat to itself, if it is leader,
		// we can ignore this
		if !rf.isLeader {
			// Will now become candidate and requestvotes.
			rf.mu.Lock()
			rf.currentTerm++
			rf.hasLeader = false
			rf.votedFor = rf.me

			rf.electionTimeout.Reset(time.Millisecond * time.Duration(rf.electionTimeoutVal))

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
			rand.Seed(int64(rf.me*100000 + 10000))
			perm := rand.Perm(len(rf.peers))
			for _, i := range perm {
				if i == rf.me {
					continue
				}
				reply := &RequestVoteReply{}

				var ok bool
				out := make(chan bool, 1)
				go func() {
					res := rf.sendRequestVote(i, arg, reply)
					out <- res
				}()

				// If the timeout occurs, start the election process again.
				select {
				case <-rf.electionTimeout.C:
					rf.electionTimeout.Reset(time.Millisecond * time.Duration(rf.electionTimeoutVal))
					goto endf
				case ok = <-out:
				}

				//fmt.Println("Starting Leader election at ",rf.me," for:",i," term:",rf.currentTerm, " at:",time.Now())
				if !ok {
					//fmt.Println("failed requestvote at ",rf.me," for:",i," term:",rf.currentTerm, " at:",time.Now())
					continue
				}

				// To prevent Term Confusion (because of stale rpcs)
				if arg.Term != rf.currentTerm {
					goto endf
				}

				rf.mu.Lock()
				if rf.hasLeader || (rf.votedFor != rf.me) {
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
					rf.electionTimeout.Reset(time.Millisecond * time.Duration(rf.electionTimeoutVal))
					goto endf
				} else {
					//fmt.Println("Vote recvd at ",rf.me," from:",i," term:",rf.currentTerm)
					votesRecvd++
				}

				if votesRecvd >= ((len(rf.peers) / 2) + 1) {
					break
				}
			}
			// Check if got majority votes, if yes, become leader
			if votesRecvd >= ((len(rf.peers) / 2) + 1) {
				// Received majority votes, can declare myself as leader and
				// start sending heartbeats to others
				rf.mu.Lock()
				rf.isLeader = true
				//fmt.Println("Leader is ",rf.me," term: ",rf.currentTerm)
				for i := 0; i < len(rf.peers); i++ {
					rf.nextIndex[i] = len(rf.log)
					rf.matchIndex[i] = 0
				}
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
		if killAllGoRoutines {
			return
		}
		if rf.isLeader {
			//fmt.Println("ENTERING HEARTBEAT for ", rf.me)
			for i := 0; i < len(rf.peers); i++ {
				if i == rf.me {
					continue
				}

				go func(i int) {
					rf.mu.Lock()

					prevLogIndex := rf.nextIndex[i] - 1
					prevLogTerm := 0
					if prevLogIndex > -1 {
						prevLogTerm = rf.log[prevLogIndex].Term
					}
					entries := make([]Log, 0)
					arg := &AppendEntriesArgs{Term: rf.currentTerm, LeaderId: rf.me,
						PrevLogIndex: prevLogIndex, PrevLogTerm: prevLogTerm,
						Entries: entries, LeaderCommit: rf.commitIndex}

					rf.mu.Unlock()

					reply := &AppendEntriesReply{}
					//fmt.Println("sending heartbeat at ",rf.me," for", i, " term: ", rf.currentTerm," at ",time.Now())
					ok := rf.sendAppendEntries(i, arg, reply)
					if !ok {
						// Node unreachable so just return
						return
					}
					//fmt.Println("getiing heartbeat reply at ",rf.me," for", i, " term: ", rf.currentTerm," at ",time.Now())

					// Handle stale rpcs
					if arg.Term != rf.currentTerm {
						return
					}

					rf.mu.Lock()
					if !reply.Success {
						if reply.Term > rf.currentTerm {
							rf.currentTerm = reply.Term
							rf.votedFor = -1
							rf.isLeader = false
							rf.hasLeader = false
							rf.mu.Unlock()
							return
						} else {
							// This means that prevLog Index or term didn't match.
							// Thus decrement the nextIndex. TODO: should we check
							// recursively also ? Might not be reqd as this is just
							// a heartbeat.
							rf.nextIndex[i]--
						}
					}
					rf.mu.Unlock()
				}(i)
			}
		}
	}
}

func (rf *Raft) applyMsg(applyCh chan ApplyMsg) {
	ticker := time.NewTicker(time.Millisecond * 100)
	for _ = range ticker.C {
		if killAllGoRoutines {
			return
		}

		// Logic to increment commitIndex at the Leader
		if rf.isLeader {
			rf.mu.Lock()
			for i := len(rf.log) - 1; i > rf.commitIndex; i-- {
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
				//fmt.Println("MANAV: majority", majority, " me:", rf.me, " i:", i, " commitindex:", rf.commitIndex)
				if majority > len(rf.peers)/2 {
					rf.commitIndex = i
					//fmt.Println("MANAV: Increasing commit index: ","rf.me: ", rf.me, "rf.commit: ", rf.commitIndex)
					break
				}
			}
			rf.mu.Unlock()
		}

		//fmt.Println("Total log: %v, rf.me: \n", rf.log, rf.me)
		if rf.commitIndex > rf.lastApplied {
			rf.mu.Lock()
			rf.lastApplied++
			//fmt.Println("MANAV test", "rf.me: ",rf.me, " ", rf.log[rf.lastApplied].Term, rf.log[rf.lastApplied].Command, "rf.commit: ", rf.commitIndex, "rf.lastap: ", rf.lastApplied)
			arg := ApplyMsg{Index: rf.lastApplied + 1,
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
	rf.commitIndex = -1
	rf.lastApplied = -1
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	// TODO check this:
	// Heartbeat to be sent every 150 ms (since no more than 10 per second)
	// Thus, eleaction timout should be randomly chosen each time between
	// (200-900 ms); need to elect a leader within 5 secs
	rand.Seed(int64(rf.me*100000 + 10000))
	rf.electionTimeoutVal = 200 + rand.Intn(700)
	fmt.Println("ELECTION VALUE at ", rf.me, " is:", rf.electionTimeoutVal)
	rf.electionTimeout = time.NewTimer(time.Millisecond * time.Duration(rf.electionTimeoutVal))
	rf.isLeader = false
	rf.hasLeader = false

	killAllGoRoutines = false
	go rf.sendHeartbeat()
	go rf.electLeader()
	go rf.applyMsg(applyCh)
	return rf
}

// TODO: (Further Design Improvements till 2B):
// 1. See if the electionTimeut values can be more streamlined and well defined.
// 2. See if the requesvote logic in electLeader can be improved. Can it be done in parallel and not break once the majority is received ?
// 3. See if we can get away with hasLeaser.
// 4. Check the apppendEntries logic for log matching and appending.
