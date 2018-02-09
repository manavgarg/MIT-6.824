package raftkv

import (
	"encoding/gob"
	"fmt"
	"labrpc"
	"log"
	"raft"
	"sync"
	"time"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key       string
	Value     string
	Op        string
	Err       Err
	ClientId  int64
	RequestNo int64
}

type channelMapVal struct {
	retChan chan Op
	index   int
}

type RaftKV struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	channelMap           map[int64]*channelMapVal
	kvStore              map[string]string
	clientLastRequestMap map[int64]Op
}

func (kv *RaftKV) listenApplyCh() {
	for {
		res := <-kv.applyCh
		command := res.Command.(Op)
		if command.Op == "Get" {
			if val, ok := kv.kvStore[command.Key]; ok {
				command.Value = val
				command.Err = OK
			} else {
				command.Err = ErrNoKey
			}
		} else {
			if command.Op == "Put" {
				// This check here and in the else case is required when raft
				// leader is able to replicate entries but then looses leadership
				// (term changes). The next leader will not commit entries from
				// old term (even if replicated). Thus, the client would be
				// waiting and eventually retry. Now, when this duplicated
				// request is committed, the original is also committed. Thus,
				// we will see the same entry applied twice (Get is idempotent).
				val, ok := kv.clientLastRequestMap[command.ClientId]
				if !ok || val.RequestNo != command.RequestNo {
					kv.kvStore[command.Key] = command.Value
				}
			} else {
				val, ok := kv.clientLastRequestMap[command.ClientId]
				if !ok || val.RequestNo != command.RequestNo {
					kv.kvStore[command.Key] += command.Value
				}
			}
			command.Err = OK
		}
		kv.mu.Lock()
		kv.clientLastRequestMap[command.ClientId] = command

		val, ok := kv.channelMap[command.ClientId]
		if !ok || res.Index != val.index {
			// This means that either this is not a leader (thus it cannot/doesnot
			// need to write to channel as no client is waiting) or this is a new
			// request by client (previous one timedout maybe).
			kv.mu.Unlock()
			continue
		}
		writeChan := val.retChan
		writeChan <- command
		kv.mu.Unlock()
	}
}

func (kv *RaftKV) preProcess(ClientId int64, RequestNo int64) (bool, Err, string) {
	kv.mu.Lock()
	// Handling duplicate requests (i.e committed requests should not be tried
	// again). However, it does not protect against the case in which the client
	// retries too soon again before actually the entry is committed (which is
	// okay as one client only makes on request at a time).
	val, ok := kv.clientLastRequestMap[ClientId]
	if ok && val.RequestNo == RequestNo {
		// This is a duplicate request, ignore it.
		kv.mu.Unlock()
		return true, val.Err, val.Value
	}

	kv.channelMap[ClientId] = &channelMapVal{retChan: make(chan Op),
		index: -1}
	kv.mu.Unlock()
	return false, "", ""
}

func (kv *RaftKV) postProcess(ClientId int64, index int, term int) (Err, string) {
	kv.mu.Lock()
	kv.channelMap[ClientId].index = index
	readChan := kv.channelMap[ClientId].retChan
	kv.mu.Unlock()
	var res Op
tryAgain:
	select {
	case res = <-readChan:
		kv.mu.Lock()
	case <-time.After(time.Second * 1):
		// There is still a small possibility of a deadlock when in listenApplyCh
		// we try to write it (acquire Lock after the above statement). That
		// routine will block writing to channel & this one won't be able to consume.
		kv.mu.Lock()
		newTerm, isLeader := kv.rf.GetState()
		if newTerm != term || !isLeader {
			// If leader changes, then maybe this will never be replicated or
			// committed, thus return an empty Op so client can retry.
			res = Op{}
		} else {
			kv.mu.Unlock()
			goto tryAgain
		}
	}
	delete(kv.channelMap, ClientId)
	kv.mu.Unlock()
	return res.Err, res.Value
}

func (kv *RaftKV) Get(args *GetArgs, reply *GetReply) {
	isDuplicate, err, value := kv.preProcess(args.ClientId, args.RequestNo)
	if isDuplicate {
		reply.WrongLeader = false
		reply.Err = err
		reply.Value = value
		return
	}

	index, term, isLeader := kv.rf.Start(Op{Key: args.Key, Op: "Get",
		ClientId: args.ClientId, RequestNo: args.RequestNo})
	if !isLeader {
		reply.WrongLeader = true
		kv.mu.Lock()
		delete(kv.channelMap, args.ClientId)
		kv.mu.Unlock()
		return
	}

	err, value = kv.postProcess(args.ClientId, index, term)
	reply.WrongLeader = false
	reply.Err = err
	reply.Value = value
}

func (kv *RaftKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	isDuplicate, err, _ := kv.preProcess(args.ClientId, args.RequestNo)
	if isDuplicate {
		reply.WrongLeader = false
		reply.Err = err
		return
	}

	index, term, isLeader := kv.rf.Start(Op{Key: args.Key, Value: args.Value,
		Op: args.Op, ClientId: args.ClientId, RequestNo: args.RequestNo})
	if !isLeader {
		reply.WrongLeader = true
		kv.mu.Lock()
		delete(kv.channelMap, args.ClientId)
		kv.mu.Unlock()
		return
	}

	err, _ = kv.postProcess(args.ClientId, index, term)
	reply.WrongLeader = false
	reply.Err = err
}

//
// the tester calls Kill() when a RaftKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *RaftKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots with persister.SaveSnapshot(),
// and Raft should save its state (including log) with persister.SaveRaftState().
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *RaftKV {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	fmt.Println("MAKE SERVER ", me)
	kv := new(RaftKV)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.
	kv.kvStore = make(map[string]string)
	kv.channelMap = make(map[int64]*channelMapVal)
	kv.clientLastRequestMap = make(map[int64]Op)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	go kv.listenApplyCh()

	return kv
}
