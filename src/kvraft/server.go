package raftkv

import (
	"encoding/gob"
	"fmt"
	"labrpc"
	"log"
	"raft"
	"sync"
	//"time"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key       string
	Value     string
	Op        string
	Err       Err
	ClientId  int64
	RequestNo int64
}

type RaftKV struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	getChannelMap        map[string]chan raft.ApplyMsg
	putChannelMap        map[string]chan raft.ApplyMsg
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
			res.Command = command
			kv.mu.Lock()
			kv.clientLastRequestMap[command.ClientId] = command
			writeChan, ok := kv.getChannelMap[command.Key]
			kv.mu.Unlock()
			if !ok {
				// This means that this is not a leader and thus it cannot/doesnot
				// need to write to channel as no client is waiting.
				continue
			}
			writeChan <- res
		} else {
			if command.Op == "Put" {
				//fmt.Println("me: ",kv.me,"Applied Put: ",command.Key,command.Value)
				kv.kvStore[command.Key] = command.Value
			} else {
				//fmt.Println("me: ",kv.me,"Applied Append: ",command.Key,command.Value)
				kv.kvStore[command.Key] += command.Value
			}
			command.Err = OK
			res.Command = command
			kv.mu.Lock()
			kv.clientLastRequestMap[command.ClientId] = command
			writeChan, ok := kv.putChannelMap[command.Key]
			kv.mu.Unlock()
			if !ok {
				// This means that this is not a leader and thus it cannot/doesnot
				// need to write to channel as no client is waiting.
				continue
			}
			writeChan <- res
		}
	}
}

func (kv *RaftKV) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	// Handling duplicate requests (i.e committed requests should not be tried
	// again). However, it does not protect against the case in which the client
	// retries too soon again before actually the entry is committed.
	val, ok := kv.clientLastRequestMap[args.ClientId]
	// TODO: should this be equality check for only the last entry ?
	if ok && val.RequestNo == args.RequestNo {
		// This is a duplicate request, ignore it.
		// TODO :How do we handle the reply ?
		reply.WrongLeader = false
		reply.Err = val.Err
		reply.Value = val.Value
		kv.mu.Unlock()
		return
	}

	// TODO: see if the keys to this map can be something else,
	kv.getChannelMap[args.Key] = make(chan raft.ApplyMsg)
	readChan := kv.getChannelMap[args.Key]
	kv.mu.Unlock()
	_, _, isLeader := kv.rf.Start(Op{Key: args.Key, Op: "Get",
		ClientId: args.ClientId, RequestNo: args.RequestNo})
	if !isLeader {
		reply.WrongLeader = true
		reply.Err = "WrongLeader"
		kv.mu.Lock()
		delete(kv.getChannelMap, args.Key)
		kv.mu.Unlock()
		return
	}

	res := <-readChan
	kv.mu.Lock()
	delete(kv.getChannelMap, args.Key)
	kv.mu.Unlock()
	// TODO: improve the logic to check if leadership lost before commit.
	// TODO: check this, need to change this to uniquely determine an entry at an index.
	if args.Key != res.Command.(Op).Key {
		reply.WrongLeader = true
		reply.Err = "WrongLeader"
		return
	}
	reply.WrongLeader = false
	reply.Err = res.Command.(Op).Err
	reply.Value = res.Command.(Op).Value
	//fmt.Println("MANAV: ",reply)
}

func (kv *RaftKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	// Handling duplicate requests (i.e committed requests should not be tried
	// again). However, it does not protect against the case in which the client
	// retries too soon again before actually the entry is committed.
	val, ok := kv.clientLastRequestMap[args.ClientId]
	// TODO: should this be equality check for only the last entry ?
	if ok && val.RequestNo == args.RequestNo {
		// This is a duplicate request, ignore it.
		// TODO :How do we handle the reply ?
		reply.WrongLeader = false
		reply.Err = val.Err
		kv.mu.Unlock()
		return
	}

	// TODO: see if the keys to this map can be something else,
	kv.putChannelMap[args.Key] = make(chan raft.ApplyMsg)
	readChan := kv.putChannelMap[args.Key]
	kv.mu.Unlock()
	_, _, isLeader := kv.rf.Start(Op{Key: args.Key, Value: args.Value, Op: args.Op,
		ClientId: args.ClientId, RequestNo: args.RequestNo})
	if !isLeader {
		reply.WrongLeader = true
		reply.Err = "WrongLeader"
		kv.mu.Lock()
		delete(kv.putChannelMap, args.Key)
		kv.mu.Unlock()
		return
	}

	res := <-readChan
	kv.mu.Lock()
	delete(kv.putChannelMap, args.Key)
	kv.mu.Unlock()
	// TODO: improve the logic to check if leadership lost before commit.
	// TODO: check this, need to change this to uniquely determine an entry at an index.
	if args.Key != res.Command.(Op).Key || args.Value != res.Command.(Op).Value || args.Op != res.Command.(Op).Op {
		//fmt.Println("aK:",args.Key," rK:",res.Command.(Op).Key," aV:",args.Value," rV:",res.Command.(Op).Value," aO:",args.Op," rO:", res.Command.(Op).Op)
		reply.WrongLeader = true
		reply.Err = "WrongLeader"
		return
	}
	reply.WrongLeader = false
	reply.Err = res.Command.(Op).Err
	//fmt.Println("MANAV: ",reply)
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
	kv.getChannelMap = make(map[string]chan raft.ApplyMsg)
	kv.putChannelMap = make(map[string]chan raft.ApplyMsg)
	kv.clientLastRequestMap = make(map[int64]Op)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	go kv.listenApplyCh()

	return kv
}

// TODOs:
// 1. Make sure that there is no chance of a 4 way deadlock.
// 2. correct races if any.
