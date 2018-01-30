package raftkv

import (
	"encoding/gob"
	"fmt"
	"labrpc"
	"log"
	"raft"
	"sync"
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
	retChan	chan Op
	index	int
	isDeleted	bool
}

type RaftKV struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	channelMap			 map[int64]*channelMapVal
	kvStore              map[string]string
	clientLastRequestMap map[int64]Op
}

func (kv *RaftKV) listenApplyCh() {
	for {
		//fmt.Println("res me:", kv.me)
		res := <-kv.applyCh
		//fmt.Println("res done me:", kv.me,  " i:", res.Index, "cmd: ", res.Command)
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
				//fmt.Println("me: ",kv.me,"Applied Put: ",command.Key,command.Value)
				kv.kvStore[command.Key] = command.Value
			} else {
				//fmt.Println("me: ",kv.me,"Applied Append: ",command.Key,command.Value)
				kv.kvStore[command.Key] += command.Value
			}
			command.Err = OK
		}
		kv.mu.Lock()
		kv.clientLastRequestMap[command.ClientId] = command
		// Loop thru all the entries in the map to track all the commands which
		// were not commited. In this scenario, we just pass an ampty Op without
		// setting the Err field.
		tempMap := make(map[int64]*channelMapVal)
		for k, v := range kv.channelMap {
			tempMap[k] = v
		}
		kv.mu.Unlock()

		//fmt.Println("me:", kv.me, "Manav cmd before:",  command.ClientId, "index:",res.Index ,"Op",command.Op,"key:",command.Key)
		for k, v := range tempMap {
			kv.mu.Lock()
			index := v.index
			kv.mu.Unlock()
			if k == command.ClientId {
				kv.mu.Lock()
				writeChan := v.retChan
				v.isDeleted = true
				kv.mu.Unlock()
				if res.Index == index {
					//fmt.Println("HERE be","client:",k, " rI:",res.Index, " VI:",v.index, " key:", command.Key)
					writeChan <- command
					//fmt.Println("HERE af",   "client:",k, " rI:",res.Index, " VI:",v.index, " key:", command.Key)
				}
				continue
			}
			if res.Index >= index  && index != -1{
				kv.mu.Lock()
				writeChan := v.retChan
				isDeleted := v.isDeleted
				kv.mu.Unlock()
				if !isDeleted {
					kv.mu.Lock()
					v.isDeleted = true
					kv.mu.Unlock()
					//fmt.Println("me:", kv.me, "MANAV before, len:", "client:",k, " rI:",res.Index, " VI:",v.index, " key:", command.Key)
					writeChan <- Op{}
					//fmt.Println("me:", kv.me, "MANAV after",  "client:",k, " rI:",res.Index, " VI:",v.index, " key:", command.Key)
				}
			}
		}
	}
}

func (kv *RaftKV) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	// Handling duplicate requests (i.e committed requests should not be tried
	// again). However, it does not protect against the case in which the client
	// retries too soon again before actually the entry is committed (which is
	// okay as one client only makes on request at a time).
	val, ok := kv.clientLastRequestMap[args.ClientId]
	// TODO: should this be equality check for only the last entry ?
	if ok && val.RequestNo == args.RequestNo {
		// This is a duplicate request, ignore it.
		// TODO :How do we handle the reply ?
		//fmt.Println("DUPLICATE REQUEST ", args)
		reply.WrongLeader = false
		reply.Err = val.Err
		reply.Value = val.Value
		kv.mu.Unlock()
		return
	}

	// TODO: see if the keys to this map can be something else,
	kv.channelMap[args.ClientId] = &channelMapVal{retChan: make(chan Op), index:-1, isDeleted:false}
	readChan := kv.channelMap[args.ClientId].retChan
	kv.mu.Unlock()
	index, _, isLeader := kv.rf.Start(Op{Key: args.Key, Op: "Get",
		ClientId: args.ClientId, RequestNo: args.RequestNo})
	if !isLeader {
		reply.WrongLeader = true
		kv.mu.Lock()
		delete(kv.channelMap, args.ClientId)
		kv.mu.Unlock()
		return
	}

	kv.mu.Lock()
	kv.channelMap[args.ClientId].index = index
	//fmt.Println("SGET ",   "client:",args.ClientId, " index:",index, "key", args.Key)
	kv.mu.Unlock()
	res := <-readChan
	kv.mu.Lock()
	//fmt.Println("DEL GET ",   "client:",args.ClientId, " index:",index, "key", args.Key)
	delete(kv.channelMap, args.ClientId)
	kv.mu.Unlock()

	reply.WrongLeader = false
	reply.Err = res.Err
	reply.Value = res.Value
}

func (kv *RaftKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	// Handling duplicate requests (i.e committed requests should not be tried
	// again). However, it does not protect against the case in which the client
	// retries too soon again before actually the entry is committed (which is
	// okay as one client only makes on request at a time).
	val, ok := kv.clientLastRequestMap[args.ClientId]
	// TODO: should this be equality check for only the last entry ?
	if ok && val.RequestNo == args.RequestNo {
		// This is a duplicate request, ignore it.
		// TODO :How do we handle the reply ?
		//fmt.Println("DUPLICATE REQUEST ", args)
		reply.WrongLeader = false
		reply.Err = val.Err
		kv.mu.Unlock()
		return
	}

	// TODO: see if the keys to this map can be something else,
	kv.channelMap[args.ClientId] = &channelMapVal{retChan: make(chan Op), index:-1 ,isDeleted:false}
	readChan := kv.channelMap[args.ClientId].retChan
	kv.mu.Unlock()
	index, _, isLeader := kv.rf.Start(Op{Key: args.Key, Value: args.Value, Op: args.Op,
		ClientId: args.ClientId, RequestNo: args.RequestNo})
	if !isLeader {
		reply.WrongLeader = true
		kv.mu.Lock()
		delete(kv.channelMap, args.ClientId)
		kv.mu.Unlock()
		return
	}

	kv.mu.Lock()
	kv.channelMap[args.ClientId].index = index
	//fmt.Println("SPA ",   "client:",args.ClientId, " index:",index, "key", args.Key)
	kv.mu.Unlock()
	res := <-readChan
	kv.mu.Lock()
	//fmt.Println("DEL PA ",  "client:",args.ClientId, " index:",index, "key", args.Key)
	delete(kv.channelMap, args.ClientId)
	kv.mu.Unlock()

	reply.WrongLeader = false
	reply.Err = res.Err
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

// TODOs:
// 1. Make sure that there is no chance of a 4 way deadlock.
// 2. correct races if any.
