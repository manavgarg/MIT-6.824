package raftkv

import "labrpc"
import "crypto/rand"
import "math/big"
import "fmt"
import "sync"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	lastLeader int64
	clientId   int64
	requestNo  int64
	mu         sync.Mutex
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	fmt.Println("MAKE CLIENT")
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.lastLeader = nrand() % int64(len(ck.servers))
	ck.clientId = nrand()
	ck.requestNo = -1
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	ck.mu.Lock()
	ck.requestNo++
	args := GetArgs{Key: key, ClientId: ck.clientId, RequestNo: ck.requestNo}
	ck.mu.Unlock()
	fmt.Println("Clerk: ", ck.clientId, "GET: ", key)
	reply := GetReply{}
	for {
		ok := ck.servers[ck.lastLeader].Call("RaftKV.Get", &args, &reply)
		if !ok {
			//fmt.Println("WHY HERE 1")
			ck.lastLeader = nrand() % int64(len(ck.servers))
			continue
		}
		if reply.Err == OK {
			//fmt.Println("Done GET: ", key)
			return reply.Value
		}
		if reply.Err == ErrNoKey {
			//fmt.Println("Done GET: ", key)
			return ""
		}
		//fmt.Println("WHY HERE 2")
		ck.lastLeader = nrand() % int64(len(ck.servers))
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	ck.mu.Lock()
	ck.requestNo++
	args := PutAppendArgs{Key: key, Value: value, Op: op,
		ClientId: ck.clientId, RequestNo: ck.requestNo}
	ck.mu.Unlock()
	//fmt.Println("Clerk: ", ck.clientId, op, ": k:", key, " v:", value)
	reply := PutAppendReply{}
	for {
		fmt.Println("Clerk: ", ck.clientId, op, ": k:", key, " v:", value)
		ok := ck.servers[ck.lastLeader].Call("RaftKV.PutAppend", &args, &reply)
		if !ok {
			//fmt.Println("WHY HERE")
			ck.lastLeader = nrand() % int64(len(ck.servers))
			continue
		}
		if reply.Err == OK {
			fmt.Println("DONE ", op, ": k:", key, " v:", value)
			return
		}
		ck.lastLeader = nrand() % int64(len(ck.servers))
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
