package kvraft

import (
	"6.5840/labrpc"
	"sync"
	"time"
)
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	mutex     sync.Mutex
	id        int64
	seq       int
	tmpLeader int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	DPrintf("ck servers. %v", ck.servers)
	// You'll have to add code here.
	ck.id = nrand()
	ck.seq = 1
	ck.tmpLeader = 0
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer."+op, &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	//ck.mutex.Lock()
	//defer ck.mutex.Unlock()
	args := GetArgs{}
	args.Key = key
	args.Id = nrand()
	args.Pid = ck.id
	// args.Version = ck.seq
	// args.Pid = os.Getpid()
	reply := GetReply{}
	for {
		ok := ck.servers[ck.tmpLeader].Call("KVServer.Get", &args, &reply)
		DPrintf("send get %v", reply)
		if (ok && reply.Err != OK) || !ok {
			//time.Sleep(time.Second)
			reply = GetReply{}
			ck.tmpLeader = (ck.tmpLeader + 1) % len(ck.servers)
			time.Sleep(50 * time.Millisecond)
			// DPrintf("resned get %v", args)
		}
		//if reply.Err == ErrWrongLeader && reply.Leader != -1 {
		//	ck.tmpLeader = reply.Leader
		//	ok = ck.servers[ck.tmpLeader].Call("KVServer.Get", &args, &reply)
		//}
		if reply.Err == OK {
			return reply.Value
		}
	}
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	args := PutAppendArgs{}
	args.Key = key
	args.Value = value
	args.Id = nrand()
	args.Version = ck.seq
	args.Pid = ck.id
	reply := PutAppendReply{}
	for {
		// var wg sync.WaitGroup
		//leader := -1
		// You will have to modify this function.
		ok := ck.servers[ck.tmpLeader].Call("KVServer."+op, &args, &reply)
		DPrintf("send put  %v %v", ck.tmpLeader, reply)
		if (ok && reply.Err != OK) || !ok {
			// time.Sleep(time.Second)
			reply = PutAppendReply{}
			ck.tmpLeader = (ck.tmpLeader + 1) % len(ck.servers)
			time.Sleep(50 * time.Millisecond)
			continue
			// DPrintf("resned get %v", args)
		}
		DPrintf("reply %v ok %v", reply, ok)
		//if reply.Err == ErrWrongLeader && reply.Leader != -1 {
		//	ck.tmpLeader = (reply.Leader + len(ck.servers) - 1) % len(ck.servers)
		//	ok = ck.servers[ck.tmpLeader].Call("KVServer."+op, &args, &reply)
		//	DPrintf("reply with leader %v %v", reply, ck.tmpLeader)
		//}
		if reply.Err == OK {
			ck.seq += 1
			return
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
