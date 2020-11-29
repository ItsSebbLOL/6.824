package kvraft

import (
	"../labrpc"
	"time"
)
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.

	leader   int
	clientId int64
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
	// You'll have to add code here.

	ck.leader = 0
	ck.clientId = time.Now().UnixNano()
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	var res string

	args := GetArgs{}

	args.Key = key
	args.RequestId = time.Now().UnixNano()
	args.ClientId = ck.clientId

	for {
		reply := GetReply{}

		ok := ck.servers[ck.leader].Call("KVServer.Get", &args, &reply)

		if !ok || reply.Err == ErrWrongLeader {
			ck.leader = (ck.leader + 1) % len(ck.servers)
			time.Sleep(100 * time.Millisecond)
			continue
		}

		if reply.Err == ErrNoKey {
			res = ""
			break
		} else {
			res = reply.Value
			break
		}
	}

	return res
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.

	args := PutAppendArgs{}

	args.Key = key
	args.Value = value
	args.Op = op
	args.RequestId = time.Now().UnixNano()
	args.ClientId = ck.clientId

	for {
		reply := PutAppendReply{}

		ok := ck.servers[ck.leader].Call("KVServer.PutAppend", &args, &reply)

		if !ok || reply.Err == ErrWrongLeader {
			ck.leader = (ck.leader + 1) % len(ck.servers)
			time.Sleep(100 * time.Millisecond)
			continue
		}
		break
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "PUT")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "APPEND")
}
