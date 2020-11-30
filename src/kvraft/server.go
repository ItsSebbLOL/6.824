package kvraft

import (
	"../labgob"
	"../labrpc"
	"../raft"
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

const (
	GET    = "GET"
	PUT    = "PUT"
	APPEND = "APPEND"
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	OpType    string
	Key       string
	Value     string
	RequestId int64
	ClientId  int64
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	kvs              map[string]string
	clientRequestIds map[int64]int64
	indexChannels    map[int]*chan result

	lastApplied int
}

type result struct {
	value string
	err   Err
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	op := Op{}

	op.OpType = GET
	op.Key = args.Key
	op.RequestId = args.RequestId
	op.ClientId = args.ClientId

	index, _, isLeader := kv.rf.Start(op)

	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	ch := make(chan result)
	kv.addIndexChannel(index, &ch)

	go kv.checkRequestTimeout(index)

	res := <-ch

	kv.removeIndexChannel(index)

	reply.Value = res.value
	reply.Err = res.err

}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	op := Op{}

	op.OpType = args.Op
	op.Key = args.Key
	op.Value = args.Value
	op.RequestId = args.RequestId
	op.ClientId = args.ClientId

	index, _, isLeader := kv.rf.Start(op)

	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	ch := make(chan result)
	kv.addIndexChannel(index, &ch)

	go kv.checkRequestTimeout(index)

	res := <-ch

	kv.removeIndexChannel(index)

	reply.Err = res.err
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) apply() {
	for {
		if kv.killed() {
			break
		}

		applyMsg := <-kv.applyCh

		//DPrintf("[%v] TOP LAYER APPLY_MSG %v", kv.rf.me, applyMsg)

		if applyMsg.IsReadSnapshot {
			kv.readSnapshot()
			//DPrintf("[%v] kv %v", k)
			continue
		}

		op := applyMsg.Command.(Op)

		res := result{}
		res.err = OK

		kv.mu.Lock()

		if op.OpType == GET {
			value, ok := kv.kvs[op.Key]
			if ok {
				res.value = value
			} else {
				res.err = ErrNoKey
			}
		} else if op.OpType == PUT {
			requestId, ok := kv.clientRequestIds[op.ClientId]

			if !ok || (ok && requestId < op.RequestId) {
				kv.clientRequestIds[op.ClientId] = op.RequestId
				kv.kvs[op.Key] = op.Value
			}
		} else if op.OpType == APPEND {
			requestId, ok := kv.clientRequestIds[op.ClientId]

			if !ok || (ok && requestId < op.RequestId) {
				kv.clientRequestIds[op.ClientId] = op.RequestId
				_, ok = kv.kvs[op.Key]

				if ok {
					kv.kvs[op.Key] += op.Value
				} else {
					kv.kvs[op.Key] = op.Value
				}
			}
		}
		kv.lastApplied = applyMsg.CommandIndex
		kv.mu.Unlock()

		go kv.saveSnapshot()

		_, isLeader := kv.rf.GetState()

		if isLeader {
			ch, ok := kv.getIndexChannel(applyMsg.CommandIndex)
			kv.removeIndexChannel(applyMsg.CommandIndex)

			if ok {
				*ch <- res
			} else {
				//DPrintf("[%v], Here", kv.rf.me)
			}
		}
	}
}

func (kv *KVServer) checkRequestTimeout(index int) {
	time.Sleep(750 * time.Millisecond)
	ch, ok := kv.getIndexChannel(index)
	kv.removeIndexChannel(index)

	if ok {
		res := result{}
		res.err = ErrWrongLeader

		*ch <- res
		//DPrintf("[%v], here", kv.rf.me)
	}
}

func (kv *KVServer) readSnapshot() {
	snapshot, index := kv.rf.ReadSnapshot()

	if snapshot == nil || len(snapshot) < 1 {
		return
	}

	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)

	kvs := make(map[string]string)
	clientRequestIds := make(map[int64]int64)

	d.Decode(&kvs)
	d.Decode(&clientRequestIds)

	kv.mu.Lock()
	kv.kvs = kvs
	kv.clientRequestIds = clientRequestIds
	kv.lastApplied = index
	kv.mu.Unlock()
}

func (kv *KVServer) saveSnapshot() {
	if kv.maxraftstate != -1 && kv.rf.GetRaftStateSize() > kv.maxraftstate {
		kv.mu.Lock()
		w := new(bytes.Buffer)
		e := labgob.NewEncoder(w)

		e.Encode(kv.kvs)
		e.Encode(kv.clientRequestIds)

		index := kv.lastApplied
		snapshot := w.Bytes()
		kv.mu.Unlock()

		//DPrintf("[%v] log %v, raftStatGETeSize %v > maxRaftState %v", kv.me, kv.rf.log, kv.rf.GetRaftStateSize(), kv.maxraftstate)
		kv.rf.SaveStateAndSnapshot(index, snapshot)
	}
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.kvs = make(map[string]string)
	kv.clientRequestIds = make(map[int64]int64)
	kv.indexChannels = make(map[int]*chan result)

	kv.readSnapshot()

	go kv.apply()
	return kv
}
