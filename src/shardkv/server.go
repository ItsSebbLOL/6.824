package shardkv

// import "../shardmaster"
import (
	"../labrpc"
	"bytes"
	"log"
	"time"
)
import "../raft"
import "sync"
import "../labgob"
import "../shardmaster"

const (
	GET        = "GET"
	PUT        = "PUT"
	APPEND     = "APPEND"
	NEW_CONFIG = "NEW_CONFIG"
)

const Debug = 1

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
	OpType string

	Key   string
	Value string

	RequestId     int64
	LastRequestId int64

	ConfigNum int

	Config shardmaster.Config
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	masters      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	mck *shardmaster.Clerk

	kvs           map[string]string
	requestIdKeys map[int64]string
	indexChannels map[int]chan result

	lastAppliedIndex int

	config shardmaster.Config
	moving bool

	prevConfigs []shardmaster.Config

	movingGroupShards   map[int][]int
	deletedConfigNumKvs map[int]map[int]deletingData

	cond4movingGroupShards *sync.Cond
}

type result struct {
	value string
	err   Err
}

type deletingData struct {
	Kvs           map[string]string
	RequestIdKeys map[int64]string
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	err, ok := kv.checkGroupAndStatus(args.ConfigNum, args.Key)

	if !ok {
		reply.Err = err
		return
	}

	op := Op{}
	op.OpType = GET
	op.Key = args.Key
	op.ConfigNum = args.ConfigNum

	index, _, isLeader := kv.rf.Start(op)

	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	ch := make(chan result)
	kv.addIndexChannel(index, ch)

	go kv.checkRequestTimeout(index)

	res := <-ch

	kv.removeIndexChannel(index)

	reply.Value = res.value
	reply.Err = res.err
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	err, ok := kv.checkGroupAndStatus(args.ConfigNum, args.Key)

	if !ok {
		reply.Err = err
		return
	}

	op := Op{}
	op.OpType = args.Op
	op.Key = args.Key
	op.Value = args.Value
	op.RequestId = args.RequestId
	op.LastRequestId = args.LastRequestId
	op.ConfigNum = args.ConfigNum

	index, _, isLeader := kv.rf.Start(op)

	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	ch := make(chan result)
	kv.addIndexChannel(index, ch)

	go kv.checkRequestTimeout(index)

	res := <-ch

	kv.removeIndexChannel(index)

	reply.Err = res.err
}

func (kv *ShardKV) MoveShards(args *MovingShardsArgs, reply *MovingShardsReply) {
	if !kv.isLeader() {
		reply.Err = ErrWrongLeader
		return
	}
	if args.ConfigNum >= kv.getConfig().Num {
		reply.Err = ErrWrongGroup
		return
	}

	reply.Err = OK
	reply.Kvs = make(map[string]string)
	reply.RequestIdKeys = make(map[int64]string)

	kv.mu.Lock()

	deletedDataset := kv.deletedConfigNumKvs[args.ConfigNum]

	for _, shard := range args.Shards {
		deletedData := deletedDataset[shard]
		for key, value := range deletedData.Kvs {
			reply.Kvs[key] = value
		}

		for key, value := range deletedData.RequestIdKeys {
			reply.RequestIdKeys[key] = value
		}
	}

	kv.mu.Unlock()
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *ShardKV) checkGroupAndStatus(configNum int, key string) (Err, bool) {
	err, ok := kv.checkGroup(configNum, key)

	if !ok {
		return err, ok
	}

	if !kv.isLeader() {
		return ErrWrongLeader, false
	}

	return OK, true
}

func (kv *ShardKV) checkRequestTimeout(index int) {
	time.Sleep(700 * time.Millisecond)
	ch, ok := kv.getIndexChannel(index)
	kv.removeIndexChannel(index)

	if ok {
		res := result{}
		res.err = ErrWrongLeader

		ch <- res
	}
}

func (kv *ShardKV) apply() {
	for {
		applyMsg := <-kv.applyCh

		if applyMsg.IsReadSnapshot {
			kv.readSnapshot()
			continue
		}

		op := applyMsg.Command.(Op)

		var res result

		if op.OpType == NEW_CONFIG {
			kv.processConfig(&op, applyMsg.CommandIndex)
			go kv.saveSnapshot()
			continue
		} else {
			res = kv.processKvs(&op, applyMsg.CommandIndex)
			go kv.saveSnapshot()
		}

		if kv.isLeader() {
			ch, ok := kv.getIndexChannel(applyMsg.CommandIndex)
			kv.removeIndexChannel(applyMsg.CommandIndex)

			if ok {
				ch <- res
			} else {
				//DPrintf("[%v], Here", kv.rf.me)
			}
		}
	}
}

func (kv *ShardKV) checkGroup(configNum int, key string) (Err, bool) {
	if configNum != kv.getConfig().Num {
		return ErrWrongGroup, false
	}

	if kv.isMoving() {
		return ErrWrongGroup, false
	}

	shard := key2shard(key)

	if kv.getConfig().Shards[shard] != kv.gid {
		return ErrWrongGroup, false
	}

	return OK, true
}

func (kv *ShardKV) checkConfig() {
	for {
		time.Sleep(100 * time.Millisecond)

		if !kv.isLeader() || kv.isMoving() {
			continue
		}

		go func() {
			nextConfigNum := kv.getConfig().Num + 1
			// query if a new config exists
			nextConfig := kv.mck.Query(nextConfigNum)

			// if exists, start a new NEW_CONFIG op
			if nextConfigNum == nextConfig.Num {
				op := Op{}
				op.OpType = NEW_CONFIG
				op.ConfigNum = nextConfigNum
				op.Config = nextConfig

				kv.rf.Start(op)
			}
		}()

		time.Sleep(100 * time.Millisecond)
	}
}

func (kv *ShardKV) processConfig(op *Op, index int) {
	if op.ConfigNum != kv.getConfig().Num+1 {
		return
	}

	kv.mu.Lock()

	kv.moving = true

	prevConfig, prevShards := kv.config, kv.config.Shards

	// appending previous config
	kv.prevConfigs = append(kv.prevConfigs, prevConfig)

	// updating config
	kv.config = op.Config
	newShards := op.Config.Shards

	// starts to move shards between replicate groups
	// gid -> shards
	kv.movingGroupShards = make(map[int][]int)
	deletedShards := make([]int, 0)

	for i := 0; i < shardmaster.NShards; i++ {
		if newShards[i] == kv.gid && prevShards[i] != kv.gid && prevShards[i] != 0 {
			// new shard on the current group, and previously not on the current group
			kv.movingGroupShards[prevShards[i]] = append(kv.movingGroupShards[prevShards[i]], i)
		} else if newShards[i] != kv.gid && prevShards[i] == kv.gid {
			// new shard not on the current group anymore
			deletedShards = append(deletedShards, i)
		}
	}

	deletedDataset := make(map[int]deletingData)

	for i := 0; i < len(deletedShards); i++ {
		// delete shard at the current group, and build deletingDateSet
		shard := deletedShards[i]

		deletedData := deletingData{}
		deletedData.Kvs = make(map[string]string)
		deletedData.RequestIdKeys = make(map[int64]string)

		for key, value := range kv.kvs {
			if key2shard(key) == shard {
				deletedData.Kvs[key] = value
				delete(kv.kvs, key)
			}
		}

		for key, value := range kv.requestIdKeys {
			if key2shard(value) == shard {
				deletedData.RequestIdKeys[key] = value
				delete(kv.requestIdKeys, key)
			}
		}

		deletedDataset[shard] = deletedData
	}

	kv.deletedConfigNumKvs[prevConfig.Num] = deletedDataset

	// no shards needed to be moved from other groups
	if len(kv.movingGroupShards) == 0 {
		kv.moving = false
		kv.lastAppliedIndex = index
		kv.mu.Unlock()
		return
	}

	// try to move shards from other groups
	for gid, shards := range kv.movingGroupShards {
		go func(gid int, shards []int, configNum int) {
			kv.moveShards(gid, shards, configNum)
		}(gid, shards, prevConfig.Num)
	}

	for len(kv.movingGroupShards) != 0 {
		kv.cond4movingGroupShards.Wait()
	}

	kv.moving = false
	kv.lastAppliedIndex = index

	kv.mu.Unlock()
}

func (kv *ShardKV) moveShards(gid int, shards []int, configNum int) {
	kv.mu.Lock()
	servers := kv.prevConfigs[configNum].Groups[gid]
	kv.mu.Unlock()

	args := MovingShardsArgs{}
	args.Shards = shards
	args.ConfigNum = configNum

	for {
		for si := 0; si < len(servers); si++ {
			srv := kv.make_end(servers[si])
			reply := MovingShardsReply{}
			ok := srv.Call("ShardKV.MoveShards", &args, &reply)
			if ok && reply.Err == OK {
				kv.mu.Lock()
				delete(kv.movingGroupShards, gid)
				for key, value := range reply.Kvs {
					kv.kvs[key] = value
				}

				for key, value := range reply.RequestIdKeys {
					kv.requestIdKeys[key] = value
				}

				kv.cond4movingGroupShards.Signal()
				kv.mu.Unlock()

				return
			}
			if ok && reply.Err == ErrWrongGroup {
				time.Sleep(200 * time.Millisecond)
				break
			}
			//... not ok, or ErrWrongLeader
		}
		time.Sleep(200 * time.Millisecond)
	}

}

func (kv *ShardKV) processKvs(op *Op, index int) result {
	err, ok := kv.checkGroup(op.ConfigNum, op.Key)

	res := result{}
	res.err = OK

	if !ok {
		res.err = err
		return res
	}

	kv.mu.Lock()

	if op.OpType == GET {
		value, ok := kv.kvs[op.Key]
		if ok {
			res.value = value
		} else {
			res.err = ErrNoKey
		}
	} else if op.OpType == PUT {
		_, ok := kv.requestIdKeys[op.RequestId]

		if !ok {
			kv.requestIdKeys[op.RequestId] = op.Key
			delete(kv.requestIdKeys, op.LastRequestId)
			kv.kvs[op.Key] = op.Value
		}
	} else if op.OpType == APPEND {
		_, ok := kv.requestIdKeys[op.RequestId]

		if !ok {
			kv.requestIdKeys[op.RequestId] = op.Key
			delete(kv.requestIdKeys, op.LastRequestId)
			kv.kvs[op.Key] += op.Value
		}
	}
	kv.lastAppliedIndex = index
	kv.mu.Unlock()

	return res
}

func (kv *ShardKV) readSnapshot() {
	snapshot, index := kv.rf.ReadSnapshot()

	if snapshot == nil || len(snapshot) == 0 {
		return
	}

	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)

	kvs := make(map[string]string)
	requestIdKeys := make(map[int64]string)

	config := shardmaster.Config{}
	prevConfigs := make([]shardmaster.Config, 0)

	movingGroupShards := make(map[int][]int)
	deletingConfigNumKvs := make(map[int]map[int]deletingData)

	d.Decode(&kvs)
	d.Decode(&requestIdKeys)

	d.Decode(&config)
	d.Decode(&prevConfigs)

	d.Decode(&movingGroupShards)
	d.Decode(&deletingConfigNumKvs)

	kv.mu.Lock()

	kv.kvs = kvs
	kv.requestIdKeys = requestIdKeys

	kv.lastAppliedIndex = index

	kv.config = config
	kv.prevConfigs = prevConfigs

	kv.movingGroupShards = movingGroupShards
	kv.deletedConfigNumKvs = deletingConfigNumKvs

	kv.mu.Unlock()
}

func (kv *ShardKV) saveSnapshot() {
	if kv.maxraftstate != -1 && kv.rf.GetRaftStateSize() > kv.maxraftstate {
		w := new(bytes.Buffer)
		e := labgob.NewEncoder(w)

		kv.mu.Lock()
		e.Encode(kv.kvs)
		e.Encode(kv.requestIdKeys)

		e.Encode(kv.config)
		e.Encode(kv.prevConfigs)

		e.Encode(kv.movingGroupShards)
		e.Encode(kv.deletedConfigNumKvs)

		index := kv.lastAppliedIndex
		snapshot := w.Bytes()
		kv.mu.Unlock()

		kv.rf.SaveStateAndSnapshot(index, snapshot)
	}
}

//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardmaster.
//
// pass masters[] to shardmaster.MakeClerk() so you can send
// RPCs to the shardmaster.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use masters[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.masters = masters

	// Your initialization code here.

	// Use something like this to talk to the shardmaster:
	// kv.mck = shardmaster.MakeClerk(kv.masters)
	kv.mck = shardmaster.MakeClerk(kv.masters)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.kvs = make(map[string]string)
	kv.requestIdKeys = make(map[int64]string)
	kv.indexChannels = make(map[int]chan result)

	kv.config = shardmaster.Config{}

	kv.movingGroupShards = make(map[int][]int)
	kv.deletedConfigNumKvs = make(map[int]map[int]deletingData)

	kv.cond4movingGroupShards = sync.NewCond(&kv.mu)

	kv.readSnapshot()

	go kv.checkConfig()
	go kv.apply()

	return kv
}
