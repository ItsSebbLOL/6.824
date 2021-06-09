package shardmaster

import (
	"../raft"
	"sort"
	"time"
)
import "../labrpc"
import "sync"
import "../labgob"

const (
	JOIN  = "JOIN"
	LEAVE = "LEAVE"
	MOVE  = "MOVE"
	QUERY = "QUERY"
)

type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	configs          []Config // indexed by config num
	clientRequestIds map[int64]int64
	indexChannels    map[int]*chan result
}

type result struct {
	wrongLeader bool
	err         Err
	config      Config
}

type Op struct {
	// Your data here.
	OpType string

	Servers map[int][]string

	GIDs []int

	Shard int
	GID   int

	Num int

	RequestId int64
	ClientId  int64
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	op := Op{}

	op.OpType = JOIN
	op.Servers = args.Servers
	op.RequestId = args.RequestId
	op.ClientId = args.ClientId

	index, _, isLeader := sm.rf.Start(op)

	if !isLeader {
		reply.WrongLeader = true
		return
	}

	ch := make(chan result)
	sm.addIndexChannel(index, &ch)

	go sm.checkRequestTimeout(index)

	res := <-ch

	sm.removeIndexChannel(index)

	reply.WrongLeader = res.wrongLeader
	reply.Err = res.err
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	op := Op{}
	op.OpType = LEAVE
	op.GIDs = args.GIDs
	op.RequestId = args.RequestId
	op.ClientId = args.ClientId

	index, _, isLeader := sm.rf.Start(op)

	if !isLeader {
		reply.WrongLeader = true
		return
	}

	ch := make(chan result)
	sm.addIndexChannel(index, &ch)

	go sm.checkRequestTimeout(index)

	res := <-ch

	sm.removeIndexChannel(index)

	reply.WrongLeader = res.wrongLeader
	reply.Err = res.err
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	op := Op{}
	op.OpType = MOVE
	op.Shard = args.Shard
	op.GID = args.GID
	op.RequestId = args.RequestId
	op.ClientId = args.ClientId

	index, _, isLeader := sm.rf.Start(op)

	if !isLeader {
		reply.WrongLeader = true
		return
	}

	ch := make(chan result)
	sm.addIndexChannel(index, &ch)

	go sm.checkRequestTimeout(index)

	res := <-ch

	sm.removeIndexChannel(index)

	reply.WrongLeader = res.wrongLeader
	reply.Err = res.err
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	op := Op{}
	op.OpType = QUERY
	op.Num = args.Num

	index, _, isLeader := sm.rf.Start(op)

	if !isLeader {
		reply.WrongLeader = true
		return
	}

	ch := make(chan result)
	sm.addIndexChannel(index, &ch)

	go sm.checkRequestTimeout(index)

	res := <-ch

	sm.removeIndexChannel(index)

	reply.WrongLeader = res.wrongLeader
	reply.Err = res.err
	reply.Config = res.config
}

//
// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sm *ShardMaster) Kill() {
	sm.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

func (sm *ShardMaster) apply() {
	for {
		applyMsg := <-sm.applyCh

		op := applyMsg.Command.(Op)

		var res result

		if op.OpType == JOIN {
			res = sm.join(&op)
			//DPrintf("%v", op)
		} else if op.OpType == LEAVE {
			res = sm.leave(&op)
			//DPrintf("%v", op)
		} else if op.OpType == MOVE {
			res = sm.move(&op)
		} else if op.OpType == QUERY {
			res = sm.query(&op)
		}

		_, isLeader := sm.rf.GetState()

		if isLeader {
			ch, ok := sm.getIndexChannel(applyMsg.CommandIndex)
			sm.removeIndexChannel(applyMsg.CommandIndex)

			if ok {
				//if op.OpType != QUERY && op.OpType != MOVE {
				//	DPrintf("%v", res)
				//}
				*ch <- res
			} else {

			}
		}
	}
}

func (sm *ShardMaster) checkRequestTimeout(index int) {
	time.Sleep(700 * time.Millisecond)
	ch, ok := sm.getIndexChannel(index)
	sm.removeIndexChannel(index)

	if ok {
		res := result{}
		res.wrongLeader = true

		*ch <- res
	}
}

func (sm *ShardMaster) join(op *Op) result {
	sm.mu.Lock()

	res := result{}
	res.err = OK

	requestId, ok := sm.clientRequestIds[op.ClientId]

	if !ok || (ok && requestId < op.RequestId) {
		sm.clientRequestIds[op.ClientId] = op.RequestId

		newGroups := make(map[int][]string)
		lastGroups := sm.configs[len(sm.configs)-1].Groups

		for key, value := range lastGroups {
			newGroups[key] = value
		}

		for key, value := range op.Servers {
			newGroups[key] = value
		}

		// do balance
		shards := sm.balanceShards(newGroups)

		config := Config{}
		config.Num = len(sm.configs)
		config.Shards = shards
		config.Groups = newGroups

		sm.configs = append(sm.configs, config)
	}

	_, isLeader := sm.rf.GetState()

	if isLeader {
		DPrintf("CONFIG %v", sm.configs)
	}

	sm.mu.Unlock()

	return res
}

func (sm *ShardMaster) leave(op *Op) result {
	sm.mu.Lock()

	res := result{}
	res.err = OK

	requestId, ok := sm.clientRequestIds[op.ClientId]

	if !ok || (ok && requestId < op.RequestId) {
		sm.clientRequestIds[op.ClientId] = op.RequestId

		newGroups := make(map[int][]string)
		lastGroups := sm.configs[len(sm.configs)-1].Groups

		for key, value := range lastGroups {
			newGroups[key] = value
		}

		for _, gid := range op.GIDs {
			delete(newGroups, gid)
		}

		shards := sm.balanceShards(newGroups)

		config := Config{}
		config.Num = len(sm.configs)
		config.Shards = shards
		config.Groups = newGroups

		sm.configs = append(sm.configs, config)
	}

	_, isLeader := sm.rf.GetState()

	if isLeader {
		DPrintf("CONFIG %v", sm.configs)
	}

	sm.mu.Unlock()

	return res
}

func (sm *ShardMaster) move(op *Op) result {
	sm.mu.Lock()

	res := result{}
	res.err = OK

	requestId, ok := sm.clientRequestIds[op.ClientId]

	if !ok || (ok && requestId < op.RequestId) {
		sm.clientRequestIds[op.ClientId] = op.RequestId

		newGroups := make(map[int][]string)
		lastGroups := sm.configs[len(sm.configs)-1].Groups

		for key, value := range lastGroups {
			newGroups[key] = value
		}

		shards := sm.configs[len(sm.configs)-1].Shards

		shards[op.Shard] = op.GID

		config := Config{}
		config.Num = len(sm.configs)
		config.Shards = shards
		config.Groups = newGroups

		sm.configs = append(sm.configs, config)
	}

	sm.mu.Unlock()

	return res
}

func (sm *ShardMaster) query(op *Op) result {
	sm.mu.Lock()

	res := result{}
	res.err = OK

	if op.Num < 0 || op.Num >= len(sm.configs) {
		res.config = sm.configs[len(sm.configs)-1]
	} else {
		res.config = sm.configs[op.Num]
	}

	//DPrintf("%v", res.config)

	sm.mu.Unlock()

	return res
}

func (sm *ShardMaster) balanceShards(groups map[int][]string) [NShards]int {
	var shards [NShards]int
	var gids []int

	for key := range groups {
		gids = append(gids, key)
	}

	sort.Ints(gids)

	if len(gids) == 0 {
		return shards
	}

	times := NShards / len(gids)

	// re-shard all gids
	for i := 0; i < len(gids); i++ {
		for j := 0; j < times; j++ {
			shards[i*times+j] = gids[i]
		}

		//balance the rest of shards
		if i == len(gids)-1 && (i+1)*times < NShards {
			for j := (i + 1) * times; j < NShards; j++ {
				shards[j] = gids[i]
			}
		}
	}

	return shards
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sm.applyCh = make(chan raft.ApplyMsg)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)

	// Your code here.
	sm.clientRequestIds = make(map[int64]int64)
	sm.indexChannels = make(map[int]*chan result)

	go sm.apply()

	return sm
}
