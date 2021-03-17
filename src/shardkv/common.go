package shardkv

//
// Sharded Key/Value server.
// Lots of replica groups, each running op-at-a-time paxos.
// Shardmaster decides which group serves each shard.
// Shardmaster may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	RequestId     int64
	LastRequestId int64

	ConfigNum int
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ConfigNum int
}

type GetReply struct {
	Err   Err
	Value string
}

type MovingShardsArgs struct {
	Shards    []int
	ConfigNum int
}

type MovingShardsReply struct {
	Err           Err
	Kvs           map[string]string
	RequestIdKeys map[int64]string
}
