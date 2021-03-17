package shardkv

import "../shardmaster"

func (kv *ShardKV) getConfig() shardmaster.Config {
	var config shardmaster.Config

	kv.mu.Lock()
	config = kv.config
	kv.mu.Unlock()
	return config
}

func (kv *ShardKV) isLeader() bool {
	_, isLeader := kv.rf.GetState()
	return isLeader
}

func (kv *ShardKV) addIndexChannel(index int, ch chan result) {
	kv.mu.Lock()
	_, ok := kv.indexChannels[index]

	if ok {
		return
	}

	kv.indexChannels[index] = ch
	kv.mu.Unlock()
}

func (kv *ShardKV) getIndexChannel(index int) (chan result, bool) {
	var ch chan result
	var ok bool

	kv.mu.Lock()
	ch, ok = kv.indexChannels[index]
	kv.mu.Unlock()

	return ch, ok
}

func (kv *ShardKV) removeIndexChannel(index int) {
	kv.mu.Lock()
	delete(kv.indexChannels, index)
	kv.mu.Unlock()
}

func (kv *ShardKV) isMoving() bool {
	var isMoving bool

	kv.mu.Lock()
	isMoving = kv.moving
	kv.mu.Unlock()
	return isMoving
}

func (kv *ShardKV) getWaitingShardsLength() int {
	var length int

	kv.mu.Lock()
	length = len(kv.movingGroupShards)
	kv.mu.Unlock()
	return length
}
