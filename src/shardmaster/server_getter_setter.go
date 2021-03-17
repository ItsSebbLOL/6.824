package shardmaster

func (sm *ShardMaster) addIndexChannel(index int, ch *chan result) {
	sm.mu.Lock()
	_, ok := sm.indexChannels[index]

	if ok {
		return
	}

	sm.indexChannels[index] = ch
	sm.mu.Unlock()
}

func (sm *ShardMaster) getIndexChannel(index int) (*chan result, bool) {
	var ch *chan result
	var ok bool

	sm.mu.Lock()
	ch, ok = sm.indexChannels[index]
	sm.mu.Unlock()

	return ch, ok
}

func (sm *ShardMaster) removeIndexChannel(index int) {
	sm.mu.Lock()
	delete(sm.indexChannels, index)
	sm.mu.Unlock()
}
