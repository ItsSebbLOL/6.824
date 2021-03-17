package kvraft

func (kv *KVServer) addIndexChannel(index int, ch chan result) {
	kv.mu.Lock()
	_, ok := kv.indexChannels[index]

	if ok {
		return
	}

	kv.indexChannels[index] = ch
	kv.mu.Unlock()
}

func (kv *KVServer) getIndexChannel(index int) (chan result, bool) {
	var ch chan result
	var ok bool

	kv.mu.Lock()
	ch, ok = kv.indexChannels[index]
	kv.mu.Unlock()

	return ch, ok
}

func (kv *KVServer) removeIndexChannel(index int) {
	kv.mu.Lock()
	delete(kv.indexChannels, index)
	kv.mu.Unlock()
}
