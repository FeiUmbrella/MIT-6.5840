package shardkv

// Shard represents a key-value store with its status
type Shard struct {
	KV     map[string]string // store the key-value pairs
	Status ShardStatus       // status of shard
}

func NewShard() *Shard {
	return &Shard{
		KV:     make(map[string]string),
		Status: Serving,
	}
}

func (shard *Shard) Put(key, value string) Err {
	shard.KV[key] = value
	return OK
}

func (shard *Shard) Get(key string) (string, Err) {
	value, ok := shard.KV[key]
	if !ok {
		return "", ErrNoKey
	} else {
		return value, OK
	}
}

func (shard *Shard) Append(key, value string) Err {
	shard.KV[key] += value
	return OK
}

func (shard *Shard) deepCopy() map[string]string {
	kv := make(map[string]string)
	for k, v := range shard.KV {
		kv[k] = v
	}
	return kv
}
