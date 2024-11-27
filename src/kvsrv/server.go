package kvsrv

import (
	"log"
	"sync"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	mu sync.Mutex

	// Your definitions here.
	kv  map[string]string
	vis sync.Map // 标识client的put/Append请求是否出现过
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	reply.Value = kv.kv[args.Key]
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	// 收到client的Report信息则将sync.map中存储的信息删除
	if args.MsgType == Report {
		kv.vis.Delete(args.MsgId)
		return
	}
	if value, ok := kv.vis.Load(args.MsgId); ok { // server的相应信息没有成功发送到client，client再次请求上次的put/append，server不能执行两次相同的put/append，直接将上次执行过的结果发送
		reply.Value = value.(string)
		return
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()

	OldValue := kv.kv[args.Key]
	kv.kv[args.Key] = args.Value
	reply.Value = OldValue
	kv.vis.Store(args.MsgId, OldValue)
	return
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	// 收到client的Report信息则将sync.map中存储的信息删除
	if args.MsgType == Report {
		kv.vis.Delete(args.MsgId)
		return
	}
	if value, ok := kv.vis.Load(args.MsgId); ok { // server的相应信息没有成功发送到client，client再次请求上次的put/append，server不能执行两次相同的put/append，直接将上次执行过的结果发送
		reply.Value = value.(string)
		return
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()

	OldValue := kv.kv[args.Key]
	kv.kv[args.Key] = OldValue + args.Value // 拼接
	reply.Value = OldValue
	kv.vis.Store(args.MsgId, OldValue)
	return
}

func StartKVServer() *KVServer {
	kv := new(KVServer)

	// You may need initialization code here.
	kv.kv = map[string]string{} // 初始化映射表
	return kv
}
