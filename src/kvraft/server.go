package kvraft

import (
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type OpType string

const (
	OpGet    OpType = "Get"
	OpPut    OpType = "Put"
	OpAppend OpType = "Append"

	HandOpTimeOut = time.Millisecond * 2000 // 2s超时
)

type Op struct {
	OpType     OpType
	Key        string
	Val        string
	Seq        uint64
	Identifier int64
}

type result struct { // 存储一个请求的序列号和结果
	LastSeq uint64
	Err     Err
	Value   string
	ResTerm int // ResTerm记录commit被apply时的term 因为其可能与Start相比发生了变化, 需要将这一信息返回给客户端
}

type KVServer struct {
	mu         sync.Mutex
	me         int
	rf         *raft.Raft
	applyCh    chan raft.ApplyMsg
	dead       int32                // set by Kill()
	waiCh      map[int]*chan result // 映射 startIndex->Ch 纪录等待commit信息的RPC handler的通道
	historyMap map[int64]*result    // 映射 Identifier->*result 记录某clerk的最高序列号的请求的序列号和结果result

	maxraftstate int // snapshot if log grows this big
	maxLen       int
	db           map[string]string

	// Code For 4B
	persister   *raft.Persister
	lastApplied int // 记录已经 Apply 的最新的索引
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// lab4B Modify
	//这里不需要判断是不是Leader，因为将这个Cmd调用raft.start的时候也会判断是否为Leader
	//_, isLeader := kv.rf.GetState()
	//if !isLeader { // 访问的server不是leader
	//	reply.Err = ErrNotLeader
	//	return
	//}

	opArgs := &Op{OpType: OpGet, Key: args.Key, Seq: args.Seq, Identifier: args.Identifier}
	res := kv.HandleOp(opArgs)
	reply.Err = res.Err
	reply.Value = res.Value
}

// Get和PutAppend都将请求封装成Op结构体, 统一给HandleOp处理
func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// lab4B Modify
	//这里不需要判断是不是Leader，因为将这个Cmd调用raft.start的时候也会判断是否为Leader
	//_, isLeader := kv.rf.GetState()
	//if !isLeader {
	//	reply.Err = ErrNotLeader
	//	return
	//}

	opArgs := &Op{Key: args.Key, Val: args.Value, Seq: args.Seq, Identifier: args.Identifier}
	if args.Op == "Put" {
		opArgs.OpType = OpPut
	}
	if args.Op == "Append" {
		opArgs.OpType = OpAppend
	}
	res := kv.HandleOp(opArgs)
	reply.Err = res.Err
}

/*
HandleOp处理ApplyHandler发过来的commit信息并生成回复,
采用的通信方式是管道, 每一个请求会将自己创建的管道存储在waiCh中, 并在函数离开时清理管道和waiCh
*/
func (kv *KVServer) HandleOp(opArgs *Op) (res result) {
	// 判断是否有历史记录
	kv.mu.Lock()
	if hisMap, exist := kv.historyMap[opArgs.Identifier]; exist && hisMap.LastSeq == opArgs.Seq {
		// 存在该操作的历史记录
		kv.mu.Unlock()
		return *hisMap
	}
	kv.mu.Unlock()

	startIndex, startTerm, isLeader := kv.rf.Start(*opArgs) // 这里调用Raft层，将Clerk的Cmd下传到Raft
	if !isLeader {
		return result{Err: ErrNotLeader, Value: ""}
	}

	kv.mu.Lock()
	newCh := make(chan result)
	kv.waiCh[startIndex] = &newCh // ApplyHandler 通过通道将Cmd的结果返回
	kv.mu.Unlock()                // Start函数耗时较长, 先解锁

	defer func() {
		kv.mu.Lock()
		delete(kv.waiCh, startIndex)
		close(newCh)
		kv.mu.Unlock()
	}()

	select { // 管道多路复用的控制结构,同时监测多个管道是否可用
	case <-time.After(HandOpTimeOut):
		res.Err = ErrHandleOpTimeOut
		return
	case msg, success := <-newCh: // 取出ApplyHandler的结果
		if !success {
			// 通道已经关闭, 有另一个协程收到了消息 或 通道被更新的RPC覆盖
			res.Err = ErrChanClose
			return
		} else if success && msg.ResTerm == startTerm {
			res = msg
			return
		} else {
			// Cmd执行完传递回来的term与一开始传入Cmd建立log的term不一致，说明这个leader可能过期了
			res.Err = ErrLeaderOutDated
			res.Value = ""
			return
		}
	}
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) DBExecute(op *Op) (res result) {
	res.LastSeq = op.Seq
	switch op.OpType {
	case OpGet:
		val, exist := kv.db[op.Key]
		if !exist {
			res.Err = ErrKeyNotExist
			res.Value = ""
			return
		} else {
			res.Value = val
			return
		}
	case OpPut:
		kv.db[op.Key] = op.Val
		return
	case OpAppend:
		val, exist := kv.db[op.Key]
		if !exist {
			kv.db[op.Key] = op.Val
			return
		} else {
			kv.db[op.Key] = val + op.Val
			return
		}
	}
	return
}

func (kv *KVServer) ApplyHandler() {
	for !kv.killed() {
		log := <-kv.applyCh // Raft层处理完负责的部分（选举、生成日志、Snapshot等），Raft将提交的Cmd通过通道应用到K/V的db（数据库）
		if log.CommandValid {
			op := log.Command.(Op) // 类型断言：检查变量是否为某种类型
			kv.mu.Lock()

			if log.CommandIndex <= kv.lastApplied {
				// 这个log已经包含在了 K/V server 的快照中
				kv.mu.Unlock()
				continue
			}

			var res result
			needApply := false //判断这个log是否需要被再次应用到K/Vdb
			if hisMap, isexist := kv.historyMap[op.Identifier]; isexist {
				if hisMap.LastSeq == op.Seq { // 历史记录存在且Seq相同，直接返回之前的历史结果
					res = *hisMap
				} else if hisMap.LastSeq < op.Seq {
					needApply = true // 历史记录中的Cmd是之前的Cmd，而这个是更新的Seq的Cmd仍需要在db中创建
				}
			} else { // 历史db中没有该记录，需要创建
				needApply = true
			}

			if needApply {
				// 在K/Vdb上执行log中的Cmd
				res = kv.DBExecute(&op)
				res.ResTerm = log.SnapshotTerm
				// 更新历史的记录
				kv.historyMap[op.Identifier] = &res
			}

			// lab4B Modify
			// 这里判断 Leader 是冗余的，因为当不是Leader时
			// 在 HandOp中，调用raft.start的startTerm != HandOp从通道中接收到的res.Term
			// 同样可以产生ErrNotLeader
			//_, isLeader := kv.rf.GetState()
			//if !isLeader { // kv.rf不是leader就处理下一个log
			//	kv.mu.Unlock()
			//	continue
			//}

			// 是leader则还需要额外通知handler处理clerk回复
			ch, isexist := kv.waiCh[log.CommandIndex]
			if isexist {
				kv.mu.Unlock()
				func() {
					defer func() {
						if recover() != nil {
							// 如果这里有 panic，是因为通道关闭
							DPrintf("leader %v ApplyHandler 发现 identifier %v Seq %v 的管道不存在, 应该是超时被关闭了", kv.me, op.Identifier, op.Seq)
						}
					}()
					res.ResTerm = log.SnapshotTerm
					*ch <- res // 这里将结果通过通道返回给
				}()
				kv.mu.Lock()
			}

			// 每收到一个log，判断是否需要生成快照
			if kv.maxraftstate != -1 && kv.persister.RaftStateSize() >= kv.maxraftstate/100*95 {
				// 当达到95%容量时生成快照
				snapShot := kv.GenSnapShot()
				kv.rf.Snapshot(log.CommandIndex, snapShot)
			}
			kv.mu.Unlock()
		} else if log.SnapshotValid {
			// 传过来的log中包含的不是一个cmd命令而是是个 快照snapshot
			kv.mu.Lock()
			if log.SnapshotIndex >= kv.lastApplied {
				kv.LoadSnapShot(log.Snapshot)
				kv.lastApplied = log.SnapshotIndex
			}
			kv.mu.Unlock()
		}
	}
}

// 生成 K/Vserver 的快照(加锁使用)
func (kv *KVServer) GenSnapShot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	e.Encode(kv.db)
	e.Encode(kv.historyMap)
	serverState := w.Bytes()
	return serverState
}

// 加载 K/Vserver 的快照(加锁使用)
func (kv *KVServer) LoadSnapShot(snapshot []byte) {
	if snapshot == nil || len(snapshot) == 0 {
		return
	}

	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)

	var db map[string]string
	var historyMap map[int64]*result
	if d.Decode(&db) != nil || d.Decode(&historyMap) != nil {
		DPrintf("server %v LoadSnapShot 加载快照失败\n", kv.me)
	} else {
		kv.db = db
		kv.historyMap = historyMap
	}
}

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
	kv.historyMap = make(map[int64]*result)
	kv.db = make(map[string]string)
	kv.waiCh = make(map[int]*chan result)
	kv.persister = persister

	// 启动前检查是否有快照
	kv.mu.Lock()
	kv.LoadSnapShot(persister.ReadSnapshot())
	kv.mu.Unlock()

	go kv.ApplyHandler()

	return kv
}
