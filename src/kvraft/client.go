package kvraft

import "6.5840/labrpc"
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers    []*labrpc.ClientEnd
	seq        uint64 // 单调递增序列号
	identifier int64  // 标识clerk
	leaderId   int    // 记录leader的id
}

func nrand() int64 {
	Max := big.NewInt(int64(1) << 62)
	Big_x, _ := rand.Int(rand.Reader, Max)
	x := Big_x.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.seq = 0
	ck.identifier = nrand()
	return ck
}

func (ck *Clerk) Get_Seq() (SendSeq uint64) {
	SendSeq = ck.seq
	ck.seq += 1
	return
}

func (ck *Clerk) Get(key string) string {

	args := &GetArgs{Key: key, Identifier: ck.identifier, Seq: ck.Get_Seq()}
	for {
		reply := GetReply{}
		ok := ck.servers[ck.leaderId].Call("KVServer.Get", args, &reply)
		if !ok || reply.Err == ErrNotLeader || reply.Err == ErrLeaderOutDated { // 询问的server是follower or 过时的leader，就继续轮询下一个server
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			continue
		}

		switch reply.Err { // 当返回 通道关闭&操作超时 则继续轮询这个leader
		case ErrChanClose:
			continue
		case ErrHandleOpTimeOut:
			continue
		case ErrKeyNotExist:
			return reply.Value // 不存在Key，那么Value就是默认零值--空字符串""
		}
		return reply.Value
	}
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// Identifier:表示该Com来自哪个clerk 、Seq：表示来自第几个Cmd。 Identifier+Seq构成Cmd的唯一标识
	args := &PutAppendArgs{Key: key, Value: value, Op: op, Identifier: ck.identifier, Seq: ck.Get_Seq()}
	for {
		reply := PutAppendReply{} // 重试RPC时, 需要新建reply结构体, 重复使用同一个结构体将导致labgob报错
		ok := ck.servers[ck.leaderId].Call("KVServer.PutAppend", args, &reply)
		if !ok || reply.Err == ErrNotLeader || reply.Err == ErrLeaderOutDated {
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			continue
		}
		switch reply.Err {
		case ErrChanClose:
			continue
		case ErrHandleOpTimeOut:
			continue
		}
		return
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
