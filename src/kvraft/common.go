package kvraft

const (
	ErrNotLeader       = "NotLeader"
	ErrKeyNotExist     = "KeyNotExist"
	ErrHandleOpTimeOut = "HandleOpTimeOut" // 超时
	ErrChanClose       = "ChanClose"       // 通道关闭
	ErrLeaderOutDated  = "LeaderOutDated"
	ERRRPCFailed       = "RPCFailed"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key        string
	Value      string
	Op         string // Op = "Put" or "Append"
	Identifier int64
	Seq        uint64
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key        string
	Identifier int64
	Seq        uint64
}

type GetReply struct {
	Err   Err
	Value string
}
