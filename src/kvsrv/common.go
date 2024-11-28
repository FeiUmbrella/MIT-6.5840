package kvsrv

type MsgType int

// 用于标识信息是修改请求还是表明收到server回复的报告
const (
	Modify = iota
	Report
)

// Put or Append
type PutAppendArgs struct {
	Key     string
	Value   string
	MsgId   int64   // 信息编号
	MsgType MsgType // 信息类型
}

type PutAppendReply struct {
	Value string
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
}

type GetReply struct {
	Value string
}
