package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"errors"
	"os"
)
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

/*
woker和coordinator需要通过rpc进行信息通讯
woker：
	1.向coordinator申请任务（map task or reduce task）
	2.向coordinator报告状况（map/reduce succeed or failed）
coordinator：
	1.向worker发出命令（分配map/reduce task、暂时没有任务-让worker休眠、所有任务完成-让worker退出）
	2.当worker分配任务时需要指定分配你哪个任务即 taskID
*/

// 用不同数字表示不同信息的类别
type MsgType int

var (
	NoMathMsgType = errors.New("not match message type")
	NoMoreTask    = errors.New("no more task left")
)

const (
	AskForTask    MsgType = iota // 表示worker向coordinator申请任务
	MapSucceed                   // 表示worker向coordinator传递Map Task完成
	MapFailed                    // 表示worker向coordinator传递Map Task失败
	ReduceSucceed                // 表示worker向coordinator传递Reduce Task完成
	ReduceFailed                 // 表示worker向coordinator传递Reduce Task失败
	MapAlloc                     // 表示coordinator向worker分配Map Task
	ReduceAlloc                  // 表示coordinator向worker分配Reduce Task
	Wait                         // 表示coordinator让worker休眠
	Shutdown                     // 表示coordinator让worker终止
)

type MsgSend struct {
	MsgType MsgType
	TaskId  int
}

type MsgReply struct {
	MsgType  MsgType
	NReduce  int
	TaskId   int    // 当worker发送MsgSend申请任务、coordinator回复任务的ID
	TaskName string //
}

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
