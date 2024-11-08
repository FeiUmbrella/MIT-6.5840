package mr

import (
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type TaskStatue int

// 定义类别
const (
	idle     TaskStatue = iota // 闲置
	finished                   // 完成
	running                    // 运行
	failed                     // 失败
)

type MapTaskInfo struct {
	statue    TaskStatue // 任务状态
	TaskId    int        // 任务编号
	startTime int64      // 分配时间，当前时间-分配时间>10s表示超时
}
type ReduceTaskInfo struct {
	statue TaskStatue
	//TaskId reduce task的编号用数组下标表示
	startTime int64
}
type Coordinator struct {
	NReduce     int // reduce tasks可用的数量
	MapTasks    map[string]*MapTaskInfo
	mu          sync.Mutex // 互斥锁
	ReduceTasks []*ReduceTaskInfo
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {

	// Your code here.
	// 遍历所有任务，全部完成则返回true，否则返回false
	for _, taskinfo := range c.MapTasks {
		if taskinfo.statue != finished {
			return false
		}
	}

	for _, taskinfo := range c.ReduceTasks {
		if taskinfo.statue != finished {
			return false
		}
	}
	return true
}

// 初始化函数
func (c *Coordinator) Init(files []string) {
	for idx, filename := range files {
		c.MapTasks[filename] = &MapTaskInfo{
			statue: idle, // 初始为闲置
			TaskId: idx,
		}
	}
	for idx := 0; idx < c.NReduce; idx++ {
		c.ReduceTasks[idx] = &ReduceTaskInfo{
			statue: idle,
		}
	}
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		NReduce:     nReduce,
		MapTasks:    make(map[string]*MapTaskInfo),
		ReduceTasks: make([]*ReduceTaskInfo, nReduce),
	}
	c.Init(files)

	c.server()
	return &c
}

// todo: 互斥锁可以细化吗？
func (c *Coordinator) NoticeResult(req *MsgSend, reply *MsgReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if req.MsgType == MapSucceed {
		for _, taskinfo := range c.MapTasks {
			if taskinfo.TaskId == req.TaskId {
				taskinfo.statue = finished
			}
		}
	} else if req.MsgType == ReduceSucceed {
		c.ReduceTasks[req.TaskId].statue = finished
	} else if req.MsgType == MapFailed {
		for _, taskinfo := range c.MapTasks {
			if taskinfo.TaskId == req.TaskId {
				taskinfo.statue = failed
			}
		}
	} else if req.MsgType == ReduceFailed {
		c.ReduceTasks[req.TaskId].statue = failed
	}
	return nil
}

// todo: 互斥锁可以细化
func (c *Coordinator) AskForTask(req *MsgSend, reply *MsgReply) error {
	if req.MsgType != AskForTask { // 传入的不是“申请任务”类型的信息
		return NoMathMsgType
	}

	// 加锁，保证每个worker申请任务时互斥
	c.mu.Lock()
	defer c.mu.Unlock()
	// 选择一个失败or闲置or超时的任务分配给worker
	MapSuccessNum := 0 // Map task 完成个数
	for filename, maptaskinfo := range c.MapTasks {
		alloc := false
		if maptaskinfo.statue == idle || maptaskinfo.statue == failed { // 该任务闲置或失败则可以分配
			alloc = true
		} else if maptaskinfo.statue == running { // 判断该任务是否超时，若超时则再分配
			if time.Now().Unix()-maptaskinfo.startTime > 10 {
				maptaskinfo.startTime = time.Now().Unix() // 再分配更新开始时间
				alloc = true
			}
		} else { // 该任务是已完成任务
			MapSuccessNum++
		}

		// 当前任务可以分配
		if alloc {
			reply.TaskId = maptaskinfo.TaskId
			reply.TaskName = filename
			reply.NReduce = c.NReduce
			reply.MsgType = MapAlloc

			maptaskinfo.statue = running
			maptaskinfo.startTime = time.Now().Unix()
			return nil
		}
	}

	// 没有任务可以分配但所有任务没有完成
	if MapSuccessNum < len(c.MapTasks) {
		reply.MsgType = Wait
		return nil
	}

	// 运行到这里表明所有的Map任务都已经完成
	ReduceSuccessNum := 0
	for idex, reducetaskinfo := range c.ReduceTasks {
		alloc := false
		if reducetaskinfo.statue == idle || reducetaskinfo.statue == failed {
			alloc = true
		} else if reducetaskinfo.statue == running {
			if time.Now().Unix()-reducetaskinfo.startTime > 10 {
				reducetaskinfo.startTime = time.Now().Unix()
				alloc = true
			}
		} else {
			ReduceSuccessNum++
		}

		if alloc {
			reply.TaskId = idex
			reply.NReduce = c.NReduce
			reply.MsgType = ReduceAlloc

			reducetaskinfo.statue = running
			reducetaskinfo.startTime = time.Now().Unix()
			return nil
		}
	}
	if ReduceSuccessNum < len(c.ReduceTasks) {
		reply.MsgType = Wait
		return nil
	}

	// 运行到这里表明所有的任务都已完成
	reply.MsgType = Shutdown

	return nil
}
