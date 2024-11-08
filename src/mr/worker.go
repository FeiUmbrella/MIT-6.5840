package mr

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"sort"
	"strconv"
	"time"
	//"6.5840/mr"
)
import "net/rpc"
import "hash/fnv"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// worker的任务就是不断请求任务、执行任务、报告执行状态
// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	for {
		// 不断请求
		replyMsg := CallForTask()
		switch replyMsg.MsgType {
		case MapAlloc: // coordinator分配了map task
			err := HandleMapTask(replyMsg, mapf)
			if err != nil { // Map Task任务完成
				_ = CallForReportStatus(MapFailed, replyMsg.TaskId)
			} else { // Map Task 任务失败
				_ = CallForReportStatus(MapSucceed, replyMsg.TaskId)
			}
		case ReduceAlloc:
			err := HandleReduceTask(replyMsg, reducef)
			if err != nil { // Map Task任务完成
				_ = CallForReportStatus(ReduceFailed, replyMsg.TaskId)
			} else { // Map Task 任务失败
				_ = CallForReportStatus(ReduceSucceed, replyMsg.TaskId)
			}
		case Wait:
			time.Sleep(time.Second * 10)
		case Shutdown:
			os.Exit(0)
		}
		time.Sleep(time.Second)
	}
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

// 处理分配的 Map 任务
func HandleMapTask(reply *MsgReply, mapf func(string, string) []KeyValue) error {
	file, err := os.Open(reply.TaskName)
	if err != nil {
		log.Fatalf("cannot open %v", reply.TaskName)
		return err
	}
	defer file.Close()

	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", reply.TaskName)
	}
	// 进行mapf
	kva := mapf(reply.TaskName, string(content))
	sort.Sort(ByKey(kva))

	// 将得到的键值对存入对应中间文件: mr-out-TaskId-由key hash出的值 % NReduce(10)
	oname_prefix := "mr-out-" + strconv.Itoa(reply.TaskId) + "-"
	key_group := map[string][]string{}
	for _, kv := range kva { // 将同一个key的value都放在一起，存入中间文件
		key_group[kv.Key] = append(key_group[kv.Key], kv.Value)
	}
	// 在存入中间文件前，需要将要存入的中间文件清空，因为有可能上次处理的worker中间出现了问题，导致文件含有上次的数据
	// todo:可以利用原子操作重写该函数？
	_ = CleanFileByMapId(reply.TaskId, "./")
	for key, values := range key_group {
		oname := oname_prefix + strconv.Itoa(ihash(key)%reply.NReduce)
		var ofile *os.File
		if _, err := os.Stat(oname); os.IsNotExist(err) { // 查看oname是否存在
			ofile, _ = os.Create(oname) // 创建后返回一个可读可写的文件指针
		} else {
			ofile, _ = os.OpenFile(oname, os.O_APPEND|os.O_WRONLY, 0644)
		}
		enc := json.NewEncoder(ofile)
		for _, value := range values {
			err := enc.Encode(&KeyValue{Key: key, Value: value}) // todo:这里为什么传入的是内容地址而非内容本身
			if err != nil {
				ofile.Close()
				return err
			}
		}
		ofile.Close()
	}
	return nil
}

// 处理分配的 Reduce 任务，处理每个MapTask产生的mr-out-*-key_id
func HandleReduceTask(reply *MsgReply, reducef func(string, []string) string) error {
	key_id := reply.TaskId
	// todo:这里的key_id要 % NReduce吗 --答：传入的TaskId一定是小于NReduce的，规约的数量就是NRduce也即Reduce Task的数量
	files, err := ReadSpecificFile(key_id, "./")
	if err != nil {
		return err
	}
	// 从所有匹配的文件中读出Json格式的键值对[k1-value]/[k2-value]，这里的ihash(k1) = ihash(k2)
	// 存在不同key，但是哈希出的值相等
	k_vs := map[string][]string{}
	for _, file := range files {
		dec := json.NewDecoder(file)
		for { // 循环读JSON数据流
			var kv KeyValue // 将读出的JSON数据解码放入kv
			if err := dec.Decode(&kv); err != nil {
				break
			}
			k_vs[kv.Key] = append(k_vs[kv.Key], kv.Value)
		}
		file.Close()
	}

	keys := []string{} // 将map中的keys拿出排序，按照keys的字典序依次写入文件
	for k, _ := range k_vs {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	oname := "mr-out-" + strconv.Itoa(reply.TaskId) // 将Reduce后的结果放入 mr-out-TaskId
	ofile, err := os.Create(oname)
	if err != nil {
		return err
	}
	defer ofile.Close()
	for _, key := range keys {
		output := reducef(key, k_vs[key])
		_, err := fmt.Fprintf(ofile, "%s %s\n", key, output) // 格式化写入文件
		if err != nil {
			return err
		}
	}

	CleanFileByReduceId(reply.TaskId, "./")
	return nil
}

// worker：1.申请任务 2.汇报上个任务的执行情况
func CallForReportStatus(TaskStatus MsgType, TaskId int) error {
	args := &MsgSend{
		MsgType: TaskStatus,
		TaskId:  TaskId,
	}
	err := call("Coordinator.NoticeResult", args, nil)
	return err
}
func CallForTask() *MsgReply {
	// 请求一个任务
	req := &MsgSend{
		MsgType: AskForTask,
	}
	reply := &MsgReply{}
	err := call("Coordinator.AskForTask", req, reply)
	if err != nil {
		return nil
	}
	return reply
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) error {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		//log.Fatal("dialing:", err)
		os.Exit(-1)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	return err
}
