package shardctrler

import (
	"6.5840/labgob"
	"6.5840/raft"
	"log"
	"sync/atomic"
	"time"
)
import "6.5840/labrpc"
import "sync"

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	dead           int32                      // set by kill(), to indicate the ShardCtrler instance is killed
	stateMachine   ConfigStateMachine         // store the configuration of the shard system
	lastOperations map[int64]OperationContext // the last operation context to avoid the duplicate request
	notifyChan     map[int]chan *CommandReply // notify channel to notify the client goroutine
}

// if the client's command is duplicate return true
func (sc *ShardCtrler) isDuplicateRequest(clientId, commandId int64) bool {
	lastApply, ok := sc.lastOperations[clientId]
	return ok && lastApply.MaxAppliedCommandId >= commandId
}

// if the channel exist return thr channel
// otherwise creat the channel with command's startIndex in raft layer's log
func (sc *ShardCtrler) GetNotifyChan(startId int) chan *CommandReply {
	notifyChan, exist := sc.notifyChan[startId]
	if !exist {
		notifyChan = make(chan *CommandReply, 1)
		sc.notifyChan[startId] = notifyChan
	}
	return notifyChan
}

// Command handles the commands form client.
// if command isn't a Query and is a duplicate command,
// returns previous reply
// otherwise transform the command to the raft.
// if the raft server isn't leader return ERR
// if the command successfully starts, it waits for the result form che channel
// or timeout after a certain period
func (sc *ShardCtrler) Command(args *CommandArgs, reply *CommandReply) {
	sc.mu.Lock()
	if args.Op != Query && sc.isDuplicateRequest(args.ClientId, args.CommandId) {
		// the command is a duplicate command return previous reply
		LastReply := sc.lastOperations[args.ClientId].LastReply
		reply.Config, reply.Err = LastReply.Config, LastReply.Err
		sc.mu.Unlock()
		return
	}
	sc.mu.Unlock()

	// start the command in raft layer and the raft server isn't Leader
	startIndex, startTerm, isLeader := sc.rf.Start(Command{args})
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	// creat the channel with command's startIndex in raft layer's log
	sc.mu.Lock()
	notifyChan := sc.GetNotifyChan(startIndex)
	sc.mu.Unlock()

	// wait the result from channel
	select {
	case result := <-notifyChan:
		if result.AppliedCmdTerm == startTerm {
			reply.Config = result.Config
			reply.Err = result.Err
		}
	case <-time.After(ExecuteTimeout):
		DPrintf("TimeOut: Client %v Seq %v 的 Cmd-%v", args.ClientId, args.CommandId, args.Op)
		reply.Err = ErrTimeOut
	}

	go func() {
		// delete the outdated notify channel to reduce the memory footprint
		sc.mu.Lock()
		delete(sc.notifyChan, startIndex)
		sc.mu.Unlock()
	}()
}

// Applier is the goroutine which gets applied commands by raft layer
// and apply the command to the state machine
func (sc *ShardCtrler) Applier() {
	for !sc.killed() {
		select {
		// get the Cmd form raft layer
		case message := <-sc.applyCh:
			if message.CommandValid {
				reply := new(CommandReply)
				command := message.Command.(Command)
				sc.mu.Lock()

				if command.Op != Query && sc.isDuplicateRequest(command.ClientId, command.CommandId) {
					reply = sc.lastOperations[command.ClientId].LastReply
				} else {
					reply = sc.ApplyCmdToStateMachine(command)
					if command.Op != Query {
						sc.lastOperations[command.ClientId] = OperationContext{
							MaxAppliedCommandId: command.CommandId,
							LastReply:           reply,
						}
					}
				}
				// Leader raft server need report the result to client
				if currentTerm, isLeader := sc.rf.GetState(); isLeader {
					notifyChan := sc.GetNotifyChan(message.CommandIndex)
					reply.AppliedCmdTerm = currentTerm
					notifyChan <- reply
				}
				sc.mu.Unlock()
			}
		}
	}
}

// Applier Handler
func (sc *ShardCtrler) ApplyCmdToStateMachine(command Command) *CommandReply {
	reply := new(CommandReply)
	switch command.Op {
	case Join:
		DPrintf("Client %v Seq %v 的 Join-Start", command.ClientId, command.CommandId)
		reply.Err = sc.stateMachine.Join(command.Servers)
		DPrintf("Client %v Seq %v 的 Join-End", command.ClientId, command.CommandId)
	case Leave:
		DPrintf("Client %v Seq %v 的 Leave-Start", command.ClientId, command.CommandId)
		reply.Err = sc.stateMachine.Leave(command.GIDs)
		DPrintf("Client %v Seq %v 的 Leave-End", command.ClientId, command.CommandId)
	case Move:
		DPrintf("Client %v Seq %v 的 Move-Start", command.ClientId, command.CommandId)
		reply.Err = sc.stateMachine.Move(command.Shard, command.GID)
		DPrintf("Client %v Seq %v 的 Move-End", command.ClientId, command.CommandId)
	case Query:
		DPrintf("Client %v Seq %v 的 Query-Start", command.ClientId, command.CommandId)
		reply.Config, reply.Err = sc.stateMachine.Query(command.Num)
		DPrintf("Client %v Seq %v 的 Query-End", command.ClientId, command.CommandId)
	}
	return reply
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
	atomic.StoreInt32(&sc.dead, 1)
}

// check if the ShardCtrler is killed
func (sc *ShardCtrler) killed() bool {
	return atomic.LoadInt32(&sc.dead) == 1
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	labgob.Register(Command{})

	sc := new(ShardCtrler)
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)
	sc.me = me
	sc.stateMachine = NewMemoryConfigStateMachine()
	sc.lastOperations = make(map[int64]OperationContext)
	sc.notifyChan = make(map[int]chan *CommandReply)
	sc.dead = 0

	go sc.Applier()

	return sc
}
