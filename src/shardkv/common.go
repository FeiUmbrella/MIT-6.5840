package shardkv

import (
	"6.5840/shardctrler"
	"log"
	"time"
)

// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

const (
	ExecuteTimeout              = 500 * time.Millisecond
	ConfigurationMonitorTimeout = 100 * time.Millisecond
	MigrationMonitorTimeout     = 50 * time.Millisecond
	GCMonitorTimeout            = 50 * time.Millisecond
	EmptyEntryDetectorTimeout   = 200 * time.Millisecond
)

// Err is the kinds of error during RPC
type Err uint8

const (
	OK Err = iota
	ErrNoKey
	ErrWrongGroup
	ErrWrongLeader
	ErrOutDated
	ErrTimeout
	ErrNotReady
)

// ShardStatus is the status of shards during executing
type ShardStatus uint8

const (
	Serving   ShardStatus = iota // shard is serving requests
	Pulling                      // shard is pulling data from other group
	BePulling                    // shard is being pulled by other group
	GCing                        // todo:表示shard的什么状态
)

// CommandType is the type of Commands
type CommandType uint8

const (
	Operation     CommandType = iota // normal operation command (like. put/get/append)
	Configuration                    // Configuration changes command
	InsertShards                     // command to insert some shards
	DeleteShards                     // command to delete some shards
	EmptyShards                      //Command to empty shards
)

// OperationType is the type of the normal Commands
type OperationType uint8

const (
	Get OperationType = iota
	Put
	Append
)

// CommandArgs holding some fields for a normal command
type CommandArgs struct {
	Key       string
	Value     string
	Op        OperationType
	ClientId  int64
	CommandId int64
}

// CommandReply indicate the result of the normal command executing
type CommandReply struct {
	Err         Err
	Value       string
	AppliedTerm int
}

// OperationContext stores the information of the result of last command executing
type OperationContext struct {
	MaxAppliedCommandId int64
	LastReply           *CommandReply
}

// todo: OperationContext.deepCopy 需要实现吗？直接赋值不就是深拷贝嘛？
// A : OperationContext中的LastReply字段是引用类型，所以不能直接复值，需要实现深拷贝
func (Oc OperationContext) deepCopy() OperationContext {
	return OperationContext{
		MaxAppliedCommandId: Oc.MaxAppliedCommandId,
		LastReply: &CommandReply{
			Err:         Oc.LastReply.Err,
			Value:       Oc.LastReply.Value,
			AppliedTerm: Oc.LastReply.AppliedTerm,
		},
	}
}

// ShardOperationArgs is related to the shards operation
type ShardOperationArgs struct {
	ConfigNum int
	ShardIDs  []int
}

// ShardOperationReply is the result of shards operation
type ShardOperationReply struct {
	Err            Err
	ConfigNum      int
	Shards         map[int]map[string]string
	LastOperations map[int64]OperationContext
}

// Command pack up the all kinds of types in CommandType
type Command struct {
	CommandType CommandType
	Data        interface{}
}

// NewOperationCommand creates a command with the normal command
func NewOperationCommand(args *CommandArgs) Command {
	return Command{Operation, *args}
}

// NewConfigurationCommand creates a command with the new configuration command
func NewConfigurationCommand(config *shardctrler.Config) Command {
	return Command{Configuration, *config}
}

// NewInsertShardsCommand creates a command with the inserting shard command
func NewInsertShardsCommand(reply *ShardOperationReply) Command {
	return Command{InsertShards, *reply}
}

// NewDeleteShardsCommand creates a command with the deleting shard command
func NewDeleteShardsCommand(reply *ShardOperationArgs) Command {
	return Command{DeleteShards, *reply}
}

// NewEmptyShardsCommand creates a new command indicating no shards
func NewEmptyShardsCommand() Command {
	return Command{EmptyShards, nil}
}
