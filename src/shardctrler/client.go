package shardctrler

//
// Shardctrler clerk.
//

import "6.5840/labrpc"
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	leaderId  int64
	commandId int64
	clientId  int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// Your code here.
	ck.clientId = nrand()
	ck.leaderId = 0
	ck.commandId = 0
	return ck
}

func (ck *Clerk) Query(num int) Config {
	args := &CommandArgs{
		Op:  Query,
		Num: num,
	}
	return ck.Command(args)
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &CommandArgs{Op: Join, Servers: servers}
	ck.Command(args)
}

func (ck *Clerk) Leave(gids []int) {
	args := &CommandArgs{Op: Leave, GIDs: gids}
	ck.Command(args)
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &CommandArgs{Op: Move, Shard: shard, GID: gid}
	ck.Command(args)
}

func (ck *Clerk) Command(args *CommandArgs) Config {
	args.ClientId, args.CommandId = ck.clientId, ck.commandId
	for {
		reply := &CommandReply{}
		if !ck.servers[ck.leaderId].Call("ShardCtrler.Command", args, reply) ||
			reply.Err == ErrWrongLeader || reply.Err == ErrTimeOut {
			// 没有发送成功，就向下个server发送
			// 如果即是因为网络原因没有向leader发送成功而跳过的话
			// 后面都会返回 ErrWrongLeader 继续跳过直到绕回Leader
			ck.leaderId = (ck.leaderId + 1) % int64(len(ck.servers))
		} else {
			// 发送成功，编号+1为下一条做准备
			ck.commandId += 1
			return reply.Config
		}
	}
}
