package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, Term, isleader)
//   start agreement on a new log entry
// rf.GetState() (Term, isLeader)
//   ask a Raft for its current Term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"math"
	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 3D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

const (
	Follower = iota
	Candidate
	Leader
)
const (
	ElectTimeOutBase        = 450
	HeartBeatTimeOut        = 101
	CommitCheckTimeInterval = time.Duration(100) * time.Millisecond // 检查是否可以commit的间隔
)

type Entry struct {
	Term int
	Cmd  interface{}
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain. Raft服务器必须维护的状态
	currentTerm int     // 当前server所处在的Term
	votedFor    int     // 投票给了谁
	log         []Entry // 存储当前server收到一系列指令的日志

	commitIndex int // log中最新entry的下标
	lastApplied int // 上次将Cmd存储（apply）至本地存储器的最后一条Entry的下标

	nextIndex  []int         // nextIndex[i]表示leader需要给server_i发log的起始下标
	matchIndex []int         // 已经被复制到follower节点的Log的最高下标
	applyCh    chan ApplyMsg // 用于在应用到状态机时传递消息

	// paper中没有的字段
	timeStamp time.Time // 收到最新cmd的时间戳，用于判断heartbeat是否过期
	role      int       // 当前server的角色 follower/candidate/leader

	muVote     sync.Mutex // 票数的锁
	votedCount int        // server得到的票数
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	//var Term int
	//var isleader bool
	// Your code here (3A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.role == Leader
	//return Term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int // 该条信息的Term
	CandidateId  int // 请求投票的candidate编号
	LastLogIndex int // 最新的log中entry的下标
	LastLogTerm  int // 最新的entry的Term
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!

type RequestVoteReply struct {
	// Your data here (3A).
	Term        int  // 选票的信息的Term
	VoteGranted bool // true表示选票+1，false表示没有candidate没有没投票
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int

	PrevLogTerm int
	Entries     []Entry

	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

// example RequestVote RPC handler.
// server收到来自某个server的信息（eg.投票信息）
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()

	if args.Term < rf.currentTerm { // 自己的Term要新于被投票的server，所以不给其投票并告知新的Term
		reply.Term = rf.currentTerm
		rf.mu.Unlock()
		reply.VoteGranted = false
		DPrintf("server %v 拒绝向 server %v投票: 旧的Term: %v,\n\targs= %+v\n", rf.me, args.CandidateId, args.Term, args)
		return
	}

	// 此时args.Term >= rf.currentTerm
	if args.Term > rf.currentTerm { // 自己的Term过时，需要重置自己的选票
		rf.votedFor = -1
		rf.role = Follower
		rf.currentTerm = args.Term
	}
	// todo:Q:为这么这里rf会votedFor出现提前给args的server投票的情况？
	// A: 当上次投给这个server时的回复信息丢失在网络中，server那边接收超时再次向该rf发出投票请求！
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		if args.LastLogTerm > rf.log[len(rf.log)-1].Term || (args.LastLogTerm == rf.log[len(rf.log)-1].Term && args.LastLogIndex >= len(rf.log)-1) {
			rf.currentTerm = args.Term
			reply.Term = rf.currentTerm
			rf.votedFor = args.CandidateId
			rf.role = Follower
			rf.timeStamp = time.Now()
			rf.mu.Unlock()

			reply.VoteGranted = true
			DPrintf("server %v 同意向 server %v投票\n\targs= %+v\n", rf.me, args.CandidateId, args)
			return
		} else {
			DPrintf("server %v 拒绝向 server %v投票: 已投票\n\targs= %+v\n", rf.me, args.CandidateId, args)
		}
	}
	// 没有给arg的server投票
	reply.Term = rf.currentTerm
	rf.mu.Unlock()
	reply.VoteGranted = false
	//return
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// Term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.role != Leader {
		return -1, -1, false
	}
	newEntry := &Entry{Term: rf.currentTerm, Cmd: command}
	rf.log = append(rf.log, *newEntry)
	return len(rf.log) - 1, rf.currentTerm, true
}

// 检查rf.commitIndex > rf.lastApplied，若有未apply到本地的就apply
func (rf *Raft) CommitChecker() {
	for !rf.killed() {
		rf.mu.Lock()
		for rf.commitIndex > rf.lastApplied {
			rf.lastApplied += 1
			msg := &ApplyMsg{
				CommandValid: true,
				Command:      rf.log[rf.lastApplied].Cmd,
				CommandIndex: rf.lastApplied,
			}
			rf.applyCh <- *msg
			DPrintf("server %v 准备将命令 %v(索引为 %v ) 应用到状态机\n", rf.me, msg.Command, msg.CommandIndex)
		}
		rf.mu.Unlock()
		time.Sleep(CommitCheckTimeInterval)
	}
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// 用于判断是否需要投票
func (rf *Raft) ticker() {
	rd := rand.New(rand.NewSource(int64(rf.me)))
	for rf.killed() == false {

		// Your code here (3A)
		// Check if a leader election should be started.
		rdTimeOut := GetRandomElectTimeOut(rd)
		rf.mu.Lock()
		// 当前server不是leader而且已经到选举时刻仍没收到任何信息，则发起选举
		if rf.role != Leader && time.Since(rf.timeStamp) > time.Duration(rdTimeOut)*time.Millisecond {
			go rf.Elect()
		}
		// pause for a random amount of time between 50 and 350
		// milliseconds.
		rf.mu.Unlock()
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

// 发起选举
func (rf *Raft) Elect() {
	rf.mu.Lock()

	rf.role = Candidate       // 临时成为候选人
	rf.timeStamp = time.Now() // 发起选举也是一种信息,若不更新时间戳,那么该server会很快发起下一轮选举
	rf.currentTerm += 1       // 当前Term自增
	rf.votedFor = rf.me       // 给自己投票
	rf.votedCount = 1         // 自己的选票为1

	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: len(rf.log) - 1,
		LastLogTerm:  rf.log[len(rf.log)-1].Term,
	}
	rf.mu.Unlock()

	// 向每个server发送选举信息，收集投票情况
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go rf.collectVote(i, args)
	}
}

// 向 server_i 收集投票情况
func (rf *Raft) collectVote(server int, args *RequestVoteArgs) {
	voteAnswer := rf.GetVoteAnswer(server, args)
	if !voteAnswer {
		return
	} // server没投给自己

	rf.muVote.Lock()
	if rf.votedCount > len(rf.peers)/2 {
		rf.muVote.Unlock()
		return
	} // 已经得到多数选票

	rf.votedCount += 1
	if rf.votedCount > len(rf.peers)/2 {
		rf.mu.Lock()
		if rf.role == Follower { // 自己再GetVoteAnswer中得知已经处于过时的Term，将自己的身份改为follower
			rf.mu.Unlock()
			rf.muVote.Unlock()
			return
		}
		rf.role = Leader
		// 重新初始化nextIndex和matchIndex
		for i := 0; i < len(rf.nextIndex); i++ {
			rf.nextIndex[i] = len(rf.log)
			rf.matchIndex[i] = 0
		}
		rf.mu.Unlock()
		go rf.SendHeartBeats() // 发送心跳至每个server告知产生了新的leader
	}
	rf.muVote.Unlock()
}

func (rf *Raft) GetVoteAnswer(serverId int, args *RequestVoteArgs) bool {
	sendArgs := *args // 这里必须复制，不然直接传指针会报错
	reply := &RequestVoteReply{}
	if ok := rf.sendRequestVote(serverId, &sendArgs, reply); !ok {
		return false
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if sendArgs.Term != rf.currentTerm {
		// 调用RPC获取选票的间隙，被修改了
		return false
	}

	if reply.Term > rf.currentTerm { // 自己Term已经过时，其他的server已经处在更新的Term
		rf.votedFor = -1            // 撤回给自己的投票
		rf.role = Follower          // 改变自己的身份
		rf.currentTerm = reply.Term // 更新自己的Term
	}
	return reply.VoteGranted
}

// leader 向其他server发送心跳发送包含cmd的Entry，当发送leader产生的心跳时Entry为空
func (rf *Raft) SendHeartBeats() {
	DPrintf("server %v 开始发送心跳\n", rf.me)

	for !rf.killed() { // 如果server没有“死”，才能发“心跳”
		rf.mu.Lock()
		if rf.role != Leader {
			rf.mu.Unlock()
			return
		}

		for i := 0; i < len(rf.peers); i++ { // 给每个follower发送心跳
			if i == rf.me {
				continue
			}
			args := &AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: rf.nextIndex[i] - 1,            // rf.nextIndex[i]就是要发送给follower_i日志的起始下标
				PrevLogTerm:  rf.log[rf.nextIndex[i]-1].Term, // 同上
				LeaderCommit: rf.commitIndex,                 // 告知follower_i提交的Com的下标已经到commitIndex了
			}
			if len(rf.log)-1 >= rf.nextIndex[i] { // 说明有需要向follower同步的Entry
				args.Entries = rf.log[rf.nextIndex[i]:] // 此时就是一个封装了“消息”的心跳
				DPrintf("leader %v 开始向 server %v 广播新的心跳, args = %+v \n", rf.me, i, args)
			} else {
				args.Entries = make([]Entry, 0)
				DPrintf("leader %v 开始向 server %v 广播新的心跳, args = %+v \n", rf.me, i, args)
			}

			go rf.handleAppendEntries(i, args)
		}
		rf.mu.Unlock()
		time.Sleep(time.Duration(HeartBeatTimeOut) * time.Millisecond)
	}
}

// Entry/心跳接收方
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	// 1.如果接收方Term更新于server，不更新且返回最新的Term
	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		rf.mu.Unlock()
		reply.Success = false
		return
	}

	// 此时 rf.currentTerm >= args.Term
	rf.timeStamp = time.Now() // 更新访问时间

	if rf.currentTerm < args.Term { // 当rf.Term < arg.Term:rf不论是leader还是candidate都应改为follower
		rf.votedFor = -1 // 将投票置为空，可能之前给别的server投过票或正处在某个投票的过程中但是两者Term都已过时，有更加新的Term
		rf.currentTerm = args.Term
		rf.role = Follower
	}

	if len(args.Entries) == 0 { // 表明此时接收的是一个心跳
		DPrintf("server %v 接收到 leader &%v 的心跳\n", rf.me, args.LeaderId)
	} else {
		DPrintf("server %v 收到 leader %v 的的AppendEntries: %+v \n", rf.me, args.LeaderId, args)
	}

	// 此次不是一个心跳，是leader发送的Entry实体
	// leader发送的实体在leader.log中的前一个Entry的位置index>=len(rf.log)表明leader还有信息没有传给rf，rf拒绝此次Entry，leader会向前递减发送前面一个Entry ||
	// leader发送的实体在leader.log中的前一个Entry的位置index相等，但是Term却不一样也返回false
	if args.PrevLogIndex >= len(rf.log) || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Term = rf.currentTerm
		rf.mu.Unlock()
		reply.Success = false
		DPrintf("server %v 检查到心跳中参数不合法:\n\t args.PrevLogIndex=%v, args.PrevLogTerm=%v, \n\tlen(self.log)=%v, self最后一个位置term为:%v\n", rf.me, args.PrevLogIndex, args.PrevLogTerm, len(rf.log), rf.log[len(rf.log)-1].Term)
		return
	}

	// 3. If an existing entry conflicts with a new one (same index
	// but different Terms), delete the existing entry and all that
	// follow it (§5.3) - 如果存在一个有相同index但是Term不同的Entry，则从index开始将rf.log后面的Entry清空
	if len(args.Entries) > 0 && len(rf.log) > args.PrevLogIndex+1 && rf.log[args.PrevLogIndex+1].Term != args.Entries[0].Term {
		// 发生了冲突, 移除冲突位置开始后面所有的内容
		DPrintf("server %v 的log与args发生冲突, 进行移除\n", rf.me)
		rf.log = rf.log[:args.PrevLogIndex+1]
	}

	// 4. Append any new entries not already in the log - 将Entry插入rf.log中
	// TODO: 补充apeend的业务
	rf.log = append(rf.log, args.Entries...)
	if len(args.Entries) > 0 {
		DPrintf("server %v 成功进行apeend\n", rf.me)
	}

	reply.Success = true
	reply.Term = rf.currentTerm
	// 更新rf.CommitIndex
	if args.LeaderCommit > rf.commitIndex {
		//If leaderCommit > commitIndex, set commitIndex =
		//	min(leaderCommit, index of last new entry)
		rf.commitIndex = int(math.Min(float64(args.LeaderCommit), float64(len(rf.log)-1))) //go1.21之后可以直接使用min函数，之前版本需要转换为float64使用math.Min
	}
	rf.mu.Unlock()
}

func (rf *Raft) sendAppendEntries(serverId int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	// 1.查看发送的心跳server的Term新于自己
	ok := rf.peers[serverId].Call("Raft.AppendEntries", args, reply)
	return ok
}

/*
Leader收到follower的回复后有以下几种情况：
1. Leader 的Term小于 follower 的Term，Leader 退化为 follower
2. Leader 的Term大于等于 follower 的Term，follower 会更新自己的Term，然后回复Leader
2.1 当 Leader 发送args中的entries正好是 follower 需要并入的，返回reply.Success = true
2.2 当 Leader 发送args中的entries不是 follower 需要并入的，返回reply.Success = false，让 Leader 的nextIndex - 1
*/
func (rf *Raft) handleAppendEntries(serverId int, args *AppendEntriesArgs) {
	reply := &AppendEntriesReply{}
	sendArgs := *args // 多个协程调用RPC来使用args，需要将args复制，传入RPCHandler，而不是将args的指针传入，会报错
	if ok := rf.sendAppendEntries(serverId, &sendArgs, reply); !ok {
		return
	}

	rf.mu.Lock()

	if sendArgs.Term != rf.currentTerm { // todo:为什么sendArgs.Term会改变？
		// 函数调用间隙Term改变
		rf.mu.Unlock()
		return
	}

	if reply.Success { // leader发送的log Entry成功并入follower的log中
		// 更新leader中关于该follower的信息：下次发送位置，已经匹配的位置
		rf.matchIndex[serverId] = args.PrevLogIndex + len(args.Entries)
		rf.nextIndex[serverId] = rf.matchIndex[serverId] + 1

		// 判断leader是否可以提交Entry
		N := len(rf.log) - 1
		for N > rf.commitIndex { // 当log中的最后一条Entry的下标大于已经提交的下标，表明有未被提交的Entry
			// 在提交某条Entry的时候，需要判断是否该条Entry是否得到了多数follower的复制，可以通过nextIndex[i]判断follower_i的log已经拥有哪些Entry了
			cnt := 1 // 提前包括自己
			for i := 0; i < len(rf.peers); i++ {
				if i == rf.me {
					continue
				} // 跳过自己

				if rf.matchIndex[i] >= N && rf.log[N].Term == rf.currentTerm { // follower_i已经复制了log[N]
					cnt += 1
				}
			}
			if cnt > len(rf.peers)/2 {
				rf.commitIndex = N
				break
			}
			N -= 1
		}
		rf.mu.Unlock()
		return
	}

	if reply.Term > rf.currentTerm { // 旧的leader收到心跳回复中的Term更加新，则旧的server转变为follower
		DPrintf("server %v 旧的leader收到了心跳函数中更新的Term: %v, 转化为Follower\n", rf.me, reply.Term)
		rf.currentTerm = reply.Term
		rf.votedFor = -1
		rf.role = Follower
		rf.timeStamp = time.Now()
		rf.mu.Unlock()
		return
	}
	if reply.Term == rf.currentTerm && rf.role == Leader { // 需要将nextIndex-1后重试
		rf.nextIndex[serverId] -= 1
		rf.mu.Unlock()
		return
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	rf.votedFor = -1
	rf.log = make([]Entry, 0)
	rf.log = append(rf.log, Entry{Term: 0})
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.timeStamp = time.Now()
	rf.role = Follower
	rf.applyCh = applyCh

	for i := 0; i < len(rf.nextIndex); i++ {
		rf.nextIndex[i] = 1
	}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.CommitChecker()

	return rf
}
