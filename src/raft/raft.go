package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"
	"math"
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

// 设置基本状态
const (
	Follower = iota
	Candidate
	Leader
)

type Entry struct {
	Term int
	Cmd  interface{}
}

const (
	HeartBeatTimeOut = 101 // 心跳重传时间
	ElectTimeOutBase = 450 // 选举超时时间
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	state       int        // 当前raft节点的状态(follow,candidate,leader)
	currentTerm int        //当前raft节点的任期
	votedFor    int        //该节点把票投给了谁
	voteCount   int        // 当前term收到的票数
	log         []Entry    // 存储的日志
	commitIndex int        // 提交日志的索引
	lastApplied int        // 给上层应用日志的索引
	nextIndex   []int      // 发给 follower[i] 的下一条日志索引
	matchIndex  []int      // follower[i] 已复制的最大日志索引
	muVote      sync.Mutex // 保护投票数据，用于细化锁粒度
	timer       *time.Timer
	rd          *rand.Rand
	applyCh     chan ApplyMsg // 向上层应用传递消息的管道
	applyCond   *sync.Cond    // 用于通知有新的日志可以应用
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (3A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = rf.state == Leader
	return term, isleader
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
	Term         int // 当前raft节点的任期
	CandidateId  int // 发送请求的候选人id
	LastLogIndex int // 候选人最后的日志索引，供选举用
	LastLogTerm  int // 候选人最后的日志任期，同样供选举使用
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int  // 其他raft节点回传的任期
	VoteGranted bool // 表示是否得到选票
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()

	if args.Term < rf.currentTerm {
		// 当前节点term比候选人节点任期更大则直接返回false
		reply.Term = rf.currentTerm
		rf.mu.Unlock()
		reply.VoteGranted = false
		DPrintf("server %v 拒绝向 server %v投票: 旧的term: %v,\n\targs= %+v\n", rf.me, args.CandidateId, args.Term, args)
		return
	}

	if args.Term > rf.currentTerm {
		// 已经是新一轮的term，之前的投票记录作废
		rf.votedFor = -1
		rf.currentTerm = args.Term
		rf.state = Follower
	}
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		if args.LastLogTerm > rf.log[len(rf.log)-1].Term ||
			(args.LastLogTerm == rf.log[len(rf.log)-1].Term) && args.LastLogIndex >= len(rf.log)-1 {
			rf.currentTerm = args.Term
			rf.votedFor = args.CandidateId
			rf.state = Follower
			rf.ResetTimer()

			reply.Term = rf.currentTerm
			rf.mu.Unlock()
			reply.VoteGranted = true
			DPrintf("server %v 同意向 server %v投票\n\targs= %+v\n", rf.me, args.CandidateId, args)
			return
		} else {
			if args.LastLogTerm < rf.log[len(rf.log)-1].Term {
				DPrintf("server %v 拒绝向 server %v 投票: 更旧的LastLogTerm, args = %+v\n", rf.me, args.CandidateId, args)
			} else {
				DPrintf("server %v 拒绝向 server %v 投票: 更短的Log, args = %+v\n", rf.me, args.CandidateId, args)
			}
		}
	} else {
		DPrintf("server %v 拒绝向 server %v投票: 已投票, args = %+v\n", rf.me, args.CandidateId, args)
	}

	reply.Term = rf.currentTerm
	rf.mu.Unlock()
	reply.VoteGranted = false
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
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	// Your code here (3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != Leader {
		return -1, -1, false
	}

	newEntry := &Entry{Term: rf.currentTerm, Cmd: command}
	rf.log = append(rf.log, *newEntry)
	return len(rf.log) - 1, rf.currentTerm, true
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

func (rf *Raft) ResetTimer() {
	rdTimeOut := GetRandomElectTimeOut(rf.rd)
	rf.timer.Reset(time.Duration(rdTimeOut) * time.Millisecond)
}

func (rf *Raft) GetVoteAnswer(server int, args *RequestVoteArgs) bool {
	sendArgs := *args
	reply := RequestVoteReply{}
	ok := rf.sendRequestVote(server, &sendArgs, &reply)
	if !ok {
		return false
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if sendArgs.Term != rf.currentTerm {
		// 在函数调用的间隙Leader诞生了
		return false
	}

	if reply.Term > rf.currentTerm {
		//退化为follower
		rf.state = Follower
		rf.votedFor = -1
		rf.currentTerm = reply.Term
	}
	return reply.VoteGranted
}

func (rf *Raft) collectVote(serverTo int, args *RequestVoteArgs) {
	voteAnswer := rf.GetVoteAnswer(serverTo, args)
	if !voteAnswer {
		return
	}
	rf.muVote.Lock()

	if rf.voteCount > len(rf.peers)/2 {
		// 已经获得半数票之后直接返回，防止其他线程进行不必要的操作
		rf.muVote.Unlock()
		return
	}

	rf.voteCount += 1
	if rf.voteCount > len(rf.peers)/2 {
		rf.mu.Lock()

		if rf.state == Follower {
			// 有另外一个投票的协程收到了更新的term而更改了自身状态为Follower
			rf.mu.Unlock()
			rf.muVote.Unlock()
			return
		}
		rf.state = Leader
		// 成为Leader之后需要重新初始化nextIndex和matchIndex
		for i := 0; i < len(rf.nextIndex); i++ {
			rf.nextIndex[i] = len(rf.log)
			rf.matchIndex[i] = 0
		}

		rf.mu.Unlock()
		go rf.sendHeartbeats()
	}
	rf.muVote.Unlock()
}

func (rf *Raft) Elect() {
	rf.mu.Lock()

	rf.currentTerm += 1  // 自增term
	rf.state = Candidate // 成为候选人
	rf.votedFor = rf.me  // 给自己投票
	rf.voteCount = 1     // 自己有一票
	// rf.timeStamp = time.Now() // 自己给自己投票也算一种消息

	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: len(rf.log) - 1,
		LastLogTerm:  rf.log[len(rf.log)-1].Term,
	}
	rf.mu.Unlock()

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go rf.collectVote(i, args)
	}
}

func (rf *Raft) ticker() {
	for !rf.killed() {
		<-rf.timer.C
		rf.mu.Lock()
		if rf.state != Leader {
			go rf.Elect()
		}
		rf.ResetTimer()
		rf.mu.Unlock()
	}
}

// 心跳/日志同步RPC参数
// field names must start with capital letters!
type AppendEntriesArgs struct {
	Term         int     //当前Leader的任期
	LeaderId     int     //Leader在peers数组中的id
	PrevLogIndex int     //新日志条目之前的那个日志索引
	PrevLogTerm  int     //PrevLogIndex 对应的任期号
	Entries      []Entry //要复制的日志
	LeaderCommit int     //leader的已提交日志索引
}

// 心跳/日志同步RPC回复
// field names must start with capital letters!
type AppendEntriesReply struct {
	Term    int  //回复raft节点的任期
	Success bool //是否成功收到raft节点的确认
}

func (rf *Raft) CommitChecker() {
	// 使用条件变量等待新的commit而不是轮询
	for !rf.killed() {
		rf.mu.Lock()

		// 当没有新日志可应用时，等待条件变量通知
		for rf.commitIndex <= rf.lastApplied && !rf.killed() {
			rf.applyCond.Wait() // 等待通知
		}

		if rf.killed() {
			rf.mu.Unlock()
			return
		}

		// 有新日志需要应用
		for rf.commitIndex > rf.lastApplied {
			rf.lastApplied += 1
			msg := &ApplyMsg{
				CommandValid: true,
				Command:      rf.log[rf.lastApplied].Cmd,
				CommandIndex: rf.lastApplied,
			}
			// 需要在释放锁的情况下发送消息，避免死锁
			rf.applyCh <- *msg
			DPrintf("server %v 准备将(索引为 %v ) 应用到状态机\n", rf.me, msg.CommandIndex)
		}

		rf.mu.Unlock()
	}
}

// 处理AppendEntries RPC（心跳）
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()

	if args.Term < rf.currentTerm {
		// 这是来自旧Leader的消息或者当前节点是一个孤立节点，
		// 因为持续增加 currentTerm 进行选举, 因此真正的leader返回了更旧的term
		reply.Term = rf.currentTerm
		rf.mu.Unlock()
		reply.Success = false
		DPrintf("server %v 收到了旧的leader% v 的心跳函数, args=%+v, 更新的term: %v\n", rf.me, args.LeaderId, args, reply.Term)
		return
	}

	rf.ResetTimer()

	if args.Term > rf.currentTerm {
		// 新Leader的第一条消息
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.state = Follower
	}

	if len(args.Entries) == 0 {
		//心跳函数
		DPrintf("server %v 接收到 leader &%v 的心跳\n", rf.me, args.LeaderId)
	} else {
		// DPrintf("server %v 收到 leader %v 的的AppendEntries: %+v \n", rf.me, args.LeaderId, args)
		DPrintf("server %v 收到 leader %v 的的AppendEntries\n", rf.me, args.LeaderId)
	}
	if args.PrevLogIndex >= len(rf.log) || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		// PrevLogIndex和PrevLogTerm不合法
		reply.Term = rf.currentTerm
		rf.mu.Unlock()
		reply.Success = false
		DPrintf("server %v 检查到心跳中参数不合法:\n\t args.PrevLogIndex=%v, args.PrevLogTerm=%v, \n\tlen(self.log)=%v, self最后一个位置term为:%v\n", rf.me, args.PrevLogIndex, args.PrevLogTerm, len(rf.log), rf.log[len(rf.log)-1].Term)
		return
	}

	// 3. If an existing entry conflicts with a new one (same index
	// but different terms), delete the existing entry and all that
	// follow it (§5.3)
	if len(args.Entries) != 0 && len(rf.log) > args.PrevLogIndex+1 && rf.log[args.PrevLogIndex+1].Term != args.Entries[0].Term {
		// 发生了冲突, 移除冲突位置开始后面所有的内容
		DPrintf("server %v 的log与args发生冲突, 进行移除\n", rf.me)
		rf.log = rf.log[:args.PrevLogIndex+1]
	}

	// append逻辑
	rf.log = append(rf.log, args.Entries...)
	if len(args.Entries) != 0 {
		DPrintf("server %v 成功进行apeend\n", rf.me)
	}

	reply.Success = true
	reply.Term = rf.currentTerm

	// 根据Leader中的提交信息更新当前节点的提交信息
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = int(math.Min(float64(args.LeaderCommit), float64(len(rf.log)-1)))
		rf.applyCond.Signal() // 通知可能有新的日志需要应用
	}

	rf.mu.Unlock()
}

func (rf *Raft) handleHeartBeat(serverTo int, args *AppendEntriesArgs) {
	reply := &AppendEntriesReply{}
	sendArgs := *args
	ok := rf.sendAppendEntries(serverTo, &sendArgs, reply)
	if !ok {
		return
	}

	rf.mu.Lock()

	if sendArgs.Term != rf.currentTerm {
		// 在函数调用的间隙Leader诞生了
		// 要先判断term是否改变, 否则后续的更改matchIndex等是不安全的
		rf.mu.Unlock()
		return
	}

	if reply.Success {
		// server回复成功
		rf.matchIndex[serverTo] = args.PrevLogIndex + len(args.Entries)
		rf.nextIndex[serverTo] = rf.matchIndex[serverTo] + 1

		// 如果有超过一半的节点都收到某一日志，则可以提交
		n := len(rf.log) - 1

		for n > rf.commitIndex {
			count := 1 // 包括Leader自己
			for i := 0; i < len(rf.peers); i++ {
				if i == rf.me {
					continue
				}
				if rf.matchIndex[i] >= n && rf.log[n].Term == rf.currentTerm {
					count += 1
				}
			}

			if count > len(rf.peers)/2 {
				break
			}
			n -= 1
		}
		// 如果至少一半的follower回复了成功, 更新commitIndex
		rf.commitIndex = n
		rf.applyCond.Signal() // 通知可能有新的日志需要应用
		DPrintf("server %v 的提交索引被设置为 %v", rf.me, rf.commitIndex)
		rf.mu.Unlock()
		return
	}

	if reply.Term > rf.currentTerm {
		// 回复了更新的term, 表示自己已经不是leader了
		DPrintf("server %v 旧的leader收到了来自 server % v 的心跳函数中更新的term: %v, 转化为Follower\n", rf.me, serverTo, reply.Term)
		rf.currentTerm = reply.Term
		rf.state = Follower
		rf.votedFor = -1
		rf.ResetTimer()
		rf.mu.Unlock()
		return
	}

	if reply.Term == rf.currentTerm && rf.state == Leader {
		// term仍然相同, 且自己还是leader, 表名对应的follower在prevLogIndex位置没有与prevLogTerm匹配的项
		// 将nextIndex自减再重试
		rf.nextIndex[serverTo] -= 1
		rf.mu.Unlock()
		return
	}
}

// leader定期发送心跳
func (rf *Raft) sendHeartbeats() {
	DPrintf("server %v 开始发送心跳\n", rf.me)
	for !rf.killed() {
		rf.mu.Lock()
		// 只要Leader才可以向其他raft节点发送心跳
		if rf.state != Leader {
			rf.mu.Unlock()
			return
		}

		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			args := &AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: rf.nextIndex[i] - 1,
				PrevLogTerm:  rf.log[rf.nextIndex[i]-1].Term,
				LeaderCommit: rf.commitIndex,
			}
			if len(rf.log)-1 >= rf.nextIndex[i] {
				// 如果有新的log需要发送，则就是一个真正的AppendEntries而不是心跳
				args.Entries = rf.log[rf.nextIndex[i]:]
				// DPrintf("leader %v 开始向 server %v 广播新的AppendEntries: %+v\n", rf.me, i, args.Entries)
				DPrintf("leader %v 开始向 server %v 广播新的AppendEntries\n", rf.me, i)
			} else {
				// 如果没有新的log发送，就发送一个长度为0的切片表示心跳
				args.Entries = make([]Entry, 0)
				DPrintf("leader %v 开始向 server %v 广播新的心跳, args = %+v \n", rf.me, i, args)
			}
			go rf.handleHeartBeat(i, args)
		}
		rf.mu.Unlock()
		time.Sleep(time.Duration(HeartBeatTimeOut) * time.Millisecond)
	}
}

// 发送AppendEntries RPC
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
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
	rf.state = Follower
	rf.votedFor = -1
	rf.log = make([]Entry, 0)
	rf.log = append(rf.log, Entry{Term: 0})
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.applyCh = applyCh
	rf.applyCond = sync.NewCond(&rf.mu) // 初始化条件变量，关联到rf.mu锁
	rf.rd = rand.New(rand.NewSource(int64(rf.me)))
	rf.timer = time.NewTimer(0)
	rf.ResetTimer()

	for i := 0; i < len(rf.nextIndex); i++ {
		rf.nextIndex[i] = 1
	}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	// 为每个raft节点开启一个向上层应用提交日志的协程
	go rf.CommitChecker()

	return rf
}
