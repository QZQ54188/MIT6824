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

const (
	tickInterval     = 100 * time.Millisecond // ticker检查间隔
	heartbeatTimeout = 150 * time.Millisecond // 心跳间隔
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
	state            int           // 当前raft节点的状态(follow,candidate,leader)
	nextTimeoutTime  time.Time     // 下次超时时间戳
	heartbeatTimeout time.Duration // 心跳定时器
	currentTerm      int           //当前raft节点的任期
	votedFor         int           //该节点把票投给了谁
	voteCount        int           // 当前term收到的票数
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
	Term        int // 当前raft节点的任期
	CandidateId int // 发送请求的候选人id
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
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = Follower
		rf.votedFor = -1
	}
	reply.Term = rf.currentTerm
	if rf.votedFor == -1 {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
	} else {
		reply.VoteGranted = false
	}
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
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).

	return index, term, isLeader
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

// 生成随机选举超时时间，并设置下次超时时间戳
func (rf *Raft) resetElectionTimeout() {
	timeout := time.Duration(300+rand.Intn(300)) * time.Millisecond
	rf.nextTimeoutTime = time.Now().Add(timeout)
}

// candidate发起选举
func (rf *Raft) startElection() {
	rf.state = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.voteCount = 1 // 给自己投票
	myTerm := rf.currentTerm
	// println(rf.me, "开始选举，term:", myTerm)
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(server int) {
			args := RequestVoteArgs{Term: myTerm, CandidateId: rf.me}
			var reply RequestVoteReply
			if rf.sendRequestVote(server, &args, &reply) {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if rf.state != Candidate || rf.currentTerm != myTerm {
					return
				}
				if reply.Term > rf.currentTerm {
					// println(rf.me, "收到更高term:", reply.Term, "，变为follower")
					rf.currentTerm = reply.Term
					rf.state = Follower
					rf.votedFor = -1
					return
				}
				if reply.VoteGranted {
					rf.voteCount++
					// println(rf.me, "获得来自", server, "的投票，当前票数:", rf.voteCount)
					if rf.voteCount > len(rf.peers)/2 && rf.state == Candidate {
						rf.state = Leader
						println(rf.me, "成为leader，term:", rf.currentTerm)
						go rf.sendHeartbeats() // 成为leader后启动心跳
					}
				}
			}
		}(i)
	}
}

func (rf *Raft) ticker() {
	for rf.killed() == false {
		rf.mu.Lock()
		// println(rf.me, "状态是:", rf.state, "下次超时时间", rf.nextTimeoutTime.Format("2006-01-02 15:04:05.000000000"))
		// 只有非Leader节点才检查选举超时
		if rf.state != Leader && time.Now().After(rf.nextTimeoutTime) {
			println(rf.me, "超时了，变身candidate")
			rf.startElection()
			rf.resetElectionTimeout()
		}
		rf.mu.Unlock()
		time.Sleep(tickInterval)
	}
}

// 心跳/日志同步RPC参数
// field names must start with capital letters!
type AppendEntriesArgs struct {
	Term     int //当前Leader的任期
	LeaderId int //Leader在peers数组中的id
}

// 心跳/日志同步RPC回复
// field names must start with capital letters!
type AppendEntriesReply struct {
	Term    int  //回复raft节点的任期
	Success bool //是否成功收到raft节点的确认
}

// 处理AppendEntries RPC（心跳）
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		//当前Leader的任期不是最新的，需要更换Leader
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}
	// 跟随leader
	if args.Term >= rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = Follower
		rf.votedFor = args.LeaderId
		rf.resetElectionTimeout() // 重置超时时间
		reply.Success = true
		reply.Term = rf.currentTerm
	}
}

// leader定期发送心跳
func (rf *Raft) sendHeartbeats() {
	for rf.killed() == false {
		rf.mu.Lock()
		// 只要Leader才可以向其他raft节点发送心跳
		if rf.state != Leader {
			rf.mu.Unlock()
			return
		}
		currentTerm := rf.currentTerm
		rf.mu.Unlock()
		for i := range rf.peers {
			if i == rf.me {
				continue
			}
			go func(server int) {
				args := AppendEntriesArgs{Term: currentTerm, LeaderId: rf.me}
				var reply AppendEntriesReply
				if rf.sendAppendEntries(server, &args, &reply) {
					rf.mu.Lock()
					defer rf.mu.Unlock()
					// 如果收到更高任期，需要转变为Follower
					if reply.Term > rf.currentTerm {
						rf.currentTerm = reply.Term
						rf.state = Follower
						rf.votedFor = -1
						rf.resetElectionTimeout()
						return
					}
				}
			}(i)
		}
		time.Sleep(heartbeatTimeout)
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
	rf.resetElectionTimeout() // 初始化时设置下次超时时间

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
