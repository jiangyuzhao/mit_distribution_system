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
	"math/rand"
	"mitlab/labrpc"
	"sync"
	"sync/atomic"
	"time"
)

// import "bytes"
// import "mitlab/labgob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

/**
 用来代表LogEntry
 */
type LogEntry struct {
	Term		int
	Command		interface{}
}

const (
	HeartbeatPeriod = 120
	ElectionTimeoutBase = 5 * HeartbeatPeriod
	Leader = iota  	//0
	Candidate       //1
	Follower        //2
)
//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// Persistent state(Updated on stable storage before responding to RPCs)
	currentTerm int
	votedFor    int			// currentTerm和voteFor都锁起来
	log []LogEntry			// 初始化大小为1, 这样append会从1开始, 而commit从0开始
	lastLogIndex int		// 初始化为0, 每次需要先append, lastLogIndex++

	// Volatile state on all servers
	commitIndex int // index of highest log entry known to be committed (initialized to 0, increases monotonically)
	lastApplied int // index of highest log entry applied to state machine (initialized to 0, increases monotonically)
	receiveFromLeader bool
	waitUntilLeader *sync.Cond			// this condition wait until raft becomes leader
	waitUntilNoLeader *sync.Cond		// this condition wait until raft becomes non-leader

	// Volatile state on leaders(Reinitialized after election)
	status   int  // if the server thinks it's a leader.
	nextIndex  []int // for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	matchIndex []int // for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.status == Leader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
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

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int // candidate’s term
	CandidateId  int // candidate requesting vote
	LastLogIndex int // index of candidate’s last log entry
	LastLogTerm  int // term of candidate’s last log entry
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.receiveFromLeader = true
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}
	if args.Term == rf.currentTerm {
		if rf.votedFor == args.CandidateId {
			// 已经投过了
			reply.Term = args.Term
			reply.VoteGranted = true
		} else if rf.votedFor != -1 {
			// 不能多投
			reply.Term = args.Term
			reply.VoteGranted = false
		} else if rf.status != Leader {
			reply.Term = args.Term
			reply.VoteGranted = true
			rf.votedFor = args.CandidateId
			_, _ = DPrintf("raft %d vote for raft %d in Term %d\n", rf.me, args.CandidateId, args.Term)
		} else {
			// rf.votedFor == -1 && rf.status == Leader
			// 它如是leader，那么不应该给同term的人投票了, 但其实这个分支应该走不到
			reply.Term = args.Term
			reply.VoteGranted = false
			panic("You should never here")
		}
		return
	}
	// Term更大, 需要响应新的投票请求, 为了预防颠簸可以优化
	rf.switchTo(Follower)			// 外部已经加锁 @already locked
	rf.currentTerm = args.Term
	reply.Term = args.Term
	if args.LastLogTerm > rf.log[rf.lastLogIndex].Term {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
	} else if args.LastLogTerm < rf.log[rf.lastLogIndex].Term {
		reply.VoteGranted = false
		rf.votedFor = -1
	} else if args.LastLogIndex >= rf.lastLogIndex {
		reply.VoteGranted = true						// 至少一样新
		rf.votedFor = args.CandidateId
	} else {
		reply.VoteGranted = false					// 拒绝没有自己新的投票请求
		rf.votedFor = -1
	}
}

//
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
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	_, _ = DPrintf("raft %d send RequestVote RPC with term %d, last log term %d, last log index %d\n", rf.me, rf.currentTerm, rf.log[rf.lastLogIndex].Term, rf.lastLogIndex)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

type AppendEntriesArgs struct {
	Term int			// leader's Term
	LeaderId int		// so follower can redirect clients(目前没什么用好像)
	// 这个论文里表示的是log entry马上要处理的新log的index, 但我直接处理成最后一个index
	PrevLogIndex int	// index of log entry immediately preceding new ones
	PrevLogTerm int		// term of prevLogIndex entry
	Entries []LogEntry	// Log entries to store(empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int	// leader's commitIndex
}

type AppendEntriesReply struct {
	Term int			// CurrentTerm, for leader to update itself
	Success bool		// true if follower contained entry matching prevLogIndex and prevLogTerm
}

//
// example AppendEntries RPC handler.
//
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// For 2A, only need to response to Heartbeat.
	// 只要收到这种RPC就抑制选举的发起, 可能是过期的leader发来的，但是这下会导致超时的leader更新为follower, 它不会再发包, 该超时还是会超时
	rf.mu.Lock()
	defer rf.mu.Unlock()
	_, _ = DPrintf("raft %d receive AppendEntries RPC from raft %d with args %v\n", rf.me, args.LeaderId, *args)
	rf.receiveFromLeader = true
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}
	// 否则更新term, 反正加锁了, 只有leader可以发这个消息, 如果它的term合法, 就一定要变成follower
	// 哪怕在选举过程中
	reply.Term = args.Term
	rf.switchTo(Follower)
	if args.PrevLogIndex > rf.lastLogIndex {
		// 超出访问空间
		reply.Success = false
		return
	}
	if index := args.PrevLogIndex; rf.log[index].Term == args.Term {
		reply.Success = true
		index++						// 首先先增加, 指向空位置
		base := index
		for ;index - base < len(args.Entries); index++ {
			if index >= len(rf.log) {
				rf.log = append(rf.log, args.Entries[index - base: ]...)
				index += len(args.Entries) - index + base
				break
			} else {
				rf.log[index] = args.Entries[index - base]
			}
		}
		// index还是指向空位置, 所以先-1
		rf.lastLogIndex = index - 1
		if rf.lastLogIndex < 0 {
			panic("rf.lastLogIndex < 0")
		}
		if args.LeaderCommit > rf.commitIndex {
			if rf.lastLogIndex > args.LeaderCommit {
				rf.commitIndex = args.LeaderCommit
			} else {
				rf.commitIndex = rf.lastLogIndex
			}
		}
	} else {
		reply.Success = false
	}
}

// need lock! 工程上这个二分查找可以做，但是为了方便还是线性了, 这个方法也许是不必要的
func (rf *Raft) findFirstIndexByTerm(Term int) int {
	i := len(rf.log) - 2
	for i--; i >= 0; i-- {
		if rf.log[i + 1].Term == Term && rf.log[i].Term < Term {
			return i + 1
		} else if rf.log[i].Term < Term {
			return -1
		}
	}
	if rf.log[0].Term == Term {
		return 0
	}
	_, _ = DPrintf("raft %d want to find term %d but all log term is larger than it.\n", rf.me, Term)
	panic("raft find index by term fall.\n")
	return -1
}

// Invoke in Start()[start agreement on a new log entry] or in Heartbeat
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	_, _ = DPrintf("raft %d send AppendEntries RPC.\n", rf.me)
	return rf.peers[server].Call("Raft.AppendEntries", args, reply)
}

// 统一处理Raft状态转换。这么做的目的是为了没有遗漏的处理nonLeader与leader状态之间转换时需要给对应的条件变量发信号的工作.
// 为了避免死锁，该操作不加锁，由外部加锁保护!
func (rf *Raft) switchTo(newStatus int)  {
	oldStatus := rf.status
	rf.status = newStatus
	if oldStatus == Leader && newStatus == Follower {
		rf.waitUntilNoLeader.Broadcast()
	} else if oldStatus == Candidate && newStatus == Leader {
		rf.waitUntilLeader.Broadcast()
	}
}

//
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
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = make([]LogEntry, 1)			// 初始化大小为1, 这样append会从1开始, 而commit从0开始
	rf.lastLogIndex = 0		// 初始化为0, 每次需要先append, lastLogIndex++

	// Volatile state on all servers
	rf.commitIndex = 0 // index of highest log entry known to be committed (initialized to 0, increases monotonically)
	rf.lastApplied = 0 // index of highest log entry applied to state machine (initialized to 0, increases monotonically)
	rf.receiveFromLeader = false
	rf.waitUntilLeader = sync.NewCond(&rf.mu)
	rf.waitUntilNoLeader = sync.NewCond(&rf.mu)

	// Volatile state on leaders(Reinitialized after election)
	rf.status = Follower
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	// 没有数据的是哈readPersist直接返回，不会覆盖上面设置的硬状态
	go rf.electionTimeoutChecker()
	go rf.heartbeatTimeoutChecker()

	return rf
}

func (rf *Raft) electionTimeoutChecker() {
	for atomic.LoadInt32(&rf.dead) != 1 {
		// 这里不加锁了，receiveFromLeader只在这里读，如果正好出现了race, 那么也认为应该重新选主
		// 可以具体测试一下加不加锁的效果差异，因为如果加锁可能会有效率损失
		if _, isLeader := rf.GetState(); isLeader {
			rf.mu.Lock()
			rf.waitUntilNoLeader.Wait()
			rf.mu.Unlock()
		} else if rf.receiveFromLeader {
			rf.receiveFromLeader = false
			time.Sleep(time.Millisecond * (ElectionTimeoutBase + time.Duration(rand.Int()%HeartbeatPeriod)))
		} else {
			// 否则发起投票, 这里另外开一个协程来完成选举是因为TimeoutChecker协程应该专注检查选举Timeout
			// 同时startElection中可能选举太久, 应该再次Timeout, 也需要checker来检查
			// startElemction由于底层函数的实现，可以保证有限时间内结束，暂时不考虑泄露问题
			_, _ = DPrintf("raft %d electionTimeout.\n", rf.me)
			go rf.startElection()
		}
	}

}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	// 必须保证状态不是Leader
	if rf.status == Leader {
		return
	}
	// 1. increase CurrentTerm(失去锁的保护的话, CurrentTerm是会被修改的
	// 有可能后面的currentTerm就不是本次选举需要的Term了
	rf.currentTerm++
	term := rf.currentTerm
	// 2. status switch(already locked)
	rf.switchTo(Candidate)
	// 3. vote for self
	rf.votedFor = rf.me
	agree := int32(1)
	_, _ = DPrintf("raft %d start elective with term %d\n.", rf.me, rf.currentTerm)
	// 4. reset receiveFromLeader to invoke anther time out
	rf.receiveFromLeader = true
	rf.mu.Unlock()


	length := len(rf.peers)
	wg := &sync.WaitGroup{}
	for i := 0; i < length; i++ {
		if i == rf.me {
			continue
		}
		num := i
		args := new(RequestVoteArgs)
		rf.mu.Lock()
		args.Term = term			// 不能使用currentTerm
		args.CandidateId = rf.me
		args.LastLogIndex = rf.lastLogIndex
		args.LastLogTerm = rf.log[rf.lastLogIndex].Term
		rf.mu.Unlock()
		reply := new(RequestVoteReply)
		wg.Add(1)
		go func() {
			if ok := rf.sendRequestVote(num, args, reply); ok {
				rf.mu.Lock()
				// reply过期了
				if reply.Term < rf.currentTerm {

				} else if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
				} else if reply.VoteGranted {
					atomic.AddInt32(&agree, 1)
				}
				rf.mu.Unlock()
			}
			wg.Done()
		}()
	}

	wg.Wait()
	if agreeNum := atomic.LoadInt32(&agree); agreeNum > int32(length/2) {
		// 选举成功, 成为leader
		rf.mu.Lock()
		// 只有在term所指定的任期是leader, 如果在选举过程中被更高级的leader抑制了, 或者自己发动了第二次选举, 都不应该成为leader
		// 在自己成为leader之后又发起一次投票是不会成功的, 因为已经是Leader了.
		// 如果已经发动了第二次投票, 那么currentTerm就不会和term相同了.
		if term == rf.currentTerm {
			_, _ = DPrintf("raft %d become leader with Term %d!\n", rf.me, term)
			// 初始化数组并马上抑制其他节点的选举
			for i := 0; i < length; i++ {
				rf.nextIndex[i] = rf.lastLogIndex
				rf.matchIndex[i] = 0
			}
			rf.switchTo(Leader)
			go rf.broadcastHeartbeat()
		}
		rf.mu.Unlock()
	} else {
		// 选举失败, 不能重回Follower, 否则会有并发问题
		// 假设这次选举非常久, 到了第二次选举, rf先成为candidate, 之后第一次选举执行到这里重回Follower
		// 而第二次选举成功, 相当于直接从Follower成为leader, 会导致条件变量错误!
		// 因此只要开始选举, 就一直是candidate, 除非成为leader或者被其他leader抑制
		_, _ = DPrintf("raft %d waiting for another elective.\n", rf.me)
	}
}

func (rf *Raft) heartbeatTimeoutChecker() {
	for atomic.LoadInt32(&rf.dead) != 1 {
		if _, isLeader := rf.GetState(); !isLeader {
			// 不是leader不需要发送心跳
			rf.mu.Lock()
			rf.waitUntilLeader.Wait()
			rf.mu.Unlock()
		} else {
			go rf.broadcastHeartbeat()
			time.Sleep(time.Millisecond * HeartbeatPeriod)
		}
	}
}

func (rf *Raft) broadcastHeartbeat() {
	rf.mu.Lock()
	// 可能leader收到了更高级的leader的心跳, 并且正在处理,占有了锁
	// 导致旧leader在调用broadcast的时候卡在lock上, 此时就leader由于响应了RPC, currentTerm已经被提高到和新leader一样的水平
	// 如果用这个Term发心跳会被接受, 导致错误, 其实应该在获取currentTerm的时候先判断还是不是Leader
	// 这样实现可以保证拿到的是leader时候的Term, 如果先响应了RPC, 那么就不是Leader
	// 如果这里先拿到锁, 那么得到的心跳包会被其他节点视为过期leader拒绝
	var term int
	if rf.status == Leader {
		term = rf.currentTerm
	} else {
		// 已经不是leader, 应该直接返回
		rf.mu.Unlock()
		return
	}
	_, _ = DPrintf("raft %d send heartbeat, with", rf.me)
	rf.mu.Unlock()

	length := len(rf.peers)
	for i := 0; i < length; i++ {
		if i == rf.me {
			continue
		}

		num := i
		args := new(AppendEntriesArgs)
		rf.mu.Lock()
		args.Term = term
		args.LeaderId = rf.me
		args.PrevLogIndex = rf.nextIndex[i]
		if args.PrevLogIndex < 0 {
			panic("args.PrevLogIndex < 0")
		}
		args.PrevLogTerm = rf.log[args.PrevLogIndex].Term
		args.LeaderCommit = rf.commitIndex
		rf.mu.Unlock()
		reply := new(AppendEntriesReply)
		_, _ = DPrintf("args %v\n", *args)
		go func() {
			if ok := rf.sendAppendEntries(num, args, reply); ok {
				rf.mu.Lock()
				if reply.Term < rf.currentTerm {
					// 过期的reply
				} else if reply.Term > rf.currentTerm {
					// 更新Term, 如果currentTerm变大了, 就会走上面那个分支, 把reply视为过期
					rf.currentTerm = reply.Term
					rf.switchTo(Follower)
				} else if !reply.Success {
					rf.nextIndex[num]--
				}
				rf.mu.Unlock()
			}
		}()
	}
}