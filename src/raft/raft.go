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
	"6.5840/labgob"
	"bytes"
	"os"
	"strconv"

	//	"bytes"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

const (
	electionTimeout = 800
	maxElectionTime = 300
	CANDIDATE       = "CANDIDATE"
	FOLLOWER        = "FOLLOWER"
	LEADER          = "LEADER"
	OFFSET          = 51200
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
	// StateMachineState []byte // 状态机状态，就是快照数据
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
	// state a Raft server must maintain.
	state       string
	currentTerm int
	votedFor    int
	log         []LogEntry
	resetTimer  chan struct{}
	timer       *time.Timer
	servern     int
	applyCh     chan ApplyMsg
	applychtmp  chan ApplyMsg
	commandch   chan LogEntry

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int

	LastIncludedIndex int
	LastIncludedTerm  int
	snapshotData      []byte

	tmpSnapshot         *os.File
	passiveSnapshotting bool
	activeSnapshotting  bool
	LeaderId            int
}

type LogEntry struct {
	Command interface{} // 客户端命令
	Term    int         // 日志的任期号
	Index   int         // 日志的索引
}

type SnapshotInfo struct {
	snapshot          []LogEntry
	lastIncludedIndex int
	lastIncludedTerm  int
}

// 检查raft是否有当前term的日志
func (rf *Raft) CheckCurrentTermLog() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if len(rf.log) > 0 {
		latestLog := rf.log[len(rf.log)-1]
		if rf.currentTerm == latestLog.Term {
			return true
		}
		return false
	}

	return true
}

// 由kvserver调用，获取rf.passiveSnapshotting标志，若其为false，则设activeSnapshotting为true
func (rf *Raft) GetPassiveFlagAndSetActiveFlag() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if !rf.passiveSnapshotting {
		rf.activeSnapshotting = true // 若没有进行被动快照则将主动快照进行标志设为true，以便后续的主动快照检查
	}
	return rf.passiveSnapshotting
}

// 由kvserver调用修改rf.passiveSnapshotting
func (rf *Raft) SetPassiveSnapshottingFlag(flag bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.passiveSnapshotting = flag
}

// 返回raft state size的字节数
func (rf *Raft) GetRaftStateSize() int {
	return rf.persister.RaftStateSize()
}

func (rf *Raft) GetRaftSnapshot() []byte {
	return rf.persister.ReadSnapshot()
}

// 由kvserver调用修改rf.activeSnapshotting
func (rf *Raft) SetActiveSnapshottingFlag(flag bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.activeSnapshotting = flag
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
	if rf.state == LEADER {
		isleader = true
	} else {
		isleader = false
	}
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
	Ct := rf.currentTerm
	Vf := rf.votedFor
	Log := rf.log
	Ls := rf.LastIncludedIndex
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(Ct)
	e.Encode(Vf)
	e.Encode(Log)
	e.Encode(Ls)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, rf.snapshotData)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte, snapshot []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var Ct int
	var Vf int
	var Log []LogEntry
	var Ls int
	if d.Decode(&Ct) != nil ||
		d.Decode(&Vf) != nil || d.Decode(&Log) != nil || d.Decode(&Ls) != nil {
		DPrintf("read persist error")
	} else {
		rf.currentTerm = Ct
		rf.votedFor = Vf
		rf.log = Log
		rf.LastIncludedIndex = Ls
		rf.commitIndex = Ls
		rf.lastApplied = Ls
		DPrintf("persist rf.lastApplied:%d", Ls)
	}
	rf.snapshotData = snapshot
	r2 := bytes.NewBuffer(snapshot)
	d2 := labgob.NewDecoder(r2)
	var lastIncludedIndex int
	var xlog []interface{}
	if d2.Decode(&lastIncludedIndex) != nil ||
		d2.Decode(&xlog) != nil {
		DPrintf("read persist error2")
	} else {
		rf.LastIncludedIndex = lastIncludedIndex
		rf.commitIndex = lastIncludedIndex
		rf.lastApplied = lastIncludedIndex
	}
}

func (rf *Raft) recoverFromSnap(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 { // 如果没有快照则直接返回
		return
	}

	//rf.lastApplied = rf.LastIncludedIndex
	//rf.commitIndex = rf.LastIncludedIndex

	// raft恢复后向kvserver发送快照
	snapshotMsg := ApplyMsg{
		SnapshotValid: true,
		CommandValid:  false,
		SnapshotIndex: rf.LastIncludedIndex,
		Snapshot:      snapshot, // sm_state
	}

	go func(msg ApplyMsg) {
		rf.applyCh <- msg // 将包含快照的ApplyMsg发送到applyCh，等待状态机处理
	}(snapshotMsg)

	DPrintf("Server %d recover from crash and send SnapshotMsg to ApplyCh.\n", rf.me)
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).
	rf.mu.Lock()
	// defer rf.mu.Unlock()
	if index <= rf.LastIncludedIndex {
		rf.mu.Unlock()
		return
	}

	rf.snapshotData = snapshot
	rf.persist()

	DPrintf("save;;;log %v, index %v commit index %v me %v", rf.log, index, rf.commitIndex, rf.me)
	if index-rf.LastIncludedIndex-1 >= len(rf.log) {
		// rf.LastIncludedTerm = rf.currentTerm
		rf.log = []LogEntry{}
		DPrintf("now log: %v, lastindex %v", rf.log, index)
		rf.LastIncludedIndex = index
	} else {
		rf.LastIncludedTerm = rf.log[index-rf.LastIncludedIndex-1].Term
		rf.log = rf.log[index-rf.LastIncludedIndex:]
		DPrintf("now log: %v, lastindex %v", rf.log, index)
		rf.LastIncludedIndex = index
	}
	//msg := ApplyMsg{}
	////msg.CommandValid = false
	//msg.SnapshotValid = true
	//msg.Snapshot = snapshot
	//// DPrintf("dddindex %v, last %v me %v", data, rf.LastIncludedIndex, rf.me)
	//msg.SnapshotTerm = rf.LastIncludedTerm
	//msg.SnapshotIndex = index
	//go func() {
	//	rf.applychtmp <- msg
	//}()
	//isLeader := (rf.state == LEADER)
	rf.mu.Unlock()
	//
	//// leader通过InstallSnapshot RPC将本次的SnapShot信息发送给其他Follower
	//if isLeader {
	//	for i, _ := range rf.peers {
	//		if i == rf.me {
	//			continue
	//		}
	//		go rf.LeaderSendSnapshot(i, snapshot)
	//	}
	//}
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Offset            int
	Data              []byte
	Done              bool
	Len               int
}

type InstallSnapshotReply struct {
	Term   int
	Accept bool
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}
	if args.Offset == 0 {
		filename := strconv.Itoa(rf.me) + ".data"
		file, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0644)
		if err != nil {
			DPrintf("open file error: %v", err)
			return
		}
		rf.tmpSnapshot = file
	}
	// rf.tmpSnapshot.Seek(int64(args.Offset), 0)
	rf.tmpSnapshot.WriteAt(args.Data, int64(args.Offset))
	if args.Done {
		data, _ := os.ReadFile(rf.tmpSnapshot.Name())
		go rf.Snapshot(args.LastIncludedIndex, data)
		msg := ApplyMsg{}
		//msg.CommandValid = false
		msg.SnapshotValid = true
		msg.Snapshot = data
		// DPrintf("dddindex %v, last %v me %v", data, rf.LastIncludedIndex, rf.me)
		msg.SnapshotTerm = rf.LastIncludedTerm
		msg.SnapshotIndex = args.LastIncludedIndex
		go func() {
			rf.applychtmp <- msg
		}()
		if rf.commitIndex < args.LastIncludedIndex {
			rf.commitIndex = args.LastIncludedIndex
			rf.lastApplied = args.LastIncludedIndex
		}

		os.Remove(rf.tmpSnapshot.Name())
	}
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

func (rf *Raft) sendInstallSnapshotWithTimeout(i int, args *InstallSnapshotArgs, reply *InstallSnapshotReply, timeout time.Duration) bool {
	resultCh := make(chan bool) // 带缓冲的通道，用于接收结果
	// quit := make(chan struct{})

	//go func() {
	//	select {
	//	case resultCh <- rf.sendAppendEntries(i, args, reply):
	//	case <-quit:
	//		// 收到退出信号
	//		return
	//	}
	//}()

	go func() {
		resultCh <- rf.sendInstallSnapshot(i, args, reply)
	}()

	select {
	case ok := <-resultCh:
		// 正常完成请求，返回结果
		return ok
	case <-time.After(timeout):
		// 超时，执行超时逻辑
		// fmt.Printf("RequestVote to %d timed out\n", i)
		DPrintf("timeout %v send snapshot to %v, term %v", rf.me, i, rf.currentTerm)
		go func() {
			<-resultCh
		}()
		return false
	}
}

func (rf *Raft) sendInstallSnapshotToFollwer(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	data := rf.persister.ReadSnapshot()
	lens := len(data)
	args.Len = lens
	// DPrintf("dddddata:%v", data)
	go func() {
		rf.resetTimer <- struct{}{}
		//select {
		//case rf.resetTimer <- struct{}{}:
		//	// DPrintf("heartbeat to %v", rf.me)
		//	// 发送成功
		//default:
		//	// 如果通道已满，跳过发送，避免阻塞
		//}
	}()
	if lens > OFFSET {
		tmp := 0
		for lens-tmp > OFFSET {
			dataset := data[tmp : tmp+OFFSET]
			args.Data = dataset
			args.Offset = tmp
			tmp += OFFSET
			ok := rf.sendInstallSnapshotWithTimeout(server, args, reply, 500*time.Millisecond)
			//for !ok {
			//	ok = rf.sendInstallSnapshotWithTimeout(server, args, reply, 500*time.Millisecond)
			//}
			if !ok {
				return false
			}
			if reply.Term > rf.currentTerm {
				rf.mu.Lock()
				rf.state = FOLLOWER
				rf.mu.Unlock()
				DPrintf("when snapshot, back to follwer %v", rf.me)
				return false
			}
		}
		dataset := data[tmp:]
		args.Data = dataset
		args.Offset = tmp
		args.Done = true
		ok := rf.sendInstallSnapshotWithTimeout(server, args, reply, 500*time.Millisecond)
		//for !ok {
		//	ok = rf.sendInstallSnapshotWithTimeout(server, args, reply, 500*time.Millisecond)
		//}
		return ok
	} else {
		args.Data = data
		args.Offset = 0
		args.Done = true
		ok := rf.sendInstallSnapshotWithTimeout(server, args, reply, 500*time.Millisecond)
		return ok
		//for !ok {
		//	ok = rf.sendInstallSnapshotWithTimeout(server, args, reply, 500*time.Millisecond)
		//}
	}
	if reply.Term > rf.currentTerm {
		rf.mu.Lock()
		rf.state = FOLLOWER
		rf.mu.Unlock()
		DPrintf("when snapshot, back to follwer %v", rf.me)
		return false
	}
	return true
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
	// Your data here (3A, 3B).
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	// DPrintf("term %v, %v receive %v req", rf.currentTerm, rf.me, args.CandidateId)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("get vote request from %v %v", rf.currentTerm, args.Term)
	if rf.currentTerm > args.Term {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}
	DPrintf("log %v, args %v lastindex:%v", rf.log, args, args.LastLogIndex)
	if rf.currentTerm < args.Term {
		if len(rf.log)+rf.LastIncludedIndex == 0 || len(rf.log) > 0 && (rf.log[len(rf.log)-1].Term < args.LastLogTerm ||
			(rf.log[len(rf.log)-1].Term == args.LastLogTerm && args.LastLogIndex >= rf.log[len(rf.log)-1].Index)) {
			reply.VoteGranted = true
			rf.currentTerm = args.Term
			reply.Term = rf.currentTerm
			rf.votedFor = args.CandidateId
			rf.persist()
			if rf.state == LEADER {
				rf.state = FOLLOWER
				DPrintf("%v receive new term vote, back to follower, term %v", rf.me, rf.currentTerm)
			}
			return
		} else if len(rf.log) == 0 && ( //rf.LastIncludedTerm < args.LastLogTerm || rf.LastIncludedTerm == args.LastLogTerm &&
		rf.LastIncludedIndex <= args.LastLogIndex) {
			// 3D code
			reply.VoteGranted = true
			rf.currentTerm = args.Term
			reply.Term = rf.currentTerm
			rf.votedFor = args.CandidateId
			rf.persist()
			if rf.state == LEADER {
				rf.state = FOLLOWER
				DPrintf("%v receive new term vote, back to follower, term %v", rf.me, rf.currentTerm)
			}
			return
		} else {
			reply.VoteGranted = false
			reply.Term = rf.currentTerm
			rf.currentTerm = args.Term
			rf.votedFor = -1
			rf.persist()
			return
		}
	} else if rf.currentTerm == args.Term {
		if rf.votedFor == args.CandidateId || (rf.votedFor == -1 && (len(rf.log)+rf.LastIncludedIndex == 0 || (len(rf.log) > 0 && (rf.log[len(rf.log)-1].Term < args.LastLogTerm ||
			(rf.log[len(rf.log)-1].Term == args.LastLogTerm && args.LastLogIndex >= rf.log[len(rf.log)-1].Index))))) {
			reply.VoteGranted = true
			reply.Term = rf.currentTerm
			rf.votedFor = args.CandidateId
			rf.persist()
			// rf.currentTerm = args.Term
			if rf.state == LEADER {
				rf.state = FOLLOWER
				DPrintf("%v receive new term vote, back to follower, term %v", rf.me, rf.currentTerm)
			}
			return
		} else if rf.votedFor == -1 && len(rf.log) == 0 && ( //rf.LastIncludedTerm < args.LastLogTerm || rf.LastIncludedTerm == args.LastLogTerm &&
		rf.LastIncludedIndex <= args.LastLogIndex) {
			// 3D code
			reply.VoteGranted = true
			reply.Term = rf.currentTerm
			rf.votedFor = args.CandidateId
			rf.persist()
			// rf.currentTerm = args.Term
			if rf.state == LEADER {
				rf.state = FOLLOWER
				DPrintf("%v receive new term vote, back to follower, term %v", rf.me, rf.currentTerm)
			}
			return
		} else {
			reply.VoteGranted = false
			reply.Term = rf.currentTerm
			return
		}
	}
	// DPrintf("term %v, %v vote %v, current req term %v,me %v, isVote:%v", rf.currentTerm, rf.me, rf.votedFor, args.Term, args.CandidateId, reply.VoteGranted)
	//if rf.votedFor == args.CandidateId || rf.votedFor == -1 || rf.currentTerm <= args.Term {
	//	if len(rf.log) == 0 {
	//		reply.VoteGranted = true
	//		rf.currentTerm = args.Term
	//		rf.votedFor = args.CandidateId
	//		if rf.state == LEADER {
	//			rf.state = FOLLOWER
	//			DPrintf("%v receive new term vote, back to follower, term %v", rf.me, rf.currentTerm)
	//		}
	//	} else if args.LastLogTerm >= rf.log[len(rf.log)-1].Term &&
	//		args.LastLogIndex >= rf.log[len(rf.log)-1].Index {
	//		reply.VoteGranted = true
	//		rf.currentTerm = args.Term
	//		rf.votedFor = args.CandidateId
	//		if rf.state == LEADER {
	//			rf.state = FOLLOWER
	//			DPrintf("%v receive new term vote, back to follower, term %v", rf.me, rf.currentTerm)
	//		}
	//	}
	//} else {
	//	reply.VoteGranted = false
	//}
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

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
	// Your data here (3A, 3B).
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type AppendEntriesReply struct {
	// Your data here (3A).
	Term      int
	Success   bool
	PrevIndex int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// DPrintf("%v receive heartbeat to %v, term %v", rf.me, args.LeaderId, rf.currentTerm)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	// DPrintf("appargs: %v, %v", args, rf.currentTerm)
	go func() {
		rf.resetTimer <- struct{}{}
		//select {
		//case rf.resetTimer <- struct{}{}:
		// DPrintf("heartbeat to %v", rf.me)
		// 发送成功
		//default:
		//	// 如果通道已满，跳过发送，避免阻塞
		//}
	}()
	if args.Term < rf.currentTerm {
		DPrintf("leader term < follwer %v", rf.currentTerm)
		reply.Success = false
		return
	} else if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.persist()
		if rf.state == LEADER {
			rf.state = FOLLOWER
			DPrintf("%v receive heartbeat from %v, term %v, back tofollwer", rf.me, args.LeaderId, rf.currentTerm)
		}
	}
	DPrintf("entries:%v, leader commitindex %v, rfci: %v", args.Entries, args.LeaderCommit, rf.commitIndex)
	DPrintf("log:%v, %v previndex %v prevterm %v me %v lastindex", rf.log, args.PrevLogIndex, args.PrevLogTerm, rf.me, rf.LastIncludedIndex)
	//if args.PrevLogIndex > 0 && len(rf.log)+rf.LastIncludedIndex > 0 && ((args.PrevLogIndex < rf.LastIncludedIndex ||
	//	(len(rf.log) > 0 &&
	//		(args.PrevLogIndex > rf.log[len(rf.log)-1].Index ||
	//			args.PrevLogIndex != rf.log[args.PrevLogIndex-1-rf.LastIncludedIndex].Index ||
	//			args.PrevLogTerm != rf.log[args.PrevLogIndex-1-rf.LastIncludedIndex].Term))) ||
	//	(args.PrevLogIndex == rf.LastIncludedIndex &&
	//		(len(rf.log) > 0 &&
	//			(args.PrevLogIndex > rf.log[len(rf.log)-1].Index ||
	//				args.PrevLogIndex != rf.LastIncludedIndex ||
	//				args.PrevLogTerm != rf.LastIncludedTerm)))) {
	if args.PrevLogIndex > len(rf.log)+rf.LastIncludedIndex ||
		(args.PrevLogIndex > rf.LastIncludedIndex && (rf.log[args.PrevLogIndex-1-rf.LastIncludedIndex].Index != args.PrevLogIndex ||
			rf.log[args.PrevLogIndex-1-rf.LastIncludedIndex].Term != args.PrevLogTerm)) ||
		// (args.PrevLogIndex == rf.LastIncludedIndex && args.PrevLogTerm != rf.LastIncludedTerm) ||
		(args.PrevLogIndex < rf.LastIncludedIndex) {
		// DPrintf("rf.log %v", rf.log)
		reply.Success = false
		// 3D code
		if args.PrevLogIndex > len(rf.log)+rf.LastIncludedIndex {
			reply.PrevIndex = len(rf.log) + rf.LastIncludedIndex
		} else {
			reply.PrevIndex = rf.LastIncludedIndex
		}
		return
	} else {
		if len(args.Entries) != 0 {
			rf.log = append(rf.log[:args.PrevLogIndex-rf.LastIncludedIndex], args.Entries...)
			rf.persist()
		}
	}
	//if args.PrevLogIndex == 0 {
	//	if args.Entries != nil {
	//		rf.log = append(rf.log, args.Entries...)
	//		// DPrintf("write log %v to rf:%v", args.Entries, rf.me)
	//		// rf.commitIndex = len(rf.log) - 1
	//	}
	// }
	//if args.PrevLogIndex > len(rf.log) {
	//	reply.Success = false
	//	return
	//} else
	//if rf.log[args.PrevLogIndex-1] {
	//	if rf.log[args.PrevLogIndex-1].Term != args.Term {
	//		rf.log = rf.log[:args.PrevLogIndex]
	//		reply.Success = false
	//		return
	//	} else {
	//		if args.Entries != nil {
	// DPrintf("loglog:%v me%v", rf.log, rf.me)
	// DPrintf("loglog:%v me%v", rf.log, rf.me)
	// rf.commitIndex = len(rf.log[:args.PrevLogIndex])
	// DPrintf("write log %v to rf:%v", args.Entries, rf.me)
	// rf.commitIndex = len(rf.log) - 1
	DPrintf("me:%v, leader commit %v, commit:%v", rf.me, args.LeaderCommit, rf.commitIndex)
	if args.LeaderCommit > rf.commitIndex {
		var tmp int
		// DPrintf("len log %v", len(rf.log))
		if args.LeaderCommit > len(rf.log)+rf.LastIncludedIndex {
			tmp = len(rf.log) + rf.LastIncludedIndex
		} else {
			tmp = args.LeaderCommit
		}
		if rf.commitIndex < rf.LastIncludedIndex {
			reply.Success = false
			reply.PrevIndex = rf.commitIndex
			return
		}
		// DPrintf("me:%v , commitindex %v tmp:%v", rf.me, rf.commitIndex, tmp)
		go func(tmp int, cidx int, log []LogEntry) {
			msg := ApplyMsg{}
			for i := cidx; i < tmp; i++ {
				msg.CommandIndex = i + 1
				msg.Command = log[i-rf.LastIncludedIndex].Command
				msg.CommandValid = true
				DPrintf("me:%v , commitindex %v，log %v", rf.me, msg.CommandIndex, msg.Command)
				rf.applychtmp <- msg
			}
		}(tmp, rf.commitIndex, rf.log)
		rf.commitIndex = tmp
		DPrintf("now follwer commit index %v me %v", rf.commitIndex, rf.me)
		rf.lastApplied = tmp
	}
	reply.Success = true
	// DPrintf("%v %v", reply.Term, rf.currentTerm)
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntriesWithTimeout(i int, args *AppendEntriesArgs, reply *AppendEntriesReply, timeout time.Duration) bool {
	resultCh := make(chan bool) // 带缓冲的通道，用于接收结果
	// quit := make(chan struct{})

	//go func() {
	//	select {
	//	case resultCh <- rf.sendAppendEntries(i, args, reply):
	//	case <-quit:
	//		// 收到退出信号
	//		return
	//	}
	//}()

	go func() {
		resultCh <- rf.sendAppendEntries(i, args, reply)
	}()

	select {
	case ok := <-resultCh:
		// 正常完成请求，返回结果
		return ok
	case <-time.After(timeout):
		// 超时，执行超时逻辑
		// fmt.Printf("RequestVote to %d timed out\n", i)
		DPrintf("timeout %v send heartbeat to %v, term %v", rf.me, i, rf.currentTerm)
		go func() {
			<-resultCh
		}()
		return false
	}
}

func (rf *Raft) sendAppendEntriesToALl(command []interface{}) {
	args := AppendEntriesArgs{}
	args.LeaderId = rf.me
	args.Term = rf.currentTerm
	success := 1
	total := 1
	cond := sync.NewCond(&rf.mu)
	// DPrintf("leaderCommitId %v, nextid %v,", rf.commitIndex, rf.nextIndex)
	//if command != nil {
	//	DPrintf("leaderCommitId %v, command %v", rf.commitIndex, command)
	//	args.Entries = []LogEntry{}
	//	log := command[0].(LogEntry)
	//	args.Entries = append(args.Entries, log)
	//}
	// DPrintf("nextindex %v", rf.nextIndex)

	// DPrintf("leader %v log %v", rf.me, rf.log)
	//chgodunc := make([]chan int, len(rf.peers))
	//for i := range chgodunc {
	//	chgodunc[i] = make(chan int, 1) // 初始化每个信道
	//}
	for i, _ := range rf.peers {
		if i == rf.me {
			continue
		}
		// DPrintf("%v send heartbeat to %v, term %v", rf.me, i, rf.currentTerm)
		go func(i int, args AppendEntriesArgs) {
			// <-rf.chgodunc[i]
			rf.mu.Lock()
			if len(rf.log)+rf.LastIncludedIndex >= rf.nextIndex[i] {
				if rf.nextIndex[i]-1-rf.LastIncludedIndex >= 0 {
					args.Entries = rf.log[rf.nextIndex[i]-1-rf.LastIncludedIndex:]
				} else {
					//data := rf.persister.ReadSnapshot()
					//lens := len(data)
					DPrintf("sendInstallSnapshotToFollwer, lastincluded %v", rf.LastIncludedIndex)
					argss := InstallSnapshotArgs{
						Term:              rf.currentTerm,
						LeaderId:          rf.me,
						LastIncludedIndex: rf.LastIncludedIndex,
						LastIncludedTerm:  rf.LastIncludedTerm,
						Done:              false,
						// Len:               lens,
					}
					replyy := InstallSnapshotReply{}
					rf.mu.Unlock()
					okk := rf.sendInstallSnapshotToFollwer(i, &argss, &replyy)
					rf.mu.Lock()
					if okk {
						rf.nextIndex[i] = rf.LastIncludedIndex + 1
						success += 1
					}
					total += 1
					rf.mu.Unlock()
					cond.Signal()
					return
				}

			}

			//if rf.nextIndex[i] <= rf.commitIndex {
			//	if args.Entries == nil {
			//		// DPrintf("nextindex %v, rf,me %v, commitindex %v", rf.nextIndex[i], rf.me, rf.commitIndex)
			//		args.Entries = append(rf.log[rf.nextIndex[i]-1:rf.commitIndex], args.Entries...)
			//	} else if rf.log[rf.commitIndex-1].Index < args.Entries[0].Index {
			//		// DPrintf("nextindex %v, rf,me %v, commitindex %v", rf.nextIndex[i], rf.me, rf.commitIndex)
			//		args.Entries = append(rf.log[rf.nextIndex[i]-1:rf.commitIndex], args.Entries...)
			//		DPrintf("command entries %v", args.Entries)
			//	}
			//}
			reply := AppendEntriesReply{}
			args.LeaderCommit = rf.commitIndex
			// args.PrevLogTerm = rf.log[rf.nextIndex[i]-1][0].(int)
			args.PrevLogIndex = rf.nextIndex[i] - 1
			if args.PrevLogIndex-rf.LastIncludedIndex == 0 || args.PrevLogIndex > rf.LastIncludedIndex+len(rf.log) {
				args.PrevLogTerm = 0
			} else {
				DPrintf("%v, %v, %v %v", rf.log, rf.nextIndex[i], i, rf.LastIncludedIndex)
				args.PrevLogTerm = rf.log[args.PrevLogIndex-1-rf.LastIncludedIndex].Term
			}
			rf.mu.Unlock()
			// DPrintf("send appendreply %v, %v, %v", rf.log, rf.nextIndex, i)
			ok := rf.sendAppendEntriesWithTimeout(i, &args, &reply, 250*time.Millisecond)
			// DPrintf("%v %v %v", i, reply, rf.currentTerm)
			//for ok && !reply.Success {
			//	args.PrevLogIndex = args.PrevLogIndex - 3
			//	args.PrevLogTerm = rf.log[args.PrevLogIndex][0].(int)
			//	args.Entries = append(rf.log[args.PrevLogIndex:args.PrevLogIndex+3], args.Entries...)
			//	ok = rf.sendAppendEntriesWithTimeout(i, &args, &reply, randomElectionTimeout())
			//}
			//for !ok {
			//	ok = rf.sendAppendEntriesWithTimeout(i, &args, &reply, randomElectionTimeout())
			//}
			DPrintf("ok %v reply::%v", ok, reply)
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if ok && !reply.Success && reply.Term <= args.Term {
				DPrintf("decrease index %v", i)
				//rf.mu.Lock()
				//if args.PrevLogIndex-5 > 0 {
				//	rf.nextIndex[i] = args.PrevLogIndex - 5
				//} else {
				//	rf.nextIndex[i] = args.PrevLogIndex
				//}
				rf.nextIndex[i] = reply.PrevIndex + 1

				//args.PrevLogIndex = rf.nextIndex[i] - 1
				//if args.PrevLogIndex == 0 {
				//	args.PrevLogTerm = 0
				//} else {
				//	//	DPrintf("%v reply %v", reply, rf.currentTerm)
				//	args.PrevLogTerm = rf.log[args.PrevLogIndex-1].Term
				//}
				// DPrintf("arg %v", args)
				// args.Entries = append(rf.log[args.PrevLogIndex:args.PrevLogIndex+1], args.Entries...)
				//rf.mu.Unlock()
				// ok = rf.sendAppendEntriesWithTimeout(i, &args, &reply, randomElectionTimeout())
			} else if ok && reply.Term > rf.currentTerm {
				rf.state = FOLLOWER
				DPrintf("term %v, leader %v back to follwer, new term %v  me:%v", rf.currentTerm, rf.me, reply.Term, i)
				rf.currentTerm = reply.Term
				rf.persist()
			} else if ok && reply.Success {
				// DPrintf("%v, %v, %v", args.PrevLogIndex, args.Entries, i)
				rf.nextIndex[i] = args.PrevLogIndex + len(args.Entries) + 1
				rf.matchIndex[i] = rf.nextIndex[i] - 1
				DPrintf("%v success", i)
				success += 1
			}
			total += 1
			cond.Signal()
			// rf.chgodunc[i] <- 1
		}(i, args)
	}
	rf.mu.Lock()
	for 2*success <= rf.servern && rf.servern > total {
		cond.Wait()
		// DPrintf("%v, %v, %v", len(rf.peers), total, vote)
		// DPrintf("%v", rf.state)
	}
	rf.mu.Unlock()
	DPrintf("me:%v, nextindex:%v, matchindex:%v", rf.me, rf.nextIndex, rf.matchIndex)
	DPrintf("sum & total %v %v me:%v", success, total, rf.me)
	_, isLeader := rf.GetState()
	if isLeader && 2*success > rf.servern {

		// DPrintf("%v prepare to commit", rf.me)
		matchIndexes := make([]int, len(rf.matchIndex))
		copy(matchIndexes, rf.matchIndex)
		matchIndexes[rf.me] = rf.LastIncludedIndex + len(rf.log)

		// 排序，找到超过一半的 matchIndex
		sort.Sort(sort.Reverse(sort.IntSlice(matchIndexes)))
		N := matchIndexes[len(rf.peers)/2] // 超过半数的日志索引
		// 确保日志 N 属于当前任期
		DPrintf("N %v, commitindex %v log %v %v", N, rf.commitIndex, rf.log, rf.LastIncludedIndex)
		//if rf.commitIndex < rf.LastIncludedIndex {
		//	rf.mu.Lock()
		//	rf.commitIndex = rf.LastIncludedIndex
		//	DPrintf("now commit index %v me %v", rf.commitIndex, rf.me)
		//	rf.lastApplied = rf.LastIncludedIndex
		//	rf.mu.Unlock()
		//}

		if N > rf.commitIndex && rf.log[len(rf.log)-1].Term == rf.currentTerm {
			DPrintf("Leader %v updated %v commitIndex to %v %v", rf.me, rf.commitIndex, N, rf.LastIncludedIndex)
			go func(tmp int, cidx int, log []LogEntry) {
				msg := ApplyMsg{}
				for i := cidx; i < tmp; i++ {
					msg.CommandIndex = i + 1
					msg.Command = log[i-rf.LastIncludedIndex].Command
					msg.CommandValid = true
					DPrintf("leader me:%v , commitindex %v ，log %v %v %v", rf.me, msg.CommandIndex, msg.Command, tmp, cidx)
					rf.applychtmp <- msg
					//rf.mu.Lock()
					//rf.commitIndex = i + 1
					//rf.mu.Unlock()
				}
			}(N, rf.commitIndex, rf.log)
			rf.mu.Lock()
			rf.commitIndex = N
			DPrintf("now commit index %v me %v", rf.commitIndex, rf.me)
			rf.lastApplied = N
			rf.mu.Unlock()
			// rf.sendAppendEntriesToALl(nil)
			// 通知状态机应用日志
		}
	}

}

func (rf *Raft) sendRequestVoteWithTimeout(i int, args *RequestVoteArgs, reply *RequestVoteReply, timeout time.Duration) bool {
	resultCh := make(chan bool) // 带缓冲的通道，用于接收结果

	go func() {
		resultCh <- rf.sendRequestVote(i, args, reply) // 执行发送请求，并将结果发送到通道
	}()

	select {
	case ok := <-resultCh:
		// 正常完成请求，返回结果
		return ok
	case <-time.After(timeout):
		// 超时，执行超时逻辑
		// fmt.Printf("RequestVote to %d timed out\n", i)
		DPrintf("timeout %v requestvote to %v, term %v", rf.me, i, rf.currentTerm)
		go func() {
			<-resultCh
		}()
		return false
	}
}

func (rf *Raft) StartElection() {
	total := 1
	vote := 1
	cond := sync.NewCond(&rf.mu)
	// var wg sync.WaitGroup
	// DPrintf("%v %v", rf.me, len(rf.peers))
	for i, _ := range rf.peers {
		if i == rf.me {
			continue
		}
		// wg.Add(1)
		go func(i int) {
			// defer wg.Done()
			args := &RequestVoteArgs{}
			args.Term = rf.currentTerm
			args.CandidateId = rf.me
			args.LastLogIndex = len(rf.log) + rf.LastIncludedIndex
			if len(rf.log) == 0 {
				args.LastLogTerm = -1
			} else {
				args.LastLogTerm = rf.log[args.LastLogIndex-1-rf.LastIncludedIndex].Term
			}
			reply := &RequestVoteReply{}
			DPrintf("term %v, node %v sent request vote to %d", rf.currentTerm, rf.me, i)
			ok := rf.sendRequestVoteWithTimeout(i, args, reply, 300*time.Millisecond)
			// DPrintf("%v %v", ok, reply)
			rf.mu.Lock()
			if ok && reply.VoteGranted {
				vote += 1
			} else if ok && !reply.VoteGranted && (reply.Term > rf.currentTerm) {
				rf.state = FOLLOWER
				DPrintf("term %v candidate %v fail, new leader term %v", rf.currentTerm, rf.me, reply.Term)
				rf.currentTerm = reply.Term
				rf.persist()
			}
			total += 1
			rf.mu.Unlock()
			cond.Signal()
		}(i)
	}
	rf.mu.Lock()
	for total < rf.servern && 2*vote <= rf.servern && rf.state == CANDIDATE {
		cond.Wait()
		// DPrintf("%v, %v, %v", len(rf.peers), total, vote)
		// DPrintf("%v", rf.state)
	}
	rf.mu.Unlock()
	// DPrintf("%v", rf.state)
	// wg.Wait()
	if rf.state == FOLLOWER {
		return
	}
	if 2*vote > rf.servern {
		rf.mu.Lock()
		rf.state = LEADER
		rf.mu.Unlock()
		DPrintf("%d become term %v leader.", rf.me, rf.currentTerm)
		// Reinitialized  2 slice
		// lastLogIndex := len(rf.log)
		rf.nextIndex = make([]int, rf.servern)
		for i := range rf.nextIndex {
			if len(rf.log) > 0 {
				rf.nextIndex[i] = rf.log[len(rf.log)-1].Index + 1
			} else {
				rf.nextIndex[i] = 1
			}
		}
		rf.matchIndex = make([]int, rf.servern)
		for i := range rf.matchIndex {
			rf.matchIndex[i] = 0
		}
		rf.sendAppendEntriesToALl(nil)
	} else {
		DPrintf("%v back to follwer, term %v", rf.me, rf.currentTerm)
		rf.state = FOLLOWER
	}
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
func (rf *Raft) Execute() {
	for command := range rf.commandch {
		DPrintf("get command %v", command)
		DPrintf("client log : %v, me:%v, index:%v term:%v", command, rf.me, command.Index, rf.currentTerm)
		rf.sendAppendEntriesToALl([]interface{}{LogEntry{Term: command.Term, Command: command.Command, Index: command.Index}})
	}
}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	// Your code here (3B).
	index := -1
	term := -1
	isLeader := rf.state == LEADER
	// log.Printf("leader %v %v", rf.state == LEADER, rf.me)
	if !isLeader {
		return -1, -1, false
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index = len(rf.log) + 1 + rf.LastIncludedIndex
	term = rf.currentTerm
	// command_ := LogEntry{Command: command, Term: term, Index: index}
	rf.log = append(rf.log, LogEntry{Term: term, Command: command, Index: index})
	rf.persist()
	DPrintf("new log %v index %v %v leader:%v", command, index, rf.LastIncludedIndex, rf.me)
	//log.Printf("leader true")
	//if len(rf.log) > 5000 {
	//	// s := SnapshotInfo{}
	//	xlog := rf.log[:rf.lastApplied]
	//	lastIncludedIndex := rf.log[rf.lastApplied-1].Index
	//	// lastIncludedTerm := rf.log[rf.lastApplied-1].Term
	//
	//	ss := new(bytes.Buffer)
	//	data := labgob.NewEncoder(ss)
	//	data.Encode(lastIncludedIndex)
	//	// data.Encode(lastIncludedTerm)
	//	data.Encode(xlog)
	//	snapshotData := ss.Bytes()
	//	DPrintf("leader snapshot")
	//	go rf.Snapshot(lastIncludedIndex, snapshotData)
	//}
	//DPrintf("%v log %v", rf.me, rf.log)
	//rf.commandch <- command_
	//
	//DPrintf("sent command %v", command_)
	return index, term, true
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

func randomElectionTimeout() time.Duration {
	return time.Duration(electionTimeout+rand.Intn(maxElectionTime)) * time.Millisecond
}

func (rf *Raft) ticker() {
	for rf.killed() == false {
		_, isLeader := rf.GetState()
		// DPrintf("%v me, state is %v", rf.me, isLeader)
		if isLeader {
			go func() {
				rf.sendAppendEntriesToALl(nil)
			}() // pause for a random amount of time between 50 and 350
			// milliseconds.
			ms := 300
			time.Sleep(time.Duration(ms) * time.Millisecond)
		} else {
			rf.timer = time.NewTimer(randomElectionTimeout())

			// Your code here (3A)
			// Check if a leader election should be started.
			select {
			case <-rf.timer.C:
				DPrintf("timeout, %v start election", rf.me)
				// 超时，转换到候选者状态并启动选举
				// log.Println("Timeout: starting election")
				rf.state = CANDIDATE
				rf.mu.Lock()
				// DPrintf("%v, %v, %v", rf.me, rf.currentTerm, rf.votedFor)
				rf.currentTerm += 1
				rf.votedFor = rf.me
				rf.persist()
				rf.mu.Unlock()
				rf.StartElection()
			case <-rf.resetTimer:
				// 收到重置信号，重置计时器
				// DPrintf("%v reveive hb", rf.me)
				// log.Println("Heartbeat received, resetting timer")
			}
		}
	}
	// DPrintf("%v dead.", rf.me)
}

func (rf *Raft) watchmsg() {
	for command := range rf.applychtmp {
		rf.applyCh <- command
	}
}

// leader发送SnapShot信息给落后的Follower
// idx是要发送给的follower的序号
func (rf *Raft) LeaderSendSnapshot(idx int, snapshotData []byte) {
	rf.mu.Lock()
	sameTerm := rf.currentTerm // 记录rf.currentTerm的副本，在goroutine中发送RPC时使用相同的term
	rf.mu.Unlock()

	// leader向follower[idx]发送InstallSnapshot RPC

	if rf.killed() { // 如果在发送InstallSnapshot RPC过程中leader被kill了就直接结束
		return
	}

	rf.mu.Lock()

	// 发送RPC之前先判断，如果自己不再是leader了则直接返回
	if rf.state != LEADER {
		rf.mu.Unlock()
		return
	}

	args := InstallSnapshotArgs{
		Term:              sameTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.LastIncludedIndex,
		LastIncludedTerm:  rf.LastIncludedTerm,
		Data:              snapshotData,
	}
	rf.mu.Unlock()
	reply := InstallSnapshotReply{}

	DPrintf("Leader %d sends InstallSnapshot RPC(term:%d, LastIncludedIndex:%d, LastIncludedTerm:%d) to server %d...\n",
		rf.me, sameTerm, args.LastIncludedIndex, args.LastIncludedTerm, idx)
	// 注意传的是args和reply的地址而不是结构体本身！
	ok := rf.sendSnapshot(idx, &args, &reply) // leader向 server idx 发送InstallSnapshot RPC

	if !ok {
		DPrintf("Leader %d calls server %d for InstallSnapshot failed!\n", rf.me, idx)
		// 如果由于网络原因或者follower故障等收不到RPC回复
		return
	}

	// 如果leader收到比自己任期更大的server的回复，则leader更新自己的任期并转为follower，跟随此server
	rf.mu.Lock() //要整体加锁，不能只给if加锁然后解锁
	defer rf.mu.Unlock()

	// 处理RPC回复之前先判断，如果自己不再是leader了则直接返回
	// 防止任期混淆（当收到旧任期的RPC回复，比较当前任期和原始RPC中发送的任期，如果两者不同，则放弃回复并返回）
	if rf.state != LEADER || rf.currentTerm != args.Term {
		return
	}

	if rf.currentTerm < reply.Term {
		rf.votedFor = -1            // 当term发生变化时，需要重置votedFor
		rf.state = FOLLOWER         // 变回Follower
		rf.currentTerm = reply.Term // 更新自己的term为较新的值
		rf.persist()
		return // 这里只是退出了协程
	}

	// 若follower接受了leader的快照则leader需要更新对应的matchIndex和nextIndex等（也得保证递增性，不能回退）
	if reply.Accept {
		possibleMatchIdx := args.LastIncludedIndex
		if possibleMatchIdx > rf.matchIndex[idx] {
			rf.matchIndex[idx] = possibleMatchIdx
		} else {
			rf.matchIndex[idx] = args.LastIncludedIndex
		}
		// 保证matchIndex单调递增，因为不可靠网络下会出现RPC延迟
		rf.nextIndex[idx] = rf.matchIndex[idx] + 1 // matchIndex安全则nextIndex这样也安全
	}

}

// 由leader调用，向其他servers发送快照
// 入参server是目的server在rf.peers[]中的索引（id）
func (rf *Raft) sendSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.CondInstallSnap", args, reply) // 调用对应server的Raft.GetSnapshot方法安装日志
	return ok
}

// 被动快照
// follower接收leader发来的InstallSnapshot RPC的handler
func (rf *Raft) CondInstallSnap(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Figure 13 rules[1]
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Accept = false
		return
	}

	// 接受leader的被动快照前先检查给server是否正在进行主动快照，若是则本次被动快照取消
	// 避免主、被动快照重叠应用导致上层kvserver状态与下层raft日志不一致
	if rf.activeSnapshotting {
		reply.Term = rf.currentTerm
		reply.Accept = false
		return
	}

	// wasLeader := (rf.state == LEADER) // 标志rf曾经是leader

	if args.Term > rf.currentTerm {
		rf.votedFor = -1           // 当term发生变化时，需要重置votedFor
		rf.currentTerm = args.Term // 更新自己的term为较新的值
		rf.persist()
	}

	rf.state = FOLLOWER         // 变回或维持Follower
	rf.LeaderId = args.LeaderId // 将rpc携带的leaderId设为自己的leaderId，记录最近的leader（client寻找leader失败时用到）

	// 如果follower的term与leader的term相等（大多数情况），那么follower收到AppendEntries RPC后也需要重置计时器
	rf.timer.Stop()
	rf.timer.Reset(time.Duration(300) * time.Millisecond)

	//if wasLeader { // 如果是leader收到AppendEntries RPC（虽然概率很小）
	//	go rf.HandleTimeout() // 如果是leader重回follower则要重新循环进行超时检测
	//}

	snapshotIndex := args.LastIncludedIndex
	snapshotTerm := args.LastIncludedTerm
	reply.Term = rf.currentTerm

	if snapshotIndex <= rf.LastIncludedIndex { // 说明snapshotIndex之前的log已经做成snapshot并删除了
		DPrintf("Server %d refuse the snapshot from leader.\n", rf.me)
		reply.Accept = false
		return
	}

	var newLog []LogEntry

	// 如果leader传来的快照比本地的快照更新

	rf.lastApplied = args.LastIncludedIndex // 下一条指令直接从快照后开始（重新）apply

	if snapshotIndex < rf.log[len(rf.log)-1].Index { // 若应用此次快照，本地还有日志要接上
		// 若快照和本地日志在snapshotIndex索引处的日志term不一致，则扔掉本地快照后的所有日志
		if rf.log[snapshotIndex-rf.LastIncludedIndex].Term != snapshotTerm {
			newLog = []LogEntry{{Term: snapshotTerm, Index: snapshotIndex}}
			rf.commitIndex = args.LastIncludedIndex // 由于后面被截断的日志无效，故等重新计算commitIndex
		} else { // 若term没冲突，则在snapshotIndex处截断并保留后续日志
			newLog = []LogEntry{{Term: snapshotTerm, Index: snapshotIndex}}
			newLog = append(newLog, rf.log[snapshotIndex-rf.LastIncludedIndex+1:]...)
			if rf.commitIndex < args.LastIncludedIndex {
				rf.commitIndex = args.LastIncludedIndex
			}
		}
	} else { // 若此快照比本地日志还要长，则应用后日志就清空了
		newLog = []LogEntry{{Term: snapshotTerm, Index: snapshotIndex}}
		rf.commitIndex = args.LastIncludedIndex
	}

	// 更新相关变量
	rf.LastIncludedIndex = args.LastIncludedIndex
	rf.LastIncludedIndex = args.LastIncludedTerm
	rf.log = newLog
	rf.passiveSnapshotting = true

	// 通过persister进行持久化存储
	rf.persist()                                               // 先持久化raft state（因为rf.log，rf.lastIncludedIndex，rf.lastIncludedTerm改变了）
	rf.persister.Save(rf.persister.ReadRaftState(), args.Data) // 持久化raft state及快照

	// 向kvserver发送带快照的ApplyMsg通知它安装
	rf.InstallSnapFromLeader(snapshotIndex, args.Data)

	DPrintf("Server %d accept the snapshot from leader(lastIncludedIndex=%v, lastIncludedTerm=%v).\n", rf.me, rf.LastIncludedIndex, rf.LastIncludedTerm)
	reply.Accept = true
	return
}

// 如果接受，则follower将leader发来的快照发到applyCh便于状态机安装
func (rf *Raft) InstallSnapFromLeader(snapshotIndex int, snapshotData []byte) {
	// follower接收到leader发来的InstallSnapshot RPC后先不要安装快照，而是发给状态机，判断为较新的快照时raft层才进行快照
	snapshotMsg := ApplyMsg{
		SnapshotValid: true,
		CommandValid:  false,
		SnapshotIndex: snapshotIndex,
		Snapshot:      snapshotData, // sm_state
	}

	rf.applyCh <- snapshotMsg // 将包含快照的ApplyMsg发送到applyCh，等待状态机处理
	DPrintf("Server %d send SnapshotMsg(snapIndex=%v) to ApplyCh.\n", rf.me, snapshotIndex)

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
	// DPrintf("%v", rf.me)

	// Your initialization code here (3A, 3B, 3C).
	// todo init
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.state = FOLLOWER
	rf.resetTimer = make(chan struct{})
	rf.servern = len(peers)
	rf.commitIndex = 0
	rf.applyCh = applyCh
	rf.commandch = make(chan LogEntry, 1000)
	rf.applychtmp = make(chan ApplyMsg, 1000)
	rf.LastIncludedIndex = 0
	rf.LastIncludedTerm = 0
	rf.persister = persister
	rf.snapshotData = nil
	//rf.chgodunc = make([]chan int, len(rf.peers))
	//for i := range rf.chgodunc {
	//	rf.chgodunc[i] = make(chan int, 1) // 初始化每个信道
	//}
	// rf.log = [][]interface{}
	// DPrintf("%v init.", rf.me)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState(), persister.ReadSnapshot())
	rf.recoverFromSnap(rf.persister.ReadSnapshot())
	// gob.Register(LogEntry{})

	// start ticker goroutine to start elections
	go rf.ticker()
	// go rf.Execute()
	go rf.watchmsg()

	return rf
}
