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
	"bytes"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labgob"
	"6.824/labrpc"
)

type EntryLog struct {
	Term    int
	Index   int
	Command interface{}
}

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// added by hs
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

const (
	ELECTION_TIMEOUT_MAX = 100
	ELECTION_TIMEOUT_MIN = 50

	HEARTBEAT_TIMEOUT = 27
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
	// 2A added by hs
	currentTerm int
	votedFor    int
	log         []EntryLog

	state StateType
	votes int

	electiontimer  *time.Timer
	heartbeattimer *time.Timer

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int

	applyCh  chan ApplyMsg
	applylog chan bool
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

func Heartbeat() time.Duration {
	return time.Millisecond * HEARTBEAT_TIMEOUT
}
func Election() time.Duration {
	// DPrintf("%v", time.Now())
	rand.Seed(time.Now().UnixNano())
	return time.Millisecond * time.Duration(rand.Intn(ELECTION_TIMEOUT_MAX-ELECTION_TIMEOUT_MIN)+ELECTION_TIMEOUT_MIN)
}

func (rf *Raft) tofollower(term int) {
	// 转变成为follower
	// 更新为新的term
	// 投票的人变为，null
	// 重置electiontime
	rf.state = StateFollower
	rf.currentTerm = term
	rf.votedFor = -1
}
func (rf *Raft) tocandidate() {
	rf.state = StateCandidate
	rf.currentTerm += 1
	rf.votedFor = rf.me
	rf.votes = 1
}
func (rf *Raft) toleader() {
	// 转变为leader
	// 不是stateCandidate就返回,一般没问题
	// 因为调用的是有锁的
	if rf.state != StateCandidate {
		return
	}
	rf.electiontimer.Stop()
	rf.state = StateLeader
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	lastindex, _ := rf.getlastlogindexandterm()
	for server := range rf.peers {
		rf.nextIndex[server] = lastindex + 1
	}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	var term int = rf.currentTerm
	var isleader bool = (rf.state == StateLeader)
	// Your code here (2A).
	return term, isleader
}

// get lastlog term and index
func (rf *Raft) getlastlogindexandterm() (int, int) {
	index := rf.log[len(rf.log)-1].Index
	term := rf.log[len(rf.log)-1].Term
	return index, term
}

// get prevlogindex and term
func (rf *Raft) getprevlogindexandterm(server int) (int, int) {
	// 找prevlog,由于snapshot的存在，所以index不一定是下标
	snapshotindex := rf.log[0].Index
	prevlogindex := rf.nextIndex[server] - 1
	prevlogterm := rf.log[0].Term
	if prevlogindex >= snapshotindex {
		prevlogterm = rf.log[prevlogindex-snapshotindex].Term
	}
	return prevlogindex, prevlogterm
}

// 日志是否至少和我一样新
func (rf *Raft) isLogUpToDate(index int, term int) bool {
	lastindex, lastterm := rf.getlastlogindexandterm()
	if lastterm == term {
		return lastindex <= index
	}
	return lastterm < term
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) raftstate() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	return data
}
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
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
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var term int
	var votedFor int
	var logs []EntryLog
	if d.Decode(&term) != nil || d.Decode(&votedFor) != nil || d.Decode(&logs) != nil {
		fmt.Printf("err")
	} else {
		// 第一个记录了apply
		rf.currentTerm = term
		rf.votedFor = votedFor
		rf.log = logs
		rf.commitIndex = logs[0].Index
		rf.lastApplied = logs[0].Index
	}
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	DPrintf("{node:%d} snapshot from %d", rf.me, index)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// DPrintf("snap")
	baseindex := rf.log[0].Index
	if index <= baseindex {
		// 收到的snapshot比我第一个小，那么什么也不做
		return
	}
	// log := make([]EntryLog, 0)
	// log = append(log, rf.log[index-baseindex:]...)
	rf.log = rf.log[index-baseindex:]
	rf.log[0].Command = nil // 第一个为dummy entry
	rf.persister.SaveStateAndSnapshot(rf.raftstate(), snapshot)
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	// 2A added by hs
	Term         int
	Candidatesid int
	LastLogIndex int
	LastLogTerm  int
}

// added by hs
// 生成一个新的投票请求
func (rf *Raft) genRequestVoteArgs() *RequestVoteArgs {
	args := &RequestVoteArgs{}
	args.Term = rf.currentTerm
	args.Candidatesid = rf.me
	args.LastLogIndex, args.LastLogTerm = rf.getlastlogindexandterm()
	return args
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	// 2A added by hs
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	// 处理收到的请求
	// 先加锁
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	if flag == "election" || flag == "all" {
		DPrintf("{Node: %d} received {Req: %v} from {Node: %d} with Term: %d in state: %v", rf.me, args, args.Candidatesid, rf.currentTerm, rf.state.String())
	}
	// 如果收到的args的term小于当前的term
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	// 无论当前的状态是follower还是别的，如果收到的请求比我term大，都要变成follower
	if args.Term > rf.currentTerm {
		// 如果原来是leader，那么变为follower应该重启timer
		if rf.state == StateLeader {
			rf.electiontimer.Reset(Election())
		}
		rf.tofollower(args.Term)

	}
	// 预设返回的值
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	// 如果没有投票，或者已经给我投票了？并且日志至少和我一样新
	if (rf.votedFor == -1 || rf.votedFor == args.Candidatesid) && rf.isLogUpToDate(args.LastLogIndex, args.LastLogTerm) {
		// 投票，并且重置election
		reply.VoteGranted = true
		rf.votedFor = args.Candidatesid
		rf.electiontimer.Reset(Election())
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
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	// 发送成功了
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.killed() {
		return ok
	}
	if flag == "election" || flag == "all" {
		DPrintf("{Node: %d} receives RequestVoteResponse %v from {Node %v} in term: %d with state: %v", rf.me, reply, server, rf.currentTerm, rf.state.String())
	}
	if ok {
		// 收到回复了，看看现在的rf是否还是原来的raft，还是原来的状态，如果不是候选者，term改变了，无意义
		if rf.currentTerm != args.Term || rf.state != StateCandidate {
			return ok
		}
		// 如果收到的回复的term比较新，那么这个回到follower,重置election 时间
		if reply.Term > rf.currentTerm {
			rf.tofollower(reply.Term)
			rf.persist()
			rf.electiontimer.Reset(Election())
			return ok
		}

		// 如果收到的term和自己一样，且投票了，那么
		if reply.Term == rf.currentTerm && reply.VoteGranted {
			// rf的选票加1
			rf.votes += 1
			// DPrintf("%d", rf.votes)
			// 如果选票超过1/2，当选leader
			if rf.votes > len(rf.peers)/2 {
				rf.toleader()
				rf.broadcastAppendEntries()
				rf.heartbeattimer.Reset(Heartbeat())
			}
		}
	}
	return ok
}

// new struct for append entries
type AppendEntriesArgs struct {
	Term         int
	Leaderid     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []EntryLog
	LeaderCommit int // leader's commit index
}

type AppendEntriesReply struct {
	Term     int // for leader to update itself
	Success  bool
	TryIndex int //如果冲突，下一个需要的Index
	TryTerm  int //冲突的term
}

// new struct for install snapshot
type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Snapshot          []byte // snapshot
}

type InstallSnapshotReply struct {
	Term int
}

// 应用已经提交的
func (rf *Raft) applylogs() {
	for rf.killed() == false {
		select {
		case <-rf.applylog:
			rf.mu.Lock()
			baseindex := rf.log[0].Index
			applied := rf.lastApplied
			commitIndex := rf.commitIndex
			entries := make([]EntryLog, commitIndex-applied)
			if flag == "append" || flag == "all" {
				DPrintf("{Node:%d} applylogs from applyindex:%d to commitIndex:%d with baseindex:%d", rf.me, applied, commitIndex, baseindex)
			}
			copy(entries, rf.log[applied+1-baseindex:commitIndex-baseindex+1])
			rf.mu.Unlock()
			for _, log := range entries {
				DPrintf("{Node:%d} Applied %d command ", rf.me, log.Index)
				rf.applyCh <- ApplyMsg{
					CommandValid: true,
					Command:      log.Command,
					CommandIndex: log.Index,
				}
			}
			rf.mu.Lock()
			if flag == "append" || flag == "all" {
				DPrintf("{Node:%d} applied entries %d-%d in term:%d", rf.me, rf.lastApplied, commitIndex, rf.currentTerm)
			}
			rf.lastApplied = commitIndex
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) genAppendEntriesArgs(server int) *AppendEntriesArgs {
	// DPrintf("here,genAppendEntriesArgs")
	snapindex := rf.log[0].Index
	prevlogindex, prevlogterm := rf.getprevlogindexandterm(server)
	entries := rf.log[rf.nextIndex[server]-snapindex:]
	args := &AppendEntriesArgs{
		Term:         rf.currentTerm,
		Leaderid:     rf.me,
		PrevLogIndex: prevlogindex,
		PrevLogTerm:  prevlogterm,
		Entries:      make([]EntryLog, len(entries)),
		LeaderCommit: rf.commitIndex,
	}
	copy(args.Entries, entries)
	// DPrintf("log len:%d, entries len:%d, rf.nextIndex[server]:%d", len(rf.log), len(args.Entries), rf.nextIndex[server])
	return args
}
func (rf *Raft) genInstallsnapshotArgs(server int) *InstallSnapshotArgs {
	installsnapshot := &InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.log[0].Index,
		LastIncludedTerm:  rf.log[0].Term,
		Snapshot:          rf.persister.ReadSnapshot(),
	}
	return installsnapshot
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// DPrintf("AppendEntries:{args:%v} in server:%d in  term:%d", args, rf.me, rf.currentTerm)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	if flag == "append" || flag == "all" {
		DPrintf("{Node:%d} receives {appendreq: %v} from {Node: %d} with term: %d in state: %v", rf.me, args, args.Leaderid, rf.currentTerm, rf.state.String())
	}

	// 如果收到的args的term小于我的term，那么我肯定拒绝
	// 这种情况下，被回复的肯定是变为follower了
	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		reply.Success = false
		reply.TryIndex = -1
		reply.TryTerm = -1
		return
	}

	// 如果当前的的term小于args.Term
	// 无论如何都要变成follower
	if rf.currentTerm < args.Term {
		rf.tofollower(args.Term)
	}

	// 当前任期领导人的AppendEntries
	// 重置时间
	rf.electiontimer.Reset(Election())
	// 给reply一个默认值
	reply.Term = rf.currentTerm
	reply.Success = false
	reply.TryIndex = -1
	reply.TryTerm = -1

	// 如果prevlog在baseindex之后，但是Term冲突
	// 如果prevlog在lastindex之后，那么，就将下一个要发送的index指向last的下一个
	lastindex, _ := rf.getlastlogindexandterm()
	baseindex := rf.log[0].Index
	if lastindex < args.PrevLogIndex {
		reply.TryIndex = lastindex + 1
		return
	}

	if args.PrevLogIndex >= baseindex && rf.log[args.PrevLogIndex-baseindex].Term != args.PrevLogTerm {
		term := rf.log[args.PrevLogIndex-baseindex].Term
		reply.Term = term
		for i := args.PrevLogIndex - 1; i >= baseindex; i-- {
			if rf.log[i-baseindex].Term != term {
				reply.TryIndex = i + 1
				return
			}
		}
	} else {
		// 这种情况是，匹配成功,那么就转变为follower
		DPrintf("{Node:%d} append succes with prelogIndex:%d and baseindex:%d", rf.me, args.PrevLogIndex, baseindex)
		rf.log = rf.log[:args.PrevLogIndex-baseindex+1]
		rf.log = append(rf.log, args.Entries...)

		reply.TryIndex = args.PrevLogIndex + len(args.Entries) + 1
		reply.Success = true
		// 这个地方会丢失voteFor
		// 会将非follower的匹配好的转换成follower
		rf.tofollower(args.Term)
		rf.votedFor = args.Leaderid
		// 成功了就不太关心那个下一个发送的是哪个了
		// 如果自己已经提交的小于leader以提交的，那么就更新
		if rf.commitIndex < args.LeaderCommit {
			lastindex, _ := rf.getlastlogindexandterm()
			if lastindex < args.LeaderCommit {
				rf.commitIndex = lastindex
			} else {
				rf.commitIndex = args.LeaderCommit
			}
			go func() { rf.applylog <- true }()
		}
	}
}

// appendentries added by hs
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	// 如果程序终结，就不用继续了
	if rf.killed() {
		return ok
	}
	if flag == "append" || flag == "all" {
		DPrintf("{Node:%d} receives {AppendResp: %v} from {Node %d} in term %d with state %v", rf.me, reply, server, rf.currentTerm, rf.state.String())
	}

	if !ok || rf.state != StateLeader || args.Term != rf.currentTerm {
		// 如果没有传过去
		// 本server已经不是leader了
		// 本机的Term相较于发送请求的时候已经更新
		return ok
	}
	if ok {
		if reply.Term > rf.currentTerm {
			// 回复的比我的term大，那么转为follower
			// 转换成为follower,重置时间
			rf.tofollower(reply.Term)
			rf.electiontimer.Reset(Election())
			return ok
		}
		// 看是否success
		if reply.Success {
			// 成功，修改next
			rf.nextIndex[server] = reply.TryIndex
			rf.matchIndex[server] = rf.nextIndex[server] - 1
		} else {
			// 没有成功匹配
			// 没有成功匹配的，nextIndex都是往前走，所以修改matchIndex不会影响commit,因为commitIndex>matchIndex
			if reply.TryTerm < 0 {
				// follower的log太短了
				rf.nextIndex[server] = reply.TryIndex
				rf.matchIndex[server] = rf.nextIndex[server] - 1
			} else {
				//
				nextindex := args.PrevLogIndex
				for ; nextindex >= 0; nextindex-- {
					if rf.log[nextindex].Term == reply.TryTerm {
						break
					}
				}
				if nextindex < 0 {
					rf.nextIndex[server] = reply.TryIndex
				} else {
					rf.nextIndex[server] = nextindex + 1
				}
				rf.matchIndex[server] = rf.nextIndex[server] - 1
			}
		}
		N, _ := rf.getlastlogindexandterm()
		// DPrintf("Apply change from %d", N)
		for ; N > rf.commitIndex; N-- {
			// 看看是否有超过一半以上提交的，有的话，应用
			// 只提交当前term的日志
			baseindex := rf.log[0].Index
			if rf.log[N-baseindex].Term != rf.currentTerm {
				break
			}
			count := 1
			for i := range rf.peers {
				if i != rf.me && rf.matchIndex[i] >= N {
					count++
				}
			}
			// DPrintf("Command:%d with %d agree", N, count)
			if count > len(rf.peers)/2 {
				rf.commitIndex = N
				go func() { rf.applylog <- true }()
				break
			}
		}
	}
	return ok
}

func (rf *Raft) applysnap(args *InstallSnapshotArgs) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 保存snap
	snapshotindex := rf.log[0].Index
	lastindex, _ := rf.getlastlogindexandterm()
	rf.persister.SaveStateAndSnapshot(rf.raftstate(), args.Snapshot)
	log := make([]EntryLog, 1)
	if lastindex >= args.LastIncludedIndex && rf.log[args.LastIncludedIndex-snapshotindex].Term == args.LastIncludedTerm {
		log = append(log, rf.log[args.LastIncludedIndex-snapshotindex+1:]...)
	}
	log[0].Index = args.LastIncludedIndex
	log[0].Term = args.LastIncludedTerm
	log[0].Command = nil
	rf.lastApplied = max(rf.lastApplied, args.LastIncludedIndex)
	rf.commitIndex = max(rf.commitIndex, args.LastIncludedIndex)
	rf.log = log
	go func() {
		DPrintf("{Node:%d} apply snap from term:%d with index %d", rf.me, args.LastIncludedTerm, args.LastIncludedIndex)
		rf.applyCh <- ApplyMsg{
			SnapshotValid: true,
			Snapshot:      args.Snapshot,
			SnapshotTerm:  args.LastIncludedTerm,
			SnapshotIndex: args.LastIncludedIndex,
		}
	}()
	return
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	if flag == "snap" || flag == "all" {
		DPrintf("{Node: %d} receives {snapreq: %v} from {Node: %d} with term: %d in state: %v", rf.me, args, args.LeaderId, rf.currentTerm, rf.state.String())
	}
	// 收到对方的InstallSnapshot
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}
	if args.Term > rf.currentTerm {
		rf.tofollower(args.Term)
	}
	// 丢弃较小索引的快照
	reply.Term = rf.currentTerm
	snapshotindex := rf.log[0].Index
	if snapshotindex >= args.LastIncludedIndex {
		return
	}
	go rf.applysnap(args)
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	if rf.killed() {
		return ok
	}
	if flag == "snap" || flag == "all" {
		DPrintf("{Node %d} receives {SnapResp: %v} from {Node %d} in term %d with state %v", rf.me, reply, server, rf.currentTerm, rf.state.String())
	}
	if !ok || rf.state != StateLeader || args.Term != rf.currentTerm {
		// 如果没有传过去
		// 本server已经不是leader了
		// 本机的Term相较于发送请求的时候已经更新
		return ok
	}
	// 回复只有term
	// 如果对方Term比我的大，那么leader失效
	if reply.Term > rf.currentTerm {
		rf.tofollower(reply.Term)
		rf.electiontimer.Reset(Election())
		return ok
	}
	rf.nextIndex[server] = args.LastIncludedIndex + 1
	rf.matchIndex[server] = args.LastIncludedIndex
	return ok
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
	// 如果当前server是主，那么把command加入log，并且返回对应的index和term
	// 如果不是主，那么就不加，然后返回
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.killed() {
		return 0, 0, false
	}
	isLeader := rf.state == StateLeader
	index, _ := rf.getlastlogindexandterm()
	index += 1
	if isLeader {
		rf.log = append(rf.log, EntryLog{
			Term:    rf.currentTerm,
			Index:   index,
			Command: command,
		})
		if flag == "append" || flag == "all" {
			DPrintf("{Node:%d}add entrylog:%v with term:%d in state:%v", rf.me, rf.log[len(rf.log)-1], rf.currentTerm, rf.state.String())
		}
		rf.persist()
	}
	// Your code here (2B).

	return index, rf.currentTerm, isLeader
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

func (rf *Raft) broadcastAppendEntries() {
	// 保证这个必须为leader才能发,不然返回去
	if rf.state != StateLeader {
		return
	}
	snapshotindex := rf.log[0].Index
	// 给每一个follower发heartbeat
	for server := range rf.peers {
		if server != rf.me {
			// DPrintf("server:%d send AppendEntries", server)
			// 这个appendentries要因主机而异
			// 根据nextindex的大小来发送
			if rf.nextIndex[server] > snapshotindex {
				appendentries := rf.genAppendEntriesArgs(server)
				go rf.sendAppendEntries(server, appendentries, &AppendEntriesReply{})
			} else {
				// DPrintf("InstallSnapshot")
				installsnapshot := rf.genInstallsnapshotArgs(server)
				go rf.sendInstallSnapshot(server, installsnapshot, &InstallSnapshotReply{})
			}
		}
	}
}
func (rf *Raft) startelection() {
	// 构造一个投票请求，然后重置自己的选票数为1,自己的一票，自己的投票指向自己
	if rf.state != StateCandidate {
		return
	}
	request := rf.genRequestVoteArgs()
	for server := range rf.peers {
		// DPrintf("%d", server)
		if server != rf.me {
			go rf.sendRequestVote(server, request, &RequestVoteReply{})
		}
	}
	// 如果rafts总数为1
	if rf.votes > len(rf.peers)/2 {
		rf.toleader()
		rf.broadcastAppendEntries()
		rf.heartbeattimer.Reset(Heartbeat())
	}
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		// DPrintf("index:%d,term:%d,state:%d", rf.me, rf.currentTerm, rf.state)
		select {
		case <-rf.heartbeattimer.C:
			// 如果心跳结束...，如果是leader,下一轮
			rf.mu.Lock()
			if rf.state == StateLeader {
				DPrintf("{Node %v} begin new heartbeat", rf.me)
				//todo 给每个peer发appendentry
				rf.broadcastAppendEntries()
				rf.heartbeattimer.Reset(Heartbeat())
			}
			rf.mu.Unlock()
		case <-rf.electiontimer.C:
			// 如果election结束...,开始新的一轮选举，并发送投票请求
			// 如果是leader不需要管
			rf.mu.Lock()
			DPrintf("{Node %v with state: %v} begin new election", rf.me, rf.state)
			//to do 给每个发投票请求,下一轮选举，term+1
			rf.tocandidate()
			rf.persist()
			rf.startelection()
			rf.electiontimer.Reset(Election())
			rf.mu.Unlock()
		}

	}
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
	// 2A
	rf.state = StateFollower
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = make([]EntryLog, 1)
	rf.votes = 0

	rf.heartbeattimer = time.NewTimer(Heartbeat())
	rf.electiontimer = time.NewTimer(Election())

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	rf.applyCh = applyCh
	rf.applylog = make(chan bool)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// 应用log
	go rf.applylogs()
	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
