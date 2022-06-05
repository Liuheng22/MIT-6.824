package shardctrler

import (
	"sync"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	ack      map[int64]int64
	resultCh map[int]chan Result

	configs []Config // indexed by config num
}

type Op struct {
	// Your data here.
	Command   string
	ClientId  int64
	RequestId int64

	Servers map[int][]string

	GIDs []int

	Shard int
	GID   int

	Num int
}

type Result struct {
	Command     string
	OK          bool
	ClientId    int64
	RequestId   int64
	WrongLeader bool
	Err         Err
	Config      Config
}

// proposal 提交entry给raft
func (sc *ShardCtrler) proposal(entry Op) Result {
	if flag == "all" {
		DPrintf("proposal %v", entry)
	}
	index, _, isleader := sc.Raft().Start(entry)
	if !isleader {
		//不是主
		return Result{OK: false}
	}
	if flag == "all" {
		id, _ := sc.Raft().GetState()
		DPrintf("proposal %v", id)
	}
	sc.mu.Lock()
	resch, ok := sc.resultCh[index]
	if !ok {
		//没有回复的chan
		sc.resultCh[index] = make(chan Result, 1)
		resch = sc.resultCh[index]
	}
	sc.mu.Unlock()

	select {
	case result := <-resch:
		go sc.removech(index)
		if isMatch(entry, result) {
			return result
		}
		return Result{OK: false}
	case <-time.After(240 * time.Millisecond):
		return Result{OK: false}
	}
}

func (sc *ShardCtrler) removech(index int) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	delete(sc.resultCh, index)
}

func isMatch(entry Op, result Result) bool {
	return entry.ClientId == result.ClientId && entry.RequestId == result.RequestId
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	//构造entry去raft
	entry := Op{}
	entry.Command = "join"
	entry.ClientId = args.ClientId
	entry.RequestId = args.RequestId
	entry.Servers = args.Servers

	result := sc.proposal(entry)
	if !result.OK {
		reply.WrongLeader = true
		return
	}
	reply.WrongLeader = false
	reply.Err = result.Err
}

// 应用join
func (sc *ShardCtrler) applyJoin(op Op) {
	config := sc.makeNextConfig()
	for gid, servers := range op.Servers {
		config.Groups[gid] = servers

		for i := 0; i < NShards; i++ {
			if config.Shards[i] == 0 {
				config.Shards[i] = gid
			}
		}
	}
	rebalanceShards(&config)
	sc.configs = append(sc.configs, config)
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	entry := Op{}
	entry.Command = "leave"
	entry.ClientId = args.ClientId
	entry.RequestId = args.RequestId
	entry.GIDs = args.GIDs

	result := sc.proposal(entry)
	if !result.OK {
		reply.WrongLeader = true
		return
	}
	reply.WrongLeader = false
	reply.Err = result.Err
}

func (sc *ShardCtrler) applyLeave(op Op) {
	config := sc.makeNextConfig()
	stayGid := 0
	//找到一个未删除的，跳出
	for gid := range config.Groups {
		stay := true
		for _, deleteGid := range op.GIDs {
			if gid == deleteGid {
				stay = false
			}
		}
		if stay {
			stayGid = gid
			break
		}
	}
	//将删除的gid的shard改成未删除的gid
	for _, gid := range op.GIDs {
		// assign shards whose replica group will leave to the stay group.
		for i := 0; i < len(config.Shards); i++ {
			if config.Shards[i] == gid {
				config.Shards[i] = stayGid
			}
		}
		delete(config.Groups, gid)
	}
	//调整平衡
	rebalanceShards(&config)
	sc.configs = append(sc.configs, config)
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	entry := Op{}
	entry.Command = "move"
	entry.ClientId = args.ClientId
	entry.RequestId = args.RequestId
	entry.Shard = args.Shard
	entry.GID = args.GID

	result := sc.proposal(entry)
	if !result.OK {
		reply.WrongLeader = true
		return
	}
	reply.WrongLeader = false
	reply.Err = result.Err
}

//移动shard到另一个gid
func (sc *ShardCtrler) applyMove(op Op) {
	config := sc.makeNextConfig()
	config.Shards[op.Shard] = op.GID
	sc.configs = append(sc.configs, config)
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	entry := Op{}
	entry.Command = "query"
	entry.ClientId = args.ClientId
	entry.RequestId = args.RequestId
	entry.Num = args.Num

	result := sc.proposal(entry)
	if !result.OK {
		reply.WrongLeader = true
		return
	}
	reply.WrongLeader = false
	reply.Err = result.Err
	reply.Config = result.Config
}

//创建下一个设置
func (sc *ShardCtrler) makeNextConfig() Config {
	next := Config{}
	curr := sc.configs[len(sc.configs)-1]

	next.Num = curr.Num + 1
	next.Shards = curr.Shards
	next.Groups = map[int][]string{}
	for gid, servers := range curr.Groups {
		next.Groups[gid] = servers
	}
	return next
}

//
func rebalanceShards(config *Config) {
	gidToshards := makeGidToshard(config)
	if len(config.Groups) == 0 {
		//no replica group
		for i := 0; i < len(config.Shards); i++ {
			config.Shards[i] = 0
		}
	} else {
		mean := NShards / len(config.Groups)
		numToMove := 0
		for _, shards := range gidToshards {
			if len(shards) > mean {
				numToMove += len(shards) - mean
			}
		}
		//劫富济贫
		for i := 0; i < numToMove; i++ {
			srcgid, dstgid := getGidPairToMove(gidToshards)
			N := len(gidToshards[srcgid]) - 1
			config.Shards[gidToshards[srcgid][N]] = dstgid
			gidToshards[dstgid] = append(gidToshards[dstgid], gidToshards[srcgid][N])
			gidToshards[srcgid] = gidToshards[srcgid][:N]
		}
	}
}

func makeGidToshard(config *Config) map[int][]int {
	gidToshards := make(map[int][]int)
	for gid := range config.Groups {
		gidToshards[gid] = make([]int, 0)
	}
	for shard, gid := range config.Shards {
		gidToshards[gid] = append(gidToshards[gid], shard)
	}
	return gidToshards
}

//找最短和最长的
func getGidPairToMove(gidToshards map[int][]int) (int, int) {
	srcgid, dstgid := 0, 0
	for gid, shards := range gidToshards {
		if srcgid == 0 || len(gidToshards[srcgid]) < len(shards) {
			srcgid = gid
		}
		if dstgid == 0 || len(gidToshards[dstgid]) > len(shards) {
			dstgid = gid
		}
	}
	return srcgid, dstgid
}

//
// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

//检测是否重复
func (sc *ShardCtrler) isDuplicated(op Op) bool {
	lastRequestId, ok := sc.ack[op.ClientId]
	if ok {
		//当前client的应用reqid
		return lastRequestId >= op.RequestId
	}
	//没有相应的map对，一定没有重复
	return false
}

//apply op
func (sc *ShardCtrler) applyOp(op Op) Result {
	result := Result{}
	result.Command = op.Command
	result.OK = true
	result.WrongLeader = false
	result.ClientId = op.ClientId
	result.RequestId = op.RequestId

	switch op.Command {
	case "join":
		if !sc.isDuplicated(op) {
			sc.applyJoin(op)
		}
		result.Err = OK
	case "leave":
		if !sc.isDuplicated(op) {
			sc.applyLeave(op)
		}
		result.Err = OK
	case "move":
		if !sc.isDuplicated(op) {
			sc.applyMove(op)
		}
		result.Err = OK
	case "query":
		sc.showConfig()
		if op.Num == -1 || op.Num >= len(sc.configs) {
			result.Config = sc.configs[len(sc.configs)-1]
		} else {
			result.Config = sc.configs[op.Num]
		}
		result.Err = OK
	}
	sc.ack[op.ClientId] = op.RequestId
	return result
}

func (sc *ShardCtrler) showConfig() {
	for i, config := range sc.configs {
		DPrintf("server:%d cid:%d config:%v", sc.me, i, config)
	}
}

// stepapply
func (sc *ShardCtrler) Apply() {
	for {
		//应用上来的消息
		msg := <-sc.applyCh
		sc.mu.Lock()
		if flag == "all" {
			DPrintf("apply %v", msg)
		}
		// 应用
		op := msg.Command.(Op)
		//应用op
		result := sc.applyOp(op)
		if ch, ok := sc.resultCh[msg.CommandIndex]; ok {
			select {
			case <-ch:
			default:
			}
		} else {
			sc.resultCh[msg.CommandIndex] = make(chan Result, 1)
		}
		sc.resultCh[msg.CommandIndex] <- result
		sc.mu.Unlock()
	}
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.ack = make(map[int64]int64)
	sc.resultCh = make(map[int]chan Result)

	go sc.Apply()

	return sc
}
