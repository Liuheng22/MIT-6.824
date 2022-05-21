package kvraft

import (
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type      string
	Key       string
	Value     string
	ClientId  int64
	RequestId int64
}

type Response struct {
	Err       Err
	Value     string
	ClientId  int64
	RequestId int64
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	db      map[string]string
	notify  map[int]chan Response
	applied map[int64]int64

	persister *raft.Persister
	lastindex int //
}

func (kv *KVServer) getreschan(index int) chan Response {
	reschan, ok := kv.notify[index]
	if !ok {
		kv.notify[index] = make(chan Response, 1)
		reschan = kv.notify[index]
	}
	return reschan
}

// 判断是否是同一个client的同一个req，因为raft层可能把别的client的别的req放在相同的index上，需要鉴别
func (kv *KVServer) issamereq(command *Op, resp *Response) bool {
	return command.ClientId == resp.ClientId && command.RequestId == resp.RequestId
}

func (kv *KVServer) Proposal(command *Op) (bool, *Response) {
	index, _, ok := kv.rf.Start(*command)
	// 不是leader
	if !ok {
		return false, &Response{}
	}
	kv.mu.Lock()
	DPrintf("KV:%d getlock", kv.me)
	reschan := kv.getreschan(index)
	kv.mu.Unlock()
	DPrintf("kv:%d free lock", kv.me)
	select {
	case resp := <-reschan:
		go kv.removech(index)
		return kv.issamereq(command, &resp), &resp
	case <-time.After(500 * time.Millisecond):
		go kv.removech(index)
		return false, &Response{}
	}
}

func (kv *KVServer) removech(index int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	delete(kv.notify, index)
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	// kv.mu.Lock()
	// defer kv.mu.Unlock()
	// 不能这样加锁，会一直等待
	// 不能提前返回
	// 先将args放进去
	command := Op{
		Type:      "Get",
		Key:       args.Key,
		ClientId:  args.Clientid,
		RequestId: args.Reqid,
	}
	DPrintf("{kv:%d} receives req:%v from clerk:%d", kv.me, command, command.ClientId)
	ok, resp := kv.Proposal(&command)
	if !ok {
		reply.Err = ErrWrongLeader
		return
	}
	reply.Err = OK
	reply.Value = resp.Value
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	command := Op{
		Type:      args.Op,
		Key:       args.Key,
		Value:     args.Value,
		ClientId:  args.Clientid,
		RequestId: args.Reqid,
	}
	DPrintf("{kv:%d} recive req %v from clerk:%d", kv.me, command, command.ClientId)
	ok, _ := kv.Proposal(&command)
	if !ok {
		reply.Err = ErrWrongLeader
		return
	}
	reply.Err = OK
}

func (kv *KVServer) isexist(clientid int64, reqid int64) bool {
	applyid, ok := kv.applied[clientid]
	if !ok {
		kv.applied[clientid] = 0
		return false
	}
	return applyid >= reqid
}

func (kv *KVServer) applydb(command *Op, resp *Response) {
	switch command.Type {
	case "Get":
		resp.Value = kv.db[command.Key]
	case "Put":
		kv.db[command.Key] = command.Value
	case "Append":
		kv.db[command.Key] += command.Value
	}
}

func (kv *KVServer) snap() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.db)
	e.Encode(kv.applied)
	e.Encode(kv.lastindex)
	data := w.Bytes()
	return data
}

func (kv *KVServer) ingestSnap(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 {
		return
	}
	var db map[string]string
	var applied map[int64]int64
	var lastIndex int
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	d.Decode(&db)
	d.Decode(&applied)
	d.Decode(&lastIndex)
	kv.db = db
	kv.applied = applied
	kv.lastindex = lastIndex
}

func (kv *KVServer) applier() {
	for kv.killed() == false {
		msg := <-kv.applyCh
		if msg.CommandValid {
			// 是command
			kv.mu.Lock()
			// 由于发送是不锁的，那么有可能有过期的command，扔掉就好
			if msg.CommandIndex <= kv.lastindex {
				kv.mu.Unlock()
				continue
			}

			index := msg.CommandIndex
			command := msg.Command.(Op)
			resp := Response{Err: OK, Value: "", ClientId: command.ClientId, RequestId: command.RequestId}

			if command.Type == "Get" {
				kv.applydb(&command, &resp)
			} else {
				// 对于非get的操作，需要判断是否已经写入了
				// 避免重复写
				applyid, ok := kv.applied[command.ClientId]
				if !ok || applyid < command.RequestId {
					kv.applydb(&command, &resp)
					kv.applied[command.ClientId] = command.RequestId
				}
			}
			// 修改应用的command index
			if msg.CommandIndex == kv.lastindex+1 {
				kv.lastindex++
			}
			if kv.maxraftstate != -1 && kv.persister.RaftStateSize() >= kv.maxraftstate {
				snapshot := kv.snap()
				kv.rf.Snapshot(kv.lastindex, snapshot)
			}
			reschan := kv.getreschan(index)
			reschan <- resp
			kv.mu.Unlock()
		} else if msg.SnapshotValid {
			// 收到了snapshot,那么
			kv.mu.Lock()
			kv.ingestSnap(msg.Snapshot)
			kv.mu.Unlock()
		}
	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) readsnap() {
	kv.ingestSnap(kv.persister.ReadSnapshot())
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.persister = persister
	kv.db = make(map[string]string)
	kv.notify = make(map[int]chan Response)
	kv.applied = make(map[int64]int64)
	kv.readsnap()
	go kv.applier()
	return kv
}
