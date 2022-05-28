package kvraft

import (
	"crypto/rand"
	"math/big"

	"6.824/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	leaderid int32
	clientid int64
	request  int64
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
	// You'll have to add code here.
	ck.leaderid = 0
	ck.clientid = nrand()
	ck.request = 0
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	// 发送rpc给kvserver
	// 发送给每一个可能的kvserver
	// 给request一个序号
	ck.request++
	leader := ck.leaderid

	value := ""
	args := &GetArgs{Key: key, Clientid: ck.clientid, Reqid: ck.request}
	for ; ; leader = (leader + 1) % int32(len(ck.servers)) {
		reply := &GetReply{}
		ok := ck.servers[leader].Call("KVServer.Get", args, reply)
		// ok为false没有发过去，err出错也重新发
		if ok && (reply.Err == OK || reply.Err == ErrNoKey) {
			value = reply.Value
			break
		}
	}
	ck.leaderid = leader
	return value
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	ck.request++
	leader := ck.leaderid
	DPrintf("Clerk putappend key:%v val:%v op:%v", key, value, op)
	args := &PutAppendArgs{Key: key, Value: value, Op: op, Clientid: ck.clientid, Reqid: ck.request}
	for ; ; leader = (leader + 1) % int32(len(ck.servers)) {
		reply := &PutAppendReply{}
		ok := ck.servers[leader].Call("KVServer.PutAppend", args, reply)
		// ok为false没有发过去，err出错也重新发
		if ok && reply.Err == OK {
			break
		}
	}
	ck.leaderid = leader
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
