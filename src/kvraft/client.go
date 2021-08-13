package kvraft

import "6.824/labrpc"
import "time"
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	lastLeader int
	lastSeq    int
	id         int64
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
	ck.lastLeader = 0
	ck.lastSeq = 0
	ck.id = time.Now().UnixNano()
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
	// DPrintf("[%d] GET %s\n", ck.id, key)
	server := ck.lastLeader
	ck.lastSeq++
	for {
		value, err := ck.get(server, key)
		if err == OK {
			ck.lastLeader = server
			// DPrintf("[%d] GET OK %s\n", ck.id, value)
			return value
		} else if err == ErrWrongLeader {
			server = (server + 1) % len(ck.servers)
			// DPrintf("[%d] RETRY\n", ck.id)
		} else if err == ErrNoKey {
			return ""
		} else {
			panic("unknown err")
		}
	}
}

func (ck *Clerk) get(server int, key string) (string, Err) {
	args := GetArgs{
		Key:       key,
		ClientId:  ck.id,
		ClientSeq: ck.lastSeq,
	}
	reply := GetReply{}
	ok := ck.servers[server].Call("KVServer.Get", &args, &reply)
	if !ok {
		return "", ErrWrongLeader
	} else {
		return reply.Value, reply.Err
	}
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
	// DPrintf("[%d] PUT/APPEND %s %s %s\n", ck.id, key, value, op)
	server := ck.lastLeader
	ck.lastSeq++
	for {
		err := ck.putAppend(server, key, value, op)
		if err == OK {
			ck.lastLeader = server
			// DPrintf("[%d] PUT/APPEND OK\n", ck.id)
			return
		} else if err == ErrWrongLeader {
			time.Sleep(100 * time.Millisecond)
			server = (server + 1) % len(ck.servers)
			// DPrintf("[%d] RETRY\n", ck.id)
		} else {
			panic("unknown err")
		}
	}
}

func (ck *Clerk) putAppend(server int, key string, value string, op string) Err {
	args := PutAppendArgs{
		Key:       key,
		Value:     value,
		Op:        op,
		ClientId:  ck.id,
		ClientSeq: ck.lastSeq,
	}
	reply := PutAppendReply{}
	ok := ck.servers[server].Call("KVServer.PutAppend", &args, &reply)
	if !ok {
		return ErrWrongLeader
	} else {
		return reply.Err
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
