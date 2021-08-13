package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"
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
	OpType string
	Key    string
	Value  string

	ClientId  int64
	ClientSeq int
	LeaderId  int
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big
	persister    *raft.Persister

	// Your definitions here.
	kvMap       map[string]string
	clientSeq   map[int64]int
	opCh        map[int]chan Err
	latestIndex int
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	err := kv.agree(Op{
		OpType:    "Get",
		Key:       args.Key,
		ClientId:  args.ClientId,
		ClientSeq: args.ClientSeq,
		LeaderId:  kv.me,
	})
	if err != OK {
		reply.Err = err
	} else {
		value, ok := kv.kvMap[args.Key]
		if ok {
			reply.Value = value
			reply.Err = OK
		} else {
			reply.Err = ErrNoKey
		}
	}
	kv.mu.Unlock()
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	err := kv.agree(Op{
		OpType:    args.Op,
		Key:       args.Key,
		Value:     args.Value,
		ClientId:  args.ClientId,
		ClientSeq: args.ClientSeq,
		LeaderId:  kv.me,
	})
	reply.Err = err
	kv.mu.Unlock()
}

func (kv *KVServer) agree(cmd interface{}) Err {
	index, term, isLeader := kv.rf.Start(cmd)
	if !isLeader {
		return ErrWrongLeader
	}
	// DPrintf("[%d] will agree on index %d, cmd %v\n", kv.me, index, cmd)

	ch := make(chan Err, 1)
	if kv.opCh[index] != nil {
		panic("")
	}
	kv.opCh[index] = ch
	kv.mu.Unlock()

	for !kv.killed() {
		select {
		case err, ok := <-ch:
			kv.mu.Lock()
			if ok {
				// DPrintf("[%d] end the agree on index %d, cmd %v\n", kv.me, index, cmd)
				delete(kv.opCh, index)
				return err
			} else {
				return ErrWrongLeader
			}
		case <-time.After(100 * time.Millisecond):
			kv.mu.Lock()
			if nowTerm, _ := kv.rf.GetState(); nowTerm > term {
				// DPrintf("[%d] term changed, error\n", kv.me)
				delete(kv.opCh, index)
				return ErrWrongLeader
			}
			kv.mu.Unlock()
		}
	}
	kv.mu.Lock()
	return ErrWrongLeader
}

func (kv *KVServer) applier() {
	for msg := range kv.applyCh {
		if kv.killed() {
			return
		}
		if msg.CommandValid {
			kv.mu.Lock()
			// DPrintf("[%d] apply cmd %v\n", kv.me, msg.Command)

			// if msg.CommandIndex != kv.latestIndex + 1 {
			// 	DPrintf("[%d] skip (cmdIndex=%d latestIndex=%d)\n", kv.me, msg.CommandIndex, kv.latestIndex)
			// 	kv.mu.Unlock()
			// 	continue
			// } else {
			// 	kv.latestIndex++
			// }

			op := msg.Command.(Op)
			lastSeq, ok := kv.clientSeq[op.ClientId]
			ignore := false

			if !ok && op.ClientSeq == 1 {
				// DPrintf("[%d] first request from client %d\n", kv.me, op.ClientId)
				kv.clientSeq[op.ClientId] = 1
			} else if ok && lastSeq >= op.ClientSeq {
				// DPrintf("[%d] duplicate request from client %d, shall ignore\n", kv.me, op.ClientId)
				ignore = true
			} else if ok && lastSeq+1 == op.ClientSeq {
				// DPrintf("[%d] request %d from client %d\n", kv.me, op.ClientSeq, op.ClientId)
				kv.clientSeq[op.ClientId] = op.ClientSeq
			} else {
				// DPrintf("[%d] lastSeq=%d ClientSeq=%d\n", kv.me, lastSeq, op.ClientSeq)
				panic("inconsistent client sequence number")
			}

			if !ignore {
				if op.OpType == "Get" {
				} else if op.OpType == "Put" {
					kv.kvMap[op.Key] = op.Value
				} else if op.OpType == "Append" {
					kv.kvMap[op.Key] += op.Value
				} else {
					panic("unknown op type")
				}
			}

			if kv.killed() {
				kv.mu.Unlock()
				return
			}

			if kv.maxraftstate != -1 && kv.persister.RaftStateSize() >= kv.maxraftstate {
				w := new(bytes.Buffer)
				encoder := labgob.NewEncoder(w)
				encoder.Encode(kv.kvMap)
				encoder.Encode(kv.clientSeq)
				// DPrintf("[%d] need create snapshot\n", kv.me)
				// DPrintf("[%d] index=%d clientSeq=%v\n", kv.me, msg.CommandIndex, kv.clientSeq)
				go kv.rf.Snapshot(msg.CommandIndex, w.Bytes())
			}

			if ch, ok := kv.opCh[msg.CommandIndex]; ok {
				var err Err
				if op.LeaderId != kv.me {
					err = ErrWrongLeader
				} else {
					err = OK
				}
				ch <- err
				delete(kv.opCh, msg.CommandIndex)
				close(ch)
			}
			kv.mu.Unlock()

		} else if msg.SnapshotValid {
			// DPrintf("[%d] applyCh is snapshot\n", kv.me)
			if kv.rf.CondInstallSnapshot(msg.SnapshotTerm, msg.SnapshotIndex, msg.Snapshot) {
				kv.mu.Lock()
				// DPrintf("[%d] install snapshot\n", kv.me)
				r := bytes.NewBuffer(msg.Snapshot)
				decoder := labgob.NewDecoder(r)
				var kvMap map[string]string
				var clientSeq map[int64]int
				if decoder.Decode(&kvMap) == nil && decoder.Decode(&clientSeq) == nil {
					kv.kvMap = kvMap
					// DPrintf("[%d] old clientSeq: %v\n", kv.me, kv.clientSeq)
					// DPrintf("[%d] new clientSeq: %v\n", kv.me, clientSeq)
					kv.clientSeq = clientSeq
					for index, ch := range kv.opCh {
						ch <- ErrWrongLeader
						close(ch)
						delete(kv.opCh, index)
					}
				} else {
					panic("decode error")
				}
				kv.latestIndex = msg.SnapshotIndex
				kv.mu.Unlock()
			}
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
	kv.persister = persister

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.latestIndex = 0

	// You may need initialization code here.
	r := bytes.NewBuffer(persister.ReadSnapshot())
	if r.Len() > 0 {
		decoder := labgob.NewDecoder(r)
		if decoder.Decode(&kv.kvMap) != nil || decoder.Decode(&kv.clientSeq) != nil {
			panic("decode error")
		}
	} else {
		kv.kvMap = make(map[string]string)
		kv.clientSeq = make(map[int64]int)
	}

	kv.opCh = make(map[int]chan Err)
	go kv.applier()

	return kv
}
