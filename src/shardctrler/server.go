package shardctrler

import (
	"6.824/raft"
	"sort"
	"time"
)
import "6.824/labrpc"
import "sync"
import "6.824/labgob"

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	persister *raft.Persister
	clientSeq map[int64]int
	opCh      map[int]chan bool
	configs   []Config // indexed by config num
}

type Op struct {
	// Your data here.
	OpArgs    interface{}
	ClientId  int64
	ClientSeq int
	LeaderId  int
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	sc.mu.Lock()
	op := Op{
		OpArgs:    *args,
		ClientId:  args.ClientId,
		ClientSeq: args.ClientSeq,
		LeaderId:  sc.me,
	}
	wrongLeader, err := sc.agree(op)
	reply.WrongLeader = wrongLeader
	reply.Err = err
	sc.mu.Unlock()
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	sc.mu.Lock()
	op := Op{
		OpArgs:    *args,
		ClientId:  args.ClientId,
		ClientSeq: args.ClientSeq,
		LeaderId:  sc.me,
	}
	wrongLeader, err := sc.agree(op)
	reply.WrongLeader = wrongLeader
	reply.Err = err
	sc.mu.Unlock()
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	sc.mu.Lock()
	op := Op{
		OpArgs:    *args,
		ClientId:  args.ClientId,
		ClientSeq: args.ClientSeq,
		LeaderId:  sc.me,
	}
	wrongLeader, err := sc.agree(op)
	reply.WrongLeader = wrongLeader
	reply.Err = err
	sc.mu.Unlock()
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	sc.mu.Lock()
	op := Op{
		OpArgs:    *args,
		ClientId:  args.ClientId,
		ClientSeq: args.ClientSeq,
		LeaderId:  sc.me,
	}
	wrongLeader, err := sc.agree(op)
	reply.WrongLeader = wrongLeader
	reply.Err = err
	if !wrongLeader && err == OK {
		configNumber := args.Num
		if configNumber == -1 || configNumber >= len(sc.configs) {
			configNumber = len(sc.configs) - 1
		}
		reply.Config = sc.configs[configNumber]
		//log.Printf("[%d] query result: num=%d config=%v\n", sc.me, configNumber, reply.Config)
	}
	sc.mu.Unlock()
}

func (sc *ShardCtrler) agree(cmd Op) (bool, Err) {
	index, term, isLeader := sc.rf.Start(cmd)
	if !isLeader {
		return true, OK
	}
	//log.Printf("[%d] will agree on index %d, cmd %v\n", sc.me, index, cmd)

	ch := make(chan bool, 1)
	if sc.opCh[index] != nil {
		panic("")
	}
	sc.opCh[index] = ch
	sc.mu.Unlock()

	for {
		select {
		case wrongLeader, ok := <-ch:
			sc.mu.Lock()
			if ok {
				//log.Printf("[%d] end the agree on index %d, cmd %v\n", sc.me, index, cmd)
				delete(sc.opCh, index)
				return wrongLeader, OK
			} else {
				return true, OK
			}
		case <-time.After(100 * time.Millisecond):
			sc.mu.Lock()
			if nowTerm, _ := sc.rf.GetState(); nowTerm > term {
				// DPrintf("[%d] term changed, error\n", kv.me)
				delete(sc.opCh, index)
				return true, OK
			}
			sc.mu.Unlock()
		}
	}
}

func (cfg *Config) rearrange() {
	//log.Printf("rearrange config: Groups=%v oldShards=%v\n", cfg.Groups, cfg.Shards)
	var gids []int
	for gid, _ := range cfg.Groups {
		gids = append(gids, gid)
	}
	sort.Ints(gids)

	if len(gids) == 0 {
		cfg.Shards = [NShards]int{}
	} else {
		for i := 0; i < NShards; i++ {
			cfg.Shards[i] = gids[i%len(gids)]
		}
	}
	//log.Printf("rearrange config: newShards=%v\n", cfg.Shards)
}

func (sc *ShardCtrler) applier() {
	for msg := range sc.applyCh {
		if msg.CommandValid {
			sc.mu.Lock()
			//log.Printf("[%d] apply cmd %v\n", sc.me, msg.Command)

			op := msg.Command.(Op)
			lastSeq, ok := sc.clientSeq[op.ClientId]
			ignore := false

			if !ok && op.ClientSeq == 1 {
				// DPrintf("[%d] first request from client %d\n", kv.me, op.ClientId)
				sc.clientSeq[op.ClientId] = 1
			} else if ok && lastSeq >= op.ClientSeq {
				// DPrintf("[%d] duplicate request from client %d, shall ignore\n", kv.me, op.ClientId)
				ignore = true
			} else if ok && lastSeq+1 == op.ClientSeq {
				// DPrintf("[%d] request %d from client %d\n", kv.me, op.ClientSeq, op.ClientId)
				sc.clientSeq[op.ClientId] = op.ClientSeq
			} else {
				// DPrintf("[%d] lastSeq=%d ClientSeq=%d\n", kv.me, lastSeq, op.ClientSeq)
				panic("inconsistent client sequence number")
			}

			if !ignore {
				newConfig := Config{
					Num:    len(sc.configs),
					Shards: sc.configs[len(sc.configs)-1].Shards,
					Groups: make(map[int][]string),
				}
				for gid, server := range sc.configs[len(sc.configs)-1].Groups {
					newConfig.Groups[gid] = server
				}

				if join, ok := op.OpArgs.(JoinArgs); ok {
					for gid, servers := range join.Servers {
						if _, ok := newConfig.Groups[gid]; ok {
							panic("gid already exists")
						}
						newConfig.Groups[gid] = append(newConfig.Groups[gid], servers...)
					}
					newConfig.rearrange()
					sc.configs = append(sc.configs, newConfig)
				} else if leave, ok := op.OpArgs.(LeaveArgs); ok {
					for _, gid := range leave.GIDs {
						delete(newConfig.Groups, gid)
					}
					newConfig.rearrange()
					sc.configs = append(sc.configs, newConfig)
				} else if move, ok := op.OpArgs.(MoveArgs); ok {
					newConfig.Shards[move.Shard] = move.GID
					sc.configs = append(sc.configs, newConfig)
				} else if _, ok := op.OpArgs.(QueryArgs); ok {
				} else {
					panic("unknown op type")
				}
			}

			if ch, ok := sc.opCh[msg.CommandIndex]; ok {
				if op.LeaderId != sc.me {
					ch <- true
				} else {
					ch <- false
				}
				delete(sc.opCh, msg.CommandIndex)
				close(ch)
			}
			sc.mu.Unlock()

		} else if msg.SnapshotValid {
			panic("snapshot not supported")
		}
	}
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
	labgob.Register(JoinArgs{})
	labgob.Register(LeaveArgs{})
	labgob.Register(MoveArgs{})
	labgob.Register(QueryArgs{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.clientSeq = make(map[int64]int)
	sc.opCh = make(map[int]chan bool)
	go sc.applier()

	return sc
}
