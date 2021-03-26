package kvraft

import (
	"encoding/gob"
	"bytes"
	"time"
	"6.824-golabs-2020/src/labgob"
	"6.824-golabs-2020/src/labrpc"
	"log"
	"6.824-golabs-2020/src/raft"
	"sync"
	"sync/atomic"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}


type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Operation	string
	Key 		string
	Value		string
	Seq			int64
	ClientId	int64
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	opLogs  	map[int]chan Op		// stored command based on log index. int ->chan Op
	kvData  	map[string]string 	// kv database
	clientSeq	map[int64]int64		// record the sequence number of each client
}


func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	op := Op{Operation: "Get", Key: args.Key, Seq: args.Seq, ClientId: args.ClientId}
	result := kv.runOp(op)
	if result != OK {
		reply.WrongLeader = true
		reply.Err = result
	} else {
		kv.mu.Lock()
		reply.WrongLeader = false
		val, ok := kv.kvData[args.Key]
		if !ok {
			reply.Err = ErrNoKey
		} else {
			reply.Err = OK
			reply.Value = val
		}
		kv.mu.Unlock()
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	op := Op{Operation: args.Op, Key: args.Key, Value: args.Value, Seq: args.Seq, ClientId: args.ClientId}
	result := kv.runOp(op)
	if result != OK {
		reply.WrongLeader = true
		reply.Err = result
	} else {
		reply.WrongLeader = false
		reply.Err = OK
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

	// You may need initialization code here.
	kv.kvData = make(map[string]string)
	kv.opLogs = make(map[int]chan Op)
	kv.clientSeq = make(map[int64]int64)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	go kv.ApplyLoop()
	// You may need initialization code here.

	return kv
}

// a go-routine listen to the applyCh channel from raft
func (kv *KVServer) ApplyLoop() {
	for{
		msg := <- kv.applyCh
		if msg.UseSnapshot {
			//使用leader给的快照
			var LastIncludedIndex int
			var LastIncludedTerm int
			c := bytes.NewBuffer(msg.Snapshot)
			decoder := gob.NewDecoder(c)
			kv.kvData = make(map[string]string)
			kv.opLogs = make(map[int]chan Op)
			kv.mu.Lock()
			decoder.Decode(&LastIncludedIndex)
			decoder.Decode(&LastIncludedTerm)
			decoder.Decode(&kv.kvData)
			// decoder.Decode(&kv.opLogs) 我认为不需要保存client的调用channel,只需要保存seq
			decoder.Decode(&kv.clientSeq)
			kv.mu.Unlock()
		}else{
			kv.mu.Lock()
			index := msg.CommandIndex
			op := msg.Command.(Op)
			if op.Seq > kv.clientSeq[op.ClientId] {
				switch op.Operation {
				case "Put":
					kv.kvData[op.Key] = op.Value
				case "Append":
					if _, ok := kv.kvData[op.Key]; !ok {
						kv.kvData[op.Key] = op.Value
					} else {
						kv.kvData[op.Key] += op.Value
					}
				default:
				}
				kv.clientSeq[op.ClientId] = op.Seq
			}
			opCh, ok := kv.opLogs[index] // 如果ok == false 说明这个command的client并不是在本节点调用的
			if ok {
				// clear the channel
				select {
				case <- opCh:
				default:
				}
				opCh <- op
			}
			if kv.maxraftstate != -1 && kv.rf.PersisterSize() > kv.maxraftstate {
				w := new(bytes.Buffer)
				e := gob.NewEncoder(w)
				e.Encode(kv.kvData)
				//e.Encode(kv.opLogs)
				e.Encode(kv.clientSeq)
				data := w.Bytes()
				go kv.rf.GoSnapshot(msg.CommandIndex, data)
			}
			kv.mu.Unlock()
		}
	
	}
}

func (kv * KVServer) runOp(op Op) Err {
	// agreement with raft
	idx, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		return ErrWrongLeader
	}
	// create a Op channel to store command log based on log index
	// because it needs to be consistent with the common written into raft logs
	kv.mu.Lock()
	opCh, ok := kv.opLogs[idx]
	if !ok {
		opCh = make(chan Op)
		kv.opLogs[idx] = opCh
	}
	kv.mu.Unlock()
	defer func() {
		kv.mu.Lock()
		delete(kv.opLogs, idx)
		kv.mu.Unlock()
	}()
	select {
		case logOp := <- opCh:
			// 如果接到同Index的report,但是内容不同,说明过程中leader发生了切换.
			if logOp == op {
				return OK
			} else {
				return ErrWrongLeader
			}
		case <- time.After(2000 * time.Millisecond):
			return Timeout
		}
}
