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
	"encoding/gob"
	"sync"
	"sync/atomic"
	"6.824-golabs-2020/src/labrpc"
	"time"
	"fmt"
	"strconv"
	"math/rand"

	"bytes"
	"6.824-golabs-2020/src/labgob"
)



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
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}


const Leader int = 0
const Follower int = 1
const Candidate int = 2

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
	role int
	//persistent state on all servers
	currentTerm int
	voteFor int
	log []RaftLog //init 1
	//Volatile State on all servers
	commitIndex int
	lastApplied int
	//Volatile State on Leader ??????????????????????????????taskQueue
	nextIndex []int  // init 1
	matchIndex []int // init 0
	//time arguments
	requestVoteRandMax int
	heartBeatInterval int
	heartBeatWaitMax int
	
	//??????????????????
	heartBeatTimer *time.Timer
	//??????????????????
	heartBeatInterTimer *time.Timer
	taskQueue chan RaftTask
	applyCh chan ApplyMsg
}


type RaftLog struct{
	Term    int
	Command interface{}
	Index int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	isleader = rf.role == Leader

	return term, isleader
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
	data := rf.getPersistData()
	rf.persister.SaveRaftState(data)
}

func (rf *Raft) getPersistData() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.voteFor)
	//e.Encode(rf.commitIndex)
	e.Encode(rf.log)
	data := w.Bytes()
	return data
}


//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var currentTerm int
	var voteFor int
	var logs []RaftLog
	//var commitIndex, lastSnapshotIndex, lastSnapshotTerm int

	if d.Decode(&currentTerm) != nil ||
		d.Decode(&voteFor) != nil ||
		d.Decode(&logs) != nil {
		fmt.Println("peer:="+strconv.Itoa(rf.me)+",readPersist????????????,??????????????????")
	} else {
		rf.currentTerm = currentTerm
		rf.voteFor = voteFor
		rf.log = logs
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
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	
	// Your code here (2A, 2B).
	//rf.taskQueue <- raftTask
	//RPC ???????????? candidate ??????????????????????????????????????????????????? candidate ?????????????????????????????????????????????
	//Raft ????????????????????????????????????????????????????????????????????????????????????????????????????????????
	//??????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????
	//salfLastLogIndex := len(rf.log) -1
	reply.Term = rf.currentTerm
	candidateLogNew := false
	if rf.log[len(rf.log)-1].Term < args.LastLogTerm {
		candidateLogNew = true
	} else if rf.log[len(rf.log)-1].Term == args.LastLogTerm && rf.log[len(rf.log)-1].Index <= args.LastLogIndex + 1{
		candidateLogNew = true
	}
	fmt.Println("peer:= "+ strconv.Itoa(rf.me)+",Candidate :="+strconv.Itoa(args.CandidateId)+"???RV argsTerm:"+strconv.Itoa(args.Term)+"  current: " + strconv.Itoa( rf.currentTerm)+", candidateLogNew: "+strconv.FormatBool(candidateLogNew)+",args.lastTerm:="+strconv.Itoa(args.LastLogTerm)+",args.lastLogIndex:="+strconv.Itoa(args.LastLogIndex)+",peerLastTerm:="+strconv.Itoa(rf.log[len(rf.log)-1].Term)+",peerLastIndex:="+strconv.Itoa(args.LastLogIndex))
	if candidateLogNew && args.Term > rf.currentTerm {
		// ????????????????????????reset,????????????????????????
		rf.heartBeatTimer.Reset(time.Duration(rf.heartBeatWaitMax) * time.Millisecond) 
		reply.GrantVote = true
		task := &VoteTask{
			RaftTaskAttr : RaftTaskAttr{
				done : false,
				doneCond : sync.NewCond(&sync.Mutex{}),
				raft : rf,
		 },
		 newTerm : args.Term,
		 newVoteFor : args.CandidateId,
		}

		rf.taskQueue <- task
		task.WaitForDone()
	}else{
		reply.GrantVote = false
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	fmt.Println("AppendEntries :peer :"+strconv.Itoa(rf.me)+", term:"+strconv.Itoa(rf.log[len(rf.log) -1].Term)+" ,leaderId = :"+strconv.Itoa(args.LeaderId)+" ,len()-1 = :"+strconv.Itoa(len(rf.log) -1)+" ,VoteFor = "+strconv.Itoa(rf.voteFor)+" ,leader's Term = "+strconv.Itoa(args.Term)+" ,PrevLogIndex:"+ strconv.Itoa(args.PrevLogIndex))

	// Your code here (2A, 2B).
	// rf.taskQueue <- raftTask
	// if len(args.Entries) > 0 {
	// 	fmt.Println("peer :"+strconv.Itoa(rf.me)+",?????????log,args?????????????????????term :"+strconv.Itoa(args.Entries[len(args.Entries)-1].Term))
	// }else {
	// 	fmt.Println("peer :"+strconv.Itoa(rf.me)+",??????appendEntries??????log")
	// }
	reply.Term = rf.currentTerm
	// ?????????????????????????????????leader
	if rf.currentTerm > args.Term { // ????????????rpc??????????????????Leader
		reply.Success = false
		return
	} else if rf.currentTerm == args.Term && rf.voteFor != args.LeaderId && rf.role != Candidate{
		//??????term??????,???????????????????????????leader
		//???????????????candidate,??????term?????????AppendEntries RPC ????????????follower
		reply.Success = false
		return
	} 
	//???????????????leader ????????????follower
	rf.becomeFollower(args.Term , args.LeaderId)
	if rf.role != Leader {
		rf.heartBeatTimer.Reset(time.Duration(rf.heartBeatWaitMax) * time.Millisecond)
		fmt.Println("AppendEntries :peer :"+strconv.Itoa(rf.me)+"?????????AppendEntries,reset???timer")
	}
	headIndex := rf.GetHeadIndex()
	// ?????????????????????leader
	if args.Snapshot != nil {
		//todo
		task := &InstallSnapShot{
			RaftTaskAttr : RaftTaskAttr{
				done : false,
				doneCond : sync.NewCond(&sync.Mutex{}),
				raft : rf,
		 	},
			 snapShot :  args.Snapshot,
			 newEntries : args.Entries,
			 commitIndex : args.LeaderCommit,
		}
		rf.taskQueue <- task
		task.WaitForDone()	
		return
	}
	// ????????????snapshot,???????????????log????????????
	if rf.LastLogIndex()< args.PrevLogIndex {
		//??????PrevLogIndex
		reply.Success = false
		return
	}else if  rf.log[args.PrevLogIndex - headIndex].Term != args.PrevLogTerm {
		// ?????????????????????PrevLogTerm
		reply.Success = false
		task := &DeleteEntriesTask{
			RaftTaskAttr : RaftTaskAttr{
				done : false,
				doneCond : sync.NewCond(&sync.Mutex{}),
				raft : rf,
		 }, 
		 deleteIndex : args.PrevLogIndex,
		}
		rf.taskQueue <- task
		task.WaitForDone()
		return
	}else if rf.LastLogIndex() > args.PrevLogIndex{
		//?????????log???leader???????????????,????????????????????????expire RPC,????????????,???????????????
		isExpireRpc := true
		checkedDeleteIndex:= -1
		var newEntries []RaftLog
		CheckEntriesExpire:
		for k,v := range args.Entries{
			//???????????????entries ????????????delete(???????????????????????????append?????????append???entries,????????????????????????)
			if args.PrevLogIndex + 1 + k - headIndex >= len(rf.log){
				isExpireRpc = false
				checkedDeleteIndex = args.PrevLogIndex + 1 + k - headIndex
				newEntries = args.Entries[k:]
				break CheckEntriesExpire
			}
			if v.Term == rf.log[args.PrevLogIndex + 1 + k - headIndex].Term{
				continue
			}else{
				isExpireRpc = false
				checkedDeleteIndex = args.PrevLogIndex + 1 + k - headIndex
				newEntries = args.Entries[k:]
				break CheckEntriesExpire
			}
		}
		
		if checkedDeleteIndex == -1 && isExpireRpc == true{
			//entries????????????
			fmt.Println("leader:"+strconv.Itoa(args.LeaderId)+"?????????peer :"+strconv.Itoa(rf.me)+" ???entries???????????? ??????return")
			reply.Success = true
			return
		}

		//entries?????????
		fmt.Println("entries?????????")
		
		task := &DeleteEntriesTask{
			RaftTaskAttr : RaftTaskAttr{
				done : false,
				doneCond : sync.NewCond(&sync.Mutex{}),
				raft : rf,
		 },
		 deleteIndex : checkedDeleteIndex,
		}
		rf.taskQueue <- task
		task.WaitForDone()	

		reply.Success = true
		// ???????????????
		task2 := &AppendEntriesTask{
			RaftTaskAttr : RaftTaskAttr{
				done : false,
				doneCond : sync.NewCond(&sync.Mutex{}),
				raft : rf,
	 	},
	 	newTerm : args.Term,
	 	LeaderId : args.LeaderId,
	 	Entries : newEntries,
	 	LeaderCommit : args.LeaderCommit,
		}
		rf.taskQueue <- task2
		task.WaitForDone()
		return
	}

	reply.Success = true
	// ???????????????
	task := &AppendEntriesTask{
		RaftTaskAttr : RaftTaskAttr{
			done : false,
			doneCond : sync.NewCond(&sync.Mutex{}),
			raft : rf,
	 },
	 newTerm : args.Term,
	 LeaderId : args.LeaderId,
	 Entries : args.Entries,
	 LeaderCommit : args.LeaderCommit,
	}
	rf.taskQueue <- task
	task.WaitForDone()
	// if rf.role != Leader {
	// 	//fmt.Println(strconv.Itoa(rf.me)+" ???AppendEntries,?????????heartBeatTimer")
	// 	rf.heartBeatTimer.Reset(time.Duration(rf.heartBeatWaitMax) * time.Millisecond)
	// }
	fmt.Println("AppendEntries ->peer: " +strconv.Itoa(rf.me)+", ?????????len(log) :"+strconv.Itoa(len(rf.log))+",lastIndex's term :"+strconv.Itoa(rf.log[len(rf.log) -1].Term ))// +" leader???PrevLogIndex :" +strconv.Itoa(args.PrevLogIndex) +" PrevLogTerm:"+strconv.Itoa(args.PrevLogTerm))
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
	return ok
}

func (rf *Raft) sendAppendEntries(server int,args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
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
	// index := -1
	// term := -1
	// isLeader := true

	// Your code here (2B).
	if rf.role != Leader || rf.killed() {
		return -1, -1, false
	}

	task := &LeaderStartTask{
		RaftTaskAttr : RaftTaskAttr{
			done : false,
		 	doneCond : sync.NewCond(&sync.Mutex{}),
		 	raft : rf,
	 },
	 Command : command,
	 AppearIndex : -1,
	 AppearTerm : -1,
	}
	rf.taskQueue <- task
	task.WaitForDone()
	fmt.Println("Start succ ,leader :="+strconv.Itoa(rf.me)+",AppearIndex :="+strconv.Itoa(task.AppearIndex)+",AppearTerm :="+strconv.Itoa(task.AppearTerm))

	return task.AppearIndex, task.AppearTerm, true
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
	rf.role = Follower
	rf.currentTerm = 0
	rf.voteFor = -1
	rf.log = MakeLog()
	
	rf.commitIndex = 0
	rf.lastApplied = 0  //??????taskQueue
	// initialize from state persisted before a crash

	rf.readPersist(persister.ReadRaftState())

	//set time param
	rf.requestVoteRandMax = 300
	rf.heartBeatInterval = 80
	rf.heartBeatWaitMax = 500
	//????????????,follower???????????? ??????timer leader????????????
	rf.heartBeatTimer = MakeTimer(rf.heartBeatWaitMax)
	//????????????
	rf.heartBeatInterTimer = MakeTimer(rf.heartBeatInterval)	
	//????????????,??????candidate?????????????????? (????????????),?????????candidate?????????

	rf.taskQueue = make(chan RaftTask)
	rf.applyCh = applyCh
	rf.nextIndex = make([]int,len(rf.peers))
	rf.matchIndex = make([]int,len(rf.peers))

	//????????????????????????state???task( ?????????AppendEntries RPC????????????timer)
	go rf.runTask()
	// ?????????????????? ??????????????????
	go rf.electionLoop()
	// ????????????
	go rf.applyLoop()
	// ??????appendentriesRPC
	go rf.pingLoop()
	return rf
}

func (rf *Raft) pingLoop() {
    for {
		if rf.role != Leader {
			time.Sleep(10 * time.Millisecond)
            continue
        }
		<- rf.heartBeatInterTimer.C
        rf.heartBeatInterTimer.Reset(time.Duration(rf.heartBeatInterval) * time.Millisecond)
		// append entries to each Peer except itself
		aliveHosts := make([]bool, len(rf.peers))
		args := make([]AppendEntriesArgs,len(rf.peers))
		replies := make([]AppendEntriesReply,len(rf.peers))

		headIndex := rf.GetHeadIndex()
		// ???????????????entries???,commitIndex??????
		validCommitIndex := rf.commitIndex
		tryCommitLastIndex := rf.LastLogIndex()
		for i:=0; i< len(rf.peers); i ++{
			if i == rf.me{
				continue
			}
			var entries []RaftLog
			if rf.nextIndex[i] <= headIndex {
				// ?????????nextIndex,?????????SnapShot?????????
				copy(entries, rf.log)
				// todo ... 
				args[i] = AppendEntriesArgs	{
					Term : rf.currentTerm,
					LeaderId : rf.me,
					PrevLogIndex : 0,
					PrevLogTerm : 0,
					Entries : entries,
					LeaderCommit : validCommitIndex,
				}
				fmt.Println("???peer: "+strconv.Itoa(i)+",?????????PrevLogIndex="+strconv.Itoa(rf.nextIndex[i] -1)+",PrevLogTerm="+strconv.Itoa(rf.log[rf.nextIndex[i] -1].Term))

				replies[i].Term = 0
				replies[i].Success = false

			}else{
				endEntriesNum := rf.LastLogIndex() + 1 - rf.nextIndex[i] 
				if endEntriesNum == 0 {
					entries =[]RaftLog{}
					fmt.Println("leader:= "+strconv.Itoa(rf.me)+" ???peer: "+strconv.Itoa(i)+" ,len(log) = "+strconv.Itoa(len(rf.log))+", ???????????????????????????"+" nextIndex:"+strconv.Itoa(rf.nextIndex[i])+" leader???len(log):"+strconv.Itoa(len(rf.log)))
				}else {
					fmt.Println("endEntriesNum :="+strconv.Itoa(endEntriesNum))
					entries = make([]RaftLog,endEntriesNum)
					fmt.Println("leader:= "+strconv.Itoa(rf.me)+" ???peer: "+strconv.Itoa(i)+"?????????????????????,endEntriesNum:="+strconv.Itoa(endEntriesNum)+" nextIndex:"+strconv.Itoa(rf.nextIndex[i])+" leader???len(log):"+strconv.Itoa(len(rf.log)))
					InsertFor:
					for k,v := range rf.log[rf.nextIndex[i] - headIndex :len(rf.log)] {
						entries[k] = v
						if k >= len(entries)-1 {
							//?????????entries???,??????for??????,append???entries
							break InsertFor
						}
					}
				}
				//fmt.Println(strconv.Itoa(i)+" ???nextIndex = "+strconv.Itoa(rf.nextIndex[i]))
				///tempEntries := rf.log[rf.nextIndex[i]:len(rf.log)-1]
				args[i] = AppendEntriesArgs	{
					Term : rf.currentTerm,
					LeaderId : rf.me,
					PrevLogIndex : rf.nextIndex[i] -1,
					PrevLogTerm : rf.log[rf.nextIndex[i] -1 - headIndex].Term,
					Entries : entries,
					LeaderCommit : validCommitIndex,
				}
				fmt.Println("???peer: "+strconv.Itoa(i)+",?????????PrevLogIndex="+strconv.Itoa(rf.nextIndex[i] -1)+",PrevLogTerm="+strconv.Itoa(rf.log[rf.nextIndex[i] -1].Term))

				replies[i].Term = 0
				replies[i].Success = false
				// replies[i].ConflictIndex = 0
				// replies[i].ConflictTerm = 0
			}
			
		}
		appendEntriesCall := &AppendEntriesCall{
				AsyncRpcCallAttr : AsyncRpcCallAttr{
				AliveCount : 0,
				SuccessCount : 1,
				TotalCount : len(rf.peers),
				CurrentCount : 1,
				AliveHosts : aliveHosts,
				raft : rf,
				mu  : sync.Mutex{},
				doneCond : sync.NewCond(&sync.Mutex{}),
				MustExit : false ,
			},
			currentLogIndex : tryCommitLastIndex,
			args : args,
			replies : replies,
		}

		rf.CallAsyncRpc(appendEntriesCall)
		if appendEntriesCall.maxRepliesTerm > rf.currentTerm {
			rf.becomeFollower(appendEntriesCall.maxRepliesTerm -1 , -1)
		}
        DPrintf("%v start next ping round", rf)
    }
}

func (rf *Raft) applyLoop(){
	for {
        time.Sleep(10 * time.Millisecond)
        rf.mu.Lock()
        for rf.lastApplied < rf.commitIndex {
			rf.lastApplied++
			fmt.Println("Apply() , peer "+strconv.Itoa(rf.me)+", lastApplied: "+ strconv.Itoa(rf.lastApplied) +". commitIndex : "+strconv.Itoa(rf.commitIndex))
            rf.apply( rf.lastApplied ) // put to applyChan in the function
        }
		rf.mu.Unlock()
	}
}

func (rf *Raft) apply(logindex int ){
	rf.applyCh <- ApplyMsg {
		CommandValid : true,
		Command : rf.log[logindex - rf.GetHeadIndex()].Command,
		CommandIndex: logindex,
	}
}



func (rf *Raft) electionLoop(){
	candidateTerm := -1
	skipController := 0
	for {
		//time.Sleep(time.Duration(rand.Intn(rf.heartBeatWaitMax  -380) + 380) * time.Millisecond)
		<-rf.heartBeatTimer.C
		//rand.Intn(rvc.raft.requestVoteRandMax) + 300
		//rf.requireVoteTimer.Reset(time.Duration(rand.Intn(rf.requestVoteRandMax)))
		if rf.role == Leader{
			continue
		}
		fmt.Println("????????????,????????????:"+strconv.Itoa(rf.me)+"??????term??? :"+strconv.Itoa(rf.currentTerm + 1))
		// request vote from each Peer except itself
		if rf.currentTerm + 1 < candidateTerm {
		}else{
			candidateTerm = rf.currentTerm + 1
		}

		//????????????term +2 ?????????
		if skipController % (5 + rf.me) == 1 {
			candidateTerm ++
		}
		
		rf.becomeCandidate(candidateTerm)
		//lastLogIndex := len(rf.log) -1
		aliveHosts := make([]bool, len(rf.peers))
		for index, _ := range aliveHosts {
			aliveHosts[index] = false
		}
		
		requestVoteCall := &RequestVoteCall{
			AsyncRpcCallAttr : AsyncRpcCallAttr{
				AliveCount : 0,
				SuccessCount : 1,
				TotalCount : len(rf.peers),
				CurrentCount : 1,
				AliveHosts : aliveHosts,
				raft : rf,
				mu  : sync.Mutex{},
				doneCond : sync.NewCond(&sync.Mutex{}),
				MustExit : false ,
			},
			args : &RequestVoteArgs{
				Term : rf.currentTerm,
				CandidateId : rf.me,
				LastLogIndex : rf.LastLogIndex(),
				LastLogTerm : rf.log[len(rf.log) -1].Term,
			},
			replies : make([]RequestVoteReply, len(rf.peers)),
			maxRepliesTerm : rf.currentTerm ,
		}
		rf.CallAsyncRpc(requestVoteCall)
		if candidateTerm < requestVoteCall.maxRepliesTerm{
			fmt.Println("peer :="+strconv.Itoa(rf.me)+" ??????????????????term,??????????????????,term:=" +strconv.Itoa(requestVoteCall.maxRepliesTerm))
		}
		candidateTerm = requestVoteCall.maxRepliesTerm + 1
		if rf.role == Candidate {
			rf.heartBeatTimer.Reset(time.Duration(rand.Intn(500) + rf.heartBeatWaitMax / 2 ) * time.Millisecond)
		}
		skipController ++ 
	}
}


func MakeTimer(millisecond int) *time.Timer{
	return time.NewTimer(time.Duration(millisecond) * time.Millisecond)
}



// MakeLog : make a new *[]RaftLog
func MakeLog() []RaftLog{
	raftLog := make([]RaftLog,1)
	//??????raftlog???1,?????????nil??????0
	raftLog[0].Term =-1
	raftLog[0].Command = nil
	raftLog[0].Index = 0
	return raftLog
}



func (rf *Raft) becomeFollower(newTerm int,newVoteFor int) {
	if rf.role == Follower {
		return
	}

	task := &BecomeFollowerTask{
		RaftTaskAttr : RaftTaskAttr{
			done : false,
		 	doneCond : sync.NewCond(&sync.Mutex{}),
		 	raft : rf,
	 },
	 	newTerm : newTerm,
		newVoteFor : newVoteFor ,
	}
	rf.taskQueue <- task
	task.WaitForDone()
	fmt.Println("peer:"+strconv.Itoa(rf.me)+" become to follower")
}

func (rf *Raft) CommitLog(commitIndex int){
	if rf.role != Leader {
		return
	}
	task := &CommitTask{
		RaftTaskAttr : RaftTaskAttr{
			done : false,
		 doneCond : sync.NewCond(&sync.Mutex{}),
		 raft : rf,
	 },
	 commitIndex : commitIndex,
	}
	rf.taskQueue <- task
	task.WaitForDone()
}

func (rf *Raft) becomeLeader() {
	if rf.role != Candidate {
		fmt.Println("peer "+ strconv.Itoa(rf.me) +" ?????????candidate,????????????leader,????????????"+ strconv.Itoa(rf.role))
		return
	}

	task := &BecomeLeaderTask{
		RaftTaskAttr{
			done : false,
		 doneCond : sync.NewCond(&sync.Mutex{}),
		 raft : rf,
	 },
	}
	rf.taskQueue <- task
	task.WaitForDone()
	fmt.Println("????????????,leader???" + strconv.Itoa(rf.me)+",term???"+strconv.Itoa(rf.currentTerm))
}

func (rf *Raft) becomeCandidate(newTerm int) {

	task := &BecomeCandidateTask{
		RaftTaskAttr : RaftTaskAttr{
   			done : false,
			doneCond : sync.NewCond(&sync.Mutex{}),
			raft : rf,
		},
		NewTerm : newTerm,
	}
	rf.taskQueue <- task
	task.WaitForDone()
}


func  (rf *Raft) runTask(){
	for {
		rf.mu.Lock()
		rf.RunOne()
		rf.persist()
		rf.mu.Unlock()
	}
}

func (rf *Raft) RunOne(){
	task := <- rf.taskQueue
	task.Execute()
	task.SetDone()
}


func (rf *Raft) String() string {
    return fmt.Sprintf("[role:%d:%d;Term:%d;VotedFor:%d;logLen:%v;Commit:%v;Apply:%v]",
        rf.role, rf.me, rf.currentTerm, rf.voteFor, len(rf.log), rf.commitIndex, rf.lastApplied)
}

// Return the size of persister
func (rf *Raft) PersisterSize() int {
	return rf.persister.RaftStateSize()
}

// Go snapshot ???kvserver??????????????????
func (rf *Raft) GoSnapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	baseIndex := rf.GetHeadIndex()
	lastIndex := rf.LastLogIndex()

	if index <= baseIndex || index > lastIndex {
		return
	}

	var newLog []RaftLog
	newLog = append(newLog, rf.log[index - baseIndex:lastIndex - baseIndex + 1]...)

	rf.log = newLog
	rf.persist()

	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(newLog[0].Index)
	e.Encode(newLog[0].Term)

	data := w.Bytes()
	data = append(data, snapshot...)
	rf.persister.SaveSnapshot(data)
}

func (ps *Persister) SaveSnapshot(snapshot []byte) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	ps.snapshot = snapshot
}

func (rf *Raft) LastLogIndex() int {
	return rf.log[len(rf.log) - 1].Index
}

func (rf *Raft) GetHeadIndex() int {
	return rf.log[0].Index
}





