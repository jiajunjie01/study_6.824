package raft

import "sync"
import "math/rand"
import "time"
import "fmt"
import "strconv"

type AsyncRpcInterface interface{
	makeRpcCall(int) bool
	callback(int)
	tryEnd() bool
	setTimeout()
	IncrementCurrentCount()
	IncrementSuccessCount()
	lock()
	unlock()
	SetMustExit()
	GetMustExit() bool
	SetAliveHosts(int)
	WaitDone()

}


type AsyncRpcCallAttr struct{
	AliveCount   int
	SuccessCount int
	TotalCount   int
	CurrentCount int
	AliveHosts   []bool
	raft         *Raft

	mu       sync.Mutex
	doneCond *sync.Cond
	MustExit bool
}

func (ri *AsyncRpcCallAttr) WaitDone(){
	ri.doneCond.L.Lock()
	for ri.MustExit == false {
		ri.doneCond.Wait()
	}
	ri.doneCond.L.Unlock()
}

func (ri *AsyncRpcCallAttr) SetAliveHosts(index int){
	ri.AliveHosts[index] = true
}

func (ri *AsyncRpcCallAttr) lock(){
	ri.mu.Lock()
}

func (ri *AsyncRpcCallAttr) unlock(){
	ri.mu.Unlock()
}

func (ri *AsyncRpcCallAttr) SetMustExit(){
	
	ri.doneCond.L.Lock()
	ri.MustExit = true
	ri.doneCond.L.Unlock()
	ri.doneCond.Broadcast()
	
}

func (ri *AsyncRpcCallAttr) GetMustExit() bool{
	return ri.MustExit
}

func (ri *AsyncRpcCallAttr) IncrementCurrentCount() {
	ri.CurrentCount++
}

func (ri *AsyncRpcCallAttr) IncrementSuccessCount() {
	ri.SuccessCount++
	//fmt.Println("my index "+strconv.Itoa(ri.raft.me)+ " request vote reply num =  "+strconv.Itoa(ri.SuccessCount))
}

type RequestVoteCall struct {
	AsyncRpcCallAttr

	args    *RequestVoteArgs
	replies []RequestVoteReply
	maxRepliesTerm int
}

func (rvc *RequestVoteCall) makeRpcCall(peerIndex int) bool {
	ok := rvc.raft.sendRequestVote(peerIndex,rvc.args,&rvc.replies[peerIndex])
	if rvc.replies[peerIndex].GrantVote {
		fmt.Println("peer:"+strconv.Itoa(rvc.raft.me)+"得到一票,投票人是:"+strconv.Itoa(peerIndex))
	} else {
		fmt.Println("peer:"+strconv.Itoa(rvc.raft.me)+"被拒绝一票,拒绝人是:"+strconv.Itoa(peerIndex)+",返回的term是:"+strconv.Itoa( rvc.replies[peerIndex].Term))
	}
	return ok
}

func (rvc *RequestVoteCall) callback(peerIndex int) {
	//如果此server已经不是candidate,则直接return
	if rvc.raft.role != Candidate{
		rvc.SetMustExit()
		return
	}
	reply := rvc.replies[peerIndex]
	// check follower's term 
	if rvc.maxRepliesTerm < reply.Term {
		rvc.maxRepliesTerm = reply.Term
		return
	}
	if reply.GrantVote {
		rvc.IncrementSuccessCount()
	}
}

func (rvc *RequestVoteCall) tryEnd() bool{

	fmt.Println("peer:"+strconv.Itoa(rvc.raft.me)+" 的现票情况success ="+strconv.Itoa(rvc.SuccessCount) + " TotalCount ="+strconv.Itoa(rvc.TotalCount)+ " CurrentCount ="+strconv.Itoa(rvc.CurrentCount))
	if rvc.SuccessCount > rvc.TotalCount/2 {
		// change raft state
		rvc.raft.becomeLeader()
		rvc.SetMustExit()
		return true
	}
	//已经不可能选举成功了
	if rvc.SuccessCount+rvc.TotalCount-rvc.CurrentCount <= rvc.TotalCount/2 {
		rvc.SetMustExit()
		return true
	}
	if rvc.CurrentCount >= rvc.TotalCount {
		rvc.SetMustExit()
		return true
	}
	return false
}

func (rvc *RequestVoteCall) setTimeout(){
	//maxtime :=rvc.raft.requestVoteRandMax
	time.Sleep(time.Duration(rand.Intn(rvc.raft.requestVoteRandMax -150) + 150) * time.Millisecond)
	//fmt.Println("wait "+strconv.Itoa(maxtime)+"选举超时")
	rvc.SetMustExit()
}

type AppendEntriesCall struct {
	AsyncRpcCallAttr

	currentLogIndex  int
	args    []AppendEntriesArgs
	replies []AppendEntriesReply
	maxRepliesTerm int
}

func (aec *AppendEntriesCall) setTimeout(){
	//超出interval会强制退出
	time.Sleep(time.Duration(rand.Intn(aec.raft.heartBeatInterval)) * time.Millisecond )
	aec.SetMustExit()
}

func (aec *AppendEntriesCall) makeRpcCall(peerIndex int) bool{
	return aec.raft.sendAppendEntries(peerIndex,&aec.args[peerIndex],&aec.replies[peerIndex])
}

func (aec *AppendEntriesCall) callback(peerIndex int){
	var newNextIndex int
	var newMatchIndex int
	if aec.raft.role != Leader{
		aec.MustExit = true
		return
	}
	reply := aec.replies[peerIndex]
	if aec.raft.currentTerm < reply.Term {
		if aec.maxRepliesTerm < reply.Term{
			aec.maxRepliesTerm = reply.Term
		}
		return
	}
	if reply.Success && len(aec.args[peerIndex].Entries) == 0{
		fmt.Println("peer :"+strconv.Itoa(peerIndex)+", 没有更新nextIndex")
		aec.IncrementSuccessCount()
		return
	}else if reply.Success{
		aec.IncrementSuccessCount()
		if aec.args[peerIndex].Snapshot != nil {
			//
			//newNextIndex = 
			//newMatchIndex = 
		}else{
			newNextIndex = aec.args[peerIndex].PrevLogIndex + len(aec.args[peerIndex].Entries) +1
			newMatchIndex = aec.args[peerIndex].PrevLogIndex + len(aec.args[peerIndex].Entries)
		}
	
		// aec.raft.nextIndex[peerIndex] = aec.args[peerIndex].PrevLogIndex + len(aec.args[peerIndex].Entries) +1
		// aec.raft.matchIndex[peerIndex] = aec.args[peerIndex].PrevLogIndex + len(aec.args[peerIndex].Entries)
	}else{
		//follower can't match entrys 
		if aec.args[peerIndex].PrevLogIndex / 2 < 50 {
			newNextIndex = aec.raft.matchIndex[peerIndex] + 1
		}else {
			newNextIndex = aec.args[peerIndex].PrevLogIndex / 2
		}
		newMatchIndex = -1

		// aec.raft.nextIndex[peerIndex] = aec.args[peerIndex].PrevLogIndex / 2 + 1
		// aec.raft.matchIndex[peerIndex] = 0
	}

	fmt.Println("leader :="+strconv.Itoa(aec.raft.me)+",peer :"+strconv.Itoa(peerIndex)+"的回复是 reply.term = "+strconv.Itoa(aec.replies[peerIndex].Term) +" reply.succ="+strconv.FormatBool(aec.replies[peerIndex].Success))
	task := &ChangePeerTwoIndexTask {
		RaftTaskAttr : RaftTaskAttr {
			done : false,
		 	doneCond : sync.NewCond(&sync.Mutex{}),
		 	raft : aec.raft,
		},
		PeerIndex : peerIndex,
		NextIndex : newNextIndex,
		MatchIndex : newMatchIndex,
	}
	aec.raft.taskQueue <- task
	task.WaitForDone()

}

func (aec *AppendEntriesCall) tryEnd() bool{
	//成功commit
	if aec.SuccessCount > aec.TotalCount/2 {
		aec.raft.CommitLog(aec.currentLogIndex)
		aec.SetMustExit()
		return true
	}
	if aec.SuccessCount+aec.TotalCount-aec.CurrentCount <= aec.TotalCount/2 {
		aec.SetMustExit()
		return true
	}
	if aec.CurrentCount >= aec.TotalCount {
		aec.SetMustExit()
		return true
	}
	return false
}

func (rf *Raft) CallAsyncRpc(asyncTask AsyncRpcInterface) {
	go asyncTask.setTimeout()
	for i:=0; i< len(rf.peers); i++ {
		//跳过自己
		if rf.me == i {
			continue
		}
		//async rpc call
		go func(callIndex int){
			ok := asyncTask.makeRpcCall(callIndex)
			asyncTask.lock()
			defer asyncTask.unlock()
			// 因为AppendEntries,需要更新所有的成员的nextIndex这里不能退出
			// if asyncTask.GetMustExit() {
			// 	return
			// }
			asyncTask.IncrementCurrentCount()
			
			//rpc成功运行,则执行callBack
			if ok {
				//先标记此peer在线
				asyncTask.SetAliveHosts(callIndex)
				asyncTask.callback(callIndex)
			}
			asyncTask.tryEnd()
		}(i)
	}
	//这里要有个东西等着
	asyncTask.WaitDone()
}