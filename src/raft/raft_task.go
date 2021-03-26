package raft

import "sync"
import "fmt"
import "strconv"
import "time"

type RaftTask interface{
	Execute()
	WaitForDone()
	SetDone()
}



type RaftTaskAttr struct {
	done     bool
	doneCond *sync.Cond

	raft *Raft
}

func (rtd *RaftTaskAttr) WaitForDone() {
	rtd.doneCond.L.Lock()
	for rtd.done == false {
		rtd.doneCond.Wait()
	}
	rtd.doneCond.L.Unlock()
}

func (rtd *RaftTaskAttr) SetDone() {
	rtd.doneCond.L.Lock()
	if rtd.done {
		panic("Task done twice...")
	}
	rtd.done = true
	rtd.doneCond.L.Unlock()
	rtd.doneCond.Broadcast()
}

type ChangePeerTwoIndexTask struct{
	RaftTaskAttr
	PeerIndex int
	NextIndex int
	MatchIndex int
}

type LeaderStartTask struct{
	RaftTaskAttr
	Command interface{}
	AppearIndex int
	AppearTerm int
}

//删除无效entries
type DeleteEntriesTask struct{
	RaftTaskAttr
	deleteIndex int
}


// 投票后改变自身状态
type VoteTask struct{
	RaftTaskAttr
	newTerm int
	newVoteFor int
}

type AppendEntriesTask struct {
	RaftTaskAttr
	newTerm int
	LeaderId int
	Entries []RaftLog
	LeaderCommit int
	snapshot []byte
}


//如果不需要更新Term newTerm=-1
type BecomeFollowerTask struct {
	RaftTaskAttr
	newTerm int
	newVoteFor int
}

type BecomeCandidateTask struct {
	RaftTaskAttr
	NewTerm int
}

type BecomeLeaderTask struct {
	RaftTaskAttr
}

type CommitTask struct {
	RaftTaskAttr
	commitIndex int
}

func(cpti *ChangePeerTwoIndexTask) Execute(){
	if cpti.NextIndex <= cpti.raft.matchIndex[cpti.PeerIndex] {
		//如果要更新的NextIndex 甚至比现在的matchIndex还小,说明这次callBack已经过期了
		return
	}
	fmt.Println("leader:="+strconv.Itoa(cpti.raft.me)+" 修改了peer:"+strconv.Itoa(cpti.PeerIndex)+",的nextIndex:"+strconv.Itoa(cpti.NextIndex)+",matchIndex:"+strconv.Itoa(cpti.MatchIndex))
	cpti.raft.nextIndex[cpti.PeerIndex] = cpti.NextIndex
	if cpti.MatchIndex != -1 {
		cpti.raft.matchIndex[cpti.PeerIndex] = cpti.MatchIndex
	}
}

func(lst *LeaderStartTask) Execute(){
	newLog := RaftLog{
		Term : lst.raft.currentTerm,
		Command : lst.Command,
	}
	lst.AppearIndex = len(lst.raft.log)
	lst.AppearTerm = lst.raft.currentTerm
	lst.raft.log = append(lst.raft.log,newLog)
	fmt.Println("leader start:新增了一条日志,len(log)= "+strconv.Itoa(len(lst.raft.log)))

}

//DeleteEntriesTask 删除包括deleteIndex以后的entries
func(det *DeleteEntriesTask) Execute(){
	fmt.Println("peer:"+strconv.Itoa(det.raft.me)+" 删除log index : "+strconv.Itoa(det.deleteIndex))
	det.raft.log = det.raft.log[:det.deleteIndex]
}

func(aet *AppendEntriesTask) Execute(){
	fmt.Println("peer:"+strconv.Itoa(aet.raft.me)+" append len(Entries) : "+strconv.Itoa(len(aet.Entries)))
	aet.raft.voteFor = aet.LeaderId
	aet.raft.currentTerm = aet.newTerm

	if aet.snapshot != nil {
		
	}else if len(aet.Entries) != 0 {
		aet.raft.log = append(aet.raft.log,aet.Entries ...)
	}
	fmt.Println("peer "+strconv.Itoa(aet.raft.me) + ",commitIndex " +strconv.Itoa( aet.LeaderCommit)+" by AppendEntries")
	aet.raft.commitIndex = aet.LeaderCommit

}

func(vote *VoteTask) Execute(){
	vote.raft.role = Follower
	vote.raft.currentTerm = vote.newTerm
	vote.raft.voteFor = vote.newVoteFor
}

func(commitTask *CommitTask) Execute(){
	if commitTask.commitIndex <= commitTask.raft.commitIndex {
		return
	}
	fmt.Println("peer:"+strconv.Itoa(commitTask.raft.me)+" commit index : "+strconv.Itoa(commitTask.commitIndex))
	commitTask.raft.commitIndex = commitTask.commitIndex
}

func(becomeFollowerTask *BecomeFollowerTask) Execute() {
	becomeFollowerTask.raft.role = Follower
	if becomeFollowerTask.newTerm != -1 {
		becomeFollowerTask.raft.currentTerm = becomeFollowerTask.newTerm
		becomeFollowerTask.raft.voteFor = becomeFollowerTask.newVoteFor
	}
	DPrintf("%s change to leader", becomeFollowerTask.raft)
}

func(becomeLeaderTask *BecomeLeaderTask) Execute() {
	if becomeLeaderTask.raft.role != Candidate {
		fmt.Println("peer "+ strconv.Itoa(becomeLeaderTask.raft.me) +" 已不是candidate")
		return
	}
	becomeLeaderTask.raft.role = Leader
	becomeLeaderTask.raft.voteFor = becomeLeaderTask.raft.me
	for i := 0; i< len(becomeLeaderTask.raft.nextIndex); i++{
		becomeLeaderTask.raft.nextIndex[i] = len(becomeLeaderTask.raft.log)
	}
	becomeLeaderTask.raft.heartBeatInterTimer.Reset(time.Duration(10) * time.Millisecond) 
	//fmt.Println(strconv.Itoa(becomeLeaderTask.raft.me) + " :成为leader")
	DPrintf("%s change to leader", becomeLeaderTask.raft)
}



func(becomeCandidateTask *BecomeCandidateTask) Execute() {
	becomeCandidateTask.raft.role = Candidate
    becomeCandidateTask.raft.currentTerm = becomeCandidateTask.NewTerm
	becomeCandidateTask.raft.voteFor = becomeCandidateTask.raft.me
	//因为改了term和voteFor这两个persistent state,所以要调用persist来持久化状态
	DPrintf("%s change to candidate", becomeCandidateTask.raft)
}

