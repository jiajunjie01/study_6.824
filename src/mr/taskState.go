package mr

import (
	"sync"
	"time"
	"fmt"
	//"strconv"
)

// TaskStat :mr has two tasks,Map task and reduce task,need a same interface,a same struct
type TaskStat struct {
	beginTime time.Time
	fileName  string
	fileIndex int
	partIndex int
	nReduce   int
	nFiles    int
}

const (
	TaskMap    = 0
	TaskReduce = 1
	TaskWait   = 2
	TaskEnd    = 3
)

type TaskInfo struct {
	/*
		Declared in consts above
			0  map
			1  reduce
			2  wait
			3  end
	*/
	State int

	FileName  string
	FileIndex int
	PartIndex int

	NReduce int
	NFiles  int
}


type TaskStatInterface interface {
	GenerateTaskInfo() TaskInfo
	OutOfTime() bool
	GetFileIndex() int
	GetPartIndex() int
	SetNow()
}

type MapTaskStat struct {
	TaskStat
}

type ReduceTaskStat struct {
	TaskStat
}

func (this *MapTaskStat) GenerateTaskInfo() TaskInfo {
	return TaskInfo{
		State:     TaskMap,
		FileName:  this.fileName,
		FileIndex: this.fileIndex, // mapFunction需要FileIndex来知道自己读哪个input文件
		PartIndex: this.partIndex, // reduceFunction需要PartIndex来知道自己读哪些mapOutputFile
		NReduce:   this.nReduce,
		NFiles:    this.nFiles,
	}
}

func (this *ReduceTaskStat) GenerateTaskInfo() TaskInfo{
	
	return TaskInfo{
		State:     TaskReduce,
		FileName:  this.fileName,
		FileIndex: this.fileIndex,
		PartIndex: this.partIndex,
		NReduce:   this.nReduce,
		NFiles:    this.nFiles,
	}
}

func (this *TaskStat) OutOfTime() bool {
	fmt.Println( " now  -- begin time  " + time.Now().Sub(this.beginTime).String() )
	return time.Now().Sub(this.beginTime) > time.Duration(time.Second*30)
}

func (this *TaskStat) SetNow() {
	this.beginTime = time.Now()
}

func (this *TaskStat) GetFileIndex() int {
	return this.fileIndex
}

func (this *TaskStat) GetPartIndex() int {
	return this.partIndex
}



type TaskStatQueue struct {
	mutex     sync.Mutex
	TaskChan chan TaskStatInterface
}

func (this *TaskStatQueue) lock() {
	this.mutex.Lock()
}

func (this *TaskStatQueue) unlock() {
	this.mutex.Unlock()
}

func (this *TaskStatQueue) RemoveTask(fileIndex int, partIndex int){
	this.lock()
	chanLen := len(this.TaskChan) 
	for index := 0 ;index < chanLen;index ++ {
		task := <- this.TaskChan
		if fileIndex == task.GetFileIndex() && partIndex == task.GetPartIndex() {
			break	
		}else{
			this.TaskChan <- task 
		}
	}
	this.unlock()
}

// func (this *TaskStatQueue) GetTimeOutTaskQueue1() TaskStatQueue{
// 	timeOutQueue := make(chan TaskStatInterface,len(this.TaskChan))
// 	this.lock()
// 	chanLen := len(this.TaskChan)
// 	for taskIndex :=0 ;taskIndex < chanLen;taskIndex ++{
// 		task := <- this.TaskChan
// 		if task.OutOfTime() {
// 			timeOutQueue <- task
// 		}
// 	}
// 	this.unlock()
// 	return TaskStatQueue{
// 		TaskChan : timeOutQueue,
// 	}
// }

func (this *TaskStatQueue) GetTimeOutTaskQueue(crashedChan chan TaskStatInterface){
	for{
		fmt.Println("begin GetTimeOutTaskQueue")
		time.Sleep(time.Duration(time.Second * 5))
		chanLen := len(this.TaskChan)
		for taskIndex :=0 ;taskIndex < chanLen;taskIndex ++{
			task := <- this.TaskChan
			if task.OutOfTime() {
				fmt.Println("GetTimeOutTaskQueue get a crashed task")
				crashedChan <- task
				fmt.Println("GetTimeOutTaskQueue put a crashed task")
			}
		}
	}
}