package mr

import (
	"strconv"
	"fmt"
	"log"
	"net"
	"os"
	"net/rpc"
	"net/http"
)


type Master struct {
	// Your definitions here.
	filenames []string

	// reduce task queue
	reduceTaskWaiting TaskStatQueue	
	reduceTaskRunning TaskStatQueue

	reduceTaskCrashed TaskStatQueue
	// map task statistics
	mapTaskWaiting TaskStatQueue
	mapTaskRunning TaskStatQueue
	
	mapTaskCrashed TaskStatQueue

	// machine state
	isDone  bool
	nReduce int
}



//AskTask :for the worker to call
func (this *Master) AskTask(args *ExampleArgs, reply *TaskInfo) error {
	if this.isDone{
		reply.State = TaskEnd
		return nil
	}

	if len(this.reduceTaskWaiting.TaskChan) > 0 {
		reduceTask := <- this.reduceTaskWaiting.TaskChan
		if reduceTask != nil{
			// an available reduce task
			//record task beginTime
			reduceTask.SetNow()
			// note task is running
			this.reduceTaskRunning.TaskChan <- reduceTask
			// setup a reply
			fmt.Printf("Distributing reduce task on part %v %vth file %v\n", reply.PartIndex, reply.FileIndex, reply.FileName)
			*reply = reduceTask.GenerateTaskInfo()
			return nil
		}
	}

	// check for map tasks
	if len(this.mapTaskWaiting.TaskChan) > 0 {
		fmt.Println("exist waiting map task")
		mapTask := <- this.mapTaskWaiting.TaskChan

		if mapTask != nil {
			// an available map task
			// record task begin time
			mapTask.SetNow()
			// note task is running
			this.mapTaskRunning.TaskChan <- mapTask
			// setup a reply
			*reply = mapTask.GenerateTaskInfo()
			fmt.Printf("Distributing map task on %vth file %v\n", reply.FileIndex, reply.FileName)
			return nil
		}
	}
	

	// all tasks distributed
	if len(this.mapTaskRunning.TaskChan) > 0 || len(this.reduceTaskRunning.TaskChan) > 0 {
		// must wait for new tasks
		reply.State = TaskWait
		return nil
	}
	// all tasks complete
	reply.State = TaskEnd
	this.isDone = true
	return nil

}


func (this *Master) TaskDone(args *TaskInfo, reply *ExampleReply) error {
	switch args.State {
	case TaskMap:
		fmt.Printf("Map task on %vth file %v complete\n", args.FileIndex, args.FileName)
		this.mapTaskRunning.RemoveTask(args.FileIndex, args.PartIndex)
		if len(this.mapTaskRunning.TaskChan) == 0 && len(this.mapTaskWaiting.TaskChan) == 0 {
			// all map tasks done
			// can distribute reduce tasks
			this.distributeReduce()
		}
		fmt.Printf("TaskDone:Map task on %vth file %v complete\n", args.FileIndex, args.FileName)
		break
	case TaskReduce:
		fmt.Printf("Reduce task on %vth part complete\n", args.PartIndex)	
		this.reduceTaskRunning.RemoveTask(args.FileIndex, args.PartIndex)
		break
	default:
		panic("Task Done error")
	}
	return nil
}

func (this *Master) distributeReduce() {
	fmt.Printf("begin distribute reduce task,and taskNum = %v\n",this.nReduce)
	reduceTask := ReduceTaskStat{
		TaskStat{
			fileIndex: 0,
			partIndex: 0,
			nReduce:   this.nReduce,
			nFiles:    len(this.filenames),
		},
	}
	for reduceIndex := 0; reduceIndex < this.nReduce; reduceIndex++ {
		task := reduceTask
		task.partIndex = reduceIndex
		this.reduceTaskWaiting.TaskChan <- &task
	}
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}


//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {

	return m.isDone
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//


	// Your definitions here.
	// filenames []string

	// // reduce task queue
	// reduceTaskWaiting TaskStatQueue	
	// reduceTaskRunning TaskStatQueue
	// // map task statistics
	// mapTaskWaiting TaskStatQueue
	// mapTaskRunning TaskStatQueue

	// // machine state
	// isDone  bool
	// nReduce int


	// beginTime time.Time
	// fileName  string
	// fileIndex int
	// partIndex int
	// nReduce   int
	// nFiles    int
func MakeMaster(files []string, nReduce int) *Master {
	fmt.Println("MakeMaster_files_len = " + strconv.Itoa(len(files)))
	mapTaskChan := make(chan TaskStatInterface,len(files))

	//put mapTask to MapTaskChan
	for file_index,file_name := range files{
		mapTask := MapTaskStat{
			TaskStat{
				fileName: file_name,
				fileIndex: file_index,
				nReduce: nReduce,
				nFiles: len(files),
			},
		}
		mapTaskChan <- &mapTask
	}
	// new Master obj
	m := Master{
		mapTaskWaiting: TaskStatQueue{TaskChan: mapTaskChan},
		mapTaskRunning: TaskStatQueue{TaskChan: make(chan TaskStatInterface,len(files))},
		mapTaskCrashed:TaskStatQueue{TaskChan: make(chan TaskStatInterface,len(files))},
		reduceTaskWaiting: TaskStatQueue{TaskChan: make(chan TaskStatInterface,nReduce)},
		reduceTaskRunning: TaskStatQueue{TaskChan: make(chan TaskStatInterface,nReduce)},
		reduceTaskCrashed:TaskStatQueue{TaskChan: make(chan TaskStatInterface,nReduce)},
		nReduce:        nReduce,
		filenames:      files,
	}
	
	if _, err := os.Stat("mr-tmp"); os.IsNotExist(err) {
		err = os.Mkdir("mr-tmp", os.ModePerm)
		if err != nil {
			fmt.Print("Create tmp directory failed... Error: %v\n", err)
			panic("Create tmp directory failed...")
		}
	}

	
	// begin a thread to collect tasks out of time
	go m.collectMapOutOfTime()
	go m.collectReduceOutOfTime()

	
	m.server()
	return &m
}


// func (this *Master) collectOutOfTime() {
// 	for {
// 		time.Sleep(time.Duration(time.Second * 5))
// 		timeouts := this.reduceTaskRunning.GetTimeOutTaskQueue()
// 		chanLen := len(timeouts.TaskChan)
// 		this.reduceTaskWaiting.lock()
// 		timeouts.lock()
// 		for index :=0 ;index < chanLen;index ++{
// 			fmt.Println("there are Reduce task time out")
// 			task:= <- timeouts.TaskChan
// 			this.reduceTaskWaiting.TaskChan <- task
// 		}
// 		this.reduceTaskWaiting.unlock()
// 		timeouts.unlock()
// 		timeouts = this.mapTaskRunning.GetTimeOutTaskQueue()
// 		chanLen = len(timeouts.TaskChan)
// 		this.mapTaskWaiting.lock()
// 		timeouts.lock()
// 		for index :=0 ;index < chanLen;index ++{
// 			fmt.Println("there are Map task time out")
// 			task:= <- timeouts.TaskChan
// 			this.mapTaskWaiting.TaskChan <- task
// 		}
// 		this.mapTaskWaiting.unlock()
// 		timeouts.unlock()
// 	}
// }

func (this *Master) collectMapOutOfTime() {
	for{
		go this.mapTaskRunning.GetTimeOutTaskQueue(this.mapTaskCrashed.TaskChan)
		task := <- this.mapTaskCrashed.TaskChan
		this.mapTaskWaiting.TaskChan <- task
		fmt.Println("get a crashed map task")
	}
}

func (this *Master) collectReduceOutOfTime() {
	for{
		go this.reduceTaskRunning.GetTimeOutTaskQueue(this.reduceTaskCrashed.TaskChan)
		task := <- this.reduceTaskCrashed.TaskChan
		this.reduceTaskWaiting.TaskChan <- task
		fmt.Println("get a crashed reduce task")
	}
}


// func Max(x, y int) int {
//     if x < y {
//         return y
//     }
//     return x
// }

