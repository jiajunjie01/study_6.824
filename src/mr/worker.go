package mr

import (
	"sort"
	"path/filepath"
	"io/ioutil"
	"os"
	"time"
	"fmt"
	"log"
	"net/rpc"
	"hash/fnv"
	"strconv"
	"encoding/json"
)


//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}
type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }


//
// main/mrworker.go calls this function.
// this function circulates to call Mater's AskTask
func Worker(mapf func(string, string) []KeyValue,reducef func(string, []string) string) {

	for {
		taskInfo := CallAskTask()
		fmt.Printf("Woker got a task,state = %v , PartIndex = %v , FileIndex = %v\n",taskInfo.State,taskInfo.PartIndex,taskInfo.FileIndex )
		switch taskInfo.State {
		case TaskMap:
			workerMap(mapf, taskInfo)
			break
		case TaskReduce:
			workerReduce(reducef, taskInfo)
			break
		case TaskWait:
			// wait for 5 seconds to requeset again
			time.Sleep(time.Duration(time.Second * 5))
			break
		case TaskEnd:
			fmt.Println("Master all tasks complete. Nothing to do...")
			// exit worker process
			return
		default:
			panic("Invalid Task state received by worker")
		}
	}

}

// this function tell the Master,one Task is done
func CallTaskDone(taskInfo *TaskInfo) {
	reply := ExampleReply{}
	call("Master.TaskDone", taskInfo, &reply)
}

func workerMap(mapf func(string, string) []KeyValue, taskInfo *TaskInfo) {
	fmt.Printf("Got assigned map task on %vth file %v\n", taskInfo.FileIndex, taskInfo.FileName)

	// read in target files as a key-value array
	intermediate := []KeyValue{}
	file, err := os.Open(taskInfo.FileName)
	if err != nil {
		log.Fatalf("cannot open %v", taskInfo.FileName)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", taskInfo.FileName)
	}
	file.Close()
	kva := mapf(taskInfo.FileName, string(content))
	intermediate = append(intermediate, kva...)

	// prepare output files and encoders
	nReduce := taskInfo.NReduce
	outprefix := "mr-tmp/mr-"
	outprefix += strconv.Itoa(taskInfo.FileIndex)
	outprefix += "-"
	outFiles := make([]*os.File, nReduce)
	fileEncs := make([]*json.Encoder, nReduce)
	for outindex := 0; outindex < nReduce; outindex++ {
		//outname := outprefix + strconv.Itoa(outindex)
		//outFiles[outindex], _ = os.Create(outname)
		outFiles[outindex], _ = ioutil.TempFile("mr-tmp", "mr-tmp-*")
		fileEncs[outindex] = json.NewEncoder(outFiles[outindex])
	}

	// distribute keys among mr-fileindex-*
	for _, kv := range intermediate {
		outindex := ihash(kv.Key) % nReduce
		file = outFiles[outindex]
		enc := fileEncs[outindex]
		err := enc.Encode(&kv)
		if err != nil {
			fmt.Printf("File %v Key %v Value %v Error: %v\n", taskInfo.FileName, kv.Key, kv.Value, err)
			panic("Json encode failed")
		}
	}

	// save as files
	for outindex, file := range outFiles {
		outname := outprefix + strconv.Itoa(outindex)
		oldpath := filepath.Join(file.Name())
		//fmt.Printf("temp file oldpath %v\n", oldpath)
		os.Rename(oldpath, outname)
		file.Close()
	}
	// acknowledge master
	CallTaskDone(taskInfo)
}

func workerReduce(reducef func(string, []string) string ,taskInfo *TaskInfo){
	fmt.Printf("Got assigned reduce task on part %v\n", taskInfo.PartIndex)
	outname := "mr-out-" + strconv.Itoa(taskInfo.PartIndex)
	// read from output files from map tasks
	// the name of map task output files is mr-tmp/mr-*-PartIndex
	mapOutPerfix := "mr-tmp/mr-"
	mapOutSuffix := "-" + strconv.Itoa(taskInfo.PartIndex)

	// read in all files as a kv array
	intermediate := []KeyValue{}
	for fileIndex := 0; fileIndex < taskInfo.NFiles ; fileIndex ++{
		inputName := mapOutPerfix + strconv.Itoa(fileIndex) + mapOutSuffix
		file ,err := os.Open(inputName)
		if err != nil {
			fmt.Printf("The reduce task failed to open %v file ,err : %v\n", inputName, err)
			panic("Open File Err")
		}
		decoder := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := decoder.Decode(&kv); err != nil {
				//fmt.Printf("%v\n", err)
				break
			}
			//fmt.Printf("%v\n", kv)
			intermediate = append(intermediate, kv)
		}
		file.Close()
	}
	
	sort.Sort(ByKey(intermediate))
	//ofile, err := os.Create(outname)
	ofile, err := ioutil.TempFile("mr-tmp", "mr-*")
	if err != nil {
		fmt.Printf("Create output file %v failed: %v\n", outname, err)
		panic("Create file error")
	}
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
	os.Rename(filepath.Join(ofile.Name()), outname)
	ofile.Close()
	// acknowledge master
	CallTaskDone(taskInfo)

}


// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}


//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

func CallAskTask() *TaskInfo {
	fmt.Println("CallAskTask begin")
	args := ExampleArgs{}
	reply := TaskInfo{}
	call("Master.AskTask", &args, &reply)
	fmt.Println("CallAskTask end")
	return &reply
}


//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
