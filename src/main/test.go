package main

import (
	"fmt"
	//"strconv"
	"sync"
	"time"
	//"6.824-golabs-2020/src/raft"
)
//import "6.824-golabs-2020/src/mr"
// import "plugin"
// import "log"

// import "plugin"
// import "log"
// import "reflect"

// type t interface {
// 	test() int
// }

// type t1 struct{

// }

// func (this *t1) test() int{
// 	return 0
// }

type test interface{
	execute(args ... int)
}

type yi struct{

}
func (y yi)execute(args ... int){
	fmt.Println("execute")
}


type task struct{

}

type task2 struct{
	task
	t int
}

func (t task)execute(args ... int){
	fmt.Println("execute")
}



func (t task2)execute2(){
	fmt.Println("execute2")
}

func mo(args ...int){
	fmt.Println("access mo")
	func(){
		fmt.Println("run function")
		defer fmt.Println("defer")
		fmt.Println("return")
		return
	}()
	fmt.Println("mo end")

}

type task3 struct{
	t int
	sshot []byte
}

func main(){
	t := task3{
		t:1,
	}
	if t.sshot == nil{
		fmt.Println("yes")
	}
	
	time.Sleep(500 * time.Millisecond)

	// a := [5]int{1, 4, 3, 4, 5}
	// a[2]= a[2]/ 2
	
	// fmt.Println(strconv.Itoa(a[2]))
	// a[1]/= 2
	// fmt.Println(strconv.Itoa(a[1]))
	// // leaders := make(map[int][]int)
	// // leaders[0] = append(leaders[0], 10)

	// // fmt.Println(strconv.Itoa(leaders[0][0]))
	// //rf := &raft.Raft{}
	// // a := make([]raft.RaftLog,1)

	// // a[0].Term =-1
	// // a[0].Command = nil



	
	// // go func(val_a *test_1){
	// // 	val_a.cond.L.Lock()
	// // 	for val_a.a == false{
	// // 		fmt.Println("等待....")
	// // 		val_a.cond.Wait()	
	// // 	}
	// // 	val_a.cond.L.Unlock()
	// // 	fmt.Println("等待完成")
	// // }(&b)

	// // time.Sleep(time.Duration(2)*time.Second)

	// go func(val_a *test_1){
	// 	val_a.cond.L.Lock()
	// 	if val_a.a{
	// 		panic("Task done twice...")
	// 	}
	// 	val_a.a =true
	// 	val_a.cond.L.Unlock()
	// 	val_a.cond.Broadcast()
	// 	fmt.Println("set done 完成")
	// }(&b)

	// time.Sleep(time.Duration(2)*time.Second)
	// fmt.Println("完成")
}
type test_1 struct{
	a bool
	cond  *sync.Cond
}




// func loadPlugin(filename string) (func(string, string) []mr.KeyValue, func(string, []string) string) {
// 	p, err := plugin.Open(filename)
// 	if err != nil {
// 		log.Fatalf("cannot load plugin %v", filename)
// 	}
// 	xmapf, err := p.Lookup("Map")
// 	if err != nil {
// 		log.Fatalf("cannot find Map in %v", filename)
// 	}
// 	mapf := xmapf.(func(string, string) []mr.KeyValue)
// 	xreducef, err := p.Lookup("Reduce")
// 	if err != nil {
// 		log.Fatalf("cannot find Reduce in %v", filename)
// 	}
// 	reducef := xreducef.(func(string, []string) string)

// 	return mapf, reducef
// }
