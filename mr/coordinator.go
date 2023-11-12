package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"
)

type Coordinator struct {
	NumReduce         int             // Number of reduce tasks
	Files             []string        // Files for map tasks, len(Files) is number of Map tasks
	IntermediateFiles [][]string      // Intermediates
	MapTasks          chan MapTask    // Channel for uncompleted map tasks
	ReduceTasks       chan ReduceTask // Channel for uncompleted reduce tasks
	CompletedTasks    map[string]bool // Map to check if task is completed
	Lock              sync.Mutex      // Lock for contolling shared variables
	IsMap             int
}

// Starting coordinator logic
func (c *Coordinator) Start() {
	fmt.Println("Starting Coordinator, adding Map Tasks to channel")

	// Prepare initial MapTasks and add them to the queue
	i := 1
	c.IsMap = 1
	for _, file := range c.Files {
		mapTask := MapTask{
			Filename:  file,
			NumReduce: c.NumReduce,
			TaskNum:   i,
		}
		i++

		fmt.Println("MapTask", mapTask, "added to channel")

		c.MapTasks <- mapTask
		c.CompletedTasks["map_"+mapTask.Filename] = false
	}

	c.server()
}

func (c *Coordinator) GeneralRequestTask(args *EmptyArgs, reply *Task) error {
	//
	fmt.Println("GeneralRequestTask Start")
	if c.IsMap == 1 {
		fmt.Println("Is Map")
		replyTask := Task{}
		replyTask.ThisMap = <-c.MapTasks
		replyTask.WorkType = 1
		fmt.Println("Map task found,", reply.ThisMap.Filename)
		*reply = replyTask
		go c.WaitForWorker(reply.ThisMap)
		fmt.Println("after wait for worker")
		return nil
	} else if c.IsMap == 2 {
		replyTask := Task{}
		replyTask.ThisReduce = <-c.ReduceTasks
		replyTask.WorkType = 2
		fmt.Println("Reduce task found")
		*reply = replyTask
		return nil
		// go c.WaitForReduceWorker(reply.ThisReduce)
	} else {
		c.IsMap = 0
		return nil
	}
	return nil
	// if statement checking types
	// 1 maptask
	// 2 reduce
	// 0 go to done
}

// RPC that worker calls when idle (worker requests a map task)
// func (c *Coordinator) RequestMapTask(args *EmptyArgs, reply *MapTask) error {
//	fmt.Println("Map task requested")

//	task, _ := <-c.MapTasks // check if there are uncompleted map tasks. Keep in mind, if MapTasks is empty, this will halt

//MAYBE CHECK IF MAPTASKS and channel IS EMPTY

// if len(c.MapTasks == 0) & len(Files == 0) {
//maybe dont do this in request map task
//CHANGE BOOLEAN
// } else {

//	fmt.Println("Map task found,", task.Filename)
//	*reply = task

//	go c.WaitForWorker(task)
//
//	return nil
// }

//func (c *Coordinator) RequestReduceTask(args *EmptyArs, reply *ReduceTask) error {
//	fmt.Println("Reduce task requested")

//	task, _ := <-c.ReduceTasks

//	fmt.Println("Map task found,", task.Filename)
//	*reply = task

//	go c.WaitForWorker(task)

//	return nil
// }

// Goroutine will wait 10 seconds and check if map task is completed or not
func (c *Coordinator) WaitForWorker(task MapTask) {
	time.Sleep(time.Second * 10)
	c.Lock.Lock()
	if c.CompletedTasks["map_"+task.Filename] == false {
		fmt.Println("Timer expired, task", task.Filename, "is not finished. Putting back in queue.")
		c.MapTasks <- task
	}
	c.Lock.Unlock()
}

func (c *Coordinator) WaitForReduceWorker(task ReduceTask) {
	time.Sleep(time.Second * 10)
	c.Lock.Lock()
	concat := fmt.Sprint("reduce_", task.Bucket)
	if c.CompletedTasks[concat] == false {
		fmt.Println("Timer expired, task", task.Bucket, "is not finished. Putting back in queue.")
		c.ReduceTasks <- task
	}
	c.Lock.Unlock()
}

func (c *Coordinator) MakeReduceTasks(numReduce int) {
	for i := 0; i < numReduce; i++ {
		reduceTask := ReduceTask{}
		for j := 1; j <= len(c.Files); j++ {
			reduceTask.Files = append(reduceTask.Files, "mr-worker-"+strconv.Itoa(j)+"-"+strconv.Itoa(i)+".json")
			c.CompletedTasks["reduce_"+strconv.Itoa(i)] = false

		}
		c.ReduceTasks <- reduceTask
	}
}

// RPC for reporting a completion of a task
func (c *Coordinator) TaskCompleted(args *MapTask, reply *EmptyReply) error {
	c.Lock.Lock()
	defer c.Lock.Unlock()

	c.CompletedTasks["map_"+args.Filename] = true

	// fmt.Println("Task", args, "completed")

	println("test after complete")
	for element := range c.CompletedTasks {
		if c.CompletedTasks[element] == false {
			return nil
		}
	}

	c.IsMap = 2
	c.MakeReduceTasks(args.NumReduce)

	return nil

}

// If all of map tasks are completed, go to reduce phase
// ...
//CODE HERE, UNSURE IF RIGHT

//	if len(c.MapTasks == 0) & len(Files == 0) {
//
// boolean to check if its time for reduce
// }
// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	//ret := false

	// Your code here.
	return c.IsMap == 3
	// Final part, making sure it exitted when it is supposed to

	//return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		NumReduce:      nReduce,
		Files:          files,
		MapTasks:       make(chan MapTask, 100),
		ReduceTasks:    make(chan ReduceTask, 100),
		CompletedTasks: make(map[string]bool),
		IsMap:          0,
	}

	fmt.Println("Starting coordinator")

	c.Start()

	return &c
}
