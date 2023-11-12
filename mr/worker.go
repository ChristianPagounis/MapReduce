package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

type WorkerSt struct {
	mapf    func(string, string) []KeyValue
	reducef func(string, []string) string
}

// func (w *WorkerSt) GeneralRequestTask() {
// 	for {
// 		args := EmptyArgs{}
// 		reply := Task{}
// 		call("Coordinator.GeneralRequestTask", &args, &reply)

// 		if reply.WorkType == 1 {
// 			w.Mapper(reply.ThisMap)
// 		} else if reply.WorkType == 2 {
// 			break
// 			w.Reducer(reply.ThisReduce)
// 		} else {
// 			break
// 		}
// 		time.Sleep(1 * time.Second)
// 	}
// }

// main/mrworker.go calls this function.
// func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
// 	w := WorkerSt{
// 		mapf:    mapf,
// 		reducef: reducef,
// 	}

// 	//SAYS TO SEND A RPC TO COORDINATOR ASKING FOR A TASK

// 	w.GeneralRequestTask()
// 	//break out of Map Task, go to reduce task
// 	// w.RequestReduceTask()

// }

// Requests map task, tries to do it, and repeats
func (w *WorkerSt) Mapper(reply MapTask) {
	// println("mapper reached")

	file, err := os.Open(reply.Filename)
	if err != nil {
		log.Fatalf("cannot open %v", reply.Filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", reply.Filename)
	}
	file.Close()

	kva := w.mapf(reply.Filename, string(content))

	sort.Sort(ByKey(kva))

	// {"hello": "1", "world": "1"}

	for _, kv := range kva {
		index := ihash(kv.Key) % reply.NumReduce
		x := strconv.Itoa(reply.TaskNum)
		y := strconv.Itoa(index)
		intername := "mr-worker-" + x + "-" + y
		intermediate_file, err := os.OpenFile(intername, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			log.Fatal("Map: failed to open file %v: %v", intermediate_file, err)
		}
		enc := json.NewEncoder(intermediate_file)
		encerror := enc.Encode(&kv)
		if encerror != nil {
			panic(encerror)
		}
	}

	//CODE WRITTEN BY ME ENDS
	// fmt.Println("Map task for", reply.Filename, "completed")
	// fmt.Println(kva)

	emptyReply := EmptyReply{}
	call("Coordinator.TaskCompleted", &reply, &emptyReply)
}

// func (w *WorkerSt) Reducer(reply ReduceTask) {

// 	dec := json.NewDecoder(reply.Bucket)
// 	for {
// 		var kv KeyValue
// 		if err := dec.Decode(&kv); err != nil {
// 			break
// 		}
// 		kva := append(kva, kv)
// 	}
// 	for z := range reply.Bucket {

// 	}
// 	i := 0
// 	for i < len(reply.Bucket) {
// 		j := i + 1
// 		for j < len(reply.Bucket) && reply.Bucket[j].Key == reply.Bucket[i].Key {
// 			j++
// 		}
// 		values := []string{}
// 		for k := i; k < j; k++ {
// 			values = append(values, reply.Bucket[k].Value)
// 		}
// 		output := reducef(reply.Bucket[i].Key, values)

// 		// this is the correct format for each line of Reduce output.
// 		fmt.Fprintf(ofile, "%v %v\n", reply.Bucket[i].Key, output)

// 		i = j
// 	}
// }

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
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
