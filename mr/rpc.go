package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

type EmptyArgs struct {
}

type EmptyReply struct {
}



// Universal Task structure

type Task struct {
	ThisMap MapTask
	ThisReduce ReduceTask
	WorkType int // 0 for nothing, 1 for map, 2 for reduce, 3 for exit
}

type MapTask struct {
	Filename  string // Filename = key
	NumReduce int    // Number of reduce tasks, used to figure out number of buckets
	TaskNum   int
}

type ReduceTask struct {
	Files []string   // Number of reduce tasks, used to figure out number of buckets
	Bucket   int
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
