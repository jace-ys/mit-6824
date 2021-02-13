package mr

import (
	"os"
	"strconv"
	"time"
)

type Operation string

const (
	OpWait     Operation = "Wait"
	OpMap                = "Map"
	OpReduce             = "Reduce"
	OpShutdown           = "Shutdown"
)

// RPC definitions
type RegisterArgs struct {
}

type RegisterReply struct {
	WorkerID int
}

type GetTaskArgs struct {
	WorkerID int
}

type GetTaskReply struct {
	TaskID      int
	Op          Operation
	LeaseExpiry time.Time
	Filename    string
	Buckets     int
}

type TaskDoneArgs struct {
	WorkerID int
	TaskID   int
	Op       Operation
}

type TaskDoneReply struct {
}

// Cook up a unique-ish UNIX-domain socket name in the local directory
func masterSock() string {
	s := "824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
