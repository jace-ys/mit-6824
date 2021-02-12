package mr

import (
	"os"
	"strconv"
)

// RPC definitions
type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Cook up a unique-ish UNIX-domain socket name in the local directory
func masterSock() string {
	s := "824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
