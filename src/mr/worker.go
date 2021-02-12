package mr

import (
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
)

type MapFunc = func(filename string, contents string) []KeyValue
type ReduceFunc = func(key string, values []string) string

type KeyValue struct {
	Key   string
	Value string
}

// Use ihash(key) % NReduce to choose the reduce task number for each KeyValue emitted by Map
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

type Worker struct {
	mapFunc    MapFunc
	reduceFunc ReduceFunc
}

func NewWorker(mapFunc MapFunc, reduceFunc ReduceFunc) *Worker {
	return &Worker{
		mapFunc:    mapFunc,
		reduceFunc: reduceFunc,
	}
}

func (w *Worker) Start() {
	if err := w.CallExample(); err != nil {
		log.Println(err)
	}
}

func (w *Worker) CallExample() error {
	args := ExampleArgs{
		X: 99,
	}

	reply := ExampleReply{}

	if err := w.callRPC("Master.Example", &args, &reply); err != nil {
		return fmt.Errorf("rpc call failed: %w", err)
	}

	fmt.Println("Y:", reply.Y)
	return nil
}

func (w *Worker) callRPC(rpcname string, args interface{}, reply interface{}) error {
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		return err
	}
	defer c.Close()

	if err := c.Call(rpcname, args, reply); err != nil {
		return err
	}

	return nil
}
