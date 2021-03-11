package mr

import (
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"time"
)

type MapFunc = func(filename string, contents string) []KeyValue
type ReduceFunc = func(key string, values []string) string

type KeyValue struct {
	Key   string
	Value string
}

type Worker struct {
	id         int
	mapFunc    MapFunc
	reduceFunc ReduceFunc
}

func NewWorker(mapFunc MapFunc, reduceFunc ReduceFunc) (*Worker, error) {
	worker := &Worker{
		mapFunc:    mapFunc,
		reduceFunc: reduceFunc,
	}

	args := RegisterArgs{}
	reply := RegisterReply{}

	if err := worker.callRPC("Master.Register", &args, &reply); err != nil {
		return nil, fmt.Errorf("Register RPC failed: %w", err)
	}

	worker.id = reply.WorkerID
	log.SetPrefix(fmt.Sprintf("[WORKER %d] ", worker.id))

	return worker, nil
}

func (w *Worker) Process() error {
	for {
		task, err := w.GetTask()
		if err != nil {
			return err
		}

		switch task.Op {
		case OpMap:
			log.Printf("Assigned Map task %d: %s", task.TaskID, task.Filename)

			done := make(chan bool)

			go func() {
				if err := w.doMap(task.TaskID, task.Filename, task.Buckets); err != nil {
					log.Println(err)
					return
				}

				done <- true
			}()

			select {
			case <-time.After(time.Until(task.LeaseExpiry)):
				log.Printf("Lease on Map task %d expired; aborting", task.TaskID)
			case <-done:
				if err := w.TaskDone(task.TaskID, task.Op); err != nil {
					log.Println(err)
				}
			}

		case OpReduce:
			log.Printf("Assigned Reduce task %d", task.TaskID)

			done := make(chan bool)

			go func() {
				if err := w.doReduce(task.TaskID); err != nil {
					log.Println(err)
					return
				}

				done <- true
			}()

			select {
			case <-time.After(time.Until(task.LeaseExpiry)):
				log.Printf("Lease on Reduce task %d expired; aborting", task.TaskID)
			case <-done:
				if err := w.TaskDone(task.TaskID, task.Op); err != nil {
					log.Println(err)
				}
			}

		case OpWait:
			time.Sleep(time.Second)
		case OpShutdown:
			return nil
		}
	}
}

func (w *Worker) GetTask() (*GetTaskReply, error) {
	args := GetTaskArgs{
		WorkerID: w.id,
	}

	reply := GetTaskReply{}

	if err := w.callRPC("Master.GetTask", &args, &reply); err != nil {
		return nil, fmt.Errorf("GetTask RPC failed: %w", err)
	}

	return &reply, nil
}

func (w *Worker) TaskDone(id int, op Operation) error {
	args := TaskDoneArgs{
		WorkerID: w.id,
		TaskID:   id,
		Op:       op,
	}

	reply := TaskDoneReply{}

	if err := w.callRPC("Master.TaskDone", &args, &reply); err != nil {
		return fmt.Errorf("TaskDone RPC failed: %w", err)
	}

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

func (w *Worker) doMap(id int, filename string, buckets int) error {
	contents, err := ioutil.ReadFile(filename)
	if err != nil {
		return fmt.Errorf("failed to read file %s: %w", filename, err)
	}

	kvs := w.mapFunc(filename, string(contents))

	bucketMap := make(map[int][]KeyValue)
	for _, kv := range kvs {
		bucket := ihash(kv.Key) % buckets
		bucketMap[bucket] = append(bucketMap[bucket], kv)
	}

	for bucket, kvs := range bucketMap {
		outfile := fmt.Sprintf("mr-%d-%d", id, bucket)

		tmpfile, err := ioutil.TempFile("", outfile)
		if err != nil {
			return fmt.Errorf("failed to create temp file %s: %w", outfile, err)
		}

		enc := json.NewEncoder(tmpfile)
		if err := enc.Encode(&kvs); err != nil {
			return fmt.Errorf("failed to encode file %s: %w", outfile, err)
		}
		if err := tmpfile.Close(); err != nil {
			return fmt.Errorf("failed to close temp file %s: %w", tmpfile.Name(), err)
		}

		if err := os.Rename(tmpfile.Name(), outfile); err != nil {
			return fmt.Errorf("failed to rename temp file %s: %w", tmpfile.Name(), err)
		}
	}

	return nil
}

func (w *Worker) doReduce(id int) error {
	filenames, err := filepath.Glob(fmt.Sprintf("mr-[0-9]*-%d", id))
	if err != nil {
		return fmt.Errorf("failed to glob files: %w", err)
	}

	kvMap := make(map[string][]string)
	for _, filename := range filenames {
		file, err := os.Open(filename)
		if err != nil {
			return fmt.Errorf("failed to open file %s: %w", filename, err)
		}

		var kvs []KeyValue
		if err := json.NewDecoder(file).Decode(&kvs); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}

			return fmt.Errorf("failed to decode file %s: %w", file.Name(), err)
		}

		if err := file.Close(); err != nil {
			return fmt.Errorf("failed to close file %s: %w", file.Name(), err)
		}

		for _, kv := range kvs {
			kvMap[kv.Key] = append(kvMap[kv.Key], kv.Value)
		}
	}

	outfile := fmt.Sprintf("mr-out-%d", id)

	tmpfile, err := ioutil.TempFile("", outfile)
	if err != nil {
		return fmt.Errorf("failed to create temp file %s: %w", tmpfile.Name(), err)
	}

	for k, v := range kvMap {
		result := w.reduceFunc(k, v)
		if _, err := fmt.Fprintf(tmpfile, "%v %v\n", k, result); err != nil {
			return fmt.Errorf("failed to write to file %s: %w", tmpfile.Name(), err)
		}
	}
	if err := tmpfile.Close(); err != nil {
		return fmt.Errorf("failed to close temp file %s: %w", tmpfile.Name(), err)
	}

	if err := os.Rename(tmpfile.Name(), outfile); err != nil {
		return fmt.Errorf("failed to rename temp file %s: %w", tmpfile.Name(), err)
	}

	// Prune intermediate files from Map tasks
	for _, filename := range filenames {
		os.Remove(filename)
	}

	return nil
}

func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}
