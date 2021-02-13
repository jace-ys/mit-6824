package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"sync"
	"time"
)

const LeaseDuration = 10 * time.Second

type Phase int

const (
	PhaseMap Phase = iota
	PhaseReduce
	PhaseDone
)

type State int

const (
	StatePending State = iota
	StateStarted
	StateFinished
)

type Master struct {
	srv     http.Server
	files   []string
	nReduce int
	workers int

	phase Phase
	tasks map[int]*Lease
	mu    sync.Mutex
}

type Lease struct {
	workerID int
	expiry   time.Time
	state    State
}

// RPC handlers
func (m *Master) Register(args *RegisterArgs, reply *RegisterReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.workers++
	reply.WorkerID = m.workers

	return nil
}

func (m *Master) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.phase == PhaseDone {
		reply.Op = OpShutdown
		return nil
	}

	for id, lease := range m.tasks {
		if lease.state == StatePending {
			reply.TaskID = id
			reply.LeaseExpiry = time.Now().Add(LeaseDuration)

			if m.phase == PhaseMap {
				reply.Op = OpMap
				reply.Filename = m.files[id]
				reply.Buckets = m.nReduce
			} else {
				reply.Op = OpReduce
			}

			m.tasks[id] = &Lease{
				workerID: args.WorkerID,
				expiry:   reply.LeaseExpiry,
				state:    StateStarted,
			}

			// Reap lease if task not finished in 10 seconds
			time.AfterFunc(time.Until(reply.LeaseExpiry), func() {
				m.mu.Lock()
				defer m.mu.Unlock()

				if m.tasks[reply.TaskID].state < StateFinished {
					log.Printf("Lease on %s task %d expired; reaped from worker %d", reply.Op, reply.TaskID, args.WorkerID)
					m.tasks[reply.TaskID] = &Lease{}
				}
			})

			return nil
		}
	}

	// No available tasks now so signal worker to check back later
	reply.Op = OpWait
	return nil
}

func (m *Master) TaskDone(args *TaskDoneArgs, reply *TaskDoneReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if args.Op != OpMap && args.Op != OpReduce {
		return fmt.Errorf("invalid operation: %v", args.Op)
	}

	if m.tasks[args.TaskID].workerID != args.WorkerID {
		return fmt.Errorf("does not own the lease on %s task %d", args.Op, args.TaskID)
	}

	if time.Since(m.tasks[args.TaskID].expiry) > 0 {
		return fmt.Errorf("lease on %s task %d expired", args.Op, args.TaskID)
	}

	m.tasks[args.TaskID].state = StateFinished

	done := true
	for _, lease := range m.tasks {
		if lease.state != StateFinished {
			done = false
			break
		}
	}

	if done {
		m.phase++

		if m.phase == PhaseReduce {
			// Initialize Reduce tasks
			m.tasks = make(map[int]*Lease)
			for i := 0; i < m.nReduce; i++ {
				m.tasks[i] = &Lease{}
			}
		}
	}

	return nil
}

func NewMaster(files []string, nReduce int) *Master {
	master := &Master{
		srv:     http.Server{},
		files:   files,
		nReduce: nReduce,
		phase:   PhaseMap,
		tasks:   make(map[int]*Lease),
	}

	// Initialize Map tasks
	for i := range files {
		master.tasks[i] = &Lease{}
	}

	return master
}

func (m *Master) Start() error {
	rpc.Register(m)
	rpc.HandleHTTP()

	sockname := masterSock()
	l, err := net.Listen("unix", sockname)
	if err != nil {
		return fmt.Errorf("listen failed: %w", err)
	}
	defer l.Close()

	return m.srv.Serve(l)
}

func (m *Master) Shutdown() error {
	return m.srv.Close()
}

func (m *Master) IsDone() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.phase == PhaseDone
}
