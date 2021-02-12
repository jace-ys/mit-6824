package mr

import (
	"fmt"
	"net"
	"net/http"
	"net/rpc"
)

type Master struct {
	srv   http.Server
	count int
}

// RPC handlers
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func NewMaster(files []string, nReduce int) *Master {
	return &Master{
		srv:   http.Server{},
		count: 0,
	}
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
	m.count++
	return m.count > 5
}
