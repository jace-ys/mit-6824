package main

import (
	"fmt"
	"log"
	"os"
	"plugin"

	"../mr"
)

func main() {
	if len(os.Args) != 2 {
		log.Fatal("Usage: mrworker [plugin]")
	}

	mapFunc, reduceFunc, err := loadPlugin(os.Args[1])
	if err != nil {
		log.Fatalf("Failed to load plugin: %s", err)
	}

	worker := mr.NewWorker(mapFunc, reduceFunc)
	worker.Start()
}

// Load a plugin's Map and Reduce functions
func loadPlugin(filename string) (mr.MapFunc, mr.ReduceFunc, error) {
	p, err := plugin.Open(filename)
	if err != nil {
		return nil, nil, err
	}

	m, err := p.Lookup("Map")
	if err != nil {
		return nil, nil, err
	}
	mapFunc, ok := m.(mr.MapFunc)
	if !ok {
		return nil, nil, fmt.Errorf("invalid MapFunc: %v", m)
	}

	r, err := p.Lookup("Reduce")
	if err != nil {
		return nil, nil, err
	}
	reduceFunc, ok := r.(mr.ReduceFunc)
	if !ok {
		return nil, nil, fmt.Errorf("invalid ReduceFunc: %v", m)
	}

	return mapFunc, reduceFunc, nil
}
