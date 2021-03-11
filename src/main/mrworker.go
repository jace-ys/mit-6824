package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"plugin"
	"syscall"

	"../mr"
)

func main() {
	if len(os.Args) != 2 {
		log.Fatal("Usage: mrworker [plugin]")
	}

	log.SetPrefix("[WORKER] ")

	mapFunc, reduceFunc, err := loadPlugin(os.Args[1])
	if err != nil {
		log.Fatalf("Failed to load plugin: %s", err)
	}

	worker, err := mr.NewWorker(mapFunc, reduceFunc)
	if err != nil {
		log.Fatalf("Failed to initialize worker: %s", err)
	}

	done := make(chan bool)
	sigc := make(chan os.Signal, 1)

	go func() {
		if err := worker.Process(); err != nil {
			log.Fatalf("Failed to process tasks: %s", err)
		}

		done <- true
	}()

	signal.Notify(sigc, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigc

		log.Println("Terminating process...")
		done <- true

		<-sigc
		close(sigc)
		log.Println("Abort!")
	}()

	<-done
	log.Println("Worker process finished")
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
