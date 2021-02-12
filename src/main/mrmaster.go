package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"../mr"
)

func main() {
	if len(os.Args) < 2 {
		log.Fatal("Usage: mrmaster [files]")
	}

	master := mr.NewMaster(os.Args[1:], 10)
	go master.Start()

	done := make(chan bool)
	sigc := make(chan os.Signal)

	go func() {
		for {
			if master.IsDone() {
				log.Println("MapReduce tasks finished successfully")
				done <- true
			}

			time.Sleep(time.Second)
		}
	}()

	signal.Notify(sigc, os.Interrupt, syscall.SIGINT, syscall.SIGQUIT, syscall.SIGTERM)
	go func() {
		<-sigc

		log.Println("Terminating process...")
		done <- true

		<-sigc
		close(sigc)
		log.Fatalf("Abort!")
	}()

	<-done
	master.Shutdown()

	log.Println("Master shutdown successfully")
}
