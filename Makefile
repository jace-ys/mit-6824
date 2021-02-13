.PHONY: clean lab1

clean:
	go clean ./...
	rm -rf bin tmp
	rm -f mr-*-* mr-out-* 824-mr-*

lab1:
	mkdir -p bin/plugins
	go build -race -o bin ./src/main/mrmaster.go
	go build -race -o bin ./src/main/mrworker.go
	go build -race -o bin ./src/main/mrsequential.go
	go build -race -buildmode=plugin -o bin/plugins ./src/mrapps/wc.go
	go build -race -buildmode=plugin -o bin/plugins ./src/mrapps/indexer.go
	go build -race -buildmode=plugin -o bin/plugins ./src/mrapps/mtiming.go
	go build -race -buildmode=plugin -o bin/plugins ./src/mrapps/rtiming.go
	go build -race -buildmode=plugin -o bin/plugins ./src/mrapps/crash.go
	go build -race -buildmode=plugin -o bin/plugins ./src/mrapps/nocrash.go
