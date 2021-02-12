package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"plugin"
	"sort"

	"../mr"
)

type ByKey []mr.KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

func main() {
	if len(os.Args) < 3 {
		log.Fatal("Usage: mrsequential [plugin] [files]")
	}

	mapFunc, reduceFunc, err := loadPlugin(os.Args[1])
	if err != nil {
		log.Fatalf("Failed to load plugin: %s", err)
	}

	//
	// read each input file,
	// pass it to Map,
	// accumulate the intermediate Map output.
	//
	intermediate := []mr.KeyValue{}
	for _, filename := range os.Args[2:] {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}
		file.Close()
		kva := mapFunc(filename, string(content))
		intermediate = append(intermediate, kva...)
	}

	//
	// a big difference from real MapReduce is that all the
	// intermediate data is in one place, intermediate[],
	// rather than being partitioned into NxM buckets.
	//

	sort.Sort(ByKey(intermediate))

	oname := "mr-out-0"
	ofile, _ := os.Create(oname)

	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-0.
	//
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reduceFunc(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	ofile.Close()
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
