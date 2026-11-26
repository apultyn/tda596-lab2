package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	args := RequestTaskArgs{}
	reply := RequestTaskReply{}

	ok := call("Coordinator.GetTask", &args, &reply)
	if !ok {
		log.Fatalf("Requesting task failed!")
	}

	var err error
	switch reply.TaskType {
	case MapTask:
		err = HandleMapTask(reply.Filename, reply.TaskID, reply.NReduce, mapf)
	case ReduceTask:
		err = HandleReduceTask(reply.TaskID, reply.NMap, reducef)
	}

	if err != nil {
		log.Fatal(err)
	}
}

func HandleMapTask(inputFileName string, taskID, nReduce int, mapf func(string, string) []KeyValue) error {
	if nReduce <= 0 {
		return fmt.Errorf("number of reduce tasks must be positive: %d", nReduce)
	}

	content, err := os.ReadFile(inputFileName)
	if err != nil {
		return fmt.Errorf("read input %q: %w", inputFileName, err)
	}

	contentText := string(content)
	intermediate := mapf(inputFileName, contentText)

	buckets := make([][]KeyValue, nReduce)
	for _, kv := range intermediate {
		partition := ihash(kv.Key) % nReduce
		buckets[partition] = append(buckets[partition], kv)
	}

	for partition, bucket := range buckets {
		tmpFile, err := os.CreateTemp(".", "mr-map-*.tmp")
		if err != nil {
			return fmt.Errorf("create temporary map output: %w", err)
		}
		defer os.Remove(tmpFile.Name())
		defer tmpFile.Close()

		enc := json.NewEncoder(tmpFile)
		for _, kv := range bucket {
			if err := enc.Encode(&kv); err != nil {
				return fmt.Errorf("encode map partition %d: %w", partition, err)
			}
		}

		if err := tmpFile.Close(); err != nil {
			return fmt.Errorf("close map partition %d: %w", partition, err)
		}

		outputFileName := fmt.Sprintf("mr-%d-%d", taskID, partition)
		if err := os.Rename(tmpFile.Name(), outputFileName); err != nil {
			return fmt.Errorf("publish map output %q: %w", outputFileName, err)
		}
	}
	return nil
}

func HandleReduceTask(taskID, nMap int, reducef func(string, []string) string) error {
	var intermediate []KeyValue

	for i := 0; i < nMap; i++ {
		inputFileName := fmt.Sprintf("mr-%d-%d", i, taskID)
		inputFile, err := os.Open(inputFileName)
		if err != nil {
			return fmt.Errorf("read input file %s:  %w", inputFileName, err)
		}
		defer inputFile.Close()

		dec := json.NewDecoder(inputFile)
		j := 0
		for {
			var kv KeyValue
			err := dec.Decode(&kv)
			if err == io.EOF {
				break
			}
			if err != nil {
				return fmt.Errorf("decoding data in file %s, line %d: %w", inputFileName, j, err)
			}
			intermediate = append(intermediate, kv)
			j++
		}
		inputFile.Close()
	}

	sort.Slice(intermediate, func(i, j int) bool {
		return intermediate[i].Key < intermediate[j].Key
	})

	tmpFile, err := os.CreateTemp(".", "mr-reduce-*.tmp")
	if err != nil {
		return fmt.Errorf("create temporary reduce output: %w", err)
	}
	defer os.Remove(tmpFile.Name())
	defer tmpFile.Close()

	j := 0
	for j < len(intermediate) {
		k := j + 1
		for k < len(intermediate) && intermediate[k].Key == intermediate[j].Key {
			k++
		}

		values := []string{}
		for l := j; l < k; l++ {
			values = append(values, intermediate[l].Value)
		}

		output := reducef(intermediate[j].Key, values)

		if _, err := fmt.Fprintf(tmpFile, "%v %v\n", intermediate[j].Key, output); err != nil {
			return err
		}

		j = k
	}

	if err := tmpFile.Close(); err != nil {
		return fmt.Errorf("close reduce file: %w", err)
	}

	outputFileName := fmt.Sprintf("mr-out-%d", taskID)
	if err := os.Rename(tmpFile.Name(), outputFileName); err != nil {
		return fmt.Errorf("publish reduce output %q: %w", outputFileName, err)
	}

	return nil
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
