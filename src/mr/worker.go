package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
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
	}

	if err != nil {
		log.Fatal(err)
	}
}

func HandleMapTask(inputFile string, taskID, nReduce int, mapf func(string, string) []KeyValue) error {
	if nReduce <= 0 {
		return fmt.Errorf("number of reduce tasks must be positive: %d", nReduce)
	}

	content, err := os.ReadFile(inputFile)
	if err != nil {
		return fmt.Errorf("read input %q: %w", inputFile, err)
	}

	contentText := string(content)
	intermediate := mapf(inputFile, contentText)

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
