package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {

	// Your worker implementation here.
	workerId := -1
	lastDone := false
	for {
		args := RequireTaskArgs{
			WorkerId: workerId,
			LastDone: lastDone,
		}
		reply := RequireTaskReply{}
		if !call("Coordinator.RequireTask", &args, &reply) {
			panic("oops error occurs in RPC")
		}
		workerId = reply.WorkerId
		lastDone = false

		if reply.MapTask != nil {
			task := reply.MapTask
			contents, err := ioutil.ReadFile(task.FilePath)
			if err != nil {
				panic(err)
			}
			kvResult := mapf(task.FilePath, string(contents))

			kvBuckets := make([][]KeyValue, task.NumReduce)
			for _, kv := range kvResult {
				bucket := ihash(kv.Key) % task.NumReduce
				kvBuckets[bucket] = append(kvBuckets[bucket], kv)
			}
			for i, kva := range kvBuckets {
				err := dumpKeyValue(fmt.Sprintf("mr-%d-%d", task.MapTaskId, i), kva)
				if err != nil {
					panic(err)
				}
			}
			lastDone = true
		} else if reply.ReduceTask != nil {
			task := reply.ReduceTask
			var intermediate []KeyValue
			for i := 0; i < task.NumMap; i++ {
				var err error
				intermediate, err = readKeyValue(fmt.Sprintf("mr-%d-%d", i, task.ReduceTaskId), intermediate)
				if err != nil {
					panic(err)
				}
			}
			sort.Sort(ByKey(intermediate))

			file, err := os.Create(fmt.Sprintf("mr-out-%d", task.ReduceTaskId))
			if err != nil {
				panic(err)
			}
			i := 0
			for i < len(intermediate) {
				j := i + 1
				for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
					j++
				}
				var values []string
				for k := i; k < j; k++ {
					values = append(values, intermediate[k].Value)
				}
				output := reducef(intermediate[i].Key, values)

				// this is the correct format for each line of Reduce output.
				fmt.Fprintf(file, "%v %v\n", intermediate[i].Key, output)

				i = j
			}

			file.Close()
			lastDone = true
		} else if reply.WaitTask {
			time.Sleep(time.Second)
			lastDone = false
		} else {
			os.Exit(0)
		}
	}
}

func dumpKeyValue(filePath string, kva []KeyValue) error {
	file, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer file.Close()
	enc := json.NewEncoder(file)
	for _, kv := range kva {
		err := enc.Encode(&kv)
		if err != nil {
			return err
		}
	}
	return nil
}

func readKeyValue(filePath string, kva []KeyValue) ([]KeyValue, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	dec := json.NewDecoder(file)
	for {
		var kv KeyValue
		if err := dec.Decode(&kv); err != nil {
			break
		}
		kva = append(kva, kv)
	}
	return kva, nil
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
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
