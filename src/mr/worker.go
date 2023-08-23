package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"strings"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func mapping(reply *ApplyReply, mapf func(string, string) []KeyValue) {
	const root = "/tmp/mr"
	filename := reply.File
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	kva := mapf(filename, string(content))

	n := reply.Nreduce
	data := make([][]KeyValue, n)
	for _, kv := range kva {
		i := ihash(kv.Key) % n
		data[i] = append(data[i], kv)
	}
	
	for i := 0; i < n; i++ {
		filename = fmt.Sprintf("mr-%d-%d", reply.Task_id, i)
		file, err := os.CreateTemp(root, filename)
		if err != nil {
			log.Fatalf("cannot create %v", filename)
		}
		enc := json.NewEncoder(file)
		for _, kv := range data[i] {
			if err := enc.Encode(&kv); err != nil {
				log.Fatalf("cannot encode %v", kv)
			}
		}
		os.Rename(file.Name(), filepath.Join(root, filename))
	}

	CallComplete(reply.Role, reply.File)
}

func reducing(reply *ApplyReply, reducef func(string, []string) string) {
	const root = "/tmp/mr"
	suffix := reply.File
	var files []string
	entries, err := os.ReadDir(root)
	if err != nil {
		log.Fatalf("cannot readdir %v", root)
	}
	for _, entry := range entries {
		filename := entry.Name()
		if !entry.IsDir() && len(filename) < 10 && strings.HasSuffix(filename, suffix) {
			files = append(files, filepath.Join(root, filename))
		}
	}

	data := make(map[string][]string)
	var kv KeyValue
	for _, filename := range files {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		dec := json.NewDecoder(file)
		for {
			if err := dec.Decode(&kv); err != nil {
				break
			}
			data[kv.Key] = append(data[kv.Key], kv.Value)
		}
		file.Close()
	}

	filename := "mr-out-" + suffix
	file, err := os.Create(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	for k, va := range data {
		v := reducef(k, va)
		file.WriteString(fmt.Sprintf("%v %v\n", k, v))
	}
	file.Close()

	for _, filename := range files {
		os.Remove(filename)
	}

	CallComplete(reply.Role, reply.File)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	var reply *ApplyReply
	ok := true
	for {
		reply, _ = CallApply()
		if reply.Phase != Mapping {
			break
		}
		if !ok {
			return
		}

		if reply.Role == "map" {
			mapping(reply, mapf)
		} else if reply.Role == "none" {
			time.Sleep(time.Second / 2)
		}
	}

	for {
		if reply.Role == "reduce" {
			reducing(reply, reducef)
		} else if reply.Role == "none" {
			time.Sleep(time.Second / 2)
		}

		reply, ok = CallApply()
		if reply.Phase != Reducing {
			break
		}
		if !ok {
			return
		}
	}
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
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

func CallApply() (*ApplyReply, bool) {
	args := ApplyArgs{}
	reply := ApplyReply{Role: "none"}
	ok := call("Coordinator.Apply", &args, &reply)
	return &reply, ok
}

func CallComplete(role string, file string) bool {
	args := CompleteArgs{role, file}
	reply := CompleteReply{}
	ok := call("Coordinator.Complete", &args, &reply)
	return ok
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
