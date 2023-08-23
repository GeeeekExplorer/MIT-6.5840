package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"
)

const (
	UnAlloc int = iota
	Alloc
	Finish
)

const (
	Mapping int = iota
	Reducing
	Finished
)

type Coordinator struct {
	// Your definitions here.
	mu			sync.Mutex
	phase		int
	task_id		int  // global unique id
	finish_num	int
	map_tasks	map[string]int
	red_tasks	map[string]int
	map_times	map[string]time.Time
	red_times	map[string]time.Time
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) Apply(args *ApplyArgs, reply *ApplyReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	reply.Nreduce = len(c.red_tasks)
	reply.Phase = c.phase
	now := time.Now()
	if c.phase == Mapping {
		for k, v := range c.map_tasks {
			if v == UnAlloc || v == Alloc && now.Sub(c.map_times[k]) > 11 * time.Second {
				reply.Role = "map"
				reply.File = k
				reply.Task_id = c.task_id
				c.task_id++
				c.map_tasks[k] = Alloc
				c.map_times[k] = now
				break
			}
		}
	} else if c.phase == Reducing {
		for k, v := range c.red_tasks {
			if v == UnAlloc || v == Alloc && now.Sub(c.red_times[k]) > 11 * time.Second {
				reply.Role = "reduce"
				reply.File = k
				reply.Task_id = c.task_id
				c.task_id++
				c.red_tasks[k] = Alloc
				c.red_times[k] = now
				break
			}
		}
	}
	return nil
}

func (c *Coordinator) Complete(args *CompleteArgs, reply *CompleteReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if (c.phase == Reducing && args.Role == "reduce" && c.red_tasks[args.File] == Alloc) {
		c.red_tasks[args.File] = Finish
		c.finish_num++
		if c.finish_num == len(c.red_tasks) {
			c.phase = Finished
			c.finish_num = 0
		}
	}
	if (c.phase == Mapping && args.Role == "map" && c.map_tasks[args.File] == Alloc) {
		c.map_tasks[args.File] = Finish
		c.finish_num++
		if c.finish_num == len(c.map_tasks) {
			c.phase = Reducing
			c.finish_num = 0
		}
	}
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	c.mu.Lock()
	ret = c.phase == Finished
	c.mu.Unlock()

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.phase = Mapping
	c.task_id = 0
	c.finish_num = 0
	c.map_tasks = make(map[string]int)
	c.red_tasks = make(map[string]int)
	c.map_times = make(map[string]time.Time)
	c.red_times = make(map[string]time.Time)
	for _, file := range files {
		c.map_tasks[file] = UnAlloc
	}
	for i := 0; i < nReduce; i++ {
		c.red_tasks[strconv.Itoa(i)] = UnAlloc
	}

	c.server()
	return &c
}
