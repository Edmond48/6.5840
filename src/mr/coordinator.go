package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"time"
	"sync"
)

type Coordinator struct {
	nWorkers int
	taskQueue chan Task
	waiting []bool
	mu sync.Mutex
	done sync.WaitGroup
}

// RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) RegisterWorker(args Empty, reply *int) error {
	log.Printf("CDNT:Registed worker W%v", c.nWorkers)
	*reply = c.nWorkers
	c.nWorkers += 1
	c.mu.Lock()
	if c.nWorkers > cap(c.waiting) {
		for c.nWorkers > cap(c.waiting) {
			c.waiting = append(c.waiting, false)
		}
	}
	c.mu.Unlock()
	return nil
}

func (c *Coordinator) RequestTask(id int, reply *Task) error {
	task := <- c.taskQueue
	reply.ttype = task.ttype
	reply.input = task.input

	c.done.Add(1)
	defer c.startWait(id, task)

	c.mu.Lock()
	c.waiting[id] = true
	c.mu.Unlock()

	log.Printf("CDNT:Assigned task %v to W%v", task.ttype, id)
	return nil
}

func (c *Coordinator) DoneTask(id int, reply Empty) error {
	c.mu.Lock()
	c.waiting[id] = false
	c.mu.Unlock()
	c.done.Done()
	return nil
}


// wait for 10 seconds, then resend the task to channel
func (c *Coordinator) startWait(id int, task Task) {
	time.Sleep(10 * time.Second)
	c.mu.Lock()
	if c.waiting[id] {
		log.Printf("CDNT:W%v failed, re-issue task")
		c.taskQueue <- task
	}
	c.mu.Unlock()
}

func (c *Coordinator) runJob(files []string, nReduce int) {
	for _, file := range files {
		c.taskQueue <- Task{MAP, []string{file}}
	}
	c.done.Wait()
	log.Printf("CDNT:Map phase done")
}

// start a thread that listens for RPCs from worker.go
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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false
	// Your code here.

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		nWorkers: 0,
		taskQueue: make(chan Task),
		waiting: make([]bool, 32),
	}

	// Your code here.
	defer c.runJob(files, nReduce)

	c.server()
	return &c
}
