package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"time"
	"sync"
	"strconv"
)

type Coordinator struct {
	taskQueue chan Task
	waiting []bool
	mapResults [][]string
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

func (c *Coordinator) RequestTask(args Empty, reply *Task) error {
	task := <- c.taskQueue
	reply.Id = task.Id
	reply.Ttype = task.Ttype
	reply.Input = task.Input

	defer c.startWait(task.Id, task)

	c.mu.Lock()
	c.waiting[task.Id] = true
	c.mu.Unlock()

	log.Printf("CDNT:Assigned task %v", task)
	return nil
}

func (c *Coordinator) DoneTask(task *Task, reply *Empty) error {
	c.mu.Lock()
	c.waiting[task.Id] = false
	if task.Ttype == MAP {
		for i, file := range task.Input {
			c.mapResults[i] = append(c.mapResults[i], file)
		}
	}
	c.mu.Unlock()

	c.done.Done()
	return nil
}


// wait for 10 seconds, then resend the task to channel
func (c *Coordinator) startWait(id int, task Task) {
	go func() {
		time.Sleep(10 * time.Second)
		c.mu.Lock()
		if c.waiting[id] {
			log.Printf("CDNT:Task id:%v failed, re-issue task", id)
			go func() { c.taskQueue <- task }()
		}
		c.mu.Unlock()
	}()
}

func (c *Coordinator) runJob(files []string, nReduce int) {
	for id, file := range files {
		go func(file string) {
			task := Task{id, MAP, []string{file, strconv.Itoa(nReduce)}}
			log.Printf("CDNT:Send task %v to queue", task)
			c.taskQueue <- task
		}(file)
		c.done.Add(1)
	}
	c.done.Wait()
	log.Printf("CDNT:Map phase done")
	for id, reduceFiles := range c.mapResults {
		go func(files []string) {
			task := Task{id, REDUCE, reduceFiles}
			log.Printf("CDNT:Send task %v to queue", task)
			c.taskQueue <- task
		}(reduceFiles)
		c.done.Add(1)
	}
	c.done.Wait()
	log.Printf("CDNT:Reduce phase done")
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
		taskQueue: make(chan Task),
		waiting: make([]bool, len(files) + nReduce),
		mapResults: make([][]string, nReduce),
	}

	// Your code here.
	defer c.runJob(files, nReduce)

	c.server()
	return &c
}
