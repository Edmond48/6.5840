package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "encoding/json"
import "io/ioutil"
import "strconv"
import "time"
import "os"


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


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	for {
		task := Task{}
		log.Printf("W:requesting work")
		ok := call("Coordinator.RequestTask", Empty{}, &task)
		if ok {
			log.Printf("W:received work %v", task)
		} else {
			log.Printf("W:Failed to request work")
			time.Sleep(time.Second)
			continue
		}
		
		switch task.Ttype {
		case MAP:
			// perform map
			filename := task.Input[0]
			file, err := os.Open(filename)
			if err != nil {
				log.Fatalf("W:cannot open %v", filename)
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("W:cannot read %v", filename)
			}
			file.Close()
			kva := mapf(filename, string(content))

			// write results
			nReduce, err := strconv.Atoi(task.Input[1])
			if err != nil {
				log.Fatalf("Could not convert nReduce string \"%v\"", task.Input[1])
			}
 			tmpFiles := make([]*os.File, nReduce)
			encoders := make([]*json.Encoder, nReduce)
			for i := 0; i < nReduce; i++ {
				tmpFile, err := ioutil.TempFile("", "mr-map-tmp-*")
				if err != nil {
					log.Fatalf("W:cannot open temp file")
				}
				tmpFiles[i] = tmpFile
				encoders[i] = json.NewEncoder(tmpFile)
			}
			for _, kv := range kva {
				enc := encoders[ihash(kv.Key) % nReduce]
				if err := enc.Encode(&kv); err != nil {
					log.Fatalf("W:Failed to write %v", kv)
				} 
			}
			response := Task{task.Id, task.Ttype, make([]string, nReduce)}
			for i := 0; i < nReduce; i++ {
				if err := tmpFiles[i].Close(); err != nil {
					log.Fatalf("W:Failed to close %v", tmpFiles[i].Name)
				}
				intermediateFileName := fmt.Sprintf("mr-%v-%v", task.Id, i)
				if err := os.Rename(tmpFiles[i].Name(), intermediateFileName); err != nil {
					log.Fatalf("W:Failed to rename file %v", tmpFiles[i].Name())
				}
				response.Input[i] = intermediateFileName
			}
			ok = call("Coordinator.DoneTask", &response, &Empty{})
			if ok {
				log.Printf("W:completed task %v", task)
			} else {
				log.Printf("W:failed to inform coordinator")
				return
			}
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
