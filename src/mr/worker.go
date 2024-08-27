package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "encoding/json"
import "io/ioutil"
import "strconv"
import "os"
import "sort"


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
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	for {
		task := Task{}
		log.Printf("W:Requesting work")
		ok := call("Coordinator.RequestTask", Empty{}, &task)
		if ok {
			log.Printf("W:Received work %v", task)
		} else {
			log.Printf("W:Failed to request work")
			break
		}
		
		switch task.Ttype {
		case MAP:
			HandleMap(&mapf, &task)
		case REDUCE:
			HandleReduce(&reducef, &task)
		case QUIT:
			log.Printf("W:Exiting")
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

func HandleMap(mapf *func(string, string) []KeyValue, task *Task) {
	// perform map
	filename := task.Input[0]
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("W:Cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("W:Cannot read %v", filename)
	}
	file.Close()
	kva := (*mapf)(filename, string(content))

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
			log.Fatalf("W:Cannot open temp file")
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
	// call done
	defer callDoneTask(&response, &Empty{})
}

func HandleReduce(reducef *func(string, []string) string, task *Task) {
	kva := make([]KeyValue, 0, 16)
	for _, filename := range task.Input {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("W:Cannot open %v", filename)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		if err := file.Close(); err != nil {
			log.Fatalf("W:Cannot close file %v", filename)
		}
	}
	
	sort.Sort(ByKey(kva))
	
	oname := fmt.Sprintf("mr-out-%v", task.Id)
	ofile, _ := os.Create(oname)

	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := (*reducef)(kva[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

		i = j
	}
	if err := ofile.Close(); err != nil {
		log.Fatalf("W:Cannot close %v", oname)
	}

	// call done
	defer callDoneTask(task, &Empty{})
}

func callDoneTask(task *Task, reply *Empty) {
	ok := call("Coordinator.DoneTask", task, &Empty{})
	if ok {
		log.Printf("W:Completed task %v", task)
	} else {
		log.Printf("W:Failed to inform completion %v", task)
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
