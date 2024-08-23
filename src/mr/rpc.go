package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
type TaskType int
const (
	MAP TaskType 	= iota
	REDUCE TaskType = iota
	QUIT TaskType	= iota
)
func (tt TaskType) String() string {
	switch tt {
	case MAP:		return "MAP"
	case REDUCE:	return "REDUCE"
	case QUIT:		return "QUIT"
	default:		return "Unknown"
	}
}

type Empty struct {}

type Task struct {
	ttype TaskType
	input []string
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
