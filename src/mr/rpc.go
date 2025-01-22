package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
)
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
type GetFileArgs struct {
	// Uid uuid.UUID
}

type GetFileReply struct {
	FileName     string
	MapNum       int
	IfSendEnd    bool
	IfProcessEnd bool
}

type GetReduceArgs struct {
}

type GetReduceReply struct {
	ReduceNum    int
	IfSendEnd    bool
	IfProcessEnd bool
}

type ReduceArgs struct {
	ReduceNum int
}

type ReduceReply struct {
}

type ReadArgs struct {
	MapNum   int
	FileName string
}

type ReadReply struct {
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
