package mr

import (
	"github.com/google/uuid"
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// Your definitions here.
	files             []string
	readList          map[string]int
	reduceList        []int
	reducesuccessList map[int]int
	fileLen           int
	mapNum            int
	reduceNum         int
	done              bool
	workNo            map[uuid.UUID]int
}

var (
	getFileMutex sync.Mutex
	taskNo       = 0
)

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) GetFileName(args *GetFileArgs, reply *GetFileReply) error {
	getFileMutex.Lock()
	defer getFileMutex.Unlock() // 确保最终释放锁

	if len(c.readList) == c.fileLen {
		reply.IfProcessEnd = true
		reply.IfSendEnd = true
	} else if len(c.files) == 0 {
		reply.IfProcessEnd = false
		reply.IfSendEnd = true
	} else {
		reply.FileName = c.files[taskNo]
		c.files = c.files[1:]
		//_, ok := c.workNo[args.Uid]
		//if !ok {
		//	c.workNo[args.Uid] = c.mapNum
		//	c.mapNum += 1
		//}
		// reply.MapNum = c.workNo[args.Uid]
		reply.MapNum = c.mapNum
		c.mapNum += 1
		reply.IfSendEnd = false
		reply.IfProcessEnd = false
		go c.Timer(reply.FileName, reply.MapNum)
	}

	// log.Printf("Reply: %+v", reply)
	return nil
}

func (c *Coordinator) GetReduceNo(args *GetReduceArgs, reply *GetReduceReply) error {
	getFileMutex.Lock()
	defer getFileMutex.Unlock() // 确保最终释放锁

	if len(c.reducesuccessList) == c.reduceNum {
		reply.IfProcessEnd = true
		reply.IfSendEnd = true
		c.done = true
	} else if len(c.reduceList) == 0 {
		reply.IfProcessEnd = false
		reply.IfSendEnd = true
	} else {
		reply.ReduceNum = c.reduceList[taskNo]
		c.reduceList = c.reduceList[1:]
		reply.IfSendEnd = false
		reply.IfProcessEnd = false
		go c.ReduceTimer(reply.ReduceNum)
	}
	// log.Printf("Reply: %+v", reply)
	return nil
}

func (c *Coordinator) ReduceTimer(num int) {
	time.Sleep(time.Second * 5)
	_, ok := c.reducesuccessList[num]
	if !ok {
		getFileMutex.Lock()
		defer getFileMutex.Unlock() // 确保最终释放锁
		c.reduceList = append(c.reduceList, num)
	}
}

func (c *Coordinator) Timer(file string, mapNum int) {
	time.Sleep(time.Second * 5)
	_, ok := c.readList[file]
	if !ok {
		getFileMutex.Lock()
		defer getFileMutex.Unlock() // 确保最终释放锁
		c.files = append(c.files, file)
	}
}

func (c *Coordinator) GetReduceN(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = c.reduceNum
	return nil
}

func (c *Coordinator) ReadSuccess(args *ReadArgs, reply *ReadReply) error {
	getFileMutex.Lock()
	defer getFileMutex.Unlock() // 确保最终释放锁
	c.readList[args.FileName] = args.MapNum
	return nil
}

func (c *Coordinator) ReduceSuccess(args *ReduceArgs, reply *ReadReply) error {
	getFileMutex.Lock()
	defer getFileMutex.Unlock() // 确保最终释放锁
	c.reducesuccessList[args.ReduceNum] = args.ReduceNum
	return nil
}

//func (c *Coordinator) ReadFail(args *ReadArgs, reply *ReadReply) error {
//	c.files = append(c.files, args.FileName)
//	return nil
//}

//func (c *Coordinator) Handler(args *ExampleArgs, reply *ExampleReply) error {
//	c.done = true
//	return nil
//}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
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
	ret := c.done

	// Your code here.

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.files = files
	c.fileLen = len(files)
	c.mapNum = 0
	c.reduceNum = nReduce
	doneList := make(map[string]int)
	c.readList = doneList
	reduceList := make(map[int]int)
	c.reducesuccessList = reduceList
	c.reduceList = []int{}
	workNo := make(map[uuid.UUID]int)
	c.workNo = workNo
	for i := 0; i < nReduce; i++ {
		c.reduceList = append(c.reduceList, i)
	}
	c.done = false
	c.server()
	return &c
}
