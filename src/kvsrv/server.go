package kvsrv

import (
	"log"
	"strings"
	"sync"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	mu sync.Mutex
	// logFile *os.File
	data    map[string]string
	version map[int64]int
	tmp     map[int64]string
	// Your definitions here.
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	//if kv.data == nil {
	//	data := make(map[string]string)
	//	scanner := bufio.NewScanner(kv.logFile)
	//	for scanner.Scan() {
	//		line := scanner.Text()
	//
	//		// 查找 key=value 格式的数据
	//		if idx := strings.Index(line, "="); idx != -1 {
	//			// 分离 key 和 value
	//			key := strings.TrimSpace(line[:idx])
	//			value := strings.TrimSpace(line[idx+1:])
	//			data[key] = value
	//		}
	//	}
	//	kv.data = data
	//}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	//if kv.version[args.Key] == nil {
	//	kv.version[args.Key] = make(map[int]int)
	//}
	// reply.Version = kv.version[args.Key][args.Pid]
	reply.Value = kv.data[args.Key]

}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	//if kv.data == nil {
	//	reply.Value = ""
	//} else {
	//	if kv.data[args.Key] != "" {
	//
	//	}
	//}
	// DPrintf("Put-server")
	if kv.version[args.Pid] == args.Version {
		return
	}

	// tmp := kv.data[args.Key]
	kv.data[args.Key] = args.Value
	// log.Println(args.Key, args.Value)
	kv.version[args.Pid] = args.Version
	reply.Value = ""
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	// DPrintf("Append-server")
	// log.Println(args.Key, args.Value)
	if args.Version == kv.version[args.Pid] {
		reply.Value = kv.tmp[args.Pid]
		DPrintf("Append-server %v %v", reply.Value, kv.data[args.Key])
		return
	}

	//if kv.tmp[args.Version]+args.Value == kv.data[args.Key] {
	//	DPrintf("Append-version %v %v", kv.version[args.Pid], args.Version)
	//}

	tmp := kv.data[args.Key]
	reply.Value = tmp
	kv.tmp[args.Pid] = tmp
	kv.version[args.Pid] = args.Version
	kv.data[args.Key] += args.Value
	if strings.Index(args.Value, reply.Value) != -1 {
		// DPrintf("Append-server %v %v", reply.Value, kv.data[args.Key])
	}
	// DPrintf("Append-server %v %v", reply.Value, kv.data[args.Key])

}

func StartKVServer() *KVServer {
	kv := new(KVServer)
	//logFileName := "logfile.log"
	//
	//// 创建或打开日志文件
	//logFile, err := os.OpenFile(logFileName, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	//if err != nil {
	//	log.Fatalf("无法创建日志文件: %s", err)
	//}
	//kv.logFile = logFile
	//
	//log.SetOutput(kv.logFile)
	// You may need initialization code here.
	kv.data = make(map[string]string)
	kv.version = make(map[int64]int)
	kv.tmp = make(map[int64]string)

	return kv
}
