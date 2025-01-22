package kvraft

import (
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Reply struct {
	Err   Err
	Value string // Get命令时有效
}
type Session struct {
	LastCmdNum int    // 该server为该client处理的上一条指令的序列号
	OpType     string // 最新处理的指令的类型
	Response   Reply  // 对应的响应
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type    string
	Key     string
	Val     string
	Id      int64
	Version int
	Pid     int64
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	getCh   chan Op
	putCh   chan Op
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big
	data         map[string]string
	version      map[int64]Session
	// tmp                   map[int64]string
	notifyMapCh           map[int]chan Reply
	logLastApplied        int
	passiveSnapshotBefore bool

	// Your definitions here.
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	op := Op{
		Type:    "get",
		Key:     args.Key,
		Id:      args.Id,
		Version: args.Version,
		Pid:     args.Pid,
	}
	kv.mu.Unlock()
	index, _, isLeader := kv.rf.Start(op)
	// DPrintf("isleader %v me: %v", isLeader, kv.me)
	if isLeader == false {
		reply.Err = ErrWrongLeader
		// kv.mu.Unlock()
		return
	}
	// kv.mu.Unlock()
	//select {
	//case msg := <-kv.getCh:
	//	DPrintf("get msg %v and args %v", msg, args.Id)
	//	// 正常完成请求，返回结果
	//	//kv.data[msg.Key] = msg.Val
	//	//kv.version[args.Pid] = args.Version
	//	reply.Err = OK
	//	reply.Value = kv.data[msg.Key]
	//	kv.mu.Unlock()
	//	DPrintf("get done %v %v", args, kv.me)
	//	return
	//case <-time.After(1000 * time.Millisecond):
	//	// 超时，执行超时逻辑
	//	// fmt.Printf("RequestVote to %d timed out\n", i)
	//	reply.Err = ErrNoKey
	//	DPrintf("put timeout %v", args)
	//	kv.mu.Unlock()
	//	return
	//
	//}
	notifyCh := kv.createNotifyCh(index)

	select {
	case res := <-notifyCh:
		reply.Err = res.Err
		reply.Value = res.Value // Get请求要返回value
	case <-time.After(500 * time.Millisecond):
		reply.Err = ErrTimeout
	}

	go kv.CloseNotifyCh(index)
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	// reply = &PutAppendReply{}
	// DPrintf("version %v %v", kv.version[args.Pid], args.Version)
	if args.Version < kv.version[args.Pid].LastCmdNum { // 如果这个请求的序号小于该client上一个已经完成的请求的序号
		// 标志这个请求Client已经收到正确的回复了，只是这个重发请求到的太慢了
		reply.Err = OK
		kv.mu.Unlock()
		return // 直接返回。因为Client在一个命令没有成功之前会无限重试。所以如果收到了一个更小CmdNum的请求，可以断定这个请求Client已经收到正确的回复了
	} else if args.Version == kv.version[args.Pid].LastCmdNum { // 回复丢失或延迟导致client没收到而重发了这个请求
		// 将session中记录的之前apply该命令的结果直接返回，使得不会apply一个命令多次
		reply.Err = kv.version[args.Pid].Response.Err
		kv.mu.Unlock()
		return
	}
	op := Op{
		Type:    "put",
		Key:     args.Key,
		Val:     args.Value,
		Id:      args.Id,
		Version: args.Version,
		Pid:     args.Pid,
	}
	kv.mu.Unlock()
	index, _, isLeader := kv.rf.Start(op)
	// DPrintf("isleader %v me: %v", isLeader, kv.me)
	if isLeader == false {
		reply.Err = ErrWrongLeader
		return
	}
	// kv.mu.Unlock()
	//select {
	//case msg := <-kv.putCh:
	//	// DPrintf("put msg %v and args %v", msg, args.Id)
	//	// 正常完成请求，返回结果
	//	//kv.data[msg.Key] = msg.Val
	//	//kv.version[args.Pid] = args.Version
	//	if msg.Id == args.Id {
	//		reply.Err = OK
	//		DPrintf("put done %v %v", args, kv.me)
	//	} else {
	//		reply.Err = ErrWrongLeader
	//		DPrintf("put match fail %v", args)
	//	}
	//	kv.mu.Unlock()
	//	return
	//case <-time.After(1000 * time.Millisecond):
	//	// 超时，执行超时逻辑
	//	// fmt.Printf("RequestVote to %d timed out\n", i)
	//	reply.Err = ErrNoKey
	//	DPrintf("put timeout %v", args)
	//	kv.mu.Unlock()
	//	return
	//
	//}
	notifyCh := kv.createNotifyCh(index)

	select {
	case res := <-notifyCh:
		reply.Err = res.Err
		// reply.Value = res.Value // Get请求要返回value
	case <-time.After(500 * time.Millisecond):
		reply.Err = ErrTimeout
	}

	go kv.CloseNotifyCh(index)
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	// DPrintf("Append-server")
	// log.Println(args.Key, args.Value)
	if args.Version < kv.version[args.Pid].LastCmdNum { // 如果这个请求的序号小于该client上一个已经完成的请求的序号
		// 标志这个请求Client已经收到正确的回复了，只是这个重发请求到的太慢了
		reply.Err = OK
		kv.mu.Unlock()
		return // 直接返回。因为Client在一个命令没有成功之前会无限重试。所以如果收到了一个更小CmdNum的请求，可以断定这个请求Client已经收到正确的回复了
	} else if args.Version == kv.version[args.Pid].LastCmdNum { // 回复丢失或延迟导致client没收到而重发了这个请求
		// 将session中记录的之前apply该命令的结果直接返回，使得不会apply一个命令多次
		reply.Err = kv.version[args.Pid].Response.Err
		kv.mu.Unlock()
		return
	}
	op := Op{
		Type:    "append",
		Key:     args.Key,
		Val:     args.Value,
		Id:      args.Id,
		Version: args.Version,
		Pid:     args.Pid,
	}
	kv.mu.Unlock()
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	} else {
		//select {
		//case msg := <-kv.putCh:
		//	// DPrintf("append msg %v and args %v", msg, args)
		//	// 正常完成请求，返回结果
		//	//kv.data[msg.Key] = msg.Val
		//	//kv.version[args.Pid] = args.Version
		//	if msg.Id == args.Id {
		//		reply.Err = OK
		//		DPrintf("append done %v", args)
		//	} else {
		//		reply.Err = ErrWrongLeader
		//		DPrintf("append match fail %v %v", args.Id, msg.Id)
		//	}
		//	kv.mu.Unlock()
		//	return
		//case <-time.After(500 * time.Millisecond):
		//	// 超时，执行超时逻辑
		//	// fmt.Printf("RequestVote to %d timed out\n", i)
		//	reply.Err = ErrNoKey
		//	DPrintf("append timeout %v", args)
		//	kv.mu.Unlock()
		//}
		notifyCh := kv.createNotifyCh(index)

		select {
		case res := <-notifyCh:
			reply.Err = res.Err
			reply.Value = res.Value // Get请求要返回value
		case <-time.After(500 * time.Millisecond):
			reply.Err = ErrTimeout
		}

		go kv.CloseNotifyCh(index)
	}
}

func (kv *KVServer) listenCh() {
	for !kv.killed() {
		command := <-kv.applyCh
		if command.CommandValid {
			kv.mu.Lock()
			if command.CommandIndex <= kv.logLastApplied {
				kv.mu.Unlock()
				continue
			}

			// 如果上一个取出的是被动快照且已安装完，则要注意排除“跨快照指令”
			// 因为被动快照安装完后，后续的指令应该从快照结束的index之后紧接着逐个apply
			if kv.passiveSnapshotBefore {
				if command.CommandIndex-kv.logLastApplied != 1 {
					kv.mu.Unlock()
					continue
				}
				kv.passiveSnapshotBefore = false
			}

			// 否则就将logLastApplied更新为较大值applyMsg.CommandIndex
			kv.logLastApplied = command.CommandIndex
			op, ok := command.Command.(Op)
			// DPrintf("msg %v", op)
			if !ok {
				kv.mu.Unlock()
				continue
			}
			reply := Reply{}
			sessionRec, exist := kv.version[op.Pid]
			if exist && op.Type != "get" && op.Version <= sessionRec.LastCmdNum {
				reply = kv.version[op.Pid].Response // 返回session中记录的回复
				DPrintf("KVServer[%d] use the reply(=%v) in sessions for %v.\n", kv.me, reply, op.Type)
			} else {
				if op.Type == "get" {
					_, leader := kv.rf.GetState()
					if leader {
						v, existKey := kv.data[op.Key]
						if !existKey { // 键不存在
							reply.Err = OK
							reply.Value = "" // key不存在则Get返回空字符串
						} else {
							reply.Err = OK
							reply.Value = v
						}
						// kv.getCh <- op
						DPrintf("already send msg")
					}
				}
				if op.Type == "put" {
					kv.data[op.Key] = op.Val
					_, leader := kv.rf.GetState()
					if leader {
						reply.Err = OK
						// kv.putCh <- op
						DPrintf("already send msg")
					}

				}
				if op.Type == "append" {
					kv.data[op.Key] += op.Val
					_, leader := kv.rf.GetState()
					if leader {
						reply.Err = OK
						// kv.putCh <- op
						DPrintf("already send msg")
					}

				}
			}
			if op.Type != "get" { // Get请求的回复就不用放到session了
				session := Session{
					LastCmdNum: op.Version,
					OpType:     op.Type,
					Response:   reply,
				}
				kv.version[op.Pid] = session
				DPrintf("KVServer[%d].sessions[%d] = %v\n", kv.me, op.Pid, session)
			}
			if _, existCh := kv.notifyMapCh[command.CommandIndex]; existCh {
				if _, isLeader := kv.rf.GetState(); isLeader {
					kv.notifyMapCh[command.CommandIndex] <- reply // 向对应client的notifyMapCh发送reply通知对应的handle回复client
				}
			}
			kv.mu.Unlock()
		} else if command.SnapshotValid { // 如果取出的是snapshot
			DPrintf("KVServer[%d] get a Snapshot applyMsg from applyCh.\n", kv.me)

			// 在raft层已经实现了follower是否安装快照的判断
			// 只有followr接受了快照才会通过applyCh通知状态机，因此这里状态机只需要安装快照即可
			kv.mu.Lock()
			kv.applySnapshotToSM(command.Snapshot)    // 将快照应用到状态机
			kv.logLastApplied = command.SnapshotIndex // 更新logLastApplied避免回滚
			kv.passiveSnapshotBefore = true           // 刚安装完被动快照，提醒下一个从channel中取出的若是指令则注意是否为“跨快照指令”
			kv.mu.Unlock()
			DPrintf("KVServer[%d] finish a negative Snapshot, kv.logLastApplied become %v.\n", kv.me, kv.logLastApplied)

			// kv.rf.SetPassiveSnapshottingFlag(false) // kvserver已将被动快照安装完成，修改对应raft的passiveSnapshotting标志
		} else { // 错误的ApplyMsg类型
			DPrintf("KVServer[%d] get an unexpected ApplyMsg!\n", kv.me)
		}

	}
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// 为leader在kv.notifyMapCh中初始化一个缓冲为1的channel完成本次与applyMessage()的通信
func (kv *KVServer) createNotifyCh(index int) chan Reply {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	notifyCh := make(chan Reply, 1)
	kv.notifyMapCh[index] = notifyCh
	return notifyCh
}

// KVServer回复client后关闭对应index的notifyCh
func (kv *KVServer) CloseNotifyCh(index int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if _, ok := kv.notifyMapCh[index]; ok { // 如果存在这个channel，就把它关闭并从map中删除
		close(kv.notifyMapCh[index])
		delete(kv.notifyMapCh, index)
	}
}

func (kv *KVServer) listenSnapshot() {
	for !kv.killed() {
		var snapshotData []byte
		var snapshotIndex int

		if kv.rf.GetPassiveFlagAndSetActiveFlag() {
			DPrintf("Server %d is passive snapshotting and refuses positive snapshot.\n", kv.me)
			time.Sleep(time.Millisecond * 50) // 检查间隔50ms
			continue
		}

		if kv.maxraftstate != -1 && float32(kv.rf.GetRaftStateSize())/float32(kv.maxraftstate) > 0.9 {
			DPrintf("start snapshot")
			kv.mu.Lock()
			// 准备进行主动快照
			DPrintf("KVServer[%d]: The Raft state size is approaching the maxraftstate, Start to snapshot...\n", kv.me)
			snapshotIndex = kv.logLastApplied
			// 将snapshot 信息编码
			w := new(bytes.Buffer)
			e := labgob.NewEncoder(w)
			e.Encode(kv.data)
			e.Encode(kv.version)

			snapshotData = w.Bytes()
			kv.mu.Unlock()
		}

		if snapshotData != nil {
			kv.rf.Snapshot(snapshotIndex, snapshotData)
		}
		kv.rf.SetActiveSnapshottingFlag(false)

		time.Sleep(time.Millisecond * 50) // 检查间隔50ms
	}

}

// 将快照应用到state machine
func (kv *KVServer) applySnapshotToSM(data []byte) {
	if data == nil || len(data) == 0 { // 如果传进来的快照为空或无效则不应用
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var kvDB map[string]string
	var sessions map[int64]Session

	if d.Decode(&kvDB) != nil || d.Decode(&sessions) != nil {
		DPrintf("KVServer %d applySnapshotToSM ERROR!\n", kv.me)
	} else {
		DPrintf("decode success.")
		kv.data = kvDB
		kv.version = sessions
	}
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.getCh = make(chan Op)
	kv.putCh = make(chan Op)
	kv.notifyMapCh = make(map[int]chan Reply)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.data = make(map[string]string)
	kv.version = make(map[int64]Session)
	// kv.applySnapshotToSM(kv.rf.GetRaftSnapshot())

	// You may need initialization code here.
	go kv.listenCh()
	go kv.listenSnapshot()
	return kv
}
