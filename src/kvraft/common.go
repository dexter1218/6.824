package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	ErrTimeout     = "ErrTimeOut"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key     string
	Value   string
	Version int
	Pid     int64
	Id      int64
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	Err   Err
	Value string
}

type GetArgs struct {
	Key     string
	Id      int64
	Pid     int64
	Version int
	// You'll have to add definitions here.
}

type GetReply struct {
	Err   Err
	Value string
}
