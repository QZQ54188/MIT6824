package kvsrv

type Mode int64

const (
	mode_modify Mode = 1
	mode_report Mode = 2
)

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	RequestId uint32 //客户端向服务器发送的请求id，用于去重
	Mode      Mode   //模式，是更新数据还是报告已经收到响应
}

type PutAppendReply struct {
	Value string
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
}

type GetReply struct {
	Value string
}
