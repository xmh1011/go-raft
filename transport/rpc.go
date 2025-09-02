package transport

import (
	"github.com/xmh1011/go-raft/param"
)

// RPCServer 定义了 Raft 节点需要暴露给 Transport 的 RPC 处理方法。
// 任何实现了这个接口的结构体都可以被 InMemoryTransport 注册和调用。
type RPCServer interface {
	RequestVote(args *param.RequestVoteArgs, reply *param.RequestVoteReply) error
	AppendEntries(args *param.AppendEntriesArgs, reply *param.AppendEntriesReply) error
	InstallSnapshot(args *param.InstallSnapshotArgs, reply *param.InstallSnapshotReply) error
	ClientRequest(args *param.ClientArgs, reply *param.ClientReply) error
}

// RaftRPC 是一个包装器，用于将 Raft 实例的方法暴露给 net/rpc 包。
type RaftRPC struct {
	Raft RPCServer
}

// RequestVote 是 RequestVote RPC 的 RPC 处理器。
func (r *RaftRPC) RequestVote(args param.RequestVoteArgs, reply *param.RequestVoteReply) error {
	return r.Raft.RequestVote(&args, reply)
}

// AppendEntries 是 AppendEntries RPC 的 RPC 处理器。
func (r *RaftRPC) AppendEntries(args param.AppendEntriesArgs, reply *param.AppendEntriesReply) error {
	return r.Raft.AppendEntries(&args, reply)
}

// InstallSnapshot 是 InstallSnapshot RPC 的 RPC 处理器。
func (r *RaftRPC) InstallSnapshot(args param.InstallSnapshotArgs, reply *param.InstallSnapshotReply) error {
	return r.Raft.InstallSnapshot(&args, reply)
}

// ClientRequest 是客户端请求的 RPC 处理器。
func (r *RaftRPC) ClientRequest(args param.ClientArgs, reply *param.ClientReply) error {
	return r.Raft.ClientRequest(&args, reply)
}
