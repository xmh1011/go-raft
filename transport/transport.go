package transport

import (
	"github.com/xmh1011/go-raft/param"
)

// Transport 定义了 Raft 节点之间以及客户端与节点之间通信所需的方法。
type Transport interface {
	// Addr 返回当前 Transport 监听的实际地址。
	Addr() string

	// SetPeers 设置节点 ID 到地址的映射。
	SetPeers(peers map[int]string)

	// RegisterRaft 注册 Raft 实例，用于处理接收到的 RPC 请求。
	RegisterRaft(raftInstance RPCServer)

	// Start 注册 RPC 服务并开始接受连接。
	Start() error

	// Close 关闭监听器，停止接受新的连接。
	Close() error

	// SendRequestVote 发送 RequestVote RPC 请求。
	SendRequestVote(target string, req *param.RequestVoteArgs, resp *param.RequestVoteReply) error

	// SendAppendEntries 发送 AppendEntries RPC 请求。
	SendAppendEntries(target string, req *param.AppendEntriesArgs, resp *param.AppendEntriesReply) error

	// SendInstallSnapshot 发送 InstallSnapshot RPC 请求。
	SendInstallSnapshot(target string, req *param.InstallSnapshotArgs, resp *param.InstallSnapshotReply) error

	// SendClientRequest 发送客户端请求到指定的 Raft 节点。
	SendClientRequest(target string, req *param.ClientArgs, resp *param.ClientReply) error
}
