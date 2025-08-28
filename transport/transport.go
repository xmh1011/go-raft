package transport

import (
	"github.com/xmh1011/go-raft/param"
)

// Transport 定义了 Raft 节点之间以及客户端与节点之间通信所需的方法。
type Transport interface {
	// SendRequestVote 发送 RequestVote RPC 请求。
	SendRequestVote(target string, req *param.RequestVoteArgs, resp *param.RequestVoteReply) error

	// SendAppendEntries 发送 AppendEntries RPC 请求。
	SendAppendEntries(target string, req *param.AppendEntriesArgs, resp *param.AppendEntriesReply) error

	// SendInstallSnapshot 发送 InstallSnapshot RPC 请求。
	SendInstallSnapshot(target string, req *param.InstallSnapshotArgs, resp *param.InstallSnapshotReply) error

	// SendClientRequest 发送客户端请求到指定的 Raft 节点。
	SendClientRequest(target string, req *param.ClientArgs, resp *param.ClientReply) error
}
