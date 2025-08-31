package inmemory

import (
	"fmt"
	"sync"

	"github.com/xmh1011/go-raft/param"
	"github.com/xmh1011/go-raft/transport"
)

// Transport 是一个基于内存的 Transport 实现，用于在单个进程内模拟 Raft 节点间的通信。
type Transport struct {
	mu        sync.RWMutex
	localAddr string                         // 本地节点的地址
	peers     map[string]transport.RPCServer // 存储集群中其他节点的引用
}

// NewInMemoryTransport 创建一个新的 Transport 实例。
// addr 是当前使用此 transport 的节点的地址。
func NewInMemoryTransport(addr string) *Transport {
	return &Transport{
		localAddr: addr,
		peers:     make(map[string]transport.RPCServer),
	}
}

// Connect 将一个节点（peer）添加到 transport 的注册表中。
// 这样，当前的 transport 就知道如何“发送”消息给这个 peer。
func (t *Transport) Connect(peerAddr string, server transport.RPCServer) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.peers[peerAddr] = server
}

// Disconnect 从 transport 的注册表中移除一个节点。
func (t *Transport) Disconnect(peerAddr string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.peers, peerAddr)
}

// getPeer 根据目标地址查找对应的 RPCServer。
func (t *Transport) getPeer(target string) (transport.RPCServer, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	peer, ok := t.peers[target]
	if !ok {
		return nil, fmt.Errorf("could not connect to peer: %s", target)
	}
	return peer, nil
}

// SendRequestVote 向目标节点发送 RequestVote RPC。
// 这是一个同步的、内存中的方法调用。
func (t *Transport) SendRequestVote(target string, req *param.RequestVoteArgs, resp *param.RequestVoteReply) error {
	peer, err := t.getPeer(target)
	if err != nil {
		return err
	}
	// 直接调用目标节点的 RequestVote 方法
	return peer.RequestVote(req, resp)
}

// SendAppendEntries 向目标节点发送 AppendEntries RPC。
func (t *Transport) SendAppendEntries(target string, req *param.AppendEntriesArgs, resp *param.AppendEntriesReply) error {
	peer, err := t.getPeer(target)
	if err != nil {
		return err
	}
	// 直接调用目标节点的 AppendEntries 方法
	return peer.AppendEntries(req, resp)
}

// SendInstallSnapshot 向目标节点发送 InstallSnapshot RPC。
func (t *Transport) SendInstallSnapshot(target string, req *param.InstallSnapshotArgs, resp *param.InstallSnapshotReply) error {
	peer, err := t.getPeer(target)
	if err != nil {
		return err
	}
	// 直接调用目标节点的 InstallSnapshot 方法
	return peer.InstallSnapshot(req, resp)
}

// SendClientRequest 将客户端请求发送到目标 Raft 节点。
func (t *Transport) SendClientRequest(target string, req *param.ClientArgs, resp *param.ClientReply) error {
	peer, err := t.getPeer(target)
	if err != nil {
		return err
	}
	// 直接调用目标节点的 ClientRequest 方法
	return peer.ClientRequest(req, resp)
}
