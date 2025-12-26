package tcp

import (
	"errors"
	"fmt"
	"log"
	"net"
	"net/rpc"
	"strconv"
	"sync"
	"time"

	"github.com/xmh1011/go-raft/param"
	"github.com/xmh1011/go-raft/transport"
)

// Transport 实现了 Transport 接口，通过 TCP 和 net/rpc 进行通信。
type Transport struct {
	listener  net.Listener
	localAddr string // 实际监听的地址 (IP:Port)

	raft   transport.RPCServer
	server *rpc.Server

	mu        sync.RWMutex
	peers     map[string]*rpc.Client // 缓存 RPC 客户端连接
	resolvers map[int]string         // NodeID -> Address 映射
}

// NewTCPTransport 创建一个新的 Transport 实例并立即开始监听端口。
// 这样可以立即获取分配的端口号（如果使用 :0）。
func NewTCPTransport(listenAddr string) (*Transport, error) {
	listener, err := net.Listen("tcp", listenAddr)
	if err != nil {
		return nil, err
	}

	return &Transport{
		listener:  listener,
		localAddr: listener.Addr().String(),
		peers:     make(map[string]*rpc.Client),
		resolvers: make(map[int]string),
		server:    rpc.NewServer(),
	}, nil
}

// Addr 返回当前 Transport 监听的实际地址。
func (t *Transport) Addr() string {
	return t.localAddr
}

// SetPeers 设置节点 ID 到地址的映射。
// 注意：这会重置所有现有的连接缓存，强制下次通信时重新建立连接。
func (t *Transport) SetPeers(peers map[int]string) {
	t.mu.Lock()
	defer t.mu.Unlock()

	// 更新地址映射
	// 注意：这里我们完全替换映射，而不是合并，以支持删除节点
	t.resolvers = make(map[int]string)
	for id, addr := range peers {
		t.resolvers[id] = addr
	}

	// 关闭并清除所有现有的客户端连接
	// 这对于网络分区测试至关重要，确保旧的连接被切断
	for _, client := range t.peers {
		client.Close()
	}
	t.peers = make(map[string]*rpc.Client)
}

// RegisterRaft 注册 Raft 实例，用于处理接收到的 RPC 请求。
func (t *Transport) RegisterRaft(raftInstance transport.RPCServer) {
	t.raft = raftInstance
}

// Start 注册 RPC 服务并开始接受连接。
// 必须在 RegisterRaft 之后调用。
func (t *Transport) Start() error {
	if t.raft == nil {
		return errors.New("raft instance not registered")
	}

	// 注册 RPC 服务到我们自己的 server 实例
	// 注意：这里不需要再 Listen 了，因为 NewTCPTransport 已经 Listen 了
	err := t.server.Register(&transport.RaftRPC{Raft: t.raft})
	if err != nil {
		return err
	}

	// 在后台接受连接
	go t.acceptConnections()

	log.Printf("[TCPTransport] Service started on %s", t.localAddr)
	return nil
}

// acceptConnections 循环接受并处理新的 TCP 连接。
func (t *Transport) acceptConnections() {
	for {
		conn, err := t.listener.Accept()
		if err != nil {
			// 如果监听器关闭了，就退出循环
			var opErr *net.OpError
			if errors.As(err, &opErr) && opErr.Err.Error() == "use of closed network connection" {
				return
			}
			// 忽略临时错误
			continue
		}
		// 为每个连接启动一个新的 goroutine 来提供 RPC 服务
		go t.server.ServeConn(conn)
	}
}

// Close 关闭监听器，停止接受新的连接。
func (t *Transport) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()

	// 关闭所有缓存的客户端连接
	for _, client := range t.peers {
		client.Close()
	}
	t.peers = make(map[string]*rpc.Client)

	if t.listener != nil {
		return t.listener.Close()
	}
	return nil
}

// getPeerAddress 根据 NodeID 获取地址。
func (t *Transport) getPeerAddress(nodeIDStr string) (string, error) {
	id, err := strconv.Atoi(nodeIDStr)
	if err != nil {
		return "", fmt.Errorf("invalid node id: %s", nodeIDStr)
	}

	t.mu.RLock()
	defer t.mu.RUnlock()
	addr, ok := t.resolvers[id]
	if !ok {
		return "", fmt.Errorf("address not found for node %d", id)
	}
	return addr, nil
}

// getPeerClient 获取或创建一个到目标节点的 RPC 客户端。
func (t *Transport) getPeerClient(targetID string) (*rpc.Client, error) {
	// 1. 解析地址
	addr, err := t.getPeerAddress(targetID)
	if err != nil {
		return nil, err
	}

	// 2. 检查缓存
	t.mu.RLock()
	client, ok := t.peers[targetID]
	t.mu.RUnlock()

	if ok && client != nil {
		return client, nil
	}

	// 3. 建立新连接
	t.mu.Lock()
	defer t.mu.Unlock()

	// 双重检查
	if client, ok := t.peers[targetID]; ok && client != nil {
		return client, nil
	}

	// 设置连接超时
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		return nil, err
	}
	client = rpc.NewClient(conn)
	t.peers[targetID] = client
	return client, nil
}

// remoteCall 是一个通用的 RPC 调用函数。
func (t *Transport) remoteCall(targetID, method string, args any, reply any) error {
	client, err := t.getPeerClient(targetID)
	if err != nil {
		return err
	}

	// 进行 RPC 调用
	err = client.Call(method, args, reply)
	if err != nil {
		// 如果是连接已关闭等错误，说明缓存的 client 失效了，移除它
		if errors.Is(err, rpc.ErrShutdown) {
			t.mu.Lock()
			delete(t.peers, targetID)
			t.mu.Unlock()
		}
		return err
	}
	return nil
}

// SendRequestVote 发送 RequestVote RPC 请求。
func (t *Transport) SendRequestVote(target string, req *param.RequestVoteArgs, resp *param.RequestVoteReply) error {
	return t.remoteCall(target, "RaftRPC.RequestVote", req, resp)
}

// SendAppendEntries 发送 AppendEntries RPC 请求。
func (t *Transport) SendAppendEntries(target string, req *param.AppendEntriesArgs, resp *param.AppendEntriesReply) error {
	return t.remoteCall(target, "RaftRPC.AppendEntries", req, resp)
}

// SendInstallSnapshot 发送 InstallSnapshot RPC 请求。
func (t *Transport) SendInstallSnapshot(target string, req *param.InstallSnapshotArgs, resp *param.InstallSnapshotReply) error {
	return t.remoteCall(target, "RaftRPC.InstallSnapshot", req, resp)
}

// SendClientRequest 发送客户端请求到指定的 Raft 节点。
func (t *Transport) SendClientRequest(target string, req *param.ClientArgs, resp *param.ClientReply) error {
	return t.remoteCall(target, "RaftRPC.ClientRequest", req, resp)
}
