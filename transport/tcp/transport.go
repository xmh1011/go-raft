package tcp

import (
	"errors"
	"log"
	"net"
	"net/rpc"
	"sync"
	"time"

	"github.com/xmh1011/go-raft/param"
	"github.com/xmh1011/go-raft/transport"
)

// Transport 实现了 Transport 接口，通过 TCP 和 net/rpc 进行通信。
type Transport struct {
	localAddr string
	listener  net.Listener
	raft      transport.RPCServer
	server    *rpc.Server
	mu        sync.RWMutex
	peers     map[string]*rpc.Client // 缓存 RPC 客户端连接
}

// NewTCPTransport 创建一个新的 Transport 实例。
// 它会开始在 localAddr 上监听传入的连接。
// raftInstance 是本地的 Raft 节点，用于处理接收到的 RPC 请求。
func NewTCPTransport(localAddr string, raftInstance transport.RPCServer) (*Transport, error) {
	t := &Transport{
		localAddr: localAddr,
		raft:      raftInstance,
		peers:     make(map[string]*rpc.Client),
		server:    rpc.NewServer(),
	}

	// 启动 RPC 服务器
	err := t.startRPCServer()
	if err != nil {
		return nil, err
	}

	// 在后台接受连接
	go t.acceptConnections()

	log.Printf("[TCPTransport] Listening on %s", localAddr)
	return t, nil
}

// startRPCServer 注册 RaftRPC 服务并准备监听。
func (t *Transport) startRPCServer() error {
	// 注册 RPC 服务到我们自己的 server 实例
	err := t.server.Register(&transport.RaftRPC{Raft: t.raft})
	if err != nil {
		return err
	}

	listener, err := net.Listen("tcp", t.localAddr)
	if err != nil {
		return err
	}
	t.listener = listener
	return nil
}

// acceptConnections 循环接受并处理新的 TCP 连接。
func (t *Transport) acceptConnections() {
	for {
		conn, err := t.listener.Accept()
		if err != nil {
			log.Printf("[TCPTransport] Accept error on %s: %v", t.localAddr, err)
			// 如果监听器关闭了，就退出循环
			var opErr *net.OpError
			if errors.As(err, &opErr) && opErr.Err.Error() == "use of closed network connection" {
				return
			}
			continue
		}
		// 为每个连接启动一个新的 goroutine 来提供 RPC 服务
		go t.server.ServeConn(conn)
	}
}

// Close 关闭监听器，停止接受新的连接。
func (t *Transport) Close() error {
	return t.listener.Close()
}

// getPeerClient 获取或创建一个到目标节点的 RPC 客户端。
func (t *Transport) getPeerClient(target string) (*rpc.Client, error) {
	t.mu.RLock()
	client, ok := t.peers[target]
	t.mu.RUnlock()

	// 如果客户端存在并且没有关闭，则复用它
	if ok && client != nil {
		return client, nil
	}

	// 否则，建立新连接
	t.mu.Lock()
	defer t.mu.Unlock()

	// 再次检查，防止在等待锁的过程中其他 goroutine 已经创建了连接
	if client, ok := t.peers[target]; ok && client != nil {
		return client, nil
	}

	// 设置连接超时
	conn, err := net.DialTimeout("tcp", target, 5*time.Second)
	if err != nil {
		return nil, err
	}
	client = rpc.NewClient(conn)
	t.peers[target] = client
	return client, nil
}

// remoteCall 是一个通用的 RPC 调用函数。
func (t *Transport) remoteCall(target, method string, args interface{}, reply interface{}) error {
	client, err := t.getPeerClient(target)
	if err != nil {
		return err
	}

	// 进行 RPC 调用
	err = client.Call(method, args, reply)
	if err != nil {
		// 如果是连接已关闭等错误，说明缓存的 client 失效了
		if errors.Is(err, rpc.ErrShutdown) {
			t.mu.Lock()
			delete(t.peers, target)
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
