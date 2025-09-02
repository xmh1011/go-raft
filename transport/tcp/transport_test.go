package tcp

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/xmh1011/go-raft/param"
)

// mockRPCServer 是 transport.RPCServer 接口的一个模拟实现，用于测试。
type mockRPCServer struct {
	lastArgs      interface{}
	replyToReturn interface{}
	errorToReturn error
}

func (m *mockRPCServer) RequestVote(args *param.RequestVoteArgs, reply *param.RequestVoteReply) error {
	m.lastArgs = args
	if m.replyToReturn != nil {
		*reply = *(m.replyToReturn.(*param.RequestVoteReply))
	}
	return m.errorToReturn
}

func (m *mockRPCServer) AppendEntries(args *param.AppendEntriesArgs, reply *param.AppendEntriesReply) error {
	m.lastArgs = args
	if m.replyToReturn != nil {
		*reply = *(m.replyToReturn.(*param.AppendEntriesReply))
	}
	return m.errorToReturn
}

func (m *mockRPCServer) InstallSnapshot(args *param.InstallSnapshotArgs, reply *param.InstallSnapshotReply) error {
	m.lastArgs = args
	if m.replyToReturn != nil {
		*reply = *(m.replyToReturn.(*param.InstallSnapshotReply))
	}
	return m.errorToReturn
}

func (m *mockRPCServer) ClientRequest(args *param.ClientArgs, reply *param.ClientReply) error {
	m.lastArgs = args
	if m.replyToReturn != nil {
		*reply = *(m.replyToReturn.(*param.ClientReply))
	}
	return m.errorToReturn
}

func TestTCPTransport(t *testing.T) {

	t.Run("Successful end-to-end RPC call", func(t *testing.T) {
		// --- 设置 Peer (RPC 服务器) ---
		mockPeer := &mockRPCServer{}
		mockPeer.replyToReturn = &param.RequestVoteReply{Term: 1, VoteGranted: true}

		// 在 "localhost:0" 上监听，让系统自动选择一个可用端口
		transPeer, err := NewTCPTransport("localhost:0", mockPeer)
		assert.NoError(t, err)
		defer transPeer.Close() // 确保测试结束后关闭监听器

		// 获取被分配的实际地址
		peerAddr := transPeer.listener.Addr().String()

		// --- 设置 Local (RPC 客户端) ---
		// 本地 transport 实际上在此测试中不需要监听，但我们仍完整创建它
		transLocal, err := NewTCPTransport("localhost:0", &mockRPCServer{})
		assert.NoError(t, err)
		defer transLocal.Close()

		// --- 执行 RPC 调用 ---
		args := &param.RequestVoteArgs{Term: 1, CandidateId: 10}
		reply := &param.RequestVoteReply{}
		err = transLocal.SendRequestVote(peerAddr, args, reply)

		// --- 断言结果 ---
		assert.NoError(t, err, "RPC call should succeed")
		assert.Equal(t, uint64(1), reply.Term, "Reply term should match mock's response")
		assert.True(t, reply.VoteGranted, "Reply vote granted should match mock's response")

		// 验证 mock server 是否收到了正确的参数
		receivedArgs, ok := mockPeer.lastArgs.(*param.RequestVoteArgs)
		assert.True(t, ok, "Mock should have received RequestVoteArgs")
		assert.Equal(t, args.Term, receivedArgs.Term)
		assert.Equal(t, args.CandidateId, receivedArgs.CandidateId)
	})

	t.Run("Dial non-existent server", func(t *testing.T) {
		transLocal, err := NewTCPTransport("localhost:0", &mockRPCServer{})
		assert.NoError(t, err)
		defer transLocal.Close()

		// 尝试连接到一个几乎不可能在使用的端口
		err = transLocal.SendRequestVote("localhost:1", &param.RequestVoteArgs{}, &param.RequestVoteReply{})

		assert.Error(t, err, "Should get an error when dialing a non-existent server")
		// 不同的操作系统可能会返回不同的错误信息，但通常会包含 "connection refused"
		assert.Contains(t, err.Error(), "connection refused", "Error message should indicate connection failure")
	})

	t.Run("Client connection caching", func(t *testing.T) {
		mockPeer := &mockRPCServer{}
		transPeer, err := NewTCPTransport("localhost:0", mockPeer)
		assert.NoError(t, err)
		defer transPeer.Close()
		peerAddr := transPeer.listener.Addr().String()

		transLocal, err := NewTCPTransport("localhost:0", &mockRPCServer{})
		assert.NoError(t, err)
		defer transLocal.Close()

		// 第一次调用，应该会创建并缓存连接
		err = transLocal.SendRequestVote(peerAddr, &param.RequestVoteArgs{}, &param.RequestVoteReply{})
		assert.NoError(t, err)
		assert.Len(t, transLocal.peers, 1, "A client connection should be cached after the first call")

		// 第二次调用，应该复用缓存的连接
		err = transLocal.SendRequestVote(peerAddr, &param.RequestVoteArgs{}, &param.RequestVoteReply{})
		assert.NoError(t, err)
		assert.Len(t, transLocal.peers, 1, "Connection cache size should not increase on subsequent calls")
	})

	t.Run("Handle server-side error", func(t *testing.T) {
		mockPeer := &mockRPCServer{}
		expectedErr := errors.New("a deliberate error from peer")
		mockPeer.errorToReturn = expectedErr

		transPeer, err := NewTCPTransport("localhost:0", mockPeer)
		assert.NoError(t, err)
		defer transPeer.Close()
		peerAddr := transPeer.listener.Addr().String()

		transLocal, err := NewTCPTransport("localhost:0", &mockRPCServer{})
		assert.NoError(t, err)
		defer transLocal.Close()

		err = transLocal.SendRequestVote(peerAddr, &param.RequestVoteArgs{}, &param.RequestVoteReply{})

		assert.Error(t, err, "Client should receive an error if the server returns one")
		// net/rpc 会包装错误，所以我们检查错误消息是否包含我们预设的错误
		assert.Contains(t, err.Error(), expectedErr.Error(), "The propagated error should contain the original error message")
	})

	t.Run("Close listener", func(t *testing.T) {
		trans, err := NewTCPTransport("localhost:0", &mockRPCServer{})
		assert.NoError(t, err)
		addr := trans.listener.Addr().String()

		err = trans.Close()
		assert.NoError(t, err, "Close() should not return an error")

		// 尝试再次创建监听相同地址的 transport 会失败，但更直接的测试是尝试连接它
		// 稍等片刻以确保端口已释放或连接会立即失败
		time.Sleep(50 * time.Millisecond)

		trans2, err := NewTCPTransport("localhost:0", &mockRPCServer{})
		assert.NoError(t, err)
		defer trans2.Close()

		err = trans2.SendRequestVote(addr, &param.RequestVoteArgs{}, &param.RequestVoteReply{})
		assert.Error(t, err, "Should not be able to connect to a closed listener")
	})
}
