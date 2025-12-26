package inmemory

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/xmh1011/go-raft/param"
)

// mockRPCServer 是 transport.RPCServer 接口的一个模拟实现，用于测试。
type mockRPCServer struct {
	// lastArgs 记录最后一次被调用时传入的参数
	lastArgs any
	// replyToReturn 是我们预设的、希望 mock 方法写入到 reply 参数中的内容
	replyToReturn any
	// errorToReturn 是我们预设的、希望 mock 方法返回的错误
	errorToReturn error
}

func (m *mockRPCServer) RequestVote(args *param.RequestVoteArgs, reply *param.RequestVoteReply) error {
	m.lastArgs = args
	if m.replyToReturn != nil {
		// 将预设的回复内容拷贝给调用者提供的 reply 指针
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

func TestInMemoryTransport(t *testing.T) {
	t.Run("New, Connect, and Disconnect", func(t *testing.T) {
		trans := NewInMemoryTransport("local")
		assert.NotNil(t, trans, "NewInMemoryTransport should not return nil")
		assert.NotNil(t, trans.peers, "peers map should be initialized")
		assert.Empty(t, trans.peers, "peers map should be initially empty")

		mockPeer := &mockRPCServer{}
		trans.Connect("peer1", mockPeer)
		assert.Len(t, trans.peers, 1, "peers map should have 1 entry after connect")
		assert.Contains(t, trans.peers, "peer1", "peers map should contain the new peer")

		trans.Disconnect("peer1")
		assert.Empty(t, trans.peers, "peers map should be empty after disconnect")
	})

	t.Run("Send successful RPC calls", func(t *testing.T) {
		trans := NewInMemoryTransport("local")
		mockPeer := &mockRPCServer{}
		trans.Connect("peer1", mockPeer)

		// --- Test SendRequestVote ---
		mockPeer.replyToReturn = &param.RequestVoteReply{Term: 1, VoteGranted: true}
		mockPeer.errorToReturn = nil
		argsRV := &param.RequestVoteArgs{Term: 1, CandidateId: 10}
		replyRV := &param.RequestVoteReply{}
		err := trans.SendRequestVote("peer1", argsRV, replyRV)
		assert.NoError(t, err, "SendRequestVote should not return an error on success")
		assert.Equal(t, argsRV, mockPeer.lastArgs, "mock server should have received the correct args for RequestVote")
		assert.Equal(t, uint64(1), replyRV.Term, "reply should be filled correctly for RequestVote")
		assert.True(t, replyRV.VoteGranted, "reply should be filled correctly for RequestVote")

		// --- Test SendAppendEntries ---
		mockPeer.replyToReturn = &param.AppendEntriesReply{Term: 2, Success: true}
		argsAE := &param.AppendEntriesArgs{Term: 2}
		replyAE := &param.AppendEntriesReply{}
		err = trans.SendAppendEntries("peer1", argsAE, replyAE)
		assert.NoError(t, err)
		assert.Equal(t, argsAE, mockPeer.lastArgs)
		assert.Equal(t, uint64(2), replyAE.Term)
		assert.True(t, replyAE.Success)

		// --- Test SendInstallSnapshot ---
		mockPeer.replyToReturn = &param.InstallSnapshotReply{Term: 3}
		argsIS := &param.InstallSnapshotArgs{Term: 3}
		replyIS := &param.InstallSnapshotReply{}
		err = trans.SendInstallSnapshot("peer1", argsIS, replyIS)
		assert.NoError(t, err)
		assert.Equal(t, argsIS, mockPeer.lastArgs)
		assert.Equal(t, uint64(3), replyIS.Term)

		// --- Test SendClientRequest ---
		mockPeer.replyToReturn = &param.ClientReply{Success: true, Result: "OK"}
		argsCR := &param.ClientArgs{Command: "SET"}
		replyCR := &param.ClientReply{}
		err = trans.SendClientRequest("peer1", argsCR, replyCR)
		assert.NoError(t, err)
		assert.Equal(t, argsCR, mockPeer.lastArgs)
		assert.True(t, replyCR.Success)
		assert.Equal(t, "OK", replyCR.Result)
	})

	t.Run("Send RPC to non-existent peer", func(t *testing.T) {
		trans := NewInMemoryTransport("local")
		// No peers are connected
		err := trans.SendRequestVote("non-existent-peer", &param.RequestVoteArgs{}, &param.RequestVoteReply{})
		assert.Error(t, err, "sending to a non-existent peer should return an error")
		assert.Contains(t, err.Error(), "could not connect to peer", "error message should be informative")
	})

	t.Run("Send RPC where peer returns an error", func(t *testing.T) {
		trans := NewInMemoryTransport("local")
		mockPeer := &mockRPCServer{}
		expectedErr := errors.New("mock RPC failure")
		mockPeer.errorToReturn = expectedErr
		trans.Connect("peer1", mockPeer)

		err := trans.SendRequestVote("peer1", &param.RequestVoteArgs{}, &param.RequestVoteReply{})
		assert.Error(t, err, "transport should propagate the error from the peer")
		assert.Equal(t, expectedErr, err, "the returned error should be the one from the mock")
	})
}
