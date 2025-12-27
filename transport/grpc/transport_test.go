package grpc

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/xmh1011/go-raft/param"
)

// MockRPCServer is a mock implementation of raft.RaftRPC
type MockRPCServer struct {
	mock.Mock
}

func (m *MockRPCServer) RequestVote(args *param.RequestVoteArgs, reply *param.RequestVoteReply) error {
	ret := m.Called(args, reply)
	return ret.Error(0)
}

func (m *MockRPCServer) AppendEntries(args *param.AppendEntriesArgs, reply *param.AppendEntriesReply) error {
	ret := m.Called(args, reply)
	return ret.Error(0)
}

func (m *MockRPCServer) InstallSnapshot(args *param.InstallSnapshotArgs, reply *param.InstallSnapshotReply) error {
	ret := m.Called(args, reply)
	return ret.Error(0)
}

func (m *MockRPCServer) ClientRequest(args *param.ClientArgs, reply *param.ClientReply) error {
	ret := m.Called(args, reply)
	return ret.Error(0)
}

func TestGRPCTransport(t *testing.T) {
	// Create two transports
	t1, err := NewTransport("127.0.0.1:0")
	assert.NoError(t, err)
	defer t1.Close()

	t2, err := NewTransport("127.0.0.1:0")
	assert.NoError(t, err)
	defer t2.Close()

	// Mock Raft implementation for t1 and t2
	mockRaft1 := new(MockRPCServer)
	t1.RegisterRaft(mockRaft1)

	mockRaft2 := new(MockRPCServer)
	t2.RegisterRaft(mockRaft2)

	// Start transports
	assert.NoError(t, t1.Start())
	assert.NoError(t, t2.Start())

	// Setup peers
	peers := map[int]string{
		1: t1.Addr(),
		2: t2.Addr(),
	}
	t1.SetPeers(peers)
	t2.SetPeers(peers)

	// Test RequestVote
	t.Run("RequestVote", func(t *testing.T) {
		req := &param.RequestVoteArgs{
			Term:         1,
			CandidateId:  1,
			LastLogIndex: 10,
			LastLogTerm:  1,
			PreVote:      false,
		}
		resp := &param.RequestVoteReply{}

		mockRaft2.On("RequestVote", mock.AnythingOfType("*param.RequestVoteArgs"), mock.AnythingOfType("*param.RequestVoteReply")).Run(func(args mock.Arguments) {
			reply := args.Get(1).(*param.RequestVoteReply)
			reply.Term = 1
			reply.VoteGranted = true
		}).Return(nil)

		err := t1.SendRequestVote("2", req, resp)
		assert.NoError(t, err)
		assert.True(t, resp.VoteGranted)
		assert.Equal(t, uint64(1), resp.Term)
	})

	// Test AppendEntries
	t.Run("AppendEntries", func(t *testing.T) {
		req := &param.AppendEntriesArgs{
			Term:     1,
			LeaderId: 1,
			Entries: []param.LogEntry{
				{Command: "cmd1", Term: 1, Index: 1},
			},
		}
		resp := &param.AppendEntriesReply{}

		mockRaft2.On("AppendEntries", mock.AnythingOfType("*param.AppendEntriesArgs"), mock.AnythingOfType("*param.AppendEntriesReply")).Run(func(args mock.Arguments) {
			reply := args.Get(1).(*param.AppendEntriesReply)
			reply.Success = true
			reply.Term = 1
		}).Return(nil)

		err := t1.SendAppendEntries("2", req, resp)
		assert.NoError(t, err)
		assert.True(t, resp.Success)
	})

	// Test ClientRequest
	t.Run("ClientRequest", func(t *testing.T) {
		req := &param.ClientArgs{
			ClientID:    100,
			SequenceNum: 1,
			Command:     "set key value",
		}
		resp := &param.ClientReply{}

		mockRaft2.On("ClientRequest", mock.AnythingOfType("*param.ClientArgs"), mock.AnythingOfType("*param.ClientReply")).Run(func(args mock.Arguments) {
			reply := args.Get(1).(*param.ClientReply)
			reply.Success = true
			reply.Result = "ok"
		}).Return(nil)

		err := t1.SendClientRequest("2", req, resp)
		assert.NoError(t, err)
		assert.True(t, resp.Success)
		assert.Equal(t, "ok", resp.Result)
	})

	// Wait for async operations to complete if any
	time.Sleep(100 * time.Millisecond)
}
