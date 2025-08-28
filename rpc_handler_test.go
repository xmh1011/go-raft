package raft

import (
	"math"
	"sync"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"

	"github.com/xmh1011/go-raft/param"
	"github.com/xmh1011/go-raft/storage"
	"github.com/xmh1011/go-raft/transport"
)

// TestRequestVote_GrantsVote 测试在所有条件都满足时，节点会正确地投出赞成票。
func TestRequestVote_GrantsVote(t *testing.T) {
	// --- Arrange ---
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockStore := storage.NewMockStorage(ctrl)

	r := &Raft{
		id:          2,
		currentTerm: 5,
		votedFor:    -1, // 尚未投票
		store:       mockStore,
		mu:          sync.Mutex{},
	}
	args := param.NewRequestVoteArgs(5, 1, uint64(10), 5)
	reply := &param.RequestVoteReply{}

	// --- 设置 Mock 期望 ---
	// 期望 isLogUpToDate 会检查本地日志
	mockStore.EXPECT().LastLogIndex().Return(uint64(10), nil)
	mockStore.EXPECT().GetEntry(uint64(10)).Return(&param.LogEntry{Term: 5, Index: 10}, nil)
	// 期望 grantVote 会持久化投票结果
	mockStore.EXPECT().SetState(param.HardState{CurrentTerm: 5, VotedFor: 1}).Return(nil)

	// --- Act ---
	err := r.RequestVote(args, reply)

	// --- Assert ---
	assert.NoError(t, err, "RequestVote should not return error")
	assert.True(t, reply.VoteGranted, "expected vote to be granted")
}

// TestRequestVote_RejectsForStaleTerm 测试当候选人任期落后时，节点会拒绝投票。
func TestRequestVote_RejectsForStaleTerm(t *testing.T) {
	// Arrange
	r := &Raft{currentTerm: 6, mu: sync.Mutex{}}
	args := &param.RequestVoteArgs{Term: 5} // 候选人任期为 5，落后于本地的 6
	reply := &param.RequestVoteReply{}

	// Act
	err := r.RequestVote(args, reply)

	// Assert
	assert.NoError(t, err, "RequestVote should not return error")
	assert.False(t, reply.VoteGranted, "expected vote to be denied for stale term")
	assert.Equal(t, uint64(6), reply.Term, "reply term should be 6")
}

// TestRequestVote_RejectsForStaleLog 测试当候选人日志落后时，节点会拒绝投票。
func TestRequestVote_RejectsForStaleLog(t *testing.T) {
	// --- Arrange ---
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockStore := storage.NewMockStorage(ctrl)

	r := &Raft{
		id:          2,
		currentTerm: 5,
		votedFor:    -1, // 尚未投票
		store:       mockStore,
		mu:          sync.Mutex{},
	}
	// 候选人日志只到索引 9，任期 5
	args := param.NewRequestVoteArgs(5, 1, uint64(9), 5)
	reply := &param.RequestVoteReply{}

	// --- 设置 Mock 期望 ---
	// 期望 isLogUpToDate 会检查本地日志，我们让本地日志更新（索引 10，任期 5）
	mockStore.EXPECT().LastLogIndex().Return(uint64(10), nil)
	mockStore.EXPECT().GetEntry(uint64(10)).Return(&param.LogEntry{Term: 5, Index: 10}, nil)

	// --- Act ---
	err := r.RequestVote(args, reply)

	// --- Assert ---
	assert.NoError(t, err, "RequestVote should not return error")
	assert.False(t, reply.VoteGranted, "expected vote to be denied for stale log")
}

// TestRequestVote_StepsDownOnHigherTerm 测试当候选人任期更高时，节点会先转为 Follower 再投票。
func TestRequestVote_StepsDownOnHigherTerm(t *testing.T) {
	// --- Arrange ---
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockStore := storage.NewMockStorage(ctrl)

	r := &Raft{id: 2, currentTerm: 5, store: mockStore, mu: sync.Mutex{}}
	args := param.NewRequestVoteArgs(6, 1, uint64(10), 5)
	reply := &param.RequestVoteReply{}

	// --- 设置 Mock 期望 ---
	gomock.InOrder(
		// 期望1: becomeFollower 会持久化新状态 (任期6, votedFor: -1)
		mockStore.EXPECT().SetState(param.HardState{CurrentTerm: 6, VotedFor: math.MaxUint64}).Return(nil),
		// 期望2: isLogUpToDate 检查日志（假设通过）
		mockStore.EXPECT().LastLogIndex().Return(uint64(10), nil),
		mockStore.EXPECT().GetEntry(uint64(10)).Return(&param.LogEntry{Term: 5, Index: 10}, nil),
		// 期望3: grantVote 会持久化投票结果 (任期6, votedFor: 1)
		mockStore.EXPECT().SetState(param.HardState{CurrentTerm: 6, VotedFor: 1}).Return(nil),
	)

	// --- Act ---
	err := r.RequestVote(args, reply)

	// --- Assert ---
	assert.NoError(t, err, "RequestVote should not return error")
	assert.True(t, reply.VoteGranted, "expected vote to be granted after stepping down")
	assert.Equal(t, uint64(6), r.currentTerm, "term should be updated to 6")
}

// TestClientRequestHandler_LeaderProcessesCommand 测试 Leader 成功处理客户端请求的场景。
func TestClientRequestHandler_LeaderProcessesCommand(t *testing.T) {
	// --- Arrange ---
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := storage.NewMockStorage(ctrl)
	mockTrans := transport.NewMockTransport(ctrl) // 需要一个 mock transport

	commitChan := make(chan param.CommitEntry, 1)

	// 期望 NewRaft 调用 GetState
	mockStore.EXPECT().GetState().Return(param.HardState{}, nil).Times(1)

	r := NewRaft(1, []int{2, 3}, mockStore, nil, mockTrans, commitChan)
	r.state = param.Leader
	r.currentTerm = 2

	args := &param.ClientArgs{ClientID: 123, SequenceNum: 1, Command: "test-command"}
	reply := &param.ClientReply{}

	// --- 设置 Mock 期望 ---
	gomock.InOrder(
		// a. proposeToLog 会调用 LastLogIndex
		mockStore.EXPECT().LastLogIndex().Return(uint64(5), nil).Times(1),
		// b. proposeToLog 会调用 AppendEntries
		mockStore.EXPECT().AppendEntries(gomock.Any()).Return(nil).Times(1),
	)

	// 期望 2: 为 Submit() 启动的【异步】后台 goroutine 设置通用的、不限次数的期望。
	mockStore.EXPECT().FirstLogIndex().Return(uint64(1), nil).AnyTimes()
	mockStore.EXPECT().GetEntry(gomock.Any()).Return(&param.LogEntry{}, nil).AnyTimes()
	mockStore.EXPECT().LastLogIndex().Return(uint64(6), nil).AnyTimes() // 注意：后台任务会看到更新后的日志索引
	mockTrans.EXPECT().SendAppendEntries(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

	// --- Act ---
	// 因为函数会阻塞等待应用，我们在后台启动它
	go r.ClientRequestHandler(args, reply)

	// 短暂等待，确保后台的 handler 已经运行到 waitForAppliedLog
	time.Sleep(50 * time.Millisecond)

	// 模拟 applyLogs 循环发出通知
	r.mu.Lock()
	notifyChan, ok := r.notifyApply[6]
	r.mu.Unlock()
	if !ok {
		t.Fatal("handler did not register a notify channel for index 6")
	}
	notifyChan <- "success-result"

	// 等待 handler 处理完通知
	time.Sleep(50 * time.Millisecond)

	// --- Assert ---
	assert.True(t, reply.Success, "expected Success to be true")
	assert.Equal(t, "success-result", reply.Result, "result should match")
	assert.Equal(t, args.SequenceNum, r.clientSessions[args.ClientID], "client session should be updated")
}

// TestClientRequestHandler_NotLeader 测试当请求发往非 Leader 节点时的重定向行为。
func TestClientRequestHandler_NotLeader(t *testing.T) {
	// Arrange
	r := &Raft{
		state:         param.Follower,
		knownLeaderID: 3, // 假设此节点知道 Leader 是 3
		mu:            sync.Mutex{},
	}
	args := &param.ClientArgs{}
	reply := &param.ClientReply{}

	// Act
	r.ClientRequestHandler(args, reply)

	// Assert
	assert.True(t, reply.NotLeader, "expected NotLeader to be true")
	assert.Equal(t, 3, reply.LeaderHint, "LeaderHint should be 3")
}

// TestClientRequestHandler_DuplicateRequest 测试 Leader 能够正确处理重复的客户端请求。
func TestClientRequestHandler_DuplicateRequest(t *testing.T) {
	// --- Arrange ---
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockStore := storage.NewMockStorage(ctrl) // 虽然不期望调用，但需要一个实例

	r := &Raft{
		id:    1,
		state: param.Leader,
		store: mockStore,
		// 关键：预设一个已处理的请求
		clientSessions: map[int64]int64{123: 5},
		mu:             sync.Mutex{},
	}
	// 客户端发送一个旧的（或相同的）序列号
	args := &param.ClientArgs{ClientID: 123, SequenceNum: 5}
	reply := &param.ClientReply{}

	// --- Act ---
	r.ClientRequestHandler(args, reply)

	// --- Assert ---
	assert.True(t, reply.Success, "expected Success to be true for duplicate request")
}
