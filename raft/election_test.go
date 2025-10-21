package raft

import (
	"math"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/xmh1011/go-raft/param"
	"github.com/xmh1011/go-raft/storage"
	"github.com/xmh1011/go-raft/transport"
)

// TestStartElection_WinsElection tests a candidate successfully winning an election.
func TestStartElection_WinsElection(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := storage.NewMockStorage(ctrl)
	mockTrans := transport.NewMockTransport(ctrl)
	peerIds := []int{2, 3}

	// This test will involve applying logs after commit, so we need a mock state machine.
	mockSM := storage.NewMockStateMachine(ctrl)

	mockStore.EXPECT().GetState().Return(param.HardState{}, nil).Times(1)
	// IMPORTANT: Pass the mockSM to the constructor.
	r := NewRaft(1, peerIds, mockStore, mockSM, mockTrans, nil)
	r.state = param.Follower
	r.currentTerm = 5

	// --- Setup mocks for the entire election and leader transition flow ---
	gomock.InOrder(
		mockStore.EXPECT().SetState(param.HardState{CurrentTerm: 6, VotedFor: 1}).Return(nil),
		mockStore.EXPECT().LastLogIndex().Return(uint64(10), nil),
		mockStore.EXPECT().GetEntry(uint64(10)).Return(&param.LogEntry{Term: 5, Index: 10}, nil),
	)

	for _, peerId := range peerIds {
		mockTrans.EXPECT().SendRequestVote(strconv.Itoa(peerId), gomock.Any(), gomock.Any()).
			DoAndReturn(func(id string, args *param.RequestVoteArgs, reply *param.RequestVoteReply) error {
				reply.Term = args.Term
				reply.VoteGranted = true
				return nil
			})
	}

	// Mock for initLeaderState
	mockStore.EXPECT().LastLogIndex().Return(uint64(10), nil)

	// Mock for the single, manually-called broadcastHeartbeat
	mockStore.EXPECT().FirstLogIndex().Return(uint64(1), nil).AnyTimes()
	mockStore.EXPECT().LastLogIndex().Return(uint64(10), nil).AnyTimes()
	mockStore.EXPECT().GetEntry(gomock.Any()).Return(&param.LogEntry{Term: 5}, nil).AnyTimes()
	mockSM.EXPECT().Apply(gomock.Any()).Return(nil).AnyTimes()

	var wg sync.WaitGroup
	wg.Add(len(peerIds))

	var doneCounter int32
	mockTrans.EXPECT().SendAppendEntries(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(id string, args *param.AppendEntriesArgs, reply *param.AppendEntriesReply) error {
			reply.Success = true
			reply.Term = args.Term
			if atomic.AddInt32(&doneCounter, 1) <= int32(len(peerIds)) {
				wg.Done()
			}
			return nil
		}).AnyTimes()

	// --- Act ---
	// Manually trigger the functions, but intercept the call to startHeartbeat
	// This replaces the single r.startElection() call with a more controlled sequence.

	// 1. Initialize candidate state
	r.mu.Lock()
	err := r.initializeCandidateState()
	r.mu.Unlock()
	assert.NoError(t, err)

	// 2. Broadcast votes and collect them (simulated)
	lastLogIndex, lastLogTerm, err := r.getLastLogInfoForElection()
	assert.NoError(t, err)
	voteChan := r.broadcastVoteRequests(6, lastLogIndex, lastLogTerm)

	// 3. Process votes until win condition is met
	ctx := newElectionContext(r)
	for i := 0; i < len(peerIds); i++ {
		result := <-voteChan
		if r.processVote(ctx, result, 6) {
			break // Election won
		}
	}

	// 4. Manually transition to leader, BUT DO NOT START THE HEARTBEAT LOOP
	r.mu.Lock()
	assert.Equal(t, param.Leader, r.state, "expected state to be Leader")
	// This is the key: we manually call broadcastHeartbeat once
	// instead of letting the code call startHeartbeat() which creates the infinite loop.
	r.broadcastHeartbeat()
	r.mu.Unlock()

	// --- Assert ---
	// Wait for the single, manually triggered heartbeat to complete.
	wg.Wait()
	// The test now finishes cleanly with no goroutines left behind.
}

// TestStartElection_LosesElection tests a candidate failing to win an election.
func TestStartElection_LosesElection(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := storage.NewMockStorage(ctrl)
	mockTrans := transport.NewMockTransport(ctrl)
	peerIds := []int{2, 3}
	revertedToFollower := make(chan struct{})

	mockStore.EXPECT().GetState().Return(param.HardState{}, nil).Times(1)
	r := NewRaft(1, peerIds, mockStore, nil, mockTrans, nil)
	r.state = param.Follower
	r.currentTerm = 5

	// Fix: Add a call to becomeFollower which persists the state change.
	// This requires adding the bug fix to your election.go source code.
	// Assuming the fix is in place, the test is as follows:
	gomock.InOrder(
		mockStore.EXPECT().SetState(param.HardState{CurrentTerm: 6, VotedFor: 1}).Return(nil),
		mockStore.EXPECT().LastLogIndex().Return(uint64(10), nil),
		mockStore.EXPECT().GetEntry(uint64(10)).Return(&param.LogEntry{Term: 5, Index: 10}, nil),
		mockStore.EXPECT().SetState(param.HardState{CurrentTerm: 6, VotedFor: math.MaxUint64}).
			Do(func(interface{}) { close(revertedToFollower) }).Return(nil),
	)

	for _, peerId := range peerIds {
		mockTrans.EXPECT().SendRequestVote(strconv.Itoa(peerId), gomock.Any(), gomock.Any()).
			DoAndReturn(func(id string, args *param.RequestVoteArgs, reply *param.RequestVoteReply) error {
				reply.Term = args.Term
				reply.VoteGranted = false
				return nil
			})
	}

	r.startElection()

	select {
	case <-revertedToFollower:
		// Success
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for node to revert to follower")
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	assert.Equal(t, param.Follower, r.state, "expected state to revert to Follower after timeout")
}

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

func TestStartElection_StepsDownOnAppendEntries(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := storage.NewMockStorage(ctrl)
	mockTrans := transport.NewMockTransport(ctrl)
	peerIds := []int{2, 3}

	// 期望初始化调用
	mockStore.EXPECT().GetState().Return(param.HardState{CurrentTerm: 5}, nil).Times(1)
	r := NewRaft(1, peerIds, mockStore, nil, mockTrans, nil)
	r.state = param.Follower

	// 模拟选举开始
	gomock.InOrder(
		// 成为 Candidate，持久化状态
		mockStore.EXPECT().SetState(param.HardState{CurrentTerm: 6, VotedFor: 1}).Return(nil),
		// 获取日志信息
		mockStore.EXPECT().LastLogIndex().Return(uint64(10), nil),
		mockStore.EXPECT().GetEntry(uint64(10)).Return(&param.LogEntry{Term: 5, Index: 10}, nil),
	)
	// 期望发出投票请求 (允许任意次数，因为可能在收到 AE 前发出)
	mockTrans.EXPECT().SendRequestVote(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

	// 启动选举过程
	go r.startElection()
	time.Sleep(5 * time.Millisecond) // 等待选举协程启动

	// 模拟收到来自更高任期 Leader 的 AppendEntries
	argsAE := &param.AppendEntriesArgs{Term: 7, LeaderId: 2} // 任期为 7
	replyAE := &param.AppendEntriesReply{}
	// 期望收到 AE 后，调用 becomeFollower 持久化新状态
	mockStore.EXPECT().SetState(param.HardState{CurrentTerm: 7, VotedFor: math.MaxUint64}).Return(nil)

	err := r.AppendEntries(argsAE, replyAE)
	assert.NoError(t, err)

	// 验证状态
	r.mu.Lock()
	defer r.mu.Unlock()
	assert.Equal(t, param.Follower, r.state, "State should revert to Follower")
	assert.Equal(t, uint64(7), r.currentTerm, "Term should be updated to 7")
}
