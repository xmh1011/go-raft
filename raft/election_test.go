package raft

import (
	"math"
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
	mockSM := storage.NewMockStateMachine(ctrl)

	mockStore.EXPECT().GetState().Return(param.HardState{}, nil).Times(1)
	commitChan := make(chan param.CommitEntry, 10)
	r := NewRaft(1, peerIds, mockStore, mockSM, mockTrans, commitChan)
	defer r.Stop()

	r.state = param.Follower
	r.currentTerm = 5

	// --- Setup mocks for the entire election and leader transition flow ---

	// 1. Pre-Vote 阶段获取日志
	mockStore.EXPECT().LastLogIndex().Return(uint64(10), nil)
	mockStore.EXPECT().GetEntry(uint64(10)).Return(&param.LogEntry{Term: 5, Index: 10}, nil)

	// 2. 正式选举阶段
	gomock.InOrder(
		// 成为 Candidate
		mockStore.EXPECT().SetState(param.HardState{CurrentTerm: 6, VotedFor: 1}).Return(nil),
		// 获取日志
		mockStore.EXPECT().LastLogIndex().Return(uint64(10), nil),
		mockStore.EXPECT().GetEntry(uint64(10)).Return(&param.LogEntry{Term: 5, Index: 10}, nil),
	)

	// Mock RPCs: Pre-Vote 和 RequestVote 都成功
	mockTrans.EXPECT().SendRequestVote(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(id string, args *param.RequestVoteArgs, reply *param.RequestVoteReply) error {
			reply.Term = args.Term
			reply.VoteGranted = true
			return nil
		}).AnyTimes()

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

	// 1. Initialize candidate state (Manually trigger election flow)
	// 注意：这里我们不再手动调用 initializeCandidateState，而是调用 startElection
	// 但为了保持测试的可控性，我们可能需要等待状态变化。
	// 不过原测试是手动一步步调用的，现在 startElection 封装了所有步骤。
	// 我们这里直接调用 startElection 并等待结果。

	// 为了让测试能等待 Leader 状态，我们使用一个循环检查
	go r.startElection()

	// 等待变为 Leader
	success := false
	for i := 0; i < 20; i++ {
		r.mu.Lock()
		if r.state == param.Leader {
			success = true
			r.mu.Unlock()
			break
		}
		r.mu.Unlock()
		time.Sleep(50 * time.Millisecond)
	}
	assert.True(t, success, "Node failed to become leader")

	// 4. Manually transition to leader, BUT DO NOT START THE HEARTBEAT LOOP
	// 由于 startElection 会自动调用 transitionToLeader 并启动心跳，
	// 我们这里的测试逻辑其实已经变了。startElection 会启动心跳 goroutine。
	// 上面的 wg.Wait() 可能会因为心跳循环而一直被触发。
	// 但我们只 Add 了 2，所以只要收到 2 个心跳就会返回。

	wg.Wait()
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

	// 1. Pre-Vote 阶段获取日志
	mockStore.EXPECT().LastLogIndex().Return(uint64(10), nil)
	mockStore.EXPECT().GetEntry(uint64(10)).Return(&param.LogEntry{Term: 5, Index: 10}, nil)

	// 2. 正式选举阶段
	gomock.InOrder(
		// 成为 Candidate
		mockStore.EXPECT().SetState(param.HardState{CurrentTerm: 6, VotedFor: 1}).Return(nil),
		// 获取日志
		mockStore.EXPECT().LastLogIndex().Return(uint64(10), nil),
		mockStore.EXPECT().GetEntry(uint64(10)).Return(&param.LogEntry{Term: 5, Index: 10}, nil),
		// 选举超时，退回 Follower
		mockStore.EXPECT().SetState(param.HardState{CurrentTerm: 6, VotedFor: math.MaxUint64}).
			Do(func(interface{}) { close(revertedToFollower) }).Return(nil),
	)

	// Mock RPCs
	mockTrans.EXPECT().SendRequestVote(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(id string, args *param.RequestVoteArgs, reply *param.RequestVoteReply) error {
			if args.PreVote {
				// Pre-Vote 成功
				reply.Term = args.Term
				reply.VoteGranted = true
			} else {
				// 正式选举失败
				reply.Term = args.Term
				reply.VoteGranted = false
			}
			return nil
		}).AnyTimes()

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
	args := param.NewRequestVoteArgs(5, 1, uint64(10), 5, false)
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
	args := param.NewRequestVoteArgs(5, 1, uint64(9), 5, false)
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
	args := param.NewRequestVoteArgs(6, 1, uint64(10), 5, false)
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
	r.currentTerm = 5

	candidateStateReached := make(chan struct{})

	// 1. Pre-Vote 阶段获取日志
	mockStore.EXPECT().LastLogIndex().Return(uint64(10), nil)
	mockStore.EXPECT().GetEntry(uint64(10)).Return(&param.LogEntry{Term: 5, Index: 10}, nil)

	// 模拟选举开始
	gomock.InOrder(
		// 成为 Candidate，持久化状态
		mockStore.EXPECT().SetState(param.HardState{CurrentTerm: 6, VotedFor: 1}).Do(func(_ interface{}) {
			close(candidateStateReached)
		}).Return(nil),
		// 获取日志信息
		mockStore.EXPECT().LastLogIndex().Return(uint64(10), nil),
		mockStore.EXPECT().GetEntry(uint64(10)).Return(&param.LogEntry{Term: 5, Index: 10}, nil),
		// 期望收到 AE 后，调用 becomeFollower 持久化新状态
		mockStore.EXPECT().SetState(param.HardState{CurrentTerm: 7, VotedFor: math.MaxUint64}).Return(nil),
	)

	// 期望发出投票请求 (Pre-Vote 成功)
	mockTrans.EXPECT().SendRequestVote(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(id string, args *param.RequestVoteArgs, reply *param.RequestVoteReply) error {
			if args.PreVote {
				reply.Term = args.Term
				reply.VoteGranted = true
			}
			return nil
		}).AnyTimes()

	// 启动选举过程
	go r.startElection()

	// 等待进入 Candidate 状态
	select {
	case <-candidateStateReached:
	case <-time.After(1 * time.Second):
		t.Fatal("timed out waiting for candidate state")
	}

	// 模拟收到来自更高任期 Leader 的 AppendEntries
	argsAE := &param.AppendEntriesArgs{Term: 7, LeaderId: 2, PrevLogIndex: 0} // 任期为 7, PrevLogIndex: 0
	replyAE := &param.AppendEntriesReply{}

	err := r.AppendEntries(argsAE, replyAE)
	assert.NoError(t, err)

	// 验证状态
	r.mu.Lock()
	defer r.mu.Unlock()
	assert.Equal(t, param.Follower, r.state, "State should revert to Follower")
	assert.Equal(t, uint64(7), r.currentTerm, "Term should be updated to 7")
}
