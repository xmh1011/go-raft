package raft

import (
	"encoding/json"
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

// TestNewRaft_RecoveryState 测试 Raft 节点是否能从持久化存储中正确恢复状态。
func TestNewRaft_RecoveryState(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockStore := storage.NewMockStorage(ctrl)
	persistedState := param.HardState{CurrentTerm: 5, VotedFor: 2}
	mockStore.EXPECT().GetState().Return(persistedState, nil).Times(1)
	r := NewRaft(1, []int{2, 3}, mockStore, nil, nil, nil)
	assert.Equal(t, persistedState.CurrentTerm, r.currentTerm, "recovered term should match")
	assert.Equal(t, int(persistedState.VotedFor), r.votedFor, "recovered votedFor should match")
}

// TestSubmit_LeaderSuccess 测试 Leader 节点成功接收并提议一个新命令的场景。
func TestSubmit_LeaderSuccess(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := storage.NewMockStorage(ctrl)
	mockTrans := transport.NewMockTransport(ctrl)
	mockSM := storage.NewMockStateMachine(ctrl)
	peerIds := []int{2, 3}

	mockStore.EXPECT().GetState().Return(param.HardState{}, nil).Times(1)
	r := NewRaft(1, peerIds, mockStore, mockSM, mockTrans, nil)
	defer r.Stop()
	r.currentTerm = 2
	r.state = param.Leader
	lastLogIndex := uint64(5)
	for _, peerId := range peerIds {
		r.nextIndex[peerId] = lastLogIndex + 1
		r.matchIndex[peerId] = 0
	}

	command := "test-command"
	var wg sync.WaitGroup
	wg.Add(len(peerIds))

	gomock.InOrder(
		mockStore.EXPECT().LastLogIndex().Return(lastLogIndex, nil).Times(1),
		mockStore.EXPECT().AppendEntries(gomock.Any()).Return(nil).Times(1),
	)

	mockStore.EXPECT().FirstLogIndex().Return(uint64(1), nil).AnyTimes()
	mockStore.EXPECT().LastLogIndex().Return(lastLogIndex+1, nil).AnyTimes()
	mockStore.EXPECT().GetEntry(gomock.Any()).
		DoAndReturn(func(index uint64) (*param.LogEntry, error) {
			return &param.LogEntry{Term: 2, Index: index}, nil
		}).AnyTimes()
	mockSM.EXPECT().Apply(gomock.Any()).Return(nil).AnyTimes()

	mockTrans.EXPECT().SendAppendEntries(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(id string, args *param.AppendEntriesArgs, reply *param.AppendEntriesReply) error {
			reply.Success = true
			reply.Term = args.Term
			wg.Done()
			return nil
		}).Times(len(peerIds))

	index, term, ok := r.Submit(command)

	assert.True(t, ok, "Submit on leader should succeed")
	assert.Equal(t, uint64(6), index, "new log index should be 6")
	assert.Equal(t, uint64(2), term, "new log term should be 2")
	wg.Wait()
}

// TestSubmit_NotLeaderFail 测试非 Leader 节点拒绝提议新命令的场景。
func TestSubmit_NotLeaderFail(t *testing.T) {
	r := &Raft{state: param.Follower}
	_, _, ok := r.Submit("test-command")
	assert.False(t, ok, "Submit on follower should fail")
}

// TestChangeConfig_LeaderSuccess 测试 Leader 成功发起一次成员变更。
func TestChangeConfig_LeaderSuccess(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := storage.NewMockStorage(ctrl)
	mockTrans := transport.NewMockTransport(ctrl)
	mockSM := storage.NewMockStateMachine(ctrl)
	currentPeers := []int{2, 3}
	newPeers := []int{1, 2, 4, 5}
	allUniquePeers := map[int]struct{}{2: {}, 3: {}, 4: {}, 5: {}}

	mockStore.EXPECT().GetState().Return(param.HardState{}, nil).Times(1)
	r := NewRaft(1, currentPeers, mockStore, mockSM, mockTrans, nil)
	defer r.Stop()
	r.currentTerm = 3
	r.state = param.Leader
	lastLogIndex := uint64(10)
	for _, peerId := range currentPeers {
		r.nextIndex[peerId] = lastLogIndex + 1
	}
	for _, peerId := range newPeers {
		if _, ok := r.nextIndex[peerId]; !ok {
			r.nextIndex[peerId] = lastLogIndex + 1
		}
	}

	var wg sync.WaitGroup
	wg.Add(len(allUniquePeers))

	gomock.InOrder(
		mockStore.EXPECT().LastLogIndex().Return(lastLogIndex, nil).Times(1),
		mockStore.EXPECT().AppendEntries(gomock.Any()).Return(nil).Times(1),
	)

	mockStore.EXPECT().FirstLogIndex().Return(uint64(1), nil).AnyTimes()
	mockStore.EXPECT().LastLogIndex().Return(lastLogIndex+1, nil).AnyTimes()
	mockStore.EXPECT().GetEntry(gomock.Any()).
		DoAndReturn(func(index uint64) (*param.LogEntry, error) {
			return &param.LogEntry{Term: 3, Index: index}, nil
		}).AnyTimes()
	mockSM.EXPECT().Apply(gomock.Any()).Return(nil).AnyTimes()

	mockTrans.EXPECT().SendAppendEntries(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(id string, args *param.AppendEntriesArgs, reply *param.AppendEntriesReply) error {
			reply.Success = true
			reply.Term = args.Term
			wg.Done()
			return nil
		}).Times(len(allUniquePeers))

	index, term, ok := r.ChangeConfig(newPeers)

	assert.True(t, ok, "ChangeConfig on leader should succeed")
	assert.Equal(t, uint64(11), index, "new log index should be 11")
	assert.Equal(t, uint64(3), term, "new log term should be 3")
	assert.True(t, r.inJointConsensus, "leader should be in joint consensus after proposing config change")
	wg.Wait()
}

// TestChangeConfig_FailWhileInJointConsensus 测试当已经处于联合共识状态时，Leader 会拒绝新的成员变更请求。
func TestChangeConfig_FailWhileInJointConsensus(t *testing.T) {
	r := &Raft{
		state:            param.Leader,
		inJointConsensus: true,
	}
	_, _, ok := r.ChangeConfig([]int{1, 2, 3})
	assert.False(t, ok, "ChangeConfig should fail while in joint consensus")
}

// TestClientRequest_LeaderProcessesCommand 测试 Leader 成功处理客户端请求的场景。
func TestClientRequest_LeaderProcessesCommand(t *testing.T) {
	// --- Arrange ---
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := storage.NewMockStorage(ctrl)
	mockTrans := transport.NewMockTransport(ctrl)
	mockSM := storage.NewMockStateMachine(ctrl)
	commitChan := make(chan param.CommitEntry, 1)
	mockStore.EXPECT().GetState().Return(param.HardState{}, nil).Times(1)

	r := NewRaft(1, []int{2, 3}, mockStore, mockSM, mockTrans, commitChan)
	defer r.Stop()
	r.state = param.Leader
	r.currentTerm = 2
	// 初始化 nextIndex，避免触发快照逻辑
	r.nextIndex[2] = 6
	r.nextIndex[3] = 6

	args := &param.ClientArgs{ClientID: 123, SequenceNum: 1, Command: "test-command"}
	reply := &param.ClientReply{}

	// --- 设置 Mock 期望 ---
	gomock.InOrder(
		mockStore.EXPECT().LastLogIndex().Return(uint64(5), nil).Times(1),
		mockStore.EXPECT().AppendEntries(gomock.Any()).Return(nil).Times(1),
	)

	mockStore.EXPECT().FirstLogIndex().Return(uint64(1), nil).AnyTimes()
	mockStore.EXPECT().GetEntry(gomock.Any()).Return(&param.LogEntry{}, nil).AnyTimes()
	mockStore.EXPECT().LastLogIndex().Return(uint64(6), nil).AnyTimes()

	// 让 mockTrans 返回 Success=true，否则会造成日志刷屏
	mockTrans.EXPECT().SendAppendEntries(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(id string, args *param.AppendEntriesArgs, reply *param.AppendEntriesReply) error { // <--- 已更正：这里是 AppendEntriesReply
			reply.Success = true
			reply.Term = r.currentTerm
			return nil
		}).AnyTimes()

	// 为后台的 applyLogs goroutine 添加 mock 期望
	mockSM.EXPECT().Apply(gomock.Any()).Return("success-result").AnyTimes()

	// --- Act ---
	requestDone := make(chan struct{})
	go func() {
		err := r.ClientRequest(args, reply)
		assert.NoError(t, err)
		close(requestDone)
	}()

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

	select {
	case <-requestDone:
		// 成功
	case <-time.After(2 * time.Second): // 添加超时以防万一
		t.Fatal("TestClientRequest_LeaderProcessesCommand timed out")
	}

	// --- Assert ---
	assert.True(t, reply.Success, "expected Success to be true")
	assert.Equal(t, "success-result", reply.Result, "result should match")
	assert.Equal(t, args.SequenceNum, r.clientSessions[args.ClientID], "client session should be updated")
}

// TestClientRequest_NotLeader 测试当请求发往非 Leader 节点时的重定向行为。
func TestClientRequest_NotLeader(t *testing.T) {
	// Arrange
	r := &Raft{
		state:         param.Follower,
		knownLeaderID: 3, // 假设此节点知道 Leader 是 3
		mu:            sync.Mutex{},
	}
	// 这是一个“写”请求（默认）
	args := &param.ClientArgs{}
	reply := &param.ClientReply{}

	// Act
	err := r.ClientRequest(args, reply)
	assert.NoError(t, err)

	// Assert
	assert.True(t, reply.NotLeader, "expected NotLeader to be true")
	assert.Equal(t, 3, reply.LeaderHint, "LeaderHint should be 3")
}

// TestClientRequest_DuplicateRequest 测试 Leader 能够正确处理重复的客户端请求。
func TestClientRequest_DuplicateRequest(t *testing.T) {
	// --- Arrange ---
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockStore := storage.NewMockStorage(ctrl)

	r := &Raft{
		id:             1,
		state:          param.Leader,
		store:          mockStore,
		clientSessions: map[int64]int64{123: 5},
		mu:             sync.Mutex{},
	}
	// 客户端发送一个旧的（或相同的）序列号
	args := &param.ClientArgs{ClientID: 123, SequenceNum: 5}
	reply := &param.ClientReply{}

	// --- Act ---
	err := r.ClientRequest(args, reply)
	assert.NoError(t, err)

	// --- Assert ---
	assert.True(t, reply.Success, "expected Success to be true for duplicate request")
}

// TestWaitForAppliedLog_Timeout 测试 waitForAppliedLog 函数的超时逻辑
func TestWaitForAppliedLog_Timeout(t *testing.T) {
	// --- Arrange ---
	r := &Raft{
		notifyApply: make(map[uint64]chan any),
		mu:          sync.Mutex{},
	}
	testIndex := uint64(10)
	testTimeout := 50 * time.Millisecond // 设置一个较短的超时时间用于测试

	// --- Act ---
	// 调用 waitForAppliedLog，但不向对应的 channel 发送任何通知
	startTime := time.Now()
	result, ok := r.waitForAppliedLog(testIndex, testTimeout)
	duration := time.Since(startTime)

	// --- Assert ---
	assert.False(t, ok, "Expected waitForAppliedLog to return false on timeout")
	assert.Nil(t, result, "Expected result to be nil on timeout")
	// 验证实际等待时间约等于我们设置的超时时间
	assert.GreaterOrEqual(t, duration, testTimeout, "Duration should be at least the timeout")
	assert.Less(t, duration, testTimeout*2, "Duration should not be excessively longer than the timeout") // 允许一些误差

	// 验证超时的 channel 是否已从 map 中移除，防止内存泄漏
	r.mu.Lock()
	_, exists := r.notifyApply[testIndex]
	r.mu.Unlock()
	assert.False(t, exists, "Notify channel for timed out index should be removed from the map")
}

// TestRandomizedElectionTimeout 验证随机超时是否落在 [T, 2T) 区间内。
func TestRandomizedElectionTimeout(t *testing.T) {
	// 创建一个 Raft 实例以访问其上的常量
	r := &Raft{}

	for i := 0; i < 100; i++ {
		timeout := r.randomizedElectionTimeout()
		assert.GreaterOrEqual(t, timeout, electionTimeout, "Timeout should be >= base electionTimeout")
		assert.Less(t, timeout, 2*electionTimeout, "Timeout should be < 2 * base electionTimeout")
	}
}

// TestRun_FollowerStartsElectionOnTimeout 测试 Follower 在超时后会启动选举。
func TestRun_FollowerStartsElectionOnTimeout(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := storage.NewMockStorage(ctrl)
	mockTrans := transport.NewMockTransport(ctrl)
	// 期望初始化调用
	mockStore.EXPECT().GetState().Return(param.HardState{}, nil).Times(1)

	r := NewRaft(1, []int{2, 3}, mockStore, nil, mockTrans, nil)

	// 1. 将状态设为 Follower
	r.state = param.Follower
	r.currentTerm = 1
	// 2. 设置一个极短的、可预测的超时时间
	r.currentElectionTimeout = 5 * time.Millisecond
	r.electionResetEvent = time.Now()

	// 3. 期望：当选举超时时，Run() 循环会调用 startElection()。
	electionStartedChan := make(chan struct{})

	// --- Pre-Vote 阶段 ---
	// 1. 获取日志
	mockStore.EXPECT().LastLogIndex().Return(uint64(0), nil)
	// (日志为空，不需要 GetEntry)

	// --- Real Vote 阶段 ---
	gomock.InOrder(
		// 2. 成为 Candidate，持久化状态
		mockStore.EXPECT().SetState(param.HardState{CurrentTerm: 2, VotedFor: 1}).Return(nil).
			Do(func(any) {
				close(electionStartedChan) // 收到调用，发出信号
			}),
		// 3. startRealElection 还会获取日志信息
		mockStore.EXPECT().LastLogIndex().Return(uint64(0), nil),
	)

	// 4. 选举启动后会广播投票请求 (Pre-Vote 和 Real Vote)
	mockTrans.EXPECT().SendRequestVote(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(id string, args *param.RequestVoteArgs, reply *param.RequestVoteReply) error {
			if args.PreVote {
				reply.Term = args.Term
				reply.VoteGranted = true
			}
			return nil
		}).AnyTimes()

	// 5. 启动 Run() 循环
	go r.Run()
	defer r.Stop() // 确保测试结束时停止

	// 6. 等待选举开始的信号
	select {
	case <-electionStartedChan:
		// 测试通过
	case <-time.After(100 * time.Millisecond):
		t.Fatal("timed out waiting for election to start")
	}
}

// TestRun_LeaderDoesNotStartElection 测试 Leader 状态不会触发选举。
func TestRun_LeaderDoesNotStartElection(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := storage.NewMockStorage(ctrl)
	mockStore.EXPECT().GetState().Return(param.HardState{}, nil).Times(1)

	r := NewRaft(1, []int{2, 3}, mockStore, nil, nil, nil)
	// 1. 将状态设为 Leader
	r.state = param.Leader
	// 2. 设置一个极短的超时
	r.currentElectionTimeout = 5 * time.Millisecond
	r.electionResetEvent = time.Now()

	// 3. 期望：SetState 永远不应该被调用（因为 Leader 不会开始选举）
	// 如果被调用，gomock 会自动失败测试
	mockStore.EXPECT().SetState(gomock.Any()).Return(nil).Times(0)

	// 4. 启动 Run() 循环
	go r.Run()
	defer r.Stop()

	// 5. 等待一段时间（超过选举超时）
	time.Sleep(20 * time.Millisecond)

	// 6. 验证状态仍然是 Leader
	r.mu.Lock()
	state := r.state
	r.mu.Unlock()
	assert.Equal(t, param.Leader, state, "Leader state should not have changed")
}

// TestRun_StopShutsDownLoop 测试 Stop() 方法能正确关闭 Run() 循环。
func TestRun_StopShutsDownLoop(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := storage.NewMockStorage(ctrl)
	mockStore.EXPECT().GetState().Return(param.HardState{}, nil).Times(1)

	r := NewRaft(1, []int{2, 3}, mockStore, nil, nil, nil)

	// 启动 Run()
	go r.Run()

	// 立即调用 Stop()
	r.Stop()

	// 验证状态
	r.mu.Lock()
	assert.Equal(t, param.Dead, r.state, "State should be Dead after Stop()")
	r.mu.Unlock()

	// 验证 channel 是否关闭 (从已关闭的 channel 读取会立即返回)
	select {
	case <-r.shutdownChan:
		// 通道已按预期关闭
	default:
		t.Fatal("shutdownChan was not closed")
	}

	// 尝试再次 Stop (应该无操作)
	r.Stop()
}

// TestTimeoutResets 验证在所有必要情况下选举超时都会被重置。
func TestTimeoutResets(t *testing.T) {
	// helper function to create a raft instance for sub-tests
	newRaftForTest := func(t *testing.T) (*gomock.Controller, *storage.MockStorage, *Raft) {
		ctrl := gomock.NewController(t)
		mockStore := storage.NewMockStorage(ctrl)
		mockStore.EXPECT().GetState().Return(param.HardState{}, nil).Times(1)
		r := NewRaft(1, []int{2, 3}, mockStore, nil, nil, nil)
		return ctrl, mockStore, r
	}

	// 1. 测试收到心跳时 (handleTermAndHeartbeat)
	t.Run("OnHeartbeat", func(t *testing.T) {
		ctrl, _, r := newRaftForTest(t)
		defer ctrl.Finish()

		r.state = param.Follower
		r.currentTerm = 5
		r.currentElectionTimeout = 12345 // 设置一个已知的哨兵值

		// 模拟一个合法的心跳 RPC
		args := &param.AppendEntriesArgs{Term: 5, LeaderId: 2, PrevLogIndex: 0} // 确保 PrevLogIndex 为 0 以匹配
		reply := &param.AppendEntriesReply{}

		// checkLogConsistency 会被调用，但由于 PrevLogIndex 为 0，它会直接返回 true
		err := r.AppendEntries(args, reply)
		assert.NoError(t, err, "AppendEntries should not return error")

		assert.True(t, reply.Success, "Heartbeat should have been accepted")
		assert.NotEqual(t, 12345, r.currentElectionTimeout, "Timeout should be reset on heartbeat")
	})

	// 2. 测试投票时 (grantVote)
	t.Run("OnGrantVote", func(t *testing.T) {
		ctrl, mockStore, r := newRaftForTest(t)
		defer ctrl.Finish()

		// 模拟 grantVote 所需的调用
		mockStore.EXPECT().LastLogIndex().Return(uint64(0), nil)
		mockStore.EXPECT().SetState(param.HardState{CurrentTerm: 5, VotedFor: 2}).Return(nil)

		r.state = param.Follower
		r.currentTerm = 5
		r.votedFor = -1                  // 确保可以投票
		r.currentElectionTimeout = 12345 // 哨兵值

		// 模拟一个合法的投票 RPC
		args := &param.RequestVoteArgs{Term: 5, CandidateId: 2, LastLogIndex: 0, LastLogTerm: 0}
		reply := &param.RequestVoteReply{}
		err := r.RequestVote(args, reply)
		assert.NoError(t, err, "RequestVote should not return error")

		assert.True(t, reply.VoteGranted, "Vote should have been granted")
		assert.NotEqual(t, 12345, r.currentElectionTimeout, "Timeout should be reset on grantVote")
	})

	// 3. 测试成为 Follower 时 (becomeFollower)
	t.Run("OnBecomeFollower", func(t *testing.T) {
		ctrl, mockStore, r := newRaftForTest(t)
		defer ctrl.Finish()

		// 模拟 becomeFollower 时的 SetState
		mockStore.EXPECT().SetState(param.HardState{CurrentTerm: 6, VotedFor: math.MaxUint64}).Return(nil)

		r.state = param.Candidate
		r.currentTerm = 5
		r.currentElectionTimeout = 12345 // 哨兵值

		// 手动调用 (模拟在 RPC 处理器中被调用)
		r.mu.Lock()
		err := r.becomeFollower(6)
		assert.NoError(t, err)
		r.mu.Unlock()

		assert.Equal(t, param.Follower, r.state)
		assert.Equal(t, uint64(6), r.currentTerm)
		assert.NotEqual(t, 12345, r.currentElectionTimeout, "Timeout should be reset on becomeFollower")
	})
}

// helper function 构造一个 "get" 命令
func newGetCommand(t *testing.T, key string) []byte {
	cmd := param.KVCommand{Op: "get", Key: key}
	b, err := json.Marshal(cmd)
	assert.NoError(t, err)
	return b
}

// helper function 构造一个 "set" 命令
func newSetCommand(t *testing.T, key, value string) []byte {
	cmd := param.KVCommand{Op: "set", Key: key, Value: value}
	b, err := json.Marshal(cmd)
	assert.NoError(t, err)
	return b
}

// TestHandleLinearizableRead_Success 测试 ReadIndex 成功路径
func TestHandleLinearizableRead_Success(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockSM := storage.NewMockStateMachine(ctrl)
	mockStore := storage.NewMockStorage(ctrl)
	mockTrans := transport.NewMockTransport(ctrl)

	// 期望 NewRaft 内部的 GetState 调用
	mockStore.EXPECT().GetState().Return(param.HardState{}, nil).Times(1)

	r := NewRaft(1, []int{2, 3}, mockStore, mockSM, mockTrans, nil)
	r.state = param.Leader
	r.currentTerm = 1
	r.commitIndex = 10
	r.lastApplied = 10 // 状态机已同步
	r.lastAck = map[int]time.Time{
		2: time.Now(), // 租约有效
		3: time.Now(),
	}
	// 初始化 nextIndex，防止 confirmLeadership 中 panic
	r.nextIndex[2] = 11
	r.nextIndex[3] = 11

	cmd := param.KVCommand{Op: "get", Key: "testKey"}
	reply := &param.ClientReply{}

	// --- 设置 Mock 期望 ---
	// confirmLeadership 会调用 getLogTerm，进而调用 store.GetEntry
	// 假设 prevLogIndex 是 10
	mockStore.EXPECT().GetEntry(uint64(10)).Return(&param.LogEntry{Term: 1, Index: 10}, nil).AnyTimes()

	// confirmLeadership 会发送心跳
	mockTrans.EXPECT().SendAppendEntries(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(id string, args *param.AppendEntriesArgs, reply *param.AppendEntriesReply) error {
			reply.Term = 1
			reply.Success = true
			return nil
		}).AnyTimes()

	// 期望状态机被 Get
	mockSM.EXPECT().Get("testKey").Return("testValue", nil).Times(1)

	err := r.handleLinearizableRead(cmd, reply)
	assert.NoError(t, err)
	assert.True(t, reply.Success)
	assert.Equal(t, "testValue", reply.Result)
}

// TestClientRequest_ReadWriteBranching 测试 ClientRequest 是否能正确区分读写请求
func TestClientRequest_ReadWriteBranching(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := storage.NewMockStorage(ctrl)
	mockSM := storage.NewMockStateMachine(ctrl)
	mockTrans := transport.NewMockTransport(ctrl)

	// 这里必须保留带缓冲的 channel，防止死锁
	commitChan := make(chan param.CommitEntry, 10)

	mockStore.EXPECT().GetState().Return(param.HardState{}, nil).Times(1)
	r := NewRaft(1, []int{2, 3}, mockStore, mockSM, mockTrans, commitChan)
	defer r.Stop()

	r.state = param.Leader
	r.currentTerm = 1
	r.commitIndex = 1
	r.lastApplied = 1
	r.lastAck[2] = time.Now()
	r.lastAck[3] = time.Now()

	lastLogIndex := r.lastApplied
	for _, peerId := range r.peerIds {
		r.nextIndex[peerId] = lastLogIndex + 1
		r.matchIndex[peerId] = 0
	}

	// 1. 测试“读”请求 (get)
	t.Run("ReadRequest", func(t *testing.T) {
		getCmd := newGetCommand(t, "key1")
		args := &param.ClientArgs{Command: getCmd}
		reply := &param.ClientReply{}

		// --- 关键修改：不要使用 gomock.Any() ---
		// confirmLeadership 会检查 prevLogIndex (即 index 1)
		// 如果使用 Any()，它会拦截后续 WriteRequest 中对 index 2 的调用！
		mockStore.EXPECT().GetEntry(uint64(1)).Return(&param.LogEntry{Term: 1, Index: 1}, nil).AnyTimes()

		mockTrans.EXPECT().SendAppendEntries(gomock.Any(), gomock.Any(), gomock.Any()).
			DoAndReturn(func(id string, args *param.AppendEntriesArgs, reply *param.AppendEntriesReply) error {
				reply.Term = 1
				reply.Success = true
				return nil
			}).AnyTimes()

		mockStore.EXPECT().LastLogIndex().Times(0)
		mockSM.EXPECT().Get("key1").Return("value1", nil).Times(1)

		err := r.ClientRequest(args, reply)
		assert.NoError(t, err)
		assert.True(t, reply.Success)
		assert.Equal(t, "value1", reply.Result)
	})

	// 2. 测试“写”请求 (set)
	t.Run("WriteRequest", func(t *testing.T) {
		setCmd := newSetCommand(t, "key1", "value1")
		args := &param.ClientArgs{ClientID: 123, SequenceNum: 1, Command: setCmd}
		reply := &param.ClientReply{}

		// 1. 期望同步调用
		callLastLog1 := mockStore.EXPECT().LastLogIndex().Return(uint64(1), nil).Times(1)
		callAppend := mockStore.EXPECT().AppendEntries(gomock.Any()).Return(nil).Times(1).After(callLastLog1)

		// 2. 期望异步调用
		mockStore.EXPECT().LastLogIndex().Return(uint64(2), nil).AnyTimes().After(callAppend)
		mockStore.EXPECT().FirstLogIndex().Return(uint64(1), nil).AnyTimes()

		// 这里的期望现在可以正确匹配了，因为之前的 GetEntry(1) 不会拦截 GetEntry(2)
		mockStore.EXPECT().GetEntry(uint64(1)).Return(&param.LogEntry{Term: 1, Index: 1}, nil).AnyTimes()
		mockStore.EXPECT().GetEntry(uint64(2)).Return(&param.LogEntry{Command: setCmd, Term: 1, Index: 2}, nil).AnyTimes()

		mockSM.EXPECT().Apply(gomock.Any()).Return(nil).AnyTimes()

		mockTrans.EXPECT().SendAppendEntries(gomock.Any(), gomock.Any(), gomock.Any()).
			DoAndReturn(func(id string, args *param.AppendEntriesArgs, reply *param.AppendEntriesReply) error {
				reply.Success = true
				reply.Term = r.currentTerm
				return nil
			}).AnyTimes()

		mockSM.EXPECT().Get(gomock.Any()).Times(0)

		// --- 运行测试 ---
		requestDone := make(chan struct{})
		go func() {
			err := r.ClientRequest(args, reply)
			assert.NoError(t, err)
			close(requestDone)
		}()

		// 等待 Raft 逻辑完成
		select {
		case <-requestDone:
			// success
		case <-time.After(1 * time.Second): // 稍微增加一点超时时间以防万一
			t.Fatal("ClientRequest goroutine did not finish. Deadlock.")
		}

		assert.True(t, reply.Success)
	})
}

// TestHandleLinearizableRead_NotLeader 测试 Follower 拒绝读
func TestHandleLinearizableRead_NotLeader(t *testing.T) {
	r := &Raft{
		state:         param.Follower,
		knownLeaderID: 3,
		mu:            sync.Mutex{},
	}
	cmd := param.KVCommand{Op: "get", Key: "testKey"}
	reply := &param.ClientReply{}

	err := r.handleLinearizableRead(cmd, reply)
	assert.NoError(t, err)
	assert.False(t, reply.Success)
	assert.True(t, reply.NotLeader)
	assert.Equal(t, 3, reply.LeaderHint)
}

// TestHandleLinearizableRead_LeaseCheckFails 测试租约失败
func TestHandleLinearizableRead_LeaseCheckFails(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockStore := storage.NewMockStorage(ctrl)
	mockTrans := transport.NewMockTransport(ctrl)

	r := &Raft{
		id:          1,
		peerIds:     []int{2, 3, 4, 5}, // 5个节点，多数需要4个 (1 + 3)
		state:       param.Leader,
		currentTerm: 1,
		store:       mockStore,
		trans:       mockTrans,
		nextIndex:   map[int]uint64{2: 1, 3: 1, 4: 1, 5: 1}, // 初始化 nextIndex
		mu:          sync.Mutex{},
	}
	cmd := param.KVCommand{Op: "get", Key: "testKey"}
	reply := &param.ClientReply{}

	// Mock confirmLeadership
	// 假设所有节点都超时或拒绝
	mockStore.EXPECT().GetEntry(gomock.Any()).Return(&param.LogEntry{Term: 1, Index: 0}, nil).AnyTimes()
	mockTrans.EXPECT().SendAppendEntries(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(id string, args *param.AppendEntriesArgs, reply *param.AppendEntriesReply) error {
			// 模拟网络分区或超时
			return nil
		}).AnyTimes()

	err := r.handleLinearizableRead(cmd, reply)
	assert.NoError(t, err)
	assert.False(t, reply.Success)
	assert.True(t, reply.NotLeader, "Lease check fail should tell client it's not leader")
}

// TestHandleLinearizableRead_WaitsForApply 测试 ReadIndex 等待状态机
func TestHandleLinearizableRead_WaitsForApply(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockSM := storage.NewMockStateMachine(ctrl)
	mockStore := storage.NewMockStorage(ctrl)
	mockTrans := transport.NewMockTransport(ctrl)

	r := &Raft{
		id:           1,
		peerIds:      []int{2, 3},
		state:        param.Leader,
		currentTerm:  1,
		commitIndex:  10, // 读索引为 10
		lastApplied:  9,  // 状态机落后
		store:        mockStore,
		trans:        mockTrans,
		stateMachine: mockSM,
		nextIndex:    map[int]uint64{2: 11, 3: 11},
		mu:           sync.Mutex{},
	}
	r.lastAppliedCond = sync.NewCond(&r.mu)

	cmd := param.KVCommand{Op: "get", Key: "testKey"}
	reply := &param.ClientReply{}
	readDone := make(chan struct{})

	// Mock confirmLeadership
	mockStore.EXPECT().GetEntry(gomock.Any()).Return(&param.LogEntry{Term: 1, Index: 10}, nil).AnyTimes()
	mockTrans.EXPECT().SendAppendEntries(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(id string, args *param.AppendEntriesArgs, reply *param.AppendEntriesReply) error {
			reply.Term = 1
			reply.Success = true
			return nil
		}).AnyTimes()

	// 期望 Get 被调用，但在 lastApplied 更新后
	mockSM.EXPECT().Get("testKey").Return("testValue", nil).Times(1)

	go func() {
		err := r.handleLinearizableRead(cmd, reply)
		assert.NoError(t, err)
		close(readDone)
	}()

	// 1. 立刻检查，goroutine 应该在 Cond.Wait() 处阻塞
	time.Sleep(10 * time.Millisecond)
	assert.False(t, reply.Success, "Read should not have completed yet")

	// 2. 模拟 applyLogs 循环唤醒等待者
	r.mu.Lock()
	r.lastApplied = 10 // 状态机追赶上
	r.lastAppliedCond.Broadcast()
	r.mu.Unlock()

	// 3. 等待读操作完成
	select {
	case <-readDone:
		// success
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Read operation timed out after broadcast")
	}

	assert.True(t, reply.Success)
	assert.Equal(t, "testValue", reply.Result)
}
