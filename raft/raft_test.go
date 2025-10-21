package raft

import (
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
	mockStore.EXPECT().GetEntry(gomock.Any()).Return(&param.LogEntry{Term: 2}, nil).AnyTimes()
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
	mockStore.EXPECT().GetEntry(gomock.Any()).Return(&param.LogEntry{Term: 3}, nil).AnyTimes()
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
	go func() {
		err := r.ClientRequest(args, reply)
		assert.NoError(t, err)
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

	// 等待 handler 处理完通知
	time.Sleep(50 * time.Millisecond)

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
