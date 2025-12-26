package raft

import (
	"errors"
	"math"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/xmh1011/go-raft/param"
	"github.com/xmh1011/go-raft/storage"
	"github.com/xmh1011/go-raft/transport"
)

// TestTakeSnapshot_Success 测试在满足条件时，成功创建并保存快照的完整流程。
func TestTakeSnapshot_Success(t *testing.T) {
	// --- Arrange: 准备阶段 ---
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	// 创建所有需要的 mock 对象
	mockStore := storage.NewMockStorage(ctrl)
	mockSM := storage.NewMockStateMachine(ctrl)
	mockStore.EXPECT().GetState().Return(param.HardState{}, nil).Times(1)

	// 创建被测试的 Raft 实例，注入 mock 依赖
	r := NewRaft(1, []int{2, 3}, mockStore, mockSM, nil, nil)
	r.lastApplied = 100 // 假设状态机已应用到索引 100
	logSizeThreshold := 1000

	// 用于同步异步操作的 channel
	done := make(chan struct{})

	// --- 设置 Mock 期望 (Expectations) ---
	gomock.InOrder(
		mockStore.EXPECT().LogSize().Return(logSizeThreshold+1, nil).Times(1),
		mockStore.EXPECT().GetEntry(uint64(100)).Return(&param.LogEntry{Term: 5, Index: 100}, nil).Times(1),
		mockSM.EXPECT().GetSnapshot().Return([]byte("test snapshot data"), nil).Times(1),

		// 期望4: 保存包含正确元数据和数据的快照
		mockStore.EXPECT().SaveSnapshot(gomock.Any()).DoAndReturn(func(snapshot *param.Snapshot) error {
			assert.Equal(t, uint64(100), snapshot.LastIncludedIndex, "snapshot LastIncludedIndex should be 100")
			assert.Equal(t, uint64(5), snapshot.LastIncludedTerm, "snapshot LastIncludedTerm should be 5")
			assert.Equal(t, "test snapshot data", string(snapshot.Data), "snapshot data should match")
			return nil
		}).Times(1),

		// 期望5: 压缩日志
		// 这是异步操作的最后一步，调用完成后关闭 done channel 通知测试结束
		mockStore.EXPECT().CompactLog(uint64(100)).Return(nil).Do(func(_ uint64) {
			close(done)
		}).Times(1),
	)

	// --- Act: 执行阶段 ---
	r.TakeSnapshot(logSizeThreshold)

	// 等待异步操作完成
	select {
	case <-done:
		// 成功
	case <-time.After(1 * time.Second):
		t.Fatal("timeout waiting for snapshot async operations")
	}
}

// TestInstallSnapshot_Success 测试 Follower 成功安装 Leader 发来的快照。
func TestInstallSnapshot_Success(t *testing.T) {
	// --- Arrange ---
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := storage.NewMockStorage(ctrl)
	mockSM := storage.NewMockStateMachine(ctrl)
	mockStore.EXPECT().GetState().Return(param.HardState{}, nil).Times(1)

	r := NewRaft(2, []int{1, 3}, mockStore, mockSM, nil, nil)
	r.currentTerm = 5

	snapshotData := []byte("snapshot data from leader")
	args := param.NewInstallSnapshotArgs(5, 1, 200, 4, snapshotData)
	reply := &param.InstallSnapshotReply{}

	// --- 设置 Mock 期望 ---
	gomock.InOrder(
		mockStore.EXPECT().SaveSnapshot(gomock.Any()).Return(nil).Times(1),
		mockStore.EXPECT().CompactLog(uint64(200)).Return(nil).Times(1),
		mockSM.EXPECT().ApplySnapshot(snapshotData).Return(nil).Times(1),
	)

	// --- Act ---
	err := r.InstallSnapshot(args, reply)
	assert.NoError(t, err, "InstallSnapshot should not return error")

	// --- Assert ---
	// 验证 Raft 内部状态是否被正确更新
	assert.Equal(t, uint64(200), r.lastApplied, "lastApplied should be updated to 200")
	assert.Equal(t, uint64(200), r.commitIndex, "commitIndex should be updated to 200")
}

// TestInstallSnapshot_StaleTerm 测试当 Leader 的任期落后时，Follower 拒绝安装快照。
func TestInstallSnapshot_StaleTerm(t *testing.T) {
	// --- Arrange ---
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	// 在这个测试中，我们不期望 stateMachine 或 storage 被调用
	mockStore := storage.NewMockStorage(ctrl)
	mockSM := storage.NewMockStateMachine(ctrl)
	mockStore.EXPECT().GetState().Return(param.HardState{}, nil).Times(1)

	r := NewRaft(2, []int{1, 3}, mockStore, mockSM, nil, nil)
	r.currentTerm = 6 // 本地任期更高

	args := &param.InstallSnapshotArgs{
		Term: 5, // Leader 任期落后
	}
	reply := &param.InstallSnapshotReply{}

	// --- Act ---
	err := r.InstallSnapshot(args, reply)
	assert.NoError(t, err, "InstallSnapshot should not return error")

	// --- Assert ---
	// 验证 reply 中的任期是否被正确设置为本地的更高任期。
	assert.Equal(t, uint64(6), reply.Term, "reply term should be 6")
}

// TestSendSnapshot_Success 测试 Leader 成功向 Follower 发送快照。
func TestSendSnapshot_Success(t *testing.T) {
	// --- Arrange ---
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := storage.NewMockStorage(ctrl)
	mockStore.EXPECT().GetState().Return(param.HardState{}, nil).Times(1)
	mockTrans := transport.NewMockTransport(ctrl)
	peerId := 2
	r := NewRaft(1, []int{2, 3}, mockStore, nil, mockTrans, nil)
	r.currentTerm = 5
	r.state = param.Leader

	snapshotToSend := param.NewSnapshot(150, 151, []byte("snapshot data"))

	// --- 设置 Mock 期望 ---
	mockStore.EXPECT().ReadSnapshot().Return(snapshotToSend, nil).Times(1)
	mockTrans.EXPECT().SendInstallSnapshot(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(id string, args *param.InstallSnapshotArgs, reply *param.InstallSnapshotReply) error {
			reply.Term = 5
			return nil
		}).Times(1)

	// --- Act ---
	r.sendSnapshot(peerId)

	// --- Assert ---
	assert.Equal(t, uint64(151), r.nextIndex[peerId], "nextIndex should be 151")
	assert.Equal(t, uint64(150), r.matchIndex[peerId], "matchIndex should be 150")
}

// TestTakeSnapshot_LogSizeBelowThreshold 测试当日志大小未达到阈值时，不应触发快照。
func TestTakeSnapshot_LogSizeBelowThreshold(t *testing.T) {
	// --- Arrange ---
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := storage.NewMockStorage(ctrl)
	mockSM := storage.NewMockStateMachine(ctrl)
	mockStore.EXPECT().GetState().Return(param.HardState{}, nil).Times(1)

	r := NewRaft(1, []int{2, 3}, mockStore, mockSM, nil, nil)
	logSizeThreshold := 1000

	// --- 设置 Mock 期望 ---
	// 期望: LogSize() 方法被调用一次，但返回一个小于阈值的日志大小。
	mockStore.EXPECT().LogSize().Return(logSizeThreshold-1, nil).Times(1)

	// --- Act ---
	r.TakeSnapshot(logSizeThreshold)
}

// TestTakeSnapshot_FailsOnSaveError 测试当持久化快照到存储时发生错误，流程应中止。
func TestTakeSnapshot_FailsOnSaveError(t *testing.T) {
	// --- Arrange ---
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := storage.NewMockStorage(ctrl)
	mockSM := storage.NewMockStateMachine(ctrl)
	mockStore.EXPECT().GetState().Return(param.HardState{}, nil).Times(1)

	r := NewRaft(1, []int{2, 3}, mockStore, mockSM, nil, nil)
	r.lastApplied = 100
	logSizeThreshold := 1000

	// 用于同步异步操作的 channel
	done := make(chan struct{})

	// --- 设置 Mock 期望 ---
	gomock.InOrder(
		mockStore.EXPECT().LogSize().Return(logSizeThreshold+1, nil),
		mockStore.EXPECT().GetEntry(uint64(100)).Return(&param.LogEntry{Term: 5, Index: 100}, nil),
		mockSM.EXPECT().GetSnapshot().Return([]byte("test snapshot data"), nil),
		// 期望: SaveSnapshot 被调用，但我们让它模拟一个错误返回。
		// 此时流程应该中止，所以在这里关闭 done channel
		mockStore.EXPECT().SaveSnapshot(gomock.Any()).Do(func(_ *param.Snapshot) {
			close(done)
		}).Return(errors.New("disk is full")),
	)

	// --- Act ---
	r.TakeSnapshot(logSizeThreshold)

	// 等待异步操作完成
	select {
	case <-done:
		// 成功
	case <-time.After(1 * time.Second):
		t.Fatal("timeout waiting for SaveSnapshot")
	}
}

// TestInstallSnapshot_FailsOnApplyError 测试当状态机应用快照失败时，流程应中止。
func TestInstallSnapshot_FailsOnApplyError(t *testing.T) {
	// --- Arrange ---
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := storage.NewMockStorage(ctrl)
	mockSM := storage.NewMockStateMachine(ctrl)
	mockStore.EXPECT().GetState().Return(param.HardState{}, nil).Times(1)

	r := NewRaft(1, []int{2, 3}, mockStore, mockSM, nil, nil)
	r.currentTerm = 5
	r.lastApplied = 50 // 假设一个旧的 applied 索引

	snapshotData := []byte("corrupted snapshot data")
	args := &param.InstallSnapshotArgs{
		Term: 5, LastIncludedIndex: 200, Data: snapshotData,
	}
	reply := &param.InstallSnapshotReply{}

	// --- 设置 Mock 期望 ---
	// 期望: ApplySnapshot 被调用，但返回一个错误。
	gomock.InOrder(
		// 期望1: 源码先调用 persistSnapshot，它内部先调用 SaveSnapshot
		mockStore.EXPECT().SaveSnapshot(gomock.Any()).Return(nil).Times(1),

		// 期望2: 然后调用 CompactLog
		mockStore.EXPECT().CompactLog(uint64(200)).Return(nil).Times(1),

		// 期望3: 最后，源码调用 ApplySnapshot，我们让它模拟一个失败。
		mockSM.EXPECT().ApplySnapshot(snapshotData).Return(errors.New("failed to decode snapshot")).Times(1),
	)

	// --- Act ---
	err := r.InstallSnapshot(args, reply)
	assert.Error(t, err, "InstallSnapshot should return an error when the state machine fails")
	assert.Equal(t, err, errors.New("failed to decode snapshot"))

	// --- Assert ---
	// 验证 Raft 的内部状态没有因为失败的操作而被错误地更新。
	assert.Equal(t, uint64(50), r.lastApplied, "lastApplied should remain 50 on failure")
}

// TestSendSnapshot_StepsDownOnHigherTerm 测试当 Leader 发送快照后，收到更高任期的响应，应立即转为 Follower。
func TestSendSnapshot_StepsDownOnHigherTerm(t *testing.T) {
	// --- Arrange ---
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := storage.NewMockStorage(ctrl)
	mockTrans := transport.NewMockTransport(ctrl)
	peerId := 2

	mockStore.EXPECT().GetState().Return(param.HardState{CurrentTerm: 5}, nil).Times(1)
	r := NewRaft(1, []int{2, 3}, mockStore, nil, mockTrans, nil)
	r.state = param.Leader
	r.currentTerm = 5

	snapshotToSend := param.NewSnapshot(150, 4, []byte("test data"))

	// --- 设置 Mock 期望 ---
	gomock.InOrder(
		// 期望1: 读取快照
		mockStore.EXPECT().ReadSnapshot().Return(snapshotToSend, nil),
		// 期望2: 发送快照RPC，但对方返回一个更高的任期号
		mockTrans.EXPECT().SendInstallSnapshot(gomock.Any(), gomock.Any(), gomock.Any()).
			DoAndReturn(func(id string, args *param.InstallSnapshotArgs, reply *param.InstallSnapshotReply) error {
				reply.Term = 6 // 返回一个更高的任期
				return nil
			}),
		// 期望3: Leader 发现更高任期后，会调用 SetState 来持久化自己的新状态。
		mockStore.EXPECT().SetState(param.HardState{CurrentTerm: 6, VotedFor: math.MaxUint64}).Return(nil),
	)

	// --- Act ---
	r.sendSnapshot(peerId)

	// --- Assert ---
	// 验证 Leader 是否已转为 Follower
	r.mu.Lock()
	defer r.mu.Unlock()
	assert.Equal(t, param.Follower, r.state, "state should become Follower")
	assert.Equal(t, uint64(6), r.currentTerm, "term should become 6")
}

// TestProcessSnapshotReply_UpdatesLastAck 测试 Leader 在处理快照响应时更新 lastAck
func TestProcessSnapshotReply_UpdatesLastAck(t *testing.T) {
	peerId := 2
	savedTerm := uint64(5)
	snapshotIndex := uint64(100)

	// --- 变更开始 ---
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockStore := storage.NewMockStorage(ctrl)
	mockTrans := transport.NewMockTransport(ctrl)

	// 期望 NewRaft 内部的 GetState 调用
	mockStore.EXPECT().GetState().Return(param.HardState{}, nil).Times(1)
	// 确保 commitChan 被初始化
	commitChan := make(chan param.CommitEntry, 1)

	r := NewRaft(1, []int{peerId}, mockStore, nil, mockTrans, commitChan)
	r.state = param.Leader
	r.currentTerm = savedTerm
	// --- 变更结束 ---

	pastTime := time.Now().Add(-1 * time.Second)
	r.lastAck[peerId] = pastTime

	// 模拟一个成功的响应
	reply := &param.InstallSnapshotReply{Term: savedTerm}

	r.processSnapshotReply(peerId, reply, snapshotIndex, savedTerm)

	// 为了安全地断言，我们在这里重新获取锁
	r.mu.Lock()
	defer r.mu.Unlock()
	assert.True(t, r.lastAck[peerId].After(pastTime), "lastAck should be updated on successful snapshot reply")
	// 同时验证它是否也正确更新了 nextIndex/matchIndex
	assert.Equal(t, snapshotIndex+1, r.nextIndex[peerId])
	assert.Equal(t, snapshotIndex, r.matchIndex[peerId])
}
