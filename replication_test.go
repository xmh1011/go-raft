package raft

import (
	"strconv"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/xmh1011/go-raft/param"
	"github.com/xmh1011/go-raft/storage"
	"github.com/xmh1011/go-raft/transport"
)

// TestDetermineReplicationAction_ShouldSendSnapshot 测试当 Follower 落后太多时，决策函数是否正确地决定发送快照。
func TestDetermineReplicationAction_ShouldSendSnapshot(t *testing.T) {
	// --- Arrange ---
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockStore := storage.NewMockStorage(ctrl)

	r := &Raft{
		id:    1,
		state: param.Leader,
		store: mockStore,
		// 假设 peer 2 需要的下一条日志是索引 5
		nextIndex: map[int]uint64{2: 5},
	}

	// --- 设置 Mock 期望 ---
	// 我们让存储层报告说，它的第一条日志的索引已经是 10 了。
	// 这意味着索引 5 的日志已经被压缩。
	mockStore.EXPECT().FirstLogIndex().Return(uint64(10), nil).Times(1)

	// --- Act ---
	action := r.determineReplicationAction(2)

	// --- Assert ---
	assert.Equal(t, actionSendSnapshot, action, "expected action to be actionSendSnapshot")
}

// TestDetermineReplicationAction_ShouldSendLogs 测试在常规情况下，决策函数是否正确地决定发送日志。
func TestDetermineReplicationAction_ShouldSendLogs(t *testing.T) {
	// --- Arrange ---
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockStore := storage.NewMockStorage(ctrl)

	r := &Raft{
		id:    1,
		state: param.Leader,
		store: mockStore,
		// 假设 peer 2 需要的下一条日志是索引 10
		nextIndex: map[int]uint64{2: 10},
	}

	// --- 设置 Mock 期望 ---
	// 存储层报告第一条日志索引是 5，Leader 拥有 Peer 需要的日志。
	mockStore.EXPECT().FirstLogIndex().Return(uint64(5), nil).Times(1)

	// --- Act ---
	action := r.determineReplicationAction(2)

	// --- Assert ---
	assert.Equal(t, actionSendLogs, action, "expected action to be actionSendLogs")
}

// TestDetermineReplicationAction_NotLeader 测试当节点不是 Leader 时，决策函数是否决定什么都不做。
func TestDetermineReplicationAction_NotLeader(t *testing.T) {
	// Arrange
	r := &Raft{
		id:    1,
		state: param.Follower, // 关键：当前节点不是 Leader
	}
	// Act
	action := r.determineReplicationAction(2)
	// Assert
	assert.Equal(t, actionDoNothing, action, "expected action to be actionDoNothing when not leader")
}

// TestReplicateLogsToPeer_Success 测试成功复制日志到 Follower 的场景。
func TestReplicateLogsToPeer_Success(t *testing.T) {
	// --- Arrange ---
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := storage.NewMockStorage(ctrl)
	mockTrans := transport.NewMockTransport(ctrl)
	peerId := 2

	commitChan := make(chan param.CommitEntry, 1)

	// 期望 NewRaft 调用 GetState
	mockStore.EXPECT().GetState().Return(param.HardState{}, nil).Times(1)

	r := NewRaft(1, []int{peerId}, mockStore, nil, mockTrans, commitChan)
	r.state = param.Leader
	r.currentTerm = 5
	r.commitIndex = 10
	r.lastApplied = 10

	// --- 修正点：必须为 Leader 的 follower 状态进行初始化 ---
	// 测试场景设定：我们要从索引 11 开始向 peer 2 复制日志。
	// 这意味着 peer 2 的 nextIndex 应该是 11，而已匹配的索引是 10。
	r.nextIndex[peerId] = 11
	r.matchIndex[peerId] = 10

	// --- 设置 Mock 期望 ---
	gomock.InOrder(
		// 期望1: prepareAppendEntriesArgs 调用 GetEntry(10) 来获取 prevLogTerm
		mockStore.EXPECT().GetEntry(uint64(10)).Return(&param.LogEntry{Term: 5, Index: 10}, nil).Times(1),

		// 期望2: prepareAppendEntriesArgs 获取要发送的日志范围
		mockStore.EXPECT().LastLogIndex().Return(uint64(11), nil).Times(1),
		mockStore.EXPECT().GetEntry(uint64(11)).Return(&param.LogEntry{Command: "test", Term: 5, Index: 11}, nil).Times(1),

		// 期望3: RPC 被发送，并且返回成功
		mockTrans.EXPECT().SendAppendEntries(strconv.Itoa(peerId), gomock.Any(), gomock.Any()).
			DoAndReturn(func(id string, args *param.AppendEntriesArgs, reply *param.AppendEntriesReply) error {
				reply.Term = 5
				reply.Success = true
				return nil
			}).Times(1),
	)

	// 期望4: 成功后，updateCommitIndex 会被调用
	mockStore.EXPECT().LastLogIndex().Return(uint64(11), nil).AnyTimes()
	mockStore.EXPECT().GetEntry(uint64(11)).Return(&param.LogEntry{Term: 5, Index: 11}, nil).AnyTimes()

	// 期望5: applyLogs 会被调用，并获取要应用的日志
	mockStore.EXPECT().GetEntry(uint64(11)).Return(&param.LogEntry{Command: "test", Term: 5, Index: 11}, nil).AnyTimes()

	// --- Act ---
	r.replicateLogsToPeer(peerId)

	// --- Assert ---
	select {
	case entry := <-commitChan:
		assert.Equal(t, uint64(11), entry.Index, "expected entry with index 11 to be committed")
	case <-time.After(100 * time.Millisecond):
		t.Fatal("timed out waiting for log to be applied")
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	assert.Equal(t, uint64(12), r.nextIndex[peerId], "nextIndex should be 12")
	assert.Equal(t, uint64(11), r.matchIndex[peerId], "matchIndex should be 11")
	assert.Equal(t, uint64(11), r.commitIndex, "commitIndex should be advanced to 11")
	assert.Equal(t, uint64(11), r.lastApplied, "lastApplied should be advanced to 11")
}

// TestReplicateLogsToPeer_FollowerRejects 测试当 Follower 因日志不一致而拒绝时，Leader 是否正确回退 nextIndex。
func TestReplicateLogsToPeer_FollowerRejects(t *testing.T) {
	// --- Arrange ---
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := storage.NewMockStorage(ctrl)
	mockTrans := transport.NewMockTransport(ctrl)
	peerId := 2

	mockStore.EXPECT().GetState().Return(param.HardState{}, nil).Times(1)

	r := NewRaft(1, []int{peerId}, mockStore, nil, mockTrans, nil)
	r.state = param.Leader
	r.currentTerm = 5
	r.nextIndex[peerId] = 11

	// --- 设置 Mock 期望 ---
	mockStore.EXPECT().LastLogIndex().Return(uint64(11), nil).AnyTimes()
	mockStore.EXPECT().GetEntry(gomock.Any()).Return(&param.LogEntry{Term: 5}, nil).AnyTimes()
	mockStore.EXPECT().FirstLogIndex().Return(uint64(1), nil).AnyTimes()

	// 期望 RPC 被发送，但返回失败和我们指定的冲突信息。
	mockTrans.EXPECT().SendAppendEntries(strconv.Itoa(peerId), gomock.Any(), gomock.Any()).
		DoAndReturn(func(id string, args *param.AppendEntriesArgs, reply *param.AppendEntriesReply) error {
			reply.Term = 5
			reply.Success = false
			reply.ConflictIndex = 8 // 每次都返回我们期望的冲突索引
			return nil
		}).AnyTimes()

	// --- Act ---
	r.replicateLogsToPeer(peerId)

	// --- Assert ---
	// 短暂等待，让至少一次 RPC 调用和响应处理完成。
	time.Sleep(50 * time.Millisecond)

	r.mu.Lock()
	defer r.mu.Unlock()

	// 验证 nextIndex 是否被正确回退到 Follower 建议的索引 8
	assert.Equal(t, uint64(8), r.nextIndex[peerId], "nextIndex should be backed off to 8")
}
