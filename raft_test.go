package raft

import (
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"

	"github.com/xmh1011/go-raft/param"
	"github.com/xmh1011/go-raft/storage"
	"github.com/xmh1011/go-raft/transport"
)

// TestNewRaft_RecoveryState 测试 Raft 节点是否能从持久化存储中正确恢复状态。
func TestNewRaft_RecoveryState(t *testing.T) {
	// --- Arrange ---
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := storage.NewMockStorage(ctrl)

	// 模拟存储中已有一个持久化的状态。
	persistedState := param.HardState{CurrentTerm: 5, VotedFor: 2}
	mockStore.EXPECT().GetState().Return(persistedState, nil).Times(1)

	// --- Act ---
	r := NewRaft(1, []int{2, 3}, mockStore, nil, nil, nil)

	// --- Assert ---
	assert.Equal(t, persistedState.CurrentTerm, r.currentTerm, "recovered term should match")
	assert.Equal(t, int(persistedState.VotedFor), r.votedFor, "recovered votedFor should match")
}

// TestSubmit_LeaderSuccess 测试 Leader 节点成功接收并提议一个新命令的场景。
func TestSubmit_LeaderSuccess(t *testing.T) {
	// --- Arrange ---
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := storage.NewMockStorage(ctrl)
	mockTrans := transport.NewMockTransport(ctrl)
	mockStore.EXPECT().GetState().Return(param.HardState{}, nil).Times(1)

	r := NewRaft(1, []int{2, 3}, mockStore, nil, mockTrans, nil)
	r.state = param.Leader // 将节点设置为 Leader
	r.currentTerm = 2

	command := "test-command"

	// --- 设置 Mock 期望 ---
	// 期望 proposeToLog 会调用 LastLogIndex 和 AppendEntries
	gomock.InOrder(
		mockStore.EXPECT().LastLogIndex().Return(uint64(5), nil),
		mockStore.EXPECT().AppendEntries(gomock.Any()).DoAndReturn(func(entries []param.LogEntry) error {
			if len(entries) != 1 || entries[0].Command != command {
				t.Error("AppendEntries called with incorrect command")
			}
			return nil
		}),
	)

	// 为后台的 sendAppendEntries 设置通用的وقعات
	mockStore.EXPECT().FirstLogIndex().Return(uint64(1), nil).AnyTimes()
	mockTrans.EXPECT().SendAppendEntries(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

	// --- Act ---
	index, term, ok := r.Submit(command)

	// --- Assert ---
	assert.True(t, ok, "Submit on leader should succeed")
	assert.Equal(t, uint64(6), index, "new log index should be 6")
	assert.Equal(t, uint64(2), term, "new log term should be 2")
}

// TestSubmit_NotLeaderFail 测试非 Leader 节点拒绝提议新命令的场景。
func TestSubmit_NotLeaderFail(t *testing.T) {
	// Arrange
	r := &Raft{state: param.Follower} // 节点是 Follower
	// Act
	_, _, ok := r.Submit("test-command")
	// Assert
	assert.False(t, ok, "Submit on follower should fail")
}

// TestChangeConfig_LeaderSuccess 测试 Leader 成功发起一次成员变更。
func TestChangeConfig_LeaderSuccess(t *testing.T) {
	// --- Arrange ---
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := storage.NewMockStorage(ctrl)
	mockTrans := transport.NewMockTransport(ctrl)
	mockStore.EXPECT().GetState().Return(param.HardState{}, nil).Times(1)

	r := NewRaft(1, []int{2, 3}, mockStore, nil, mockTrans, nil)
	r.state = param.Leader
	r.currentTerm = 3

	newPeers := []int{1, 2, 4, 5}

	// --- 设置 Mock 期望 ---
	gomock.InOrder(
		mockStore.EXPECT().LastLogIndex().Return(uint64(10), nil),
		mockStore.EXPECT().AppendEntries(gomock.Any()).DoAndReturn(func(entries []param.LogEntry) error {
			// 验证写入日志的命令是否是正确的配置变更命令
			cmd, ok := entries[0].Command.(param.ConfigChangeCommand)
			if !ok {
				t.Fatal("expected command to be ConfigChangeCommand")
			}
			if len(cmd.NewPeerIDs) != len(newPeers) {
				t.Errorf("ConfigChangeCommand has incorrect peer list")
			}
			return nil
		}),
	)

	// 为后台广播设置通用وقعات
	mockStore.EXPECT().FirstLogIndex().Return(uint64(1), nil).AnyTimes()
	mockTrans.EXPECT().SendAppendEntries(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

	// --- Act ---
	index, term, ok := r.ChangeConfig(newPeers)

	// --- Assert ---
	assert.True(t, ok, "ChangeConfig on leader should succeed")
	assert.Equal(t, uint64(11), index, "new log index should be 11")
	assert.Equal(t, uint64(3), term, "new log term should be 3")
	assert.True(t, r.inJointConsensus, "leader should be in joint consensus after proposing config change")
	assert.Equal(t, len(newPeers), len(r.newPeerIds), "newPeerIds should be updated correctly")
}

// TestChangeConfig_FailWhileInJointConsensus 测试当已经处于联合共识状态时，Leader 会拒绝新的成员变更请求。
func TestChangeConfig_FailWhileInJointConsensus(t *testing.T) {
	// Arrange
	r := &Raft{
		state:            param.Leader,
		inJointConsensus: true, // 关键：已处于联合共识状态
	}
	// Act
	_, _, ok := r.ChangeConfig([]int{1, 2, 3})
	// Assert
	assert.False(t, ok, "ChangeConfig should fail while in joint consensus")
}
