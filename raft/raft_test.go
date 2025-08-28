package raft

import (
	"sync"
	"testing"

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
