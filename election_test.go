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

// TestStartElection_WinsElection 测试候选人成功赢得选举并成为 Leader 的场景。
func TestStartElection_WinsElection(t *testing.T) {
	// --- Arrange: 准备阶段 ---
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := storage.NewMockStorage(ctrl)
	mockTrans := transport.NewMockTransport(ctrl)
	peerIds := []int{2, 3}

	mockStore.EXPECT().GetState().Return(param.HardState{}, nil).Times(1)

	// 创建被测试的 Raft 实例。此时，上面的 GetState() 调用会发生。
	r := NewRaft(1, peerIds, mockStore, nil, mockTrans, nil)
	r.state = param.Follower
	r.currentTerm = 5

	// startElection 函数内部的调用序列。
	gomock.InOrder(
		// 持久化选举状态
		mockStore.EXPECT().SetState(param.HardState{CurrentTerm: 6, VotedFor: 1}).Return(nil).Times(1),
		// 获取日志信息
		mockStore.EXPECT().LastLogIndex().Return(uint64(10), nil).Times(1),
		mockStore.EXPECT().GetEntry(uint64(10)).Return(&param.LogEntry{Term: 5, Index: 10}, nil).Times(1),
	)

	// 向所有对等节点发送 RequestVote RPC，并获得赞成票。
	for _, peerId := range peerIds {
		mockTrans.EXPECT().SendRequestVote(strconv.Itoa(peerId), gomock.Any(), gomock.Any()).
			DoAndReturn(func(id string, args *param.RequestVoteArgs, reply *param.RequestVoteReply) error {
				reply.Term = args.Term
				reply.VoteGranted = true
				return nil
			}).Times(1)
	}

	// 当选为 Leader 后，initLeaderState 会再次获取 LastLogIndex。
	mockStore.EXPECT().LastLogIndex().Return(uint64(10), nil).Times(1)

	// 成为 Leader 后，后台的心跳 goroutine 会被启动。
	// 心跳会触发 sendAppendEntries -> determineReplicationAction -> FirstLogIndex 调用。
	// 接着会触发 replicateLogsToPeer -> prepareAppendEntriesArgs -> LastLogIndex 和 GetEntry 调用。
	// 因为心跳是周期性的，我们使用 .AnyTimes() 来表示这些调用可能在测试期间发生任意次。
	mockStore.EXPECT().FirstLogIndex().Return(uint64(1), nil).AnyTimes()
	mockStore.EXPECT().LastLogIndex().Return(uint64(10), nil).AnyTimes()
	mockStore.EXPECT().GetEntry(gomock.Any()).Return(&param.LogEntry{Term: 5}, nil).AnyTimes()
	// 同时，心跳会向 follower 发送空的 AppendEntries RPC
	mockTrans.EXPECT().SendAppendEntries(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(id string, args *param.AppendEntriesArgs, reply *param.AppendEntriesReply) error {
			reply.Term = args.Term
			reply.Success = true
			return nil
		}).AnyTimes()

	r.startElection()

	time.Sleep(electionTimeout + 100*time.Millisecond)

	r.mu.Lock()
	defer r.mu.Unlock()

	assert.Equal(t, param.Leader, r.state, "expected state to be Leader")
}

// TestStartElection_LosesElection 测试候选人因超时未能获得足够票数而选举失败的场景。
func TestStartElection_LosesElection(t *testing.T) {
	// --- Arrange ---
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := storage.NewMockStorage(ctrl)
	mockTrans := transport.NewMockTransport(ctrl)
	peerIds := []int{2, 3}

	mockStore.EXPECT().GetState().Return(param.HardState{}, nil).Times(1)

	r := NewRaft(1, peerIds, mockStore, nil, mockTrans, nil)
	r.state = param.Follower
	r.currentTerm = 5

	gomock.InOrder(
		mockStore.EXPECT().SetState(param.HardState{CurrentTerm: 6, VotedFor: 1}).Return(nil).Times(1),
		mockStore.EXPECT().LastLogIndex().Return(uint64(10), nil).Times(1),
		mockStore.EXPECT().GetEntry(uint64(10)).Return(&param.LogEntry{Term: 5, Index: 10}, nil).Times(1),
	)

	// 期望 3: 向所有对等节点发送 RequestVote RPC，但这次返回反对票。
	for _, peerId := range peerIds {
		mockTrans.EXPECT().SendRequestVote(strconv.Itoa(peerId), gomock.Any(), gomock.Any()).
			DoAndReturn(func(id string, args *param.RequestVoteArgs, reply *param.RequestVoteReply) error {
				reply.Term = args.Term
				reply.VoteGranted = false
				return nil
			}).Times(1)
	}

	// --- Act ---
	r.startElection()

	// --- Assert ---
	time.Sleep(electionTimeout + 100*time.Millisecond)

	r.mu.Lock()
	defer r.mu.Unlock()

	assert.Equal(t, param.Follower, r.state, "expected state to revert to Follower after timeout")
}
