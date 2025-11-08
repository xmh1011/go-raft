package raft

import (
	"math"
	"strconv"
	"sync"
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
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockStore := storage.NewMockStorage(ctrl)

	r := &Raft{
		id:        1,
		state:     param.Leader,
		store:     mockStore,
		nextIndex: map[int]uint64{2: 5},
	}
	mockStore.EXPECT().FirstLogIndex().Return(uint64(10), nil).Times(1)
	action := r.determineReplicationAction(2)
	assert.Equal(t, actionSendSnapshot, action, "expected action to be actionSendSnapshot")
}

// TestDetermineReplicationAction_ShouldSendLogs 测试在常规情况下，决策函数是否正确地决定发送日志。
func TestDetermineReplicationAction_ShouldSendLogs(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockStore := storage.NewMockStorage(ctrl)

	r := &Raft{
		id:        1,
		state:     param.Leader,
		store:     mockStore,
		nextIndex: map[int]uint64{2: 10},
	}

	mockStore.EXPECT().FirstLogIndex().Return(uint64(5), nil).Times(1)
	action := r.determineReplicationAction(2)
	assert.Equal(t, actionSendLogs, action, "expected action to be actionSendLogs")
}

// TestDetermineReplicationAction_NotLeader 测试当节点不是 Leader 时，决策函数是否决定什么都不做。
func TestDetermineReplicationAction_NotLeader(t *testing.T) {
	r := &Raft{id: 1, state: param.Follower}
	action := r.determineReplicationAction(2)
	assert.Equal(t, actionDoNothing, action, "expected action to be actionDoNothing when not leader")
}

func TestReplicateLogsToPeer_Success(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := storage.NewMockStorage(ctrl)
	mockTrans := transport.NewMockTransport(ctrl)
	mockSM := storage.NewMockStateMachine(ctrl)
	peerId := 2
	commitChan := make(chan param.CommitEntry, 1)

	mockStore.EXPECT().GetState().Return(param.HardState{}, nil).Times(1)

	r := NewRaft(1, []int{peerId}, mockStore, mockSM, mockTrans, commitChan)
	defer r.Stop()
	r.state = param.Leader
	r.currentTerm = 5
	r.commitIndex = 10
	r.lastApplied = 10
	r.nextIndex[peerId] = 11
	r.matchIndex[peerId] = 10

	gomock.InOrder(
		mockStore.EXPECT().GetEntry(uint64(10)).Return(&param.LogEntry{Term: 5, Index: 10}, nil).Times(1),
		mockStore.EXPECT().LastLogIndex().Return(uint64(11), nil).Times(1),
		mockStore.EXPECT().GetEntry(uint64(11)).Return(&param.LogEntry{Command: "test", Term: 5, Index: 11}, nil).Times(1),
		mockTrans.EXPECT().SendAppendEntries(strconv.Itoa(peerId), gomock.Any(), gomock.Any()).
			DoAndReturn(func(id string, args *param.AppendEntriesArgs, reply *param.AppendEntriesReply) error {
				reply.Term = 5
				reply.Success = true
				return nil
			}).Times(1),
	)

	mockStore.EXPECT().LastLogIndex().Return(uint64(11), nil).AnyTimes()
	mockStore.EXPECT().GetEntry(uint64(11)).Return(&param.LogEntry{Term: 5, Index: 11}, nil).AnyTimes()
	mockStore.EXPECT().FirstLogIndex().Return(uint64(1), nil).AnyTimes()
	mockSM.EXPECT().Apply(gomock.Any()).Return(nil).AnyTimes()

	r.replicateLogsToPeer(peerId)

	select {
	case entry := <-commitChan:
		assert.Equal(t, uint64(11), entry.Index, "expected entry with index 11 to be committed")
	case <-time.After(500 * time.Millisecond): // 增加超时时间以适应 CI 环境
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
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := storage.NewMockStorage(ctrl)
	mockTrans := transport.NewMockTransport(ctrl)
	mockSM := storage.NewMockStateMachine(ctrl)
	peerId := 2
	commitChan := make(chan param.CommitEntry, 1)
	mockStore.EXPECT().GetState().Return(param.HardState{}, nil).Times(1)
	r := NewRaft(1, []int{peerId}, mockStore, mockSM, mockTrans, commitChan)
	defer r.Stop()
	r.state = param.Leader
	r.currentTerm = 5
	r.nextIndex[peerId] = 11

	mockStore.EXPECT().LastLogIndex().Return(uint64(11), nil).AnyTimes()
	mockStore.EXPECT().GetEntry(gomock.Any()).
		DoAndReturn(func(index uint64) (*param.LogEntry, error) {
			return &param.LogEntry{Term: 5, Index: index}, nil
		}).AnyTimes()
	mockStore.EXPECT().FirstLogIndex().Return(uint64(1), nil).AnyTimes()
	mockSM.EXPECT().Apply(gomock.Any()).Return(nil).AnyTimes()
	gomock.InOrder(
		// 第一次调用：失败并触发回退
		mockTrans.EXPECT().SendAppendEntries(strconv.Itoa(peerId), gomock.Any(), gomock.Any()).
			DoAndReturn(func(id string, args *param.AppendEntriesArgs, reply *param.AppendEntriesReply) error {
				reply.Term = 5
				reply.Success = false
				reply.ConflictIndex = 8
				return nil
			}).Times(1),

		// 后续重试调用：成功，以终止 goroutine 循环
		mockTrans.EXPECT().SendAppendEntries(strconv.Itoa(peerId), gomock.Any(), gomock.Any()).
			DoAndReturn(func(id string, args *param.AppendEntriesArgs, reply *param.AppendEntriesReply) error {
				reply.Term = 5
				reply.Success = true
				return nil
			}).AnyTimes(),
	)

	r.replicateLogsToPeer(peerId)

	time.Sleep(100 * time.Millisecond) // 等待后台 goroutine 完成

	r.mu.Lock()
	defer r.mu.Unlock()
	assert.Equal(t, uint64(12), r.nextIndex[peerId], "nextIndex should be 12 after successful retry")
}

// TestAppendEntries_FollowerLogic 测试 Follower 节点处理 AppendEntries RPC 的核心逻辑
func TestAppendEntries_FollowerLogic(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockStore := storage.NewMockStorage(ctrl)

	// --- 场景1: 接收到来自旧任期 Leader 的请求 ---
	t.Run("Reject stale term", func(t *testing.T) {
		r := &Raft{currentTerm: 5, mu: sync.Mutex{}}
		args := &param.AppendEntriesArgs{Term: 4} // Leader 任期为 4，落后于自己的 5
		reply := &param.AppendEntriesReply{}
		err := r.AppendEntries(args, reply)
		assert.NoError(t, err, "AppendEntries should not return error")
		assert.False(t, reply.Success, "should reject request with stale term")
		assert.Equal(t, uint64(5), reply.Term, "should return current term")
	})

	// --- 场景2: 日志不一致 (prevLogTerm 不匹配) ---
	t.Run("Reject inconsistent log", func(t *testing.T) {
		r := &Raft{currentTerm: 5, store: mockStore, mu: sync.Mutex{}}
		args := &param.AppendEntriesArgs{Term: 5, PrevLogIndex: 10, PrevLogTerm: 4}
		reply := &param.AppendEntriesReply{}
		// Mock 本地日志在索引 10 处的任期是 5，与 Leader 的期望(4)不符
		mockStore.EXPECT().GetEntry(uint64(10)).Return(&param.LogEntry{Term: 5, Index: 10}, nil)
		err := r.AppendEntries(args, reply)
		assert.NoError(t, err, "AppendEntries should not return error")
		assert.False(t, reply.Success, "should reject request with inconsistent log")
	})

	// --- 场景3: 成功追加日志 ---
	t.Run("Successfully append entries", func(t *testing.T) {
		mockSM := storage.NewMockStateMachine(ctrl)
		commitChan := make(chan param.CommitEntry, 1)

		r := &Raft{
			currentTerm:  5,
			store:        mockStore,
			mu:           sync.Mutex{},
			commitChan:   commitChan,
			stateMachine: mockSM, // 确保 stateMachine 不为 nil
			lastApplied:  10,     // 设置正确的 lastApplied 初始值
		}
		r.lastAppliedCond = sync.NewCond(&r.mu)

		newEntries := []param.LogEntry{{Command: "cmd1", Term: 5, Index: 11}}
		args := &param.AppendEntriesArgs{
			Term:         5,
			PrevLogIndex: 10,
			PrevLogTerm:  5,
			Entries:      newEntries,
			LeaderCommit: 11,
		}
		reply := &param.AppendEntriesReply{}

		gomock.InOrder(
			// 日志一致性检查
			mockStore.EXPECT().GetEntry(uint64(10)).Return(&param.LogEntry{Term: 5, Index: 10}, nil),
			// 追加日志
			mockStore.EXPECT().TruncateLog(uint64(11)).Return(nil),
			mockStore.EXPECT().AppendEntries(newEntries).Return(nil),
			// 更新 commit index
			mockStore.EXPECT().LastLogIndex().Return(uint64(11), nil),
		)

		mockStore.EXPECT().GetEntry(uint64(11)).Return(&newEntries[0], nil)
		mockSM.EXPECT().Apply(gomock.Any()).Return("success")

		err := r.AppendEntries(args, reply)
		assert.NoError(t, err, "AppendEntries should not return error")

		// 等待后台的 applyLogs goroutine 将结果发送到 commitChan
		select {
		case entry := <-commitChan:
			assert.Equal(t, uint64(11), entry.Index)
		case <-time.After(100 * time.Millisecond):
			t.Fatal("timed out waiting for entry to be applied")
		}

		assert.True(t, reply.Success, "should accept valid append entries request")
		assert.Equal(t, uint64(11), r.commitIndex, "commitIndex should be updated")
	})
}

// TestIsReplicatedByMajority 测试 Leader 判断日志是否被多数节点复制的逻辑
func TestIsReplicatedByMajority(t *testing.T) {
	// 场景1: 普通模式，3/5 节点复制，满足多数
	t.Run("Simple majority met", func(t *testing.T) {
		r := &Raft{
			id:      1,
			peerIds: []int{2, 3, 4, 5},
			matchIndex: map[int]uint64{
				1: 10, 2: 10, 3: 10, 4: 9, 5: 9,
			},
			inJointConsensus: false,
		}
		assert.True(t, r.isReplicatedByMajority(10), "3/5 should be majority")
	})

	// 场景2: 普通模式，2/5 节点复制，不满足多数
	t.Run("Simple majority not met", func(t *testing.T) {
		r := &Raft{
			id:      1,
			peerIds: []int{2, 3, 4, 5},
			matchIndex: map[int]uint64{
				1: 10, 2: 10, 3: 9, 4: 9, 5: 9,
			},
			inJointConsensus: false,
		}
		assert.False(t, r.isReplicatedByMajority(10), "2/5 should not be majority")
	})

	// 场景3: 联合共识，新旧配置都满足多数
	t.Run("Joint consensus majority met", func(t *testing.T) {
		r := &Raft{
			id:         1,
			peerIds:    []int{2, 3},    // 旧配置 C_old: {1,2,3}, 多数为 2
			newPeerIds: []int{3, 4, 5}, // 新配置 C_new: {3,4,5}, 多数为 2
			matchIndex: map[int]uint64{
				1: 10, 2: 10, // C_old 满足多数
				3: 10, 4: 10, // C_new 满足多数
				5: 9,
			},
			inJointConsensus: true,
		}
		assert.True(t, r.isReplicatedByMajority(10), "majority in both old and new configs should pass")
	})

	// 场景4: 联合共识，旧配置满足但新配置不满足
	t.Run("Joint consensus majority not met", func(t *testing.T) {
		r := &Raft{
			id:         1,
			peerIds:    []int{2, 3},    // 旧配置 C_old: {1,2,3}, 多数为 2
			newPeerIds: []int{3, 4, 5}, // 新配置 C_new: {3,4,5}, 多数为 2
			matchIndex: map[int]uint64{
				1: 10, 2: 10, // C_old 满足多数
				3: 10, 4: 9, 5: 9, // C_new 不满足多数
			},
			inJointConsensus: true,
		}
		assert.False(t, r.isReplicatedByMajority(10), "majority in only one config should fail")
	})
}

// TestDispatchEntries 测试应用日志到状态机的分发逻辑
func TestDispatchEntries(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockSM := storage.NewMockStateMachine(ctrl)

	// 场景1: 应用普通命令
	t.Run("Apply normal command", func(t *testing.T) {
		notifyChan := make(chan any, 1)
		r := &Raft{
			stateMachine: mockSM,
			notifyApply:  map[uint64]chan any{10: notifyChan},
			mu:           sync.Mutex{},
		}
		r.lastAppliedCond = sync.NewCond(&r.mu)

		entry := param.LogEntry{Command: "test", Index: 10}

		mockSM.EXPECT().Apply(entry).Return("test_result")
		r.commitChan = make(chan param.CommitEntry, 1)

		r.dispatchEntries([]param.LogEntry{entry})

		select {
		case result := <-notifyChan:
			assert.Equal(t, "test_result", result)
		case <-time.After(50 * time.Millisecond):
			t.Fatal("timed out waiting for notification")
		}
	})

	// 场景2: 应用配置变更命令（进入联合共识）
	t.Run("Apply config change to enter joint consensus", func(t *testing.T) {
		r := &Raft{inJointConsensus: false, mu: sync.Mutex{}}
		r.lastAppliedCond = sync.NewCond(&r.mu)

		cmd := param.ConfigChangeCommand{NewPeerIDs: []int{1, 2, 3}}
		entry := param.LogEntry{Command: cmd, Index: 10}

		r.dispatchEntries([]param.LogEntry{entry})

		assert.True(t, r.inJointConsensus, "should enter joint consensus")
		assert.Equal(t, cmd.NewPeerIDs, r.newPeerIds)
	})
}

// TestProcessAppendEntriesReply_StepsDownOnHigherTerm 测试 Leader 在处理 AppendEntries 响应时
// 发现更高任期，应立即转为 Follower。
func TestProcessAppendEntriesReply_StepsDownOnHigherTerm(t *testing.T) {
	// --- Arrange ---
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := storage.NewMockStorage(ctrl)
	// 期望初始化调用 GetState
	mockStore.EXPECT().GetState().Return(param.HardState{CurrentTerm: 5}, nil).Times(1)
	// 期望调用 SetState 持久化 Follower 状态
	mockStore.EXPECT().SetState(param.HardState{CurrentTerm: 6, VotedFor: math.MaxUint64}).Return(nil).Times(1)

	r := NewRaft(1, []int{2}, mockStore, nil, nil, nil)
	r.state = param.Leader
	r.currentTerm = 5
	peerId := 2

	// 模拟一个来自 Peer 的、包含更高任期的响应
	args := &param.AppendEntriesArgs{Term: 5} // Leader 发送请求时的任期
	reply := &param.AppendEntriesReply{
		Term:    6, // Follower 返回了更高的任期
		Success: false,
	}
	savedCurrentTerm := uint64(5) // 模拟调用 replicateLogsToPeer 时的任期

	// --- Act ---
	r.mu.Lock()
	r.processAppendEntriesReply(peerId, args, reply, savedCurrentTerm)
	r.mu.Unlock()

	// --- Assert ---
	r.mu.Lock()
	defer r.mu.Unlock()
	assert.Equal(t, param.Follower, r.state, "State should become Follower")
	assert.Equal(t, uint64(6), r.currentTerm, "Term should be updated to 6")
	assert.Equal(t, -1, r.votedFor, "VotedFor should be reset")
}

// TestAppendEntries_FollowerLogConflict_LongerLog 测试 Follower 日志在 PrevLogIndex 之后
// 与 Leader 不一致（例如 Follower 日志更长）的情况。
func TestAppendEntries_FollowerLogConflict_LongerLog(t *testing.T) {
	// --- Arrange ---
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockStore := storage.NewMockStorage(ctrl)

	r := &Raft{
		id:          2,
		currentTerm: 5,
		store:       mockStore,
		mu:          sync.Mutex{},
	}
	r.lastAppliedCond = sync.NewCond(&r.mu)

	// Leader 发来的请求：期望 Follower 在索引 10 处有任期 5 的日志，并追加索引 11 的日志
	args := &param.AppendEntriesArgs{
		Term:         5,
		LeaderId:     1,
		PrevLogIndex: 10,
		PrevLogTerm:  5,
		Entries:      []param.LogEntry{{Command: "cmd11", Term: 5, Index: 11}},
	}
	reply := &param.AppendEntriesReply{}

	// --- 设置 Mock 期望 ---
	gomock.InOrder(
		// 1. 检查 PrevLogIndex(10) 处的日志 -> 假设匹配成功
		mockStore.EXPECT().GetEntry(uint64(10)).Return(&param.LogEntry{Term: 5, Index: 10}, nil),
		// 2. 在 appendAndStoreEntries 中，会尝试截断从 PrevLogIndex+1 (即 11) 开始的日志
		mockStore.EXPECT().TruncateLog(uint64(11)).Return(nil),
		// 3. 然后尝试追加新的日志条目
		mockStore.EXPECT().AppendEntries(args.Entries).Return(nil),
		// 4. (如果需要更新 commitIndex) 可能会调用 LastLogIndex
		mockStore.EXPECT().LastLogIndex().Return(uint64(11), nil).AnyTimes(), // 允许任意次调用
	)

	// --- Act ---
	err := r.AppendEntries(args, reply)

	// --- Assert ---
	assert.NoError(t, err, "AppendEntries should not return error")
	assert.True(t, reply.Success, "Expected AppendEntries to succeed")
}

// TestAppendEntries_FollowerLogConflict_TermMismatch 测试 Follower 在 PrevLogIndex 处的任期
// 与 Leader 的 PrevLogTerm 不匹配的情况。
func TestAppendEntries_FollowerLogConflict_TermMismatch(t *testing.T) {
	// --- Arrange ---
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockStore := storage.NewMockStorage(ctrl)

	r := &Raft{
		id:          2,
		currentTerm: 5,
		store:       mockStore,
		mu:          sync.Mutex{},
	}

	// Leader 发来的请求：期望 Follower 在索引 10 处有任期 4 的日志
	args := &param.AppendEntriesArgs{
		Term:         5,
		LeaderId:     1,
		PrevLogIndex: 10,
		PrevLogTerm:  4, // Leader 认为应该是任期 4
		Entries:      []param.LogEntry{{Command: "cmd11", Term: 5, Index: 11}},
	}
	reply := &param.AppendEntriesReply{}

	// --- 设置 Mock 期望 ---
	// 期望 GetEntry(10) 返回一个任期为 5 的日志，与 Leader 的期望(4)冲突
	followerEntryAtIndex10 := &param.LogEntry{Term: 5, Index: 10}
	mockStore.EXPECT().GetEntry(uint64(10)).Return(followerEntryAtIndex10, nil).Times(1)
	// 期望在 checkLogConsistency 中，当发现任期不匹配时，不再进行后续操作 (如 TruncateLog, AppendEntries)

	// --- Act ---
	err := r.AppendEntries(args, reply)

	// --- Assert ---
	assert.NoError(t, err, "AppendEntries should not return error")
	assert.False(t, reply.Success, "Expected AppendEntries to fail due to term mismatch")
	assert.Equal(t, uint64(5), reply.Term, "Reply term should be follower's current term")
	assert.Equal(t, followerEntryAtIndex10.Term, reply.ConflictTerm, "ConflictTerm should be the follower's term at the conflict index")
	assert.Equal(t, args.PrevLogIndex, reply.ConflictIndex, "ConflictIndex should be PrevLogIndex")
}

// TestProcessAppendEntriesReply_UpdatesLastAck 测试 Leader 在处理响应时更新 lastAck
func TestProcessAppendEntriesReply_UpdatesLastAck(t *testing.T) {
	peerId := 2
	savedTerm := uint64(5)

	// 场景 1: 成功的响应
	t.Run("OnSuccess", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		mockStore := storage.NewMockStorage(ctrl)
		// --- 修复：添加 mockSM 和 mockTrans ---
		mockSM := storage.NewMockStateMachine(ctrl)
		mockTrans := transport.NewMockTransport(ctrl)

		mockStore.EXPECT().GetEntry(gomock.Any()).Return(&param.LogEntry{Term: 5}, nil).AnyTimes()
		mockStore.EXPECT().LastLogIndex().Return(uint64(10), nil).AnyTimes()
		mockSM.EXPECT().Apply(gomock.Any()).Return(nil).AnyTimes()
		mockStore.EXPECT().GetState().Return(param.HardState{}, nil).Times(1)
		commitChan := make(chan param.CommitEntry, 1)
		r := NewRaft(1, []int{peerId}, mockStore, mockSM, mockTrans, commitChan)
		defer r.Stop()
		r.state = param.Leader
		r.currentTerm = savedTerm

		pastTime := time.Now().Add(-1 * time.Second)
		r.lastAck[peerId] = pastTime

		reply := &param.AppendEntriesReply{Term: savedTerm, Success: true}
		args := &param.AppendEntriesArgs{PrevLogIndex: 9, Entries: []param.LogEntry{{Index: 10}}}

		r.mu.Lock()
		r.processAppendEntriesReply(peerId, args, reply, savedTerm)
		r.mu.Unlock()

		assert.True(t, r.lastAck[peerId].After(pastTime), "lastAck should be updated on success")
		r.mu.Lock()
		assert.Equal(t, uint64(11), r.nextIndex[peerId], "nextIndex should be updated on success")
		assert.Equal(t, uint64(10), r.matchIndex[peerId], "matchIndex should be updated on success")
		r.mu.Unlock() // 释放锁，以便后台goroutine可以运行
	})

	// 场景 2: 失败但任期匹配的响应 (这也是一个 ACK)
	t.Run("OnFailureMatchingTerm", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		mockStore := storage.NewMockStorage(ctrl)
		mockSM := storage.NewMockStateMachine(ctrl)
		mockTrans := transport.NewMockTransport(ctrl)

		mockStore.EXPECT().LastLogIndex().Return(uint64(10), nil).AnyTimes()
		mockStore.EXPECT().FirstLogIndex().Return(uint64(1), nil).AnyTimes()
		mockStore.EXPECT().GetEntry(gomock.Any()).Return(&param.LogEntry{Term: 5}, nil).AnyTimes()
		mockTrans.EXPECT().SendAppendEntries(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
		mockStore.EXPECT().GetState().Return(param.HardState{}, nil).Times(1)
		commitChan := make(chan param.CommitEntry, 1)
		r := NewRaft(1, []int{peerId}, mockStore, mockSM, mockTrans, commitChan)
		defer r.Stop()
		r.state = param.Leader
		r.currentTerm = savedTerm

		pastTime := time.Now().Add(-1 * time.Second)
		r.lastAck[peerId] = pastTime

		reply := &param.AppendEntriesReply{Term: savedTerm, Success: false} // 失败
		args := &param.AppendEntriesArgs{}

		r.mu.Lock()
		r.processAppendEntriesReply(peerId, args, reply, savedTerm)
		r.mu.Unlock()

		assert.True(t, r.lastAck[peerId].After(pastTime), "lastAck should be updated even on failure if term matches")
	})

	// 场景 3: 更高任期的响应 (此部分已正确，因为它调用了 NewRaft)
	t.Run("OnHigherTerm", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		mockStore := storage.NewMockStorage(ctrl)
		mockStore.EXPECT().GetState().Return(param.HardState{}, nil).Times(1)
		mockStore.EXPECT().SetState(gomock.Any()).Return(nil).Times(1)

		// --- 修复：确保 NewRaft 传入 non-nil transport ---
		mockTrans := transport.NewMockTransport(ctrl)
		r := NewRaft(1, []int{peerId}, mockStore, nil, mockTrans, nil)
		defer r.Stop()
		r.state = param.Leader
		r.currentTerm = savedTerm
		pastTime := time.Now().Add(-1 * time.Second)
		r.lastAck[peerId] = pastTime

		reply := &param.AppendEntriesReply{Term: savedTerm + 1, Success: false} // 更高任期
		args := &param.AppendEntriesArgs{}

		r.mu.Lock()
		r.processAppendEntriesReply(peerId, args, reply, savedTerm)
		r.mu.Unlock()

		assert.False(t, r.lastAck[peerId].After(pastTime), "lastAck should NOT be updated on higher term")
		r.mu.Lock() // 重新获取锁以检查状态
		assert.Equal(t, param.Follower, r.state, "Should have stepped down to Follower")
		r.mu.Unlock()
	})
}
