package raft

import (
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
	peerId := 2

	mockStore.EXPECT().GetState().Return(param.HardState{}, nil).Times(1)
	r := NewRaft(1, []int{peerId}, mockStore, nil, mockTrans, nil)
	r.state = param.Leader
	r.currentTerm = 5
	r.nextIndex[peerId] = 11

	mockStore.EXPECT().LastLogIndex().Return(uint64(11), nil).AnyTimes()
	mockStore.EXPECT().GetEntry(gomock.Any()).Return(&param.LogEntry{Term: 5}, nil).AnyTimes()
	mockStore.EXPECT().FirstLogIndex().Return(uint64(1), nil).AnyTimes()

	mockTrans.EXPECT().SendAppendEntries(strconv.Itoa(peerId), gomock.Any(), gomock.Any()).
		DoAndReturn(func(id string, args *param.AppendEntriesArgs, reply *param.AppendEntriesReply) error {
			reply.Term = 5
			reply.Success = false
			reply.ConflictIndex = 8
			return nil
		}).AnyTimes()

	r.replicateLogsToPeer(peerId)

	time.Sleep(100 * time.Millisecond)

	r.mu.Lock()
	defer r.mu.Unlock()
	assert.Equal(t, uint64(8), r.nextIndex[peerId], "nextIndex should be backed off to 8")
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
		entry := param.LogEntry{Command: "test", Index: 10}

		mockSM.EXPECT().Apply(entry).Return("test_result")
		// 模拟 commitChan, 因为 applyStateMachineCommand 是一个空接口，所以我们在这里不检查
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
		cmd := param.ConfigChangeCommand{NewPeerIDs: []int{1, 2, 3}}
		entry := param.LogEntry{Command: cmd, Index: 10}

		r.dispatchEntries([]param.LogEntry{entry})

		assert.True(t, r.inJointConsensus, "should enter joint consensus")
		assert.Equal(t, cmd.NewPeerIDs, r.newPeerIds)
	})
}
