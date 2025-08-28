package raft

import (
	"math"
	"strconv"
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

	// This test will involve applying logs after commit, so we need a mock state machine.
	mockSM := storage.NewMockStateMachine(ctrl)

	mockStore.EXPECT().GetState().Return(param.HardState{}, nil).Times(1)
	// IMPORTANT: Pass the mockSM to the constructor.
	r := NewRaft(1, peerIds, mockStore, mockSM, mockTrans, nil)
	r.state = param.Follower
	r.currentTerm = 5

	// --- Setup mocks for the entire election and leader transition flow ---
	gomock.InOrder(
		mockStore.EXPECT().SetState(param.HardState{CurrentTerm: 6, VotedFor: 1}).Return(nil),
		mockStore.EXPECT().LastLogIndex().Return(uint64(10), nil),
		mockStore.EXPECT().GetEntry(uint64(10)).Return(&param.LogEntry{Term: 5, Index: 10}, nil),
	)

	for _, peerId := range peerIds {
		mockTrans.EXPECT().SendRequestVote(strconv.Itoa(peerId), gomock.Any(), gomock.Any()).
			DoAndReturn(func(id string, args *param.RequestVoteArgs, reply *param.RequestVoteReply) error {
				reply.Term = args.Term
				reply.VoteGranted = true
				return nil
			})
	}

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

	// --- Act ---
	// Manually trigger the functions, but intercept the call to startHeartbeat
	// This replaces the single r.startElection() call with a more controlled sequence.

	// 1. Initialize candidate state
	r.mu.Lock()
	err := r.initializeCandidateState()
	r.mu.Unlock()
	assert.NoError(t, err)

	// 2. Broadcast votes and collect them (simulated)
	lastLogIndex, lastLogTerm, err := r.getLastLogInfoForElection()
	assert.NoError(t, err)
	voteChan := r.broadcastVoteRequests(6, lastLogIndex, lastLogTerm)

	// 3. Process votes until win condition is met
	ctx := newElectionContext(r)
	for i := 0; i < len(peerIds); i++ {
		result := <-voteChan
		if r.processVote(ctx, result, 6) {
			break // Election won
		}
	}

	// 4. Manually transition to leader, BUT DO NOT START THE HEARTBEAT LOOP
	r.mu.Lock()
	assert.Equal(t, param.Leader, r.state, "expected state to be Leader")
	// This is the key: we manually call broadcastHeartbeat once
	// instead of letting the code call startHeartbeat() which creates the infinite loop.
	r.broadcastHeartbeat()
	r.mu.Unlock()

	// --- Assert ---
	// Wait for the single, manually triggered heartbeat to complete.
	wg.Wait()
	// The test now finishes cleanly with no goroutines left behind.
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

	// Fix: Add a call to becomeFollower which persists the state change.
	// This requires adding the bug fix to your election.go source code.
	// Assuming the fix is in place, the test is as follows:
	gomock.InOrder(
		mockStore.EXPECT().SetState(param.HardState{CurrentTerm: 6, VotedFor: 1}).Return(nil),
		mockStore.EXPECT().LastLogIndex().Return(uint64(10), nil),
		mockStore.EXPECT().GetEntry(uint64(10)).Return(&param.LogEntry{Term: 5, Index: 10}, nil),
		mockStore.EXPECT().SetState(param.HardState{CurrentTerm: 6, VotedFor: math.MaxUint64}).
			Do(func(interface{}) { close(revertedToFollower) }).Return(nil),
	)

	for _, peerId := range peerIds {
		mockTrans.EXPECT().SendRequestVote(strconv.Itoa(peerId), gomock.Any(), gomock.Any()).
			DoAndReturn(func(id string, args *param.RequestVoteArgs, reply *param.RequestVoteReply) error {
				reply.Term = args.Term
				reply.VoteGranted = false
				return nil
			})
	}

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
