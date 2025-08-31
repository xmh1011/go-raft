package raft

import (
	"log"
	"strconv"
	"time"

	"github.com/xmh1011/go-raft/param"
)

// InstallSnapshot 是 Follower 上的 RPC 处理函数，用于接收并安装 Leader 发来的快照。
func (r *Raft) InstallSnapshot(args *param.InstallSnapshotArgs, reply *param.InstallSnapshotReply) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	// 1. 处理任期检查。如果 Leader 的任期有效，则继续；否则拒绝。
	if !r.handleSnapshotTerm(args, reply) {
		return nil
	}

	log.Printf("[Snapshot] Node %d received snapshot from leader %d (lastIncludedIndex=%d)", r.id, args.LeaderID, args.LastIncludedIndex)

	// 2. 将快照持久化到存储并压缩本地日志。
	snapshot := param.NewSnapshot(args.LastIncludedIndex, args.LastIncludedTerm, args.Data)
	if err := r.persistSnapshot(snapshot); err != nil {
		log.Printf("[ERROR] Node %d failed to persist snapshot: %v", r.id, err)
		return err
	}

	// 3. 将快照数据应用到上层状态机
	if err := r.stateMachine.ApplySnapshot(snapshot.Data); err != nil {
		log.Printf("[ERROR] Node %d failed to apply snapshot to state machine: %v", r.id, err)
		return err
	}

	// 4. 更新本地的 commitIndex 和 lastApplied 索引。
	r.updateStateAfterSnapshot(snapshot.LastIncludedIndex)

	log.Printf("[Snapshot] Node %d successfully installed snapshot. lastApplied is now %d.", r.id, r.lastApplied)
	return nil
}

// TakeSnapshot 由上层应用（状态机）在合适的时候调用，以触发一次快照。
// logSizeThreshold 是触发快照的日志大小阈值（例如，字节数或条目数）。
func (r *Raft) TakeSnapshot(logSizeThreshold int) {
	r.mu.Lock()
	defer r.mu.Unlock()

	// 1. 检查是否满足创建快照的条件（例如日志大小超过阈值）。
	logSize, err := r.store.LogSize() // 假设 Storage 接口有一个 LogSize 方法
	if err != nil || logSize < logSizeThreshold {
		return
	}

	log.Printf("[Snapshot] Node %d log size %d exceeds threshold %d, starting snapshot.", r.id, logSize, logSizeThreshold)

	// 2. 获取快照元数据并创建快照。
	snapshot, err := r.createSnapshot()
	if err != nil {
		log.Printf("[ERROR] Node %d failed to create snapshot metadata: %v", r.id, err)
		return
	}

	// 3. 将快照持久化并压缩日志。
	if err := r.persistSnapshot(snapshot); err != nil {
		log.Printf("[ERROR] Node %d failed to persist snapshot: %v", r.id, err)
		return
	}

	log.Printf("[Snapshot] Node %d created and saved snapshot up to index %d.", r.id, snapshot.LastIncludedIndex)
}

// createSnapshot 负责获取快照所需的元数据，并从状态机获取数据来构建快照对象。
func (r *Raft) createSnapshot() (*param.Snapshot, error) {
	snapshotIndex := r.lastApplied
	snapshotTermEntry, err := r.store.GetEntry(snapshotIndex)
	if err != nil {
		log.Printf("[ERROR] Node %d failed to get entry at index %d: %v", r.id, snapshotIndex, err)
		return nil, err
	}

	snapshotData, err := r.stateMachine.GetSnapshot()
	if err != nil {
		log.Printf("[ERROR] Node %d failed to get snapshot data from state machine: %v", r.id, err)
		return nil, err
	}

	snapshot := param.NewSnapshot(snapshotIndex, snapshotTermEntry.Term, snapshotData)
	return snapshot, nil
}

// handleSnapshotTerm 负责处理 InstallSnapshot RPC 中的任期检查和心跳逻辑。
// 如果 Leader 的任期有效，返回 true。此函数必须在持有锁的情况下被调用。
func (r *Raft) handleSnapshotTerm(args *param.InstallSnapshotArgs, reply *param.InstallSnapshotReply) bool {
	reply.Term = r.currentTerm
	if args.Term < r.currentTerm {
		return false
	}

	if args.Term > r.currentTerm {
		if err := r.becomeFollower(args.Term); err != nil {
			return false
		}
		reply.Term = r.currentTerm
	}
	r.electionResetEvent = time.Now()
	return true
}

// persistSnapshot 负责将快照保存到稳定存储，并根据快照索引压缩日志。
func (r *Raft) persistSnapshot(snapshot *param.Snapshot) error {
	// 将快照持久化到存储。
	if err := r.store.SaveSnapshot(snapshot); err != nil {
		log.Printf("[ERROR] Node %d failed to save received snapshot: %v", r.id, err)
		return err
	}
	// 更新内存中的快照引用，避免频繁从存储读取。
	r.snapshot = snapshot

	// 压缩本地日志，删除所有已被快照覆盖的条目。
	if err := r.store.CompactLog(snapshot.LastIncludedIndex); err != nil {
		log.Printf("[ERROR] Node %d failed to compact log after installing snapshot: %v", r.id, err)
		return err
	}
	return nil
}

// updateStateAfterSnapshot 在成功安装快照后，更新节点的内部状态索引。
func (r *Raft) updateStateAfterSnapshot(snapshotIndex uint64) {
	r.commitIndex = max(r.commitIndex, snapshotIndex)
	r.lastApplied = max(r.lastApplied, snapshotIndex)
}

// sendSnapshot 是 Leader 用于向落后的 Follower 发送快照
func (r *Raft) sendSnapshot(peerId int) {
	// 1. 从存储中读取最新的快照以准备发送。
	snapshot, err := r.readSnapshotForSending(peerId)
	if err != nil {
		return
	}

	// 2. 准备 RPC 参数。
	r.mu.Lock()
	args := param.NewInstallSnapshotArgs(r.currentTerm, uint64(r.id), snapshot.LastIncludedIndex, snapshot.LastIncludedTerm, snapshot.Data)
	savedCurrentTerm := r.currentTerm
	r.mu.Unlock()

	// 3. 发起 RPC 调用。
	reply := &param.InstallSnapshotReply{}
	if err := r.trans.SendInstallSnapshot(strconv.Itoa(peerId), args, reply); err != nil {
		log.Printf("[Snapshot] Node %d failed to send snapshot to %d: %v", r.id, peerId, err)
		return
	}

	// 4. 处理 RPC 响应。
	r.processSnapshotReply(peerId, reply, snapshot.LastIncludedIndex, savedCurrentTerm)
}

// readSnapshotForSending 负责加锁并从存储中读取最新的快照。
func (r *Raft) readSnapshotForSending(peerId int) (*param.Snapshot, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	snapshot, err := r.store.ReadSnapshot()
	if err != nil {
		log.Printf("[ERROR] Node %d failed to read snapshot to send to peer %d: %v", r.id, peerId, err)
		return nil, err
	}
	return snapshot, nil
}

// processSnapshotReply 负责处理来自 Follower 的 InstallSnapshot RPC 响应。
func (r *Raft) processSnapshotReply(peerId int, reply *param.InstallSnapshotReply, snapshotLastIndex uint64, savedCurrentTerm uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()

	// 检查响应是否已过期（例如，在 RPC 通信期间，Leader 身份或任期已发生变化）。
	if r.currentTerm != savedCurrentTerm || r.state != param.Leader {
		return
	}

	// 如果对方的任期更高，说明自己已不再是 Leader，应立即转为 Follower。
	if reply.Term > r.currentTerm {
		if err := r.becomeFollower(reply.Term); err != nil {
			log.Printf("[ERROR] Node %d failed to persist state after processing snapshot reply: %v", r.id, err)
		}
		return
	}

	// 如果一切正常，说明快照已成功发送并被对方接收。
	// 更新该 Follower 的 nextIndex 和 matchIndex，使其指向快照之后的第一个位置。
	r.nextIndex[peerId] = snapshotLastIndex + 1
	r.matchIndex[peerId] = snapshotLastIndex
	log.Printf("[Snapshot] Node %d successfully sent snapshot to peer %d. nextIndex=%d, matchIndex=%d", r.id, peerId, r.nextIndex[peerId], r.matchIndex[peerId])
}
