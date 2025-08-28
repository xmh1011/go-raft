package raft

import (
	"log"
	"sync"
	"time"

	"github.com/xmh1011/go-raft/param"
	"github.com/xmh1011/go-raft/storage"
	"github.com/xmh1011/go-raft/transport"
)

type Raft struct {
	// mu 保护对 Raft 状态的并发访问
	mu sync.Mutex

	// id 是当前节点的服务器ID
	id int

	// Configuration state
	peerIds          []int // 代表当前有效的配置 (Cold)
	newPeerIds       []int // 在转换期间代表新配置 (Cnew)
	inJointConsensus bool  // 标记集群是否处于联合共识状态
	knownLeaderID    int   // 当前节点已知的 Leader ID

	// store 负责持久化 Raft 状态和日志信息
	store storage.Storage
	// trans 负责网络通信
	trans transport.Transport
	// stateMachine 应用层的状态机接口
	stateMachine storage.StateMachine

	// --- Raft 核心状态 ---
	currentTerm uint64
	votedFor    int
	state       param.State

	// --- 日志与状态机相关 ---
	commitIndex uint64
	lastApplied uint64
	commitChan  chan<- param.CommitEntry

	// --- 快照相关 ---
	// snapshot 在内存中持有当前最新的快照，避免频繁从存储中读取
	snapshot *param.Snapshot

	// --- 选举相关 ---
	electionResetEvent time.Time

	// --- Leader 的易失性状态 ---
	nextIndex  map[int]uint64
	matchIndex map[int]uint64

	// --- 客户端交互状态 ---
	clientSessions map[int64]int64
	notifyApply    map[uint64]chan any
}

// NewRaft 创建一个新的 Raft 节点。
// 注意：store 参数的类型现在是 storage.KVStorage。
func NewRaft(id int, peerIds []int, store storage.Storage, stateMachine storage.StateMachine, trans transport.Transport, commitChan chan<- param.CommitEntry) *Raft {
	r := &Raft{
		id:               id,
		peerIds:          peerIds,
		store:            store,
		stateMachine:     stateMachine,
		trans:            trans,
		inJointConsensus: false,
		state:            param.Follower,
		votedFor:         -1, // -1 表示未投票
		commitChan:       commitChan,
		nextIndex:        make(map[int]uint64),
		matchIndex:       make(map[int]uint64),
		clientSessions:   make(map[int64]int64),
		notifyApply:      make(map[uint64]chan any),
	}
	// 从稳定存储中恢复状态。
	if store != nil {
		hardState, err := store.GetState()
		if err != nil {
			log.Fatalf("failed to get hard state from storage: %s", err.Error())
		}
		r.currentTerm = hardState.CurrentTerm
		r.votedFor = int(hardState.VotedFor)
	}

	r.electionResetEvent = time.Now()

	return r
}

// getLastLogInfo 获取最后一条日志的索引和任期。
func (r *Raft) getLastLogInfo() (index uint64, term uint64) {
	lastIndex, err := r.store.LastLogIndex()
	if err != nil || lastIndex == 0 {
		// 如果出错或日志为空，则返回 0, 0
		return 0, 0
	}

	lastEntry, err := r.store.GetEntry(lastIndex)
	if err != nil || lastEntry == nil {
		return 0, 0
	}

	return lastEntry.Index, lastEntry.Term
}

// getLogTerm 返回指定索引的日志条目的任期。
// 它现在从存储层查询。
func (r *Raft) getLogTerm(index uint64) (uint64, error) {
	if index == 0 {
		return 0, nil
	}
	entry, err := r.store.GetEntry(index)
	if err != nil {
		return 0, err
	}
	if entry == nil {
		// 条目不存在，根据 Raft 论文，这类似于越界，返回 0
		return 0, nil
	}
	return entry.Term, nil
}

// getFirstLogIndex 返回日志中的第一条条目的索引。从存储层查询。
func (r *Raft) getFirstLogIndex() (uint64, error) {
	// 假设快照逻辑还未完全集成到存储层，先处理内存快照
	if r.snapshot != nil {
		return r.snapshot.LastIncludedIndex + 1, nil
	}
	// 从存储中获取第一条日志的索引
	firstIndex, err := r.store.FirstLogIndex()
	if err != nil {
		log.Printf("[ERROR] failed to get first log index: %v", err)
		return 0, err
	}
	return firstIndex, nil
}

// proposeToLog 是一个通用的辅助函数，负责将任何命令（普通命令或配置变更）作为新的日志条目提交到 Raft 集群。
// 它封装了成为 Leader 后提议日志的完整流程。
// proposeToLog 现在只负责在【持有锁】的情况下，将命令写入本地日志。
// 它不再负责启动广播。
func (r *Raft) proposeToLog(command interface{}) (param.LogEntry, error) {
	// 1. 从存储中获取最后一条日志的索引，以确定新日志的索引。
	lastIndex, err := r.store.LastLogIndex()
	if err != nil {
		log.Printf("[ERROR] Leader %d failed to get last log index to propose new entry: %v", r.id, err)
		return param.LogEntry{}, err
	}
	newIndex := lastIndex + 1

	// 2. 将新条目原子性地追加并持久化到 Leader 的本地存储中。
	newLogEntry := param.NewLogEntry(command, r.currentTerm, newIndex)
	if err := r.store.AppendEntries([]param.LogEntry{newLogEntry}); err != nil {
		log.Printf("leader %d failed to append new log entry: %s", r.id, err.Error())
		return param.LogEntry{}, err
	}
	log.Printf("leader %d proposed new log entry at index %d", r.id, newIndex)

	return newLogEntry, nil
}

// Submit 将一个普通的客户端命令追加到 Raft 日志中。
func (r *Raft) Submit(command interface{}) (uint64, uint64, bool) {
	r.mu.Lock()

	// 1. 检查当前节点是否为 Leader。
	if r.state != param.Leader {
		r.mu.Unlock()
		return 0, 0, false
	}

	// 2. 将命令写入本地日志（此过程在持有锁的情况下完成）。
	newLogEntry, err := r.proposeToLog(command)
	if err != nil {
		r.mu.Unlock()
		return 0, 0, false
	}

	// 3. 在启动 goroutine 之前，我们先获取需要通知的 peer 列表，然后立即解锁。
	peersToNotify := r.getAllPeerIDs()
	r.mu.Unlock()

	// 4. 在没有持有锁的情况下，安全地启动广播 goroutine。
	for _, peerId := range peersToNotify {
		if peerId == r.id {
			continue
		}
		go r.sendAppendEntries(peerId)
	}

	return newLogEntry.Index, newLogEntry.Term, true
}

// ChangeConfig 发起一次集群成员变更。
// 它处理成员变更特有的前置检查和状态更新，并将核心的日志提议工作委托给通用函数。
// 实现动态成员变更，支持两阶段提交以确保安全性。
func (r *Raft) ChangeConfig(newPeerIDs []int) (uint64, uint64, bool) {
	r.mu.Lock()

	// 1. 前置检查：确保当前是 Leader 并且没有正在进行的成员变更。
	if r.inJointConsensus || r.state != param.Leader {
		r.mu.Unlock()
		return 0, 0, false // 变更已在进行中
	}

	// 2. 创建配置变更命令并写入本地日志。
	newLogEntry, err := r.proposeToLog(param.NewConfigChangeCommand(newPeerIDs))
	if err != nil {
		r.mu.Unlock()
		return 0, 0, false
	}

	// 3. 提议成功后，Leader 自身立即进入“联合共识”状态。
	r.inJointConsensus = true
	r.newPeerIds = newPeerIDs
	peersToNotify := r.getAllPeerIDs()
	r.mu.Unlock()

	// 4. 在没有持有锁的情况下，安全地广播。
	for _, peerId := range peersToNotify {
		if peerId == r.id {
			continue
		}
		go r.sendAppendEntries(peerId)
	}

	return newLogEntry.Index, newLogEntry.Term, true
}

// getAllPeerIDs is a helper to get all unique peers from both configurations.
func (r *Raft) getAllPeerIDs() []int {
	peerSet := make(map[int]struct{})
	for _, p := range r.peerIds {
		peerSet[p] = struct{}{}
	}
	if r.inJointConsensus {
		for _, p := range r.newPeerIds {
			peerSet[p] = struct{}{}
		}
	}

	allPeers := make([]int, 0, len(peerSet))
	for p := range peerSet {
		allPeers = append(allPeers, p)
	}
	return allPeers
}
