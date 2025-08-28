package raft

import (
	"log"
	"time"

	"github.com/xmh1011/go-raft/param"
)

// RequestVote 是处理投票请求的 RPC 入口。
func (r *Raft) RequestVote(args *param.RequestVoteArgs, reply *param.RequestVoteReply) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	// 1. 处理任期相关的检查和状态更新。如果任期检查失败，则直接返回。
	if proceed, err := r.handleVoteTermLogic(args, reply); !proceed {
		return err
	}

	// 2. 根据 Raft 的投票规则（日志新旧、是否已投票）做出最终决定。
	return r.decideVote(args, reply)
}

// ClientRequestHandler 是处理来自客户端请求的 RPC 函数。
// 它现在只负责协调三个主要阶段：前置检查、提交并等待、处理最终结果。
func (r *Raft) ClientRequestHandler(args *param.ClientArgs, reply *param.ClientReply) {
	// 1. 执行前置检查。如果不是 Leader 或请求重复，则提前返回。
	if proceed := r.preHandleClientRequest(args, reply); !proceed {
		return
	}

	// 2. 将命令提交到 Raft 日志，并同步等待其被状态机应用。
	result, ok, leaderId := r.submitAndWaitForCommit(args.Command)

	// 3. 根据提交和等待的结果，最终填充客户端的响应。
	r.finalizeClientReply(args, reply, result, ok, leaderId)
}

// preHandleClientRequest 封装了所有在提交日志前需要进行的前置检查。
// 返回值 bool 表示是否应继续处理该请求。
func (r *Raft) preHandleClientRequest(args *param.ClientArgs, reply *param.ClientReply) bool {
	if !r.isLeader() {
		reply.NotLeader = true
		reply.LeaderHint = r.knownLeaderID
		return false
	}
	if r.isDuplicateRequest(args.ClientID, args.SequenceNum) {
		reply.Success = true // 对于重复请求，直接返回成功。
		return false
	}
	return true
}

// submitAndWaitForCommit 封装了将命令提交到 Raft 日志并等待其被应用的全过程。
// 它返回从状态机获得的结果，一个表示成功的布尔值，以及当前的 Leader ID。
func (r *Raft) submitAndWaitForCommit(command interface{}) (any, bool, int) {
	index, _, isLeader := r.Submit(command)
	if !isLeader {
		// 在提交过程中，可能失去了 Leader 地位。
		return nil, false, r.knownLeaderID
	}

	// 等待命令被状态机成功应用，或等待超时。
	result, ok := r.waitForAppliedLog(index, 2*time.Second)
	return result, ok, r.id
}

// finalizeClientReply 负责根据执行结果，最终构建给客户端的响应。
func (r *Raft) finalizeClientReply(args *param.ClientArgs, reply *param.ClientReply, result any, ok bool, leaderId int) {
	if ok {
		// 命令成功应用。
		r.mu.Lock()
		r.clientSessions[args.ClientID] = args.SequenceNum
		r.mu.Unlock()
		reply.Success = true
		reply.Result = result
	} else {
		// 如果失败，可能是因为超时，也可能是因为中途失去了 Leader 身份。
		reply.Success = false
		if !r.isLeader() {
			reply.NotLeader = true
			reply.LeaderHint = leaderId
		}
	}
}

// handleVoteTermLogic 封装了所有与任期相关的逻辑。
// 返回值 bool 表示是否应继续后续的投票判断。
// 此函数必须在持有锁的情况下被调用。
func (r *Raft) handleVoteTermLogic(args *param.RequestVoteArgs, reply *param.RequestVoteReply) (bool, error) {
	// 如果对方的任期低于自己，这是一个过时的请求，直接拒绝。
	if args.Term < r.currentTerm {
		reply.Term = r.currentTerm
		reply.VoteGranted = false
		return false, nil
	}

	// 如果对方的任期高于自己，则更新自己的状态为 Follower。
	if args.Term > r.currentTerm {
		if err := r.becomeFollower(args.Term); err != nil {
			reply.VoteGranted = false
			return false, err // 持久化失败是严重错误
		}
	}
	// 更新 reply 中的任期号以匹配当前（可能已更新的）任期。
	reply.Term = r.currentTerm
	return true, nil
}

// decideVote 封装了最终的投票决策逻辑。
// 它检查投票资格和日志新鲜度，并据此授予或拒绝投票。
// 此函数必须在持有锁的情况下被调用。
func (r *Raft) decideVote(args *param.RequestVoteArgs, reply *param.RequestVoteReply) error {
	// 检查自己是否有资格投票（在本任期内还未投票，或已投给当前候选人）。
	canVote := r.votedFor == -1 || r.votedFor == args.CandidateId

	// 检查候选人的日志是否至少和自己一样新。
	logIsUpToDate, err := r.isLogUpToDate(args.LastLogIndex, args.LastLogTerm)
	if err != nil {
		reply.VoteGranted = false
		return err
	}

	// 只有同时满足两个条件时，才授予投票。
	if canVote && logIsUpToDate {
		if err := r.grantVote(args.CandidateId); err != nil {
			reply.VoteGranted = false
			return err
		}
		reply.VoteGranted = true
	} else {
		// 否则，拒绝投票，并记录详细原因。
		log.Printf("[RequestVote] Node %d denying vote for term %d to candidate %d. (canVote=%t, logIsUpToDate=%t)", r.id, r.currentTerm, args.CandidateId, canVote, logIsUpToDate)
		reply.VoteGranted = false
	}
	return nil
}

// isLeader 是一个简单的辅助函数，用于检查节点是否为 Leader。
// 此函数必须在持有锁的情况下被调用。
func (r *Raft) isLeader() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.state == param.Leader
}

// isDuplicateRequest 检查一个客户端请求是否是重复的。
// 它通过比较请求的序列号和服务器记录的该客户端的最后一个序列号来判断。
func (r *Raft) isDuplicateRequest(clientID int64, sequenceNum int64) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	lastSeqNum, exists := r.clientSessions[clientID]
	if exists && sequenceNum <= lastSeqNum {
		log.Printf("[Client] Duplicate request detected from client %d (seq: %d)", clientID, sequenceNum)
		return true
	}
	return false
}

// waitForAppliedLog 等待一个特定索引的日志被状态机应用。
// 它通过一个注册在 notifyApply 映射中的 channel 来实现同步等待。
func (r *Raft) waitForAppliedLog(index uint64, timeout time.Duration) (any, bool) {
	r.mu.Lock()
	// 创建一个通知 channel，并注册到 map 中。
	notifyChan := make(chan any, 1)
	r.notifyApply[index] = notifyChan
	r.mu.Unlock()

	// 等待 applyLogs 发出通知，或等待超时。
	select {
	case result := <-notifyChan:
		log.Printf("[Client] Notified that log index %d has been applied.", index)
		return result, true
	case <-time.After(timeout):
		log.Printf("[Client] Timed out waiting for log index %d to be applied.", index)
		// 超时后，需要清理掉注册的 channel 以防内存泄漏。
		r.mu.Lock()
		delete(r.notifyApply, index)
		r.mu.Unlock()
		return nil, false
	}
}

// becomeFollower 将节点的状态更新为指定新任期的 Follower。
// 它会持久化新状态，并且必须在持有锁的情况下被调用。
func (r *Raft) becomeFollower(newTerm uint64) error {
	log.Printf("[State Change] Node %d received higher term %d. Updating term and becoming follower.", r.id, newTerm)
	r.currentTerm = newTerm
	r.state = param.Follower
	r.votedFor = -1 // 进入新任期时，重置投票记录。

	if err := r.store.SetState(param.HardState{CurrentTerm: r.currentTerm, VotedFor: uint64(r.votedFor)}); err != nil {
		log.Printf("[ERROR] Node %d failed to persist state after becoming follower: %v", r.id, err)
		return err
	}
	return nil
}

// isLogUpToDate 检查候选人的日志是否至少和本节点一样新。
// 这是 Raft 选举安全规则的核心实现。此函数必须在持有锁的情况下被调用。
func (r *Raft) isLogUpToDate(candidateLastLogIndex, candidateLastLogTerm uint64) (bool, error) {
	// 从存储中获取本节点的最后一条日志信息。
	localLastLogIndex, err := r.store.LastLogIndex()
	if err != nil {
		log.Printf("[ERROR] Node %d failed to get last log index from store: %v", r.id, err)
		return false, err
	}

	localLastLogTerm, err := r.getLogTerm(localLastLogIndex)
	if err != nil { // 检查 err 是否为 nil
		log.Printf("[ERROR] Node %d failed to get last log entry from store: %v", r.id, err)
		return false, err
	}

	// 1. 如果任期号不同，任期号大的日志更新。
	// 2. 如果任期号相同，日志更长的（索引更大）的更新。
	if candidateLastLogTerm > localLastLogTerm || (candidateLastLogTerm == localLastLogTerm && candidateLastLogIndex >= localLastLogIndex) {
		return true, nil
	}

	return false, nil
}

// grantVote 记录为指定候选人投票的动作，并将其持久化。
// 此函数必须在持有锁的情况下被调用。
func (r *Raft) grantVote(candidateId int) error {
	log.Printf("[RequestVote] Node %d granting vote for term %d to candidate %d.", r.id, r.currentTerm, candidateId)
	r.votedFor = candidateId
	r.electionResetEvent = time.Now()

	if err := r.store.SetState(param.HardState{CurrentTerm: r.currentTerm, VotedFor: uint64(r.votedFor)}); err != nil {
		log.Printf("[ERROR] Node %d failed to persist vote: %v", r.id, err)
		return err
	}
	return nil
}
