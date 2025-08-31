package raft

import (
	"log"
	"strconv"
	"time"

	"github.com/xmh1011/go-raft/param"
)

const (
	heartbeatInterval = 10 * time.Millisecond // 心跳间隔
	electionTimeout   = 50 * time.Millisecond // 选举超时时间
)

// electionContext 是一个辅助结构体，用于封装单次选举过程中的所有状态。
type electionContext struct {
	// --- 配置信息 ---
	inJoint     bool  // 当前是否处于联合共识状态
	oldPeers    []int // 旧配置的完整节点列表（包含自身）
	newPeers    []int // 新配置的完整节点列表（包含自身）
	majorityOld int   // 旧配置需要的多数票
	majorityNew int   // 新配置需要的多数票

	// --- 计票器 ---
	votesOldConfig int // 在旧配置中已获得的票数
	votesNewConfig int // 在新配置中已获得的票数
}

// newElectionContext 是 electionContext 的构造函数。
// 它从 Raft 实例中获取当前选举所需的全部上下文信息。
func newElectionContext(r *Raft) *electionContext {
	r.mu.Lock()
	defer r.mu.Unlock()

	// 复制当前配置，确保选举期间的计票逻辑不受外部状态变更的影响。
	oldPeers := append([]int(nil), r.peerIds...)
	newPeers := append([]int(nil), r.newPeerIds...)
	oldPeers = append(oldPeers, r.id) // 将自身加入计票列表

	ctx := &electionContext{
		inJoint:        r.inJointConsensus,
		oldPeers:       oldPeers,
		newPeers:       newPeers,
		majorityOld:    len(oldPeers)/2 + 1,
		votesOldConfig: 1, // 初始化计票器，首先计入自己的那一票。
		votesNewConfig: 0,
	}
	if ctx.inJoint {
		ctx.newPeers = append(ctx.newPeers, r.id)
		ctx.majorityNew = len(ctx.newPeers)/2 + 1
		if _, isNew := findPeer(r.id, ctx.newPeers); isNew {
			ctx.votesNewConfig = 1
		}
	}

	return ctx
}

// startElection 主函数：发起选举并处理结果
// 当一个 Follower 节点的选举计时器超时后，它会转变为 Candidate 状态并发起新一轮的选举。此函数负责：
// - 增加 currentTerm（当前任期号）。
// - 投票给自己 (votedFor = r.id)。
// - 重置选举计时器。
// - 向集群中的其他所有节点并行发送 RequestVote RPC（远程过程调用）来请求投票。
func (r *Raft) startElection() {
	r.mu.Lock()

	// 1. 初始化候选人状态：更新任期、投票给自己并持久化。
	// 如果此步骤失败（例如持久化失败），则无法安全地进行选举。
	if err := r.initializeCandidateState(); err != nil {
		r.mu.Unlock()
		return
	}

	// 2. 获取用于投票请求的日志信息。
	// 这是 Raft 安全性的一部分，确保日志旧的候选人无法当选。
	lastLogIndex, lastLogTerm, err := r.getLastLogInfoForElection()
	if err != nil {
		r.mu.Unlock()
		return
	}

	// 保存当前的选举任期，用于后续在处理投票结果时进行比较。
	savedCurrentTerm := r.currentTerm
	r.mu.Unlock() // 在发起网络调用前解锁。

	// 3. 并发地向所有对等节点广播投票请求。
	voteChan := r.broadcastVoteRequests(savedCurrentTerm, lastLogIndex, lastLogTerm)

	// 4. 在一个新的 goroutine 中异步处理选举结果。
	go r.handleElectionResult(voteChan, savedCurrentTerm)
}

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

// initializeCandidateState 负责将节点状态转换为 Candidate，更新任期，投票给自己，并持久化这些变更。
// 这是成为候选人的第一步，且必须是原子性的。
func (r *Raft) initializeCandidateState() error {
	// 将状态更新为 Candidate，增加当前任期号，并给自己投票。
	r.state = param.Candidate
	r.currentTerm++
	r.votedFor = r.id
	// 重置选举计时器，为本轮选举设定新的超时时间。
	r.electionResetEvent = time.Now()

	// 持久化更新后的任期和投票记录。
	// 这是至关重要的一步：必须在发送投票请求（RPC）之前将新状态写入稳定存储。
	// 这样可以确保即使节点在发送请求后立即崩溃，重启后也不会忘记自己已经进入了新的任期并投了票，
	// 从而避免在同一个任期内投票给其他候选人，防止脑裂。
	if err := r.store.SetState(param.HardState{CurrentTerm: r.currentTerm, VotedFor: uint64(r.votedFor)}); err != nil {
		log.Printf("[ERROR] Node %d failed to persist state before election: %v", r.id, err)
		return err
	}

	log.Printf("[Election] Node %d starts election for term %d", r.id, r.currentTerm)
	return nil
}

// getLastLogInfoForElection 从存储中获取最后一条日志的索引和任期。
// 这些信息将用于填充 RequestVote RPC 参数，以供其他节点进行日志新旧检查。
func (r *Raft) getLastLogInfoForElection() (lastLogIndex uint64, lastLogTerm uint64, err error) {
	// 从存储中获取自己最后一条日志的索引。
	lastLogIndex, err = r.store.LastLogIndex()
	if err != nil {
		log.Printf("[ERROR] Node %d failed to get last log index for election: %v", r.id, err)
		return 0, 0, err
	}

	// 如果日志不为空，则获取最后一条日志的任期。
	if lastLogIndex > 0 {
		lastLogTerm, err = r.getLogTerm(lastLogIndex)
		if err != nil {
			log.Printf("[ERROR] Node %d failed to get last log term for election: %v", r.id, err)
			return 0, 0, err
		}
	}
	return lastLogIndex, lastLogTerm, nil
}

// broadcastVoteRequests 负责向集群中所有其他节点并行地发送投票请求。
// 它会返回一个 channel，用于接收投票结果。
func (r *Raft) broadcastVoteRequests(term uint64, lastLogIndex uint64, lastLogTerm uint64) <-chan *param.VoteResult {
	// 创建一个带缓冲的 channel 用于接收投票结果。
	voteChan := make(chan *param.VoteResult, len(r.getAllPeerIDs()))

	// 并发地向集群中的所有其他节点发送投票请求。
	// 使用 goroutine 可以确保所有请求并行发出，加快选举过程。
	for _, peerId := range r.getAllPeerIDs() {
		// 跳过自己
		if peerId == r.id {
			continue
		}

		go r.sendVoteRequest(peerId, term, lastLogIndex, lastLogTerm, voteChan)
	}
	return voteChan
}

// handleElectionResult 处理投票结果和超时，决定是否成为Leader或回退
// 如果收到了超过半数节点的投票，则该 Candidate 节点成为 Leader。
// 成为 Leader 后，会立即初始化 Leader 的状态（nextIndex 和 matchIndex）并开始发送心跳。
// 如果在选举超时时间内未能获得多数票，选举失败，节点退回为 Follower 状态。
func (r *Raft) handleElectionResult(voteChan <-chan *param.VoteResult, electionTerm uint64) {
	// 1. 初始化选举上下文。
	ctx := newElectionContext(r)

	// 2. 启动选举计时器。
	electionTimer := time.NewTimer(electionTimeout)
	defer electionTimer.Stop()

	// 3. 循环等待，直到选举获胜或超时。
	for {
		select {
		case result := <-voteChan:
			// 处理收到的投票。如果计票结果显示选举获胜，则转换状态并结束。
			if r.processVote(ctx, result, electionTerm) { //
				return
			}

		case <-electionTimer.C:
			// 处理选举超时。
			r.handleElectionTimeout(electionTerm) //
			return
		}
	}
}

// processVote 处理单张选票，更新计票器，并检查是否赢得选举。
// 如果赢得选举，则返回 true；否则返回 false。
func (r *Raft) processVote(ctx *electionContext, result *param.VoteResult, electionTerm uint64) (won bool) {
	// 只处理投赞成票的结果。
	if result.VoteGranted {
		log.Printf("[Election] Node %d received a vote from node %d for term %d", r.id, result.VoterID, electionTerm)
		// 检查投票者属于哪个配置，并为相应的计票器加一。
		if _, isOld := findPeer(result.VoterID, ctx.oldPeers); isOld {
			ctx.votesOldConfig++
		}
		if ctx.inJoint {
			if _, isNew := findPeer(result.VoterID, ctx.newPeers); isNew {
				ctx.votesNewConfig++
			}
		}

		// 检查是否满足获胜条件。
		if ctx.checkWinCondition() {
			r.transitionToLeader(electionTerm)
			return true // 选举已获胜
		}
	}
	return false // 尚未获胜
}

// checkWinCondition 根据当前的计票结果和配置状态，判断选举是否获胜。
func (ctx *electionContext) checkWinCondition() bool {
	if ctx.inJoint {
		// 在联合共识状态下，必须同时获得新旧配置的多数票。
		return ctx.votesOldConfig >= ctx.majorityOld && ctx.votesNewConfig >= ctx.majorityNew
	}
	// 在普通状态下，只需获得当前配置的多数票。
	return ctx.votesOldConfig >= ctx.majorityOld
}

// transitionToLeader 封装了当选为 Leader 后的状态转换逻辑。
func (r *Raft) transitionToLeader(electionTerm uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()

	// 再次确认自己仍然是本轮选举的候选人，防止因状态变更导致的问题。
	if r.state == param.Candidate && r.currentTerm == electionTerm {
		log.Printf("[Election] Node %d elected as Leader for term %d", r.id, r.currentTerm)
		r.state = param.Leader
		r.initLeaderState()
		r.startHeartbeat()
	}
}

// handleElectionTimeout 封装了选举超时后的状态转换逻辑。
func (r *Raft) handleElectionTimeout(electionTerm uint64) {
	log.Printf("[Election] Node %d election timed out for term %d", r.id, electionTerm)
	r.mu.Lock()
	defer r.mu.Unlock()

	// 确认自己仍然是本轮选举的候选人，然后退回为 Follower 状态。
	if r.state == param.Candidate && r.currentTerm == electionTerm {
		log.Printf("[Election] Node %d election failed, reverting to Follower", r.id)
		// 调用 becomeFollower 会更新 term, state, votedFor 并持久化
		// 因为任期没有变，所以传入 r.currentTerm 即可
		if err := r.becomeFollower(r.currentTerm); err != nil {
			log.Printf("[ERROR] Node %d failed to revert to Follower: %v", r.id, err)
		}
	}
}

// sendVoteRequest 向单个Peer发送投票请求
// 请求，并处理响应。如果响应中包含更高的任期号，当前节点会立即更新自己的任期并转为 Follower。
func (r *Raft) sendVoteRequest(peerId int, term uint64, lastLogIndex uint64, lastLogTerm uint64, voteChan chan<- *param.VoteResult) {
	args := param.NewRequestVoteArgs(term, r.id, lastLogIndex, lastLogTerm)
	reply := param.NewRequestVoteReply()

	if err := r.trans.SendRequestVote(strconv.Itoa(peerId), args, reply); err != nil {
		log.Printf("[Election] Node %d failed to request vote from %d: %v", r.id, peerId, err)
		voteChan <- &param.VoteResult{VoterID: peerId, VoteGranted: false}
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	// 如果收到更高term的响应，立即转为Follower
	if reply.Term > r.currentTerm {
		log.Printf("[Election] Node %d found higher term %d from peer %d, becomes Follower", r.id, reply.Term, peerId)
		r.currentTerm = reply.Term
		r.state = param.Follower
		r.votedFor = -1
		if err := r.store.SetState(param.HardState{CurrentTerm: r.currentTerm, VotedFor: uint64(r.votedFor)}); err != nil {
			log.Printf("[ERROR] Node %d failed to persist state after finding higher term: %v", r.id, err)
		}
	}

	voteChan <- &param.VoteResult{VoterID: peerId, VoteGranted: reply.VoteGranted}
}

// initLeaderState initializes leader state after election
func (r *Raft) initLeaderState() {
	// This method is called with the lock held.
	lastLogIndex, err := r.store.LastLogIndex()
	if err != nil {
		log.Printf("[ERROR] Node %d (new leader) failed to get last log index to initialize state: %v", r.id, err)
		// This is a critical error, might need to step down.
		r.state = param.Follower
		return
	}

	r.nextIndex = make(map[int]uint64)
	r.matchIndex = make(map[int]uint64)
	for _, peerId := range r.peerIds {
		r.nextIndex[peerId] = lastLogIndex + 1
		r.matchIndex[peerId] = 0
	}
}

// startHeartbeat starts periodic heartbeat loops
func (r *Raft) startHeartbeat() {
	// This method is called with the lock held.
	go func() {
		ticker := time.NewTicker(heartbeatInterval)
		defer ticker.Stop()

		// Send an initial heartbeat immediately without waiting for the first tick.
		r.broadcastHeartbeat()

		for {
			<-ticker.C

			r.mu.Lock()
			if r.state != param.Leader {
				r.mu.Unlock()
				return
			}
			r.broadcastHeartbeat()
			r.mu.Unlock()
		}
	}()
}

// broadcastHeartbeat is a helper to send AppendEntries to all peers.
func (r *Raft) broadcastHeartbeat() {
	// This method must be called with the lock held.
	for _, peerId := range r.peerIds {
		go r.sendAppendEntries(peerId)
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
