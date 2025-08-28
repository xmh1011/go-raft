package param

// State 定义节点的状态（Consensus Module State）
type State int

const (
	Follower State = iota
	Candidate
	Leader
	Dead // 可选：表示节点已终止（用于优雅关闭）
)

func (s State) String() string {
	switch s {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	case Dead:
		return "Dead"
	default:
		return "Unknown"
	}
}

// RequestVoteArgs See figure 2 in the paper.
type RequestVoteArgs struct {
	Term         uint64 // 候选人的任期号
	CandidateId  int    // 候选人的ID
	LastLogIndex uint64 // 候选人最后一条日志的索引
	LastLogTerm  uint64 // 候选人最后一条日志的任期号
}

func NewRequestVoteArgs(term uint64, candidateId int, lastLogIndex, lastLogTerm uint64) *RequestVoteArgs {
	return &RequestVoteArgs{
		Term:         term,
		CandidateId:  candidateId,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}
}

// RequestVoteReply 定义RequestVote RPC响应 See figure 2 in the paper.
type RequestVoteReply struct {
	Term        uint64 // 当前节点的任期号（用于候选者更新自身）
	VoteGranted bool   // 是否投票给候选者
}

func NewRequestVoteReply() *RequestVoteReply {
	return &RequestVoteReply{
		Term:        0,
		VoteGranted: false,
	}
}

// AppendEntriesArgs is the RPC argument for appendEntries requests (log replication + heartbeats).
type AppendEntriesArgs struct {
	Term         uint64     // Leader's current term
	LeaderId     int        // Leader's ID (for follower redirection)
	PrevLogIndex uint64     // Index of log entry immediately preceding new ones
	PrevLogTerm  uint64     // Term of PrevLogIndex entry
	Entries      []LogEntry // Log entries to store (empty for heartbeat)
	LeaderCommit uint64     // Leader's commitIndex
}

// NewAppendEntriesArgs creates a new AppendEntriesArgs struct.
func NewAppendEntriesArgs(term uint64, leaderId int, prevLogIndex, prevLogTerm, leaderCommit uint64, entries []LogEntry) *AppendEntriesArgs {
	return &AppendEntriesArgs{
		Term:         term,
		LeaderId:     leaderId,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: leaderCommit,
	}
}

// AppendEntriesReply is the RPC response for appendEntries requests.
type AppendEntriesReply struct {
	Term          uint64 // Current term (for leader to update itself)
	Success       bool   // True if follower contained entry matching PrevLogIndex/Term
	ConflictIndex uint64 // If conflict, the first index where they differ
	ConflictTerm  uint64 // If conflict, the term of the conflicting entry
}

// NewAppendEntriesReply creates a new AppendEntriesReply struct.
func NewAppendEntriesReply() *AppendEntriesReply {
	return &AppendEntriesReply{
		Term:          0,
		Success:       false,
		ConflictIndex: 0,
		ConflictTerm:  0,
	}
}

// LogEntry represents a single log entry in the Raft log.
type LogEntry struct {
	Command any
	Term    uint64
	Index   uint64 // Log index, 0 means no index (e.g., for heartbeats)
}

// NewLogEntry creates a new LogEntry.
func NewLogEntry(command any, term, index uint64) LogEntry {
	return LogEntry{
		Command: command,
		Term:    term,
		Index:   index,
	}
}

// CommitEntry is the data reported by Raft to the commit channel.
// Each commit entry notifies the client that consensus was reached on a command,
// and it can be applied to the client's state machine.
type CommitEntry struct {
	// Command is the client command being committed
	Command any

	// Index is the log index at which the client command is committed
	Index uint64

	// Term is the Raft term at which the client command is committed
	Term uint64
}

// Snapshot 表示 Raft 的快照结构
type Snapshot struct {
	LastIncludedIndex uint64 // 快照中包含的最后一条日志的索引
	LastIncludedTerm  uint64 // 快照中包含的最后一条日志的任期
	Data              []byte // 状态机的快照数据
}

// NewSnapshot 创建一个新的 Snapshot 实例
func NewSnapshot(lastIncludedIndex, lastIncludedTerm uint64, data []byte) *Snapshot {
	return &Snapshot{
		LastIncludedIndex: lastIncludedIndex,
		LastIncludedTerm:  lastIncludedTerm,
		Data:              data,
	}
}

// HardState 定义需要持久化的状态（必须稳定存储）
type HardState struct {
	CurrentTerm uint64 // 当前任期号
	VotedFor    uint64 // 当前任期内投票给的候选者ID（0表示未投票）
	CommitIndex uint64 // 已知已提交的最高日志索引
}

// InstallSnapshotArgs 定义InstallSnapshot RPC请求
type InstallSnapshotArgs struct {
	Term              uint64 // 领导者的任期号
	LeaderID          uint64 // 领导者ID
	LastIncludedIndex uint64 // 快照中包含的最后一条日志的索引
	LastIncludedTerm  uint64 // 快照中包含的最后一条日志的任期号
	Offset            uint64 // 数据偏移量（分块传输时使用）
	Data              []byte // 原始快照数据
	Done              bool   // 是否为最后一块
}

// NewInstallSnapshotArgs 创建一个新的 InstallSnapshotArgs 实例
func NewInstallSnapshotArgs(term, leaderID, lastIncludedIndex, lastIncludedTerm uint64, data []byte) *InstallSnapshotArgs {
	return &InstallSnapshotArgs{
		Term:              term,
		LeaderID:          leaderID,
		LastIncludedIndex: lastIncludedIndex,
		LastIncludedTerm:  lastIncludedTerm,
		Offset:            0,
		Data:              data,
		Done:              true,
	}
}

// InstallSnapshotReply 定义InstallSnapshot RPC响应
type InstallSnapshotReply struct {
	Term uint64 // 当前节点的任期号
}

// ConfigChangeCommand holds the new list of peer IDs for a configuration change.
// This command is stored in a LogEntry to be replicated.
type ConfigChangeCommand struct {
	NewPeerIDs []int
}

// NewConfigChangeCommand creates a new ConfigChangeCommand.
func NewConfigChangeCommand(newPeerIDs []int) ConfigChangeCommand {
	return ConfigChangeCommand{
		NewPeerIDs: newPeerIDs,
	}
}

// VoteResult is used to carry the result of a RequestVote RPC,
// including the voter's ID for joint consensus counting.
type VoteResult struct {
	VoterID     int  // The ID of the node that cast the vote.
	VoteGranted bool // Whether the vote was granted.
}

// ClientArgs 封装了来自客户端的请求。
type ClientArgs struct {
	ClientID    int64 // 客户端的唯一ID
	SequenceNum int64 // 客户端为每个请求生成的、单调递增的序列号
	Command     any   // 需要在状态机上执行的命令
}

// NewClientArgs 创建一个新的 ClientArgs 实例。
func NewClientArgs(clientID, sequenceNum int64, command any) *ClientArgs {
	return &ClientArgs{
		ClientID:    clientID,
		SequenceNum: sequenceNum,
		Command:     command,
	}
}

// ClientReply 是 Raft 节点对客户端请求的响应。
type ClientReply struct {
	Success    bool // 请求是否成功处理
	Result     any  // 命令执行后的返回值
	NotLeader  bool // 如果当前节点不是 Leader，此项为 true
	LeaderHint int  // 当前已知的 Leader ID，用于客户端重定向
}

// KVCommand 定义了客户端与状态机交互的命令格式。
type KVCommand struct {
	Op    string `json:"op"`
	Key   string `json:"key"`
	Value string `json:"value"`
}
