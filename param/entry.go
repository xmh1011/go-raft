package param

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
