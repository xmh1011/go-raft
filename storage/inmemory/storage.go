package inmemory

import (
	"errors"
	"sync"

	"github.com/xmh1011/go-raft/param"
)

var (
	ErrLogNotFound      = errors.New("log entry not found")
	ErrIndexOutOfBounds = errors.New("index is out of bounds")
)

// Storage 是 Storage 接口的一个线程安全的内存实现，主要用于测试。
type Storage struct {
	mu sync.RWMutex

	// HardState (term, votedFor)
	hardState param.HardState

	// Snapshot
	snapshot *param.Snapshot

	// Log entries
	// 为了处理日志压缩，我们使用一个偏移量来记录第一个日志条目的实际 Raft 索引。
	// log[0] 的真实索引是 logOffset。
	log       []param.LogEntry
	logOffset uint64
}

// NewInMemoryStorage 创建一个新的内存存储实例。
func NewInMemoryStorage() *Storage {
	s := &Storage{
		log:       make([]param.LogEntry, 1), // 日志索引从1开始，所以log[0]是一个哑元
		logOffset: 0,
	}
	return s
}

// --- HardState 操作 ---

func (s *Storage) SetState(state param.HardState) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.hardState = state
	return nil
}

func (s *Storage) GetState() (param.HardState, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.hardState, nil
}

// --- 日志条目操作 ---

func (s *Storage) AppendEntries(entries []param.LogEntry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.log = append(s.log, entries...)
	return nil
}

func (s *Storage) GetEntry(index uint64) (*param.LogEntry, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// 检查索引是否在当前日志范围内
	if index < s.logOffset || index >= s.logOffset+uint64(len(s.log)) {
		return nil, ErrLogNotFound
	}
	return &s.log[index-s.logOffset], nil
}

func (s *Storage) TruncateLog(fromIndex uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if fromIndex < s.logOffset {
		return ErrIndexOutOfBounds
	}
	if fromIndex >= s.logOffset+uint64(len(s.log)) {
		// 如果索引超出当前日志范围，无需截断
		return nil
	}

	s.log = s.log[:fromIndex-s.logOffset]
	return nil
}

// --- 日志元数据操作 ---

func (s *Storage) FirstLogIndex() (uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	// 真实的第一个日志索引是偏移量+1
	return s.logOffset + 1, nil
}

func (s *Storage) LastLogIndex() (uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.logOffset + uint64(len(s.log)) - 1, nil
}

func (s *Storage) LogSize() (int, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.log), nil
}

// --- 快照操作 ---

func (s *Storage) SaveSnapshot(snapshot *param.Snapshot) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.snapshot = snapshot
	return nil
}

func (s *Storage) ReadSnapshot() (*param.Snapshot, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.snapshot, nil
}

func (s *Storage) CompactLog(upToIndex uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if upToIndex < s.logOffset {
		// 已经压缩过了
		return nil
	}
	if upToIndex >= s.logOffset+uint64(len(s.log)) {
		return ErrIndexOutOfBounds
	}

	sliceIndex := upToIndex - s.logOffset
	// 保留 upToIndex 之后的所有日志
	remainingLogs := make([]param.LogEntry, len(s.log)-int(sliceIndex))
	copy(remainingLogs, s.log[sliceIndex:])

	s.log = remainingLogs
	s.logOffset = upToIndex
	return nil
}

// Close 在内存实现中通常是无操作的。
func (s *Storage) Close() error {
	return nil
}
