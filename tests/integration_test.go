package tests

import (
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/xmh1011/go-raft/param"
	"github.com/xmh1011/go-raft/raft"
	"github.com/xmh1011/go-raft/storage/inmemory"
	"github.com/xmh1011/go-raft/transport/tcp"
)

// cluster 封装了测试集群的组件
type cluster struct {
	nodes         []*raft.Raft
	transports    []*tcp.Transport
	stateMachines []*inmemory.StateMachine
	commitChans   []chan param.CommitEntry
	peerMap       map[int]string
}

// newCluster 创建并启动一个新的测试集群
func newCluster(t *testing.T, nodeCount int) *cluster {
	c := &cluster{
		nodes:         make([]*raft.Raft, nodeCount),
		transports:    make([]*tcp.Transport, nodeCount),
		stateMachines: make([]*inmemory.StateMachine, nodeCount),
		commitChans:   make([]chan param.CommitEntry, nodeCount),
		peerMap:       make(map[int]string),
	}

	peerIDs := make([]int, 0)

	// 1. 初始化 Transport
	for i := 0; i < nodeCount; i++ {
		id := i + 1
		peerIDs = append(peerIDs, id)
		trans, err := tcp.NewTCPTransport("127.0.0.1:0")
		if err != nil {
			t.Fatalf("failed to create transport for node %d: %v", id, err)
		}
		c.transports[i] = trans
		c.peerMap[id] = trans.Addr()
	}

	// 2. 初始化并启动节点
	for i := 0; i < nodeCount; i++ {
		id := i + 1
		store := inmemory.NewInMemoryStorage()
		sm := inmemory.NewInMemoryStateMachine()
		c.stateMachines[i] = sm
		c.commitChans[i] = make(chan param.CommitEntry, 100)

		c.transports[i].SetPeers(c.peerMap)

		rf := raft.NewRaft(id, peerIDs, store, sm, c.transports[i], c.commitChans[i])
		c.nodes[i] = rf

		c.transports[i].RegisterRaft(rf)
		if err := c.transports[i].Start(); err != nil {
			t.Fatalf("failed to start transport for node %d: %v", id, err)
		}

		go rf.Run()
	}

	return c
}

// shutdown 关闭集群
func (c *cluster) shutdown() {
	for i := 0; i < len(c.nodes); i++ {
		c.nodes[i].Stop()
		c.transports[i].Close()
	}
}

// getLeader 等待并返回当前的 Leader
func (c *cluster) getLeader(t *testing.T) *raft.Raft {
	probeCmd := param.KVCommand{Op: "get", Key: "probe-key"}
	probeCmdBytes, _ := json.Marshal(probeCmd)

	for i := 0; i < 40; i++ { // 增加重试次数
		time.Sleep(200 * time.Millisecond)
		for _, node := range c.nodes {
			if node.IsStopped() {
				continue
			}

			args := &param.ClientArgs{
				ClientID:    100,
				SequenceNum: int64(i),
				Command:     probeCmdBytes,
			}
			reply := &param.ClientReply{}

			// 忽略错误，因为节点可能正在选举中
			_ = node.ClientRequest(args, reply)

			// 只有当节点不是Leader且读请求成功时，才认为它是真正的Leader
			// reply.Success为true意味着ReadIndex检查已通过，保证了Leader的稳定性
			if !reply.NotLeader {
				if reply.Success {
					return node
				}
				// 如果请求失败是因为 key not found，说明已经通过了 ReadIndex 检查并执行了状态机查询，
				// 这也证明了它是 Leader。
				if errMsg, ok := reply.Result.(string); ok && errMsg == "key not found" {
					return node
				}
				// 打印调试信息
				t.Logf("Node %d is leader but request failed: Success=%v, Result=%v", node.ID(), reply.Success, reply.Result)
			}
		}
	}
	t.Fatal("Cluster failed to elect a leader within timeout")
	return nil
}

// TestCluster_ElectionAndReplication 测试基本的选举和日志复制
func TestCluster_ElectionAndReplication(t *testing.T) {
	c := newCluster(t, 3)
	defer c.shutdown()

	leader := c.getLeader(t)
	t.Logf("Leader elected: Node %d", leader.ID())

	// 发送写请求
	key := "test-key"
	value := "test-value"
	cmd := param.KVCommand{Op: "set", Key: key, Value: value}
	cmdBytes, _ := json.Marshal(cmd)

	clientArgs := &param.ClientArgs{ClientID: 999, SequenceNum: 1, Command: cmdBytes}
	clientReply := &param.ClientReply{}

	err := leader.ClientRequest(clientArgs, clientReply)
	assert.NoError(t, err)
	assert.True(t, clientReply.Success)

	// 验证一致性
	time.Sleep(1 * time.Second)
	for i := 0; i < 3; i++ {
		val, err := c.stateMachines[i].Get(key)
		assert.NoError(t, err)
		assert.Equal(t, value, val)
	}
}

// TestCluster_LeaderFailover 测试 Leader 宕机后的故障转移
func TestCluster_LeaderFailover(t *testing.T) {
	c := newCluster(t, 3)
	defer c.shutdown()

	// 1. 找到第一个 Leader
	oldLeader := c.getLeader(t)
	t.Logf("Original Leader: Node %d", oldLeader.ID())

	// 2. 写入数据
	cmd1, _ := json.Marshal(param.KVCommand{Op: "set", Key: "k1", Value: "v1"})
	reply1 := &param.ClientReply{}
	err := oldLeader.ClientRequest(&param.ClientArgs{ClientID: 1, SequenceNum: 1, Command: cmd1}, reply1)
	assert.NoError(t, err)
	assert.True(t, reply1.Success, "Write to old leader should succeed")

	// 3. 停止 Leader (模拟宕机)
	t.Logf("Stopping Leader Node %d...", oldLeader.ID())
	oldLeader.Stop()
	// 关闭 Transport 以模拟网络不可达
	for i, node := range c.nodes {
		if node == oldLeader {
			c.transports[i].Close()
			break
		}
	}

	// 4. 等待新 Leader 产生
	t.Log("Waiting for new leader...")
	// 给足够的时间让其他节点超时并选举
	time.Sleep(2 * time.Second)

	newLeader := c.getLeader(t)
	t.Logf("New Leader: Node %d", newLeader.ID())
	assert.NotEqual(t, oldLeader.ID(), newLeader.ID(), "New leader should be different")

	// 5. 向新 Leader 写入数据
	cmd2, _ := json.Marshal(param.KVCommand{Op: "set", Key: "k2", Value: "v2"})
	reply2 := &param.ClientReply{}
	err = newLeader.ClientRequest(&param.ClientArgs{ClientID: 1, SequenceNum: 2, Command: cmd2}, reply2)
	assert.NoError(t, err)
	assert.True(t, reply2.Success, "Write to new leader should succeed")

	// 6. 验证数据 (k1 应该还在，k2 应该被写入)
	time.Sleep(1 * time.Second)
	for i, node := range c.nodes {
		if node.ID() == oldLeader.ID() {
			continue // 跳过已停止的节点
		}
		val1, err1 := c.stateMachines[i].Get("k1")
		assert.NoError(t, err1, "Should be able to get k1 from node %d", node.ID())
		assert.Equal(t, "v1", val1, "Data from old leader should be preserved on node %d", node.ID())

		val2, err2 := c.stateMachines[i].Get("k2")
		assert.NoError(t, err2, "Should be able to get k2 from node %d", node.ID())
		assert.Equal(t, "v2", val2, "Data from new leader should be replicated to node %d", node.ID())
	}
}

// TestCluster_NetworkPartition 测试网络分区场景
// 场景：3个节点 [1, 2, 3]，Leader 是 1。
// 分区：[1] 隔离，[2, 3] 连通。
// 预期：[2, 3] 选出新 Leader，[1] 降级或无法提交。
func TestCluster_NetworkPartition(t *testing.T) {
	c := newCluster(t, 3)
	defer c.shutdown()

	leader := c.getLeader(t)
	leaderID := leader.ID()
	t.Logf("Leader: Node %d", leaderID)

	// 确定分区方案：Leader 独自一组，其他节点一组
	partitionedNodeID := leaderID

	// 模拟分区：修改 Transport 的 Peer 映射
	// 让 Leader 无法连接其他人，其他人也无法连接 Leader
	t.Logf("Isolating Node %d...", partitionedNodeID)

	// 1. 清空 Leader 的 Peer 映射 (它发不出消息)
	for i, node := range c.nodes {
		if node.ID() == partitionedNodeID {
			c.transports[i].SetPeers(make(map[int]string)) // 空映射
		} else {
			// 2. 从其他节点的映射中移除 Leader (它们发给 Leader 的消息会失败)
			newPeers := make(map[int]string)
			for id, addr := range c.peerMap {
				if id != partitionedNodeID {
					newPeers[id] = addr
				}
			}
			t.Logf("Node %d new peers: %v", node.ID(), newPeers) // 打印调试信息
			c.transports[i].SetPeers(newPeers)
		}
	}

	// 等待新 Leader 在多数派分区产生
	t.Log("Waiting for new leader in majority partition...")
	time.Sleep(5 * time.Second) // 增加等待时间

	// 在多数派节点中寻找新 Leader
	var newLeader *raft.Raft
	var majorityNodes []*raft.Raft
	for _, node := range c.nodes {
		if node.ID() != partitionedNodeID {
			majorityNodes = append(majorityNodes, node)
		}
	}

	// 使用 getLeader 在多数派分区中寻找新 Leader
	probeCmd := param.KVCommand{Op: "get", Key: "probe-key"}
	probeCmdBytes, _ := json.Marshal(probeCmd)
	foundLeader := false
	for i := 0; i < 20 && !foundLeader; i++ {
		time.Sleep(200 * time.Millisecond)
		for _, node := range majorityNodes {
			reply := &param.ClientReply{}
			node.ClientRequest(&param.ClientArgs{Command: probeCmdBytes}, reply)
			if !reply.NotLeader {
				if reply.Success {
					newLeader = node
					foundLeader = true
					break
				}
				if errMsg, ok := reply.Result.(string); ok && errMsg == "key not found" {
					newLeader = node
					foundLeader = true
					break
				}
			}
		}
	}

	if newLeader == nil {
		t.Fatal("Majority partition failed to elect a new leader")
	}

	assert.NotEqual(t, partitionedNodeID, newLeader.ID())
	t.Logf("New Leader in majority partition: Node %d", newLeader.ID())

	// 向新 Leader 写入
	cmd, _ := json.Marshal(param.KVCommand{Op: "set", Key: "partition-key", Value: "val"})
	reply := &param.ClientReply{}
	newLeader.ClientRequest(&param.ClientArgs{Command: cmd}, reply)
	assert.True(t, reply.Success)

	// 恢复分区
	t.Log("Healing partition...")
	for i := 0; i < 3; i++ {
		c.transports[i].SetPeers(c.peerMap)
	}

	// 等待集群同步
	time.Sleep(3 * time.Second)

	// 验证旧 Leader 是否追上了数据
	for i, node := range c.nodes {
		if node.ID() == partitionedNodeID {
			val, err := c.stateMachines[i].Get("partition-key")
			assert.NoError(t, err)
			assert.Equal(t, "val", val, "Healed node should have the new data")
		}
	}
}

// TestCluster_ConcurrentClientRequests 测试并发客户端请求
func TestCluster_ConcurrentClientRequests(t *testing.T) {
	c := newCluster(t, 3)
	defer c.shutdown()

	leader := c.getLeader(t)
	t.Logf("Leader: Node %d", leader.ID())

	// 并发请求数量
	const concurrentRequests = 50
	var wg sync.WaitGroup
	wg.Add(concurrentRequests)

	// 启动多个goroutine发送并发请求
	for i := 0; i < concurrentRequests; i++ {
		go func(seq int) {
			defer wg.Done()
			key := fmt.Sprintf("concurrent-key-%d", seq)
			value := fmt.Sprintf("value-%d", seq)
			cmd := param.KVCommand{Op: "set", Key: key, Value: value}
			cmdBytes, _ := json.Marshal(cmd)

			args := &param.ClientArgs{
				ClientID:    int64(100 + seq), // Use different client IDs for simplicity
				SequenceNum: 1,
				Command:     cmdBytes,
			}
			reply := &param.ClientReply{}
			err := leader.ClientRequest(args, reply)
			assert.NoError(t, err, "Concurrent request should not return error")
			assert.True(t, reply.Success, "Concurrent request should succeed")
		}(i)
	}

	wg.Wait()

	// 验证数据一致性
	t.Log("Verifying data consistency after concurrent requests...")
	time.Sleep(2 * time.Second)

	for i := 0; i < concurrentRequests; i++ {
		key := fmt.Sprintf("concurrent-key-%d", i)
		expectedValue := fmt.Sprintf("value-%d", i)

		// Check leader first
		leaderVal, err := c.stateMachines[leader.ID()-1].Get(key)
		assert.NoError(t, err)
		assert.Equal(t, expectedValue, leaderVal)

		// Check followers
		for j := 0; j < 3; j++ {
			if c.nodes[j].ID() == leader.ID() {
				continue
			}
			followerVal, err := c.stateMachines[j].Get(key)
			assert.NoError(t, err)
			assert.Equal(t, expectedValue, followerVal, "Follower should have the same data")
		}
	}
}

// TestCluster_LogReplication 测试大量日志复制和并发写入
func TestCluster_LogReplication(t *testing.T) {
	c := newCluster(t, 3)
	defer c.shutdown()

	leader := c.getLeader(t)
	t.Logf("Leader elected: Node %d", leader.ID())

	// 1. 顺序写入 50 条日志
	logCount := 50
	t.Logf("Writing %d logs sequentially...", logCount)
	for i := 0; i < logCount; i++ {
		key := fmt.Sprintf("seq-key-%d", i)
		value := fmt.Sprintf("seq-val-%d", i)
		cmd, _ := json.Marshal(param.KVCommand{Op: "set", Key: key, Value: value})

		reply := &param.ClientReply{}
		err := leader.ClientRequest(&param.ClientArgs{ClientID: 1, SequenceNum: int64(i), Command: cmd}, reply)
		assert.NoError(t, err)
		assert.True(t, reply.Success)
	}

	// 2. 并发写入 20 条日志
	t.Log("Writing 20 logs concurrently...")
	var wg sync.WaitGroup
	concurrency := 20
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			key := fmt.Sprintf("conc-key-%d", idx)
			value := fmt.Sprintf("conc-val-%d", idx)
			cmd, _ := json.Marshal(param.KVCommand{Op: "set", Key: key, Value: value})

			reply := &param.ClientReply{}
			// 注意：并发写入时，SequenceNum 应该不同，或者由 Client 管理
			// 这里简单起见，使用不同的 ClientID
			err := leader.ClientRequest(&param.ClientArgs{ClientID: int64(100 + idx), SequenceNum: 1, Command: cmd}, reply)
			assert.NoError(t, err)
			assert.True(t, reply.Success)
		}(i)
	}
	wg.Wait()

	// 3. 验证所有节点的数据一致性
	t.Log("Verifying data consistency...")
	time.Sleep(2 * time.Second) // 等待复制完成

	for i := 0; i < 3; i++ {
		// 验证顺序写入的数据
		for j := 0; j < logCount; j++ {
			key := fmt.Sprintf("seq-key-%d", j)
			val, err := c.stateMachines[i].Get(key)
			assert.NoError(t, err)
			assert.Equal(t, fmt.Sprintf("seq-val-%d", j), val)
		}
		// 验证并发写入的数据
		for j := 0; j < concurrency; j++ {
			key := fmt.Sprintf("conc-key-%d", j)
			val, err := c.stateMachines[i].Get(key)
			assert.NoError(t, err)
			assert.Equal(t, fmt.Sprintf("conc-val-%d", j), val)
		}
	}
}
