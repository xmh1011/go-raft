package tests

import (
	"encoding/json"
	"fmt"
	"math/rand"
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
	storages      []*inmemory.Storage
	commitChans   []chan param.CommitEntry
	peerMap       map[int]string
}

// newCluster 创建并启动一个新的测试集群
// 它只是 newClusterWithConfig 的一个特例：物理节点数 = 初始配置节点数
func newCluster(t *testing.T, nodeCount int) *cluster {
	return newClusterWithConfig(t, nodeCount, nodeCount)
}

// newClusterWithConfig 是核心构建函数
// totalNodes: 物理启动的节点总数 (例如 5 个，对应 Transport 和 Storage)
// initialConfigSize: 初始 Raft 配置中的节点数 (例如 3 个，即 Node 1,2,3 在集群配置中)
func newClusterWithConfig(t *testing.T, totalNodes int, initialConfigSize int) *cluster {
	c := &cluster{
		nodes:         make([]*raft.Raft, totalNodes),
		transports:    make([]*tcp.Transport, totalNodes),
		stateMachines: make([]*inmemory.StateMachine, totalNodes),
		storages:      make([]*inmemory.Storage, totalNodes),
		commitChans:   make([]chan param.CommitEntry, totalNodes),
		peerMap:       make(map[int]string),
	}

	// 1. 初始化所有节点的 Transport (物理网络全通)
	// 无论是初始节点还是待加入的节点，都需要有网络地址
	for i := 0; i < totalNodes; i++ {
		id := i + 1
		trans, err := tcp.NewTCPTransport("127.0.0.1:0")
		if err != nil {
			t.Fatalf("failed to create transport for node %d: %v", id, err)
		}
		c.transports[i] = trans
		c.peerMap[id] = trans.Addr()
	}

	// 2. 构造 Raft 的初始配置列表 (只包含前 initialConfigSize 个节点)
	// 例如 totalNodes=5, initialConfigSize=3, 则 peerIDs=[1, 2, 3]
	initialPeerIDs := make([]int, 0)
	for i := 0; i < initialConfigSize; i++ {
		initialPeerIDs = append(initialPeerIDs, i+1)
	}

	// 3. 启动所有节点
	for i := 0; i < totalNodes; i++ {
		id := i + 1
		store := inmemory.NewInMemoryStorage()
		c.storages[i] = store
		sm := inmemory.NewInMemoryStateMachine()
		c.stateMachines[i] = sm
		c.commitChans[i] = make(chan param.CommitEntry, 100)

		// 注册所有的物理 Peer 地址
		// 注意：Transport 层需要知道所有人的地址以便建立连接，
		// 但 Raft 层逻辑只认 initialPeerIDs 里的节点为“选民”。
		c.transports[i].SetPeers(c.peerMap)

		// 创建 Raft 实例
		// 对于不在 initialPeerIDs 中的节点（如 Node 4），它们启动时认为集群由 [1,2,3] 组成，
		// 自己只是一个观察者（或者 Follower），等待被 ConfigChange 加入。
		rf := raft.NewRaft(id, initialPeerIDs, store, sm, c.transports[i], c.commitChans[i])
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
				// t.Logf("Node %d is leader but request failed: Success=%v, Result=%v", node.ID(), reply.Success, reply.Result)
			}
		}
	}
	t.Fatal("Cluster failed to elect a leader within timeout")
	return nil
}

// restartNode 模拟节点崩溃重启：停止旧节点，使用相同的 Storage/StateMachine 启动新节点
func (c *cluster) restartNode(t *testing.T, nodeIndex int) {
	oldNode := c.nodes[nodeIndex]
	id := oldNode.ID()

	// 1. 停止旧节点
	oldNode.Stop()
	c.transports[nodeIndex].Close()

	// 2. 创建新的 Transport (模拟进程重启，端口重新绑定)
	// 注意：这里必须复用之前的地址，否则 Peer 找不到它
	prevAddr := c.peerMap[id]
	newTrans, err := tcp.NewTCPTransport(prevAddr)
	if err != nil {
		t.Fatalf("failed to recreate transport for node %d: %v", id, err)
	}
	c.transports[nodeIndex] = newTrans

	// 3. 关键：复用旧的 Storage 和 StateMachine
	store := c.storages[nodeIndex]
	sm := c.stateMachines[nodeIndex]

	// 重新构建 peerIDs 列表
	peerIDs := make([]int, 0)
	for k := range c.peerMap {
		peerIDs = append(peerIDs, k)
	}

	// 4. 创建新 Raft 实例
	newRaft := raft.NewRaft(id, peerIDs, store, sm, newTrans, c.commitChans[nodeIndex])
	c.nodes[nodeIndex] = newRaft

	// 5. 重新配置 Transport
	newTrans.SetPeers(c.peerMap)
	newTrans.RegisterRaft(newRaft)
	newTrans.Start()

	// 6. 启动
	go newRaft.Run()
	t.Logf("Node %d restarted with persistent state.", id)
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

// TestCluster_TakeSnapshot 测试手动触发快照及其对数据一致性的影响
func TestCluster_TakeSnapshot(t *testing.T) {
	c := newCluster(t, 3)
	defer c.shutdown()

	leader := c.getLeader(t)
	t.Logf("Leader elected: Node %d", leader.ID())

	// 1. 写入一定量的数据
	logCount := 30
	t.Logf("Writing %d logs...", logCount)
	for i := 0; i < logCount; i++ {
		key := fmt.Sprintf("snap-key-%d", i)
		value := fmt.Sprintf("snap-val-%d", i)
		cmd, _ := json.Marshal(param.KVCommand{Op: "set", Key: key, Value: value})

		reply := &param.ClientReply{}
		err := leader.ClientRequest(&param.ClientArgs{ClientID: 1, SequenceNum: int64(i), Command: cmd}, reply)
		assert.NoError(t, err)
		assert.True(t, reply.Success)
	}

	// 等待所有节点同步
	time.Sleep(1 * time.Second)

	// 2. 在 Leader 上触发快照
	// 假设 TakeSnapshot 会对当前的 commitIndex/lastApplied 进行快照
	t.Log("Taking snapshot on Leader...")
	leader.TakeSnapshot()

	// 3. 验证快照后的数据依然可以读取（验证状态机没有丢失数据）
	for i := 0; i < logCount; i++ {
		key := fmt.Sprintf("snap-key-%d", i)
		val, err := c.stateMachines[leader.ID()-1].Get(key)
		assert.NoError(t, err)
		assert.Equal(t, fmt.Sprintf("snap-val-%d", i), val)
	}

	// 4. 快照后继续写入新数据，验证系统依然正常工作
	newKey := "post-snap-key"
	newValue := "post-snap-value"
	cmd, _ := json.Marshal(param.KVCommand{Op: "set", Key: newKey, Value: newValue})
	reply := &param.ClientReply{}
	err := leader.ClientRequest(&param.ClientArgs{ClientID: 1, SequenceNum: 100, Command: cmd}, reply)
	assert.NoError(t, err)
	assert.True(t, reply.Success)

	// 验证新数据同步
	time.Sleep(500 * time.Millisecond)
	val, err := c.stateMachines[leader.ID()-1].Get(newKey)
	assert.Equal(t, newValue, val)
}

// TestCluster_InstallSnapshot 测试落后节点通过 InstallSnapshot RPC 恢复数据
// 场景：
// 1. 隔离 Follower。
// 2. Leader 写入大量数据并执行快照（此时旧日志被丢弃）。
// 3. Leader 再写入少量新数据。
// 4. 重新连接 Follower。
// 5. Follower 发现日志缺失，Leader 必须发送快照。
func TestCluster_InstallSnapshot(t *testing.T) {
	c := newCluster(t, 3)
	defer c.shutdown()

	leader := c.getLeader(t)
	leaderID := leader.ID()
	t.Logf("Leader: Node %d", leaderID)

	// 1. 找一个 Follower 并将其隔离
	var follower *raft.Raft
	for _, node := range c.nodes {
		if node.ID() != leaderID {
			follower = node
			break
		}
	}
	assert.NotNil(t, follower)
	t.Logf("Isolating Follower: Node %d", follower.ID())

	// 修改 Transport 隔离该 Follower
	// 移除 Follower 的 Peer 列表，让它无法联系别人
	c.transports[follower.ID()-1].SetPeers(make(map[int]string))
	// 从其他节点移除该 Follower
	newPeers := make(map[int]string)
	for id, addr := range c.peerMap {
		if id != follower.ID() {
			newPeers[id] = addr
		}
	}
	for i, node := range c.nodes {
		if node.ID() != follower.ID() {
			c.transports[i].SetPeers(newPeers)
		}
	}

	// 2. Leader 写入数据 (Part 1)，这部分数据将被快照
	snapLogCount := 20
	t.Logf("Writing %d logs to be snapshotted...", snapLogCount)
	for i := 0; i < snapLogCount; i++ {
		key := fmt.Sprintf("k-%d", i)
		cmd, _ := json.Marshal(param.KVCommand{Op: "set", Key: key, Value: "v"})
		err := leader.ClientRequest(&param.ClientArgs{ClientID: 1, SequenceNum: int64(i), Command: cmd}, &param.ClientReply{})
		assert.NoError(t, err)
	}

	// 3. Leader 执行快照
	// 关键点：执行快照后，Leader 的日志中 index 1~20 应该被压缩/删除
	t.Log("Leader taking snapshot...")
	leader.TakeSnapshot()

	// 4. Leader 继续写入数据 (Part 2)，这部分是快照后的日志
	// 这样 Follower 恢复时，应该先接收快照(0-20)，再接收日志(21-30)
	extraLogCount := 10
	t.Logf("Writing %d extra logs after snapshot...", extraLogCount)
	for i := snapLogCount; i < snapLogCount+extraLogCount; i++ {
		key := fmt.Sprintf("k-%d", i)
		cmd, _ := json.Marshal(param.KVCommand{Op: "set", Key: key, Value: "v"})
		err := leader.ClientRequest(&param.ClientArgs{ClientID: 1, SequenceNum: int64(i), Command: cmd}, &param.ClientReply{})
		assert.NoError(t, err)
	}

	// 5. 重新连接 Follower
	assert.NotNil(t, follower)
	t.Logf("Reconnecting Follower Node %d...", follower.ID())
	for i := 0; i < 3; i++ {
		c.transports[i].SetPeers(c.peerMap)
	}

	// 6. 等待同步
	// 由于涉及发送快照（数据量较大）和日志回放，给予较长的等待时间
	t.Log("Waiting for Follower to catch up via Snapshot + Logs...")
	time.Sleep(3 * time.Second)

	// 7. 验证 Follower 数据
	// 验证它是否拥有快照里的数据
	val, err := c.stateMachines[follower.ID()-1].Get("k-0")
	assert.NoError(t, err)
	assert.Equal(t, "v", val, "Follower should recover data from Snapshot")

	// 验证它是否拥有快照后的新日志数据
	lastLogKey := fmt.Sprintf("k-%d", snapLogCount+extraLogCount-1)
	val, err = c.stateMachines[follower.ID()-1].Get(lastLogKey)
	assert.NoError(t, err)
	assert.Equal(t, "v", val, "Follower should recover data from subsequent AppendEntries")

	t.Log("InstallSnapshot test passed!")
}

// TestCluster_Persistence_Restart 测试持久化存储
// 场景：Leader 写入数据后崩溃，重启后应当记住之前的 Term 和 Log，不能导致数据丢失或脑裂。
func TestCluster_Persistence_Restart(t *testing.T) {
	c := newCluster(t, 3)
	defer c.shutdown()

	leader := c.getLeader(t)
	leaderID := leader.ID()
	t.Logf("Leader: Node %d", leaderID)

	// 1. 写入数据
	key := "persist-key"
	value := "persist-value"
	cmd, _ := json.Marshal(param.KVCommand{Op: "set", Key: key, Value: value})
	reply := &param.ClientReply{}
	err := leader.ClientRequest(&param.ClientArgs{ClientID: 1, SequenceNum: 1, Command: cmd}, reply)
	assert.NoError(t, err)
	assert.True(t, reply.Success)

	// 确保所有节点都同步了
	time.Sleep(1 * time.Second)

	// 2. 停止 Leader (模拟 Crash)
	t.Logf("Crashing Leader Node %d...", leaderID)
	c.restartNode(t, leaderID-1)

	// 等待选举稳定（原来的 Leader 重启后是 Follower，集群需要重新选主或它重新当选）
	time.Sleep(2 * time.Second)

	newLeader := c.getLeader(t)
	t.Logf("Cluster stabilized, new leader: %d", newLeader.ID())

	// 3. 验证数据一致性
	// 重启的节点（原 Leader）应该从磁盘加载了数据
	val, err := c.stateMachines[leaderID-1].Get(key)
	assert.NoError(t, err)
	assert.Equal(t, value, val, "Restarted node lost its state machine data!")

	// 验证 Raft Log 的持久化
	lastIndex, _ := c.storages[leaderID-1].LastLogIndex()
	assert.Greater(t, lastIndex, uint64(0), "Restarted node lost its Raft logs!")

	// 4. 再次写入新数据，确保集群功能正常
	key2 := "persist-key-2"
	cmd2, _ := json.Marshal(param.KVCommand{Op: "set", Key: key2, Value: "v2"})
	err = newLeader.ClientRequest(&param.ClientArgs{ClientID: 1, SequenceNum: 2, Command: cmd2}, reply)
	assert.NoError(t, err)
	assert.True(t, reply.Success)

	time.Sleep(1 * time.Second)

	val2, err := c.stateMachines[leaderID-1].Get(key2)
	assert.NoError(t, err)
	assert.Equal(t, "v2", val2, "Restarted node failed to receive new updates")
}

// TestCluster_UnreliableNetwork_Churn 混沌测试
// 场景：在进行高并发写入的同时，随机断开和恢复节点连接，验证系统最终一致性。
func TestCluster_UnreliableNetwork_Churn(t *testing.T) {
	c := newCluster(t, 5) // 使用 5 个节点，容错能力更强 (容忍 2 个失效)
	defer c.shutdown()

	// 1. 启动写入协程
	stopCh := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		i := 0
		for {
			select {
			case <-stopCh:
				return
			default:
				// 寻找当前的 Leader
				var leader *raft.Raft
				for _, n := range c.nodes {
					if !n.IsStopped() && n.State() == param.Leader {
						leader = n
						break
					}
				}

				if leader != nil {
					key := fmt.Sprintf("churn-%d", i)
					cmd, _ := json.Marshal(param.KVCommand{Op: "set", Key: key, Value: "v"})
					// 忽略错误，因为网络可能正在动荡
					leader.ClientRequest(&param.ClientArgs{ClientID: 1, SequenceNum: int64(i), Command: cmd}, &param.ClientReply{})
				}
				i++
				time.Sleep(50 * time.Millisecond)
			}
		}
	}()

	// 2. 启动干扰协程 (Chaos Monkey)
	// 运行 10 秒
	testDuration := 10 * time.Second
	start := time.Now()

	t.Log("Starting network churn for 10 seconds...")
	for time.Since(start) < testDuration {
		// 随机选择一个节点断网
		target := rand.Intn(5)

		// 断开连接
		c.transports[target].SetPeers(make(map[int]string))

		time.Sleep(time.Duration(rand.Intn(500)+200) * time.Millisecond)

		// 恢复连接
		c.transports[target].SetPeers(c.peerMap)

		time.Sleep(time.Duration(rand.Intn(500)+200) * time.Millisecond)
	}

	close(stopCh)
	wg.Wait()

	// 3. 恢复所有网络并等待集群收敛
	t.Log("Network churn stopped. Waiting for convergence...")
	for i := 0; i < 5; i++ {
		c.transports[i].SetPeers(c.peerMap)
	}

	// 给足够的时间让日志复制完成，并让落后的节点截断脏日志
	time.Sleep(5 * time.Second)

	// 4. 验证所有存活节点的 Log 长度和状态机是否一致 (使用重试机制)
	// 我们不能只检查一次，因为某些节点可能正在截断日志的过程中

	// 获取期望的 Leader 状态
	leader := c.getLeader(t)
	expectedLastIndex, _ := leader.Storage().LastLogIndex()
	t.Logf("Convergence Target: Leader LastLogIndex = %d", expectedLastIndex)

	// 使用重试循环来检查一致性
	for i := 0; i < 5; i++ {
		node := c.nodes[i]
		nodeID := node.ID()

		// 定义一个辅助函数来检查单个节点是否对齐
		checkNode := func() error {
			// 1. 检查 Log Index
			idx, err := node.Storage().LastLogIndex()
			if err != nil {
				return fmt.Errorf("failed to get log index: %v", err)
			}
			if idx != expectedLastIndex {
				return fmt.Errorf("log index mismatch: expected %d, got %d", expectedLastIndex, idx)
			}

			// 2. 检查状态机数据 (随机抽样一个 key)
			// 只要 Leader 有这个 key，Follower 也必须有
			checkKey := "churn-0"
			leaderVal, _ := c.stateMachines[leader.ID()-1].Get(checkKey)
			nodeVal, _ := c.stateMachines[i].Get(checkKey)
			if leaderVal != nodeVal {
				return fmt.Errorf("state machine mismatch for key %s: expected %s, got %s", checkKey, leaderVal, nodeVal)
			}
			return nil
		}

		// 循环重试：给每个节点最多 5 秒的时间来追上（或截断）
		var lastErr error
		success := false
		for retry := 0; retry < 50; retry++ { // 50 * 100ms = 5秒
			if err := checkNode(); err == nil {
				success = true
				break
			} else {
				lastErr = err
				time.Sleep(100 * time.Millisecond)
			}
		}

		if !success {
			t.Fatalf("Node %d failed to converge after timeout. Last error: %v", nodeID, lastErr)
		}
	}

	t.Log("Chaos test passed: All nodes converged.")
}

// TestCluster_MembershipChange 测试集群成员变更 (Add/Remove Server)
func TestCluster_MembershipChange(t *testing.T) {
	// 启动 5 个物理节点，但 Raft 初始只包含 [1, 2, 3]
	// 节点 4 和 5 处于运行状态，但不在集群配置中
	c := newClusterWithConfig(t, 5, 3)
	defer c.shutdown()

	leader := c.getLeader(t)
	t.Logf("Initial Leader: Node %d (Config: [1, 2, 3])", leader.ID())

	// 1. 验证基本读写 (KV命令依然使用 JSON，因为状态机是这么设计的)
	cmd, _ := json.Marshal(param.KVCommand{Op: "set", Key: "k1", Value: "v1"})
	err := leader.ClientRequest(&param.ClientArgs{ClientID: 1, SequenceNum: 1, Command: cmd}, &param.ClientReply{})
	assert.NoError(t, err)

	// 2. 动态添加 Node 4
	newPeersAdd := []int{1, 2, 3, 4}
	t.Log("Changing config: Adding Node 4 -> [1, 2, 3, 4]")

	configCmdAdd := param.NewConfigChangeCommand(newPeersAdd)

	reply := &param.ClientReply{}
	// 直接传入 configCmdAdd 对象
	err = leader.ClientRequest(&param.ClientArgs{ClientID: 0, SequenceNum: 0, Command: configCmdAdd}, reply)
	assert.NoError(t, err)
	assert.True(t, reply.Success, "Config change (add node) failed")

	// 等待配置同步 (给予更充足的时间)
	time.Sleep(3 * time.Second)

	// 3. 验证新节点 Node 4 是否同步了数据
	val, err := c.stateMachines[3].Get("k1") // Index 3 is Node 4
	assert.NoError(t, err)
	assert.Equal(t, "v1", val, "Node 4 should catch up logs after joining")

	// 4. 写入新数据，确保 4 个节点达成共识
	cmd2, _ := json.Marshal(param.KVCommand{Op: "set", Key: "k2", Value: "v2"})
	err = leader.ClientRequest(&param.ClientArgs{ClientID: 1, SequenceNum: 2, Command: cmd2}, reply)
	assert.NoError(t, err)
	assert.True(t, reply.Success)

	time.Sleep(1 * time.Second)
	val2, _ := c.stateMachines[3].Get("k2")
	assert.Equal(t, "v2", val2, "Node 4 should receive new logs")

	// 5. 动态移除 Node 2
	newPeersRemove := []int{1, 3, 4}
	t.Log("Changing config: Removing Node 2 -> [1, 3, 4]")

	// 直接传递结构体
	configCmdRemove := param.NewConfigChangeCommand(newPeersRemove)

	// 重新获取 Leader，防止 Leader 变动
	leader = c.getLeader(t)
	err = leader.ClientRequest(&param.ClientArgs{ClientID: 0, SequenceNum: 0, Command: configCmdRemove}, reply)
	assert.NoError(t, err)

	time.Sleep(2 * time.Second)

	// 6. 验证被移除的节点 (Node 2) 不再收到新数据
	// 停止 Node 2
	c.nodes[1].Stop() // Index 1 is Node 2
	c.transports[1].Close()

	// 写入新数据
	leader = c.getLeader(t)
	cmd3, _ := json.Marshal(param.KVCommand{Op: "set", Key: "k3", Value: "v3"})
	err = leader.ClientRequest(&param.ClientArgs{ClientID: 1, SequenceNum: 3, Command: cmd3}, reply)
	assert.NoError(t, err)
	assert.True(t, reply.Success)

	// 验证 Node 1, 3, 4 有数据
	// 使用循环重试，等待 Follower 同步 CommitIndex
	targetNodes := []int{0, 2, 3} // Index for Node 1, 3, 4
	for _, idx := range targetNodes {
		nodeID := idx + 1
		var val string
		var err error

		// 尝试最多 2 秒，每 100ms 检查一次
		success := false
		for i := 0; i < 20; i++ {
			val, err = c.stateMachines[idx].Get("k3")
			if err == nil && val == "v3" {
				success = true
				break
			}
			time.Sleep(100 * time.Millisecond)
		}

		if !success {
			t.Fatalf("Node %d failed to receive new data 'v3'. Last error: %v, Last Val: %s", nodeID, err, val)
		}

		t.Logf("Node %d successfully synced 'v3'", nodeID)
	}
}

// TestCluster_FullClusterRestart 测试全集群崩溃恢复
// 场景：所有节点同时宕机，然后全部重启。
func TestCluster_FullClusterRestart(t *testing.T) {
	c := newCluster(t, 3)
	defer c.shutdown()

	leader := c.getLeader(t)

	// 1. 写入关键数据
	cmd, _ := json.Marshal(param.KVCommand{Op: "set", Key: "critical", Value: "data"})
	err := leader.ClientRequest(&param.ClientArgs{ClientID: 1, SequenceNum: 1, Command: cmd}, &param.ClientReply{})
	assert.NoError(t, err)
	time.Sleep(1 * time.Second)

	// 2. 模拟灾难：停止所有节点
	t.Log("Simulating full cluster crash...")
	for i := 0; i < 3; i++ {
		c.nodes[i].Stop()
		c.transports[i].Close()
	}

	// 3. 重启所有节点 (使用相同的存储)
	t.Log("Restarting all nodes...")
	for i := 0; i < 3; i++ {
		// 重启逻辑与 restartNode 类似
		id := i + 1
		prevAddr := c.peerMap[id]

		newTrans, _ := tcp.NewTCPTransport(prevAddr)
		c.transports[i] = newTrans

		// 复用存储和状态机
		store := c.storages[i]
		sm := c.stateMachines[i]
		peerIDs := []int{1, 2, 3}

		newRaft := raft.NewRaft(id, peerIDs, store, sm, newTrans, c.commitChans[i])
		c.nodes[i] = newRaft

		newTrans.SetPeers(c.peerMap)
		newTrans.RegisterRaft(newRaft)
		newTrans.Start()
		go newRaft.Run()
	}

	// 4. 等待集群自愈
	t.Log("Waiting for cluster to recover...")
	time.Sleep(3 * time.Second)

	// 5. 验证数据并未丢失
	newLeader := c.getLeader(t)
	t.Logf("Cluster recovered. New Leader: %d", newLeader.ID())

	val, err := c.stateMachines[newLeader.ID()-1].Get("critical")
	assert.NoError(t, err)
	assert.Equal(t, "data", val, "Data should persist after full cluster restart")

	// 6. 验证集群仍可写入
	cmd2, _ := json.Marshal(param.KVCommand{Op: "set", Key: "new-data", Value: "after-crash"})
	reply := &param.ClientReply{}
	err = newLeader.ClientRequest(&param.ClientArgs{ClientID: 1, SequenceNum: 2, Command: cmd2}, reply)
	assert.NoError(t, err)
	assert.True(t, reply.Success)
}

// TestCluster_StaleRead 测试陈旧读 (Stale Read) 防护
// 验证当发生网络分区时，旧 Leader 无法服务读请求 (需要实现 ReadIndex 或 Lease)
func TestCluster_StaleRead(t *testing.T) {
	c := newCluster(t, 3)
	defer c.shutdown()

	leader := c.getLeader(t)
	leaderID := leader.ID()
	t.Logf("Original Leader: %d", leaderID)

	// 写入初始数据
	cmd, _ := json.Marshal(param.KVCommand{Op: "set", Key: "k", Value: "v"})
	err := leader.ClientRequest(&param.ClientArgs{ClientID: 1, SequenceNum: 1, Command: cmd}, &param.ClientReply{})
	assert.NoError(t, err)

	// 制造分区：Leader [1] 被隔离， [2, 3] 连通
	t.Log("Isolating old leader...")
	c.transports[leaderID-1].SetPeers(make(map[int]string)) // 切断 Leader 发出的包

	// 更新其他节点，切断发往 Leader 的包
	newPeers := make(map[int]string)
	for id, addr := range c.peerMap {
		if id != leaderID {
			newPeers[id] = addr
		}
	}
	for i := 0; i < 3; i++ {
		if c.nodes[i].ID() != leaderID {
			c.transports[i].SetPeers(newPeers)
		}
	}

	// 等待 [2, 3] 选出新 Leader
	time.Sleep(3 * time.Second)

	// 此时旧 Leader (leader变量) 仍然认为自己是 Leader，但它已经无法联系多数派
	// 尝试向旧 Leader 发起读请求
	t.Log("Sending read request to isolated (old) leader...")

	readCmd, _ := json.Marshal(param.KVCommand{Op: "get", Key: "k"})
	reply := &param.ClientReply{}

	// 设置一个较短的超时，或者期望它返回 NotLeader / Error
	// 如果你的实现有 ReadIndex check，这里应该会失败或阻塞直到超时
	done := make(chan bool)
	go func() {
		err := leader.ClientRequest(&param.ClientArgs{ClientID: 2, SequenceNum: 1, Command: readCmd}, reply)
		assert.NoError(t, err)
		done <- true
	}()

	select {
	case <-done:
		// 如果请求返回了，必须检查结果
		assert.False(t, reply.Success, "Old leader should not respond to read requests.")
	case <-time.After(2 * time.Second):
		t.Log("Pass: Old leader timed out (likely stuck in ReadIndex check).")
	}
}
