package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/spf13/cobra"

	"github.com/xmh1011/go-raft/param"
	"github.com/xmh1011/go-raft/raft"
	"github.com/xmh1011/go-raft/storage/inmemory"
	"github.com/xmh1011/go-raft/transport/tcp"
)

var (
	nodeID   int
	peersStr string
	dataDir  string
)

func main() {
	var rootCmd = &cobra.Command{
		Use:   "raft-server",
		Short: "A simple Raft server implementation",
		Run:   runServer,
	}

	rootCmd.Flags().IntVar(&nodeID, "id", 1, "Node ID")
	rootCmd.Flags().StringVar(&peersStr, "peers", "1=127.0.0.1:8001,2=127.0.0.1:8002,3=127.0.0.1:8003", "Comma-separated list of peer ID=Address pairs")
	rootCmd.Flags().StringVar(&dataDir, "data", "raft-data", "Directory to store raft data")

	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func runServer(cmd *cobra.Command, args []string) {
	// 1. 解析 peers 配置
	peerMap := make(map[int]string)
	peerIDs := make([]int, 0)
	for _, p := range strings.Split(peersStr, ",") {
		parts := strings.Split(p, "=")
		if len(parts) != 2 {
			log.Fatalf("Invalid peer format: %s", p)
		}
		var pid int
		if _, err := fmt.Sscanf(parts[0], "%d", &pid); err != nil {
			log.Fatalf("Invalid peer ID: %s", parts[0])
		}
		peerMap[pid] = parts[1]
		peerIDs = append(peerIDs, pid)
	}

	myAddr, ok := peerMap[nodeID]
	if !ok {
		log.Fatalf("My ID %d not found in peers list", nodeID)
	}

	// 2. 初始化存储
	nodeDir := fmt.Sprintf("%s/node-%d", dataDir, nodeID)
	if err := os.MkdirAll(nodeDir, 0755); err != nil {
		log.Fatalf("Failed to create data directory: %v", err)
	}

	// 使用内存存储作为示例，实际生产环境应使用持久化存储
	store := inmemory.NewInMemoryStorage()
	stateMachine := inmemory.NewInMemoryStateMachine()

	// 3. 初始化网络传输 (第一阶段：仅创建实例并监听端口)
	trans, err := tcp.NewTCPTransport(myAddr)
	if err != nil {
		log.Fatalf("Failed to create TCP transport: %v", err)
	}
	// 设置集群节点地址映射
	trans.SetPeers(peerMap)

	// 4. 创建 Raft 节点
	// 将 Transport 实例注入 Raft
	commitChan := make(chan param.CommitEntry, 100)
	rf := raft.NewRaft(nodeID, peerIDs, store, stateMachine, trans, commitChan)

	// 5. 完成网络传输初始化 (第二阶段：注入依赖并启动服务)
	// 将 Raft 实例注册回 Transport，以便处理 RPC 请求
	trans.RegisterRaft(rf)

	// 启动网络服务
	go func() {
		log.Printf("Starting TCP transport service on %s", myAddr)
		if err := trans.Start(); err != nil {
			log.Fatalf("Failed to start transport service: %v", err)
		}
	}()

	// 6. 启动 Raft 主循环
	go rf.Run()

	// 7. 处理应用层提交的日志 (示例：打印日志)
	go func() {
		for entry := range commitChan {
			log.Printf("Node %d committed entry: index=%d term=%d command=%v", nodeID, entry.Index, entry.Term, entry.Command)
			// 在这里可以将 entry 应用到具体的业务状态机
		}
	}()

	log.Printf("Raft node %d started on %s", nodeID, myAddr)

	// 8. 等待退出信号
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	log.Println("Shutting down...")
	rf.Stop()
	trans.Close()
	log.Println("Node stopped")
}
