package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/spf13/cobra"

	"github.com/xmh1011/go-raft/client"
	"github.com/xmh1011/go-raft/param"
	"github.com/xmh1011/go-raft/transport"
)

var (
	peersStr      string
	transportType string
	op            string
	key           string
	value         string
)

func main() {
	var rootCmd = &cobra.Command{
		Use:   "raft-client",
		Short: "A client for the Raft cluster",
		Run:   runClient,
	}

	rootCmd.Flags().StringVar(&peersStr, "peers", "1=127.0.0.1:8001,2=127.0.0.1:8002,3=127.0.0.1:8003", "Comma-separated list of peer ID=Address pairs")
	rootCmd.Flags().StringVar(&transportType, "transport", "grpc", "Transport type: tcp, grpc")
	rootCmd.Flags().StringVar(&op, "op", "get", "Operation type: get or set")
	rootCmd.Flags().StringVar(&key, "key", "foo", "Key to operate on")
	rootCmd.Flags().StringVar(&value, "value", "", "Value to set (only for set operation)")

	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func runClient(cmd *cobra.Command, args []string) {
	// 1. 解析 peers
	peerMap := make(map[int]string)
	for _, p := range strings.Split(peersStr, ",") {
		parts := strings.Split(p, "=")
		if len(parts) != 2 {
			log.Fatalf("Invalid peer format: %s", p)
		}
		var id int
		if _, err := fmt.Sscanf(parts[0], "%d", &id); err != nil {
			log.Fatalf("Invalid peer ID: %s", parts[0])
		}
		peerMap[id] = parts[1]
	}

	// 2. 初始化网络传输
	var trans transport.Transport
	var err error

	// 使用端口 0 让系统自动分配一个临时端口，作为客户端的源端口
	clientAddr := "127.0.0.1:0"
	trans, err = transport.NewClientTransport(clientAddr, transportType)
	if err != nil {
		log.Fatalf("Failed to initialize transport: %v", err)
	}

	// 必须设置 Peers 映射，否则 Transport 不知道如何连接目标节点
	trans.SetPeers(peerMap)
	defer trans.Close()

	// 3. 创建客户端实例
	c := client.NewClient(peerMap, trans)

	// 4. 构造命令
	kvCmd := param.KVCommand{
		Op:    op,
		Key:   key,
		Value: value,
	}
	// 序列化为 JSON
	cmdBytes, err := json.Marshal(kvCmd)
	if err != nil {
		log.Fatalf("Failed to marshal command: %v", err)
	}

	// 5. 发送命令
	log.Printf("Sending command: %s key=%s val=%s (via %s)", op, key, value, transportType)
	result, success := c.SendCommand(cmdBytes)

	if success {
		fmt.Printf("✅ Success! Result: %v\n", result)
	} else {
		fmt.Printf("❌ Failed to execute command.\n")
	}
}
