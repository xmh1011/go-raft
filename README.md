# Go-Raft

Go-Raft 是一个用 Go 语言编写的 Raft 分布式共识算法的实现。该项目旨在提供一个清晰、模块化且功能相对完整的 Raft 协议参考实现，适合用于学习分布式系统原理或作为构建分布式应用的基础组件。

## ✨ 功能特性

本项目实现了 Raft 论文及其后续优化中的大部分核心功能：

*   **领导者选举 (Leader Election)**
    *   包含随机化选举超时时间以避免选票瓜分。
    *   实现了 **Pre-Vote** 机制，防止网络分区的节点在恢复后干扰集群。
    *   支持 **Check Quorum** (Leader Lease) 机制。
*   **日志复制 (Log Replication)**
    *   基于 AppendEntries RPC 的日志同步。
    *   支持日志一致性检查和冲突解决。
    *   实现了流水线 (Pipeline) 复制以提高吞吐量。
*   **持久化 (Persistence)**
    *   定义了抽象的 `Storage` 接口。
    *   目前提供 `InMemory` 存储实现（用于测试和演示）。
    *   支持保存 `HardState` (Term, Vote) 和日志条目。
*   **快照 (Snapshotting)**
    *   支持日志压缩，防止日志无限增长。
    *   实现了 `InstallSnapshot` RPC，用于向落后太多的 Follower 发送完整状态。
    *   异步快照生成与持久化，避免阻塞 Raft 主循环。
*   **成员变更 (Membership Change)**
    *   实现了 **联合共识 (Joint Consensus)** 机制，支持安全的动态成员配置变更。
*   **线性一致性读 (Linearizable Read)**
    *   实现了 **ReadIndex** 机制，确保在读取操作时能够读取到最新的数据，防止从过期的 Leader 读取脏数据。

## 🏗 架构设计

项目采用了模块化的分层设计，主要分为以下几个部分：

### 1. 核心层 (`raft/`)
这是 Raft 算法的大脑，负责处理所有的状态转换和核心逻辑。
*   **`raft.go`**: 定义了 Raft 结构体和主循环 (`Run`)，处理状态机驱动。
*   **`election.go`**: 处理选举逻辑，包括 RequestVote RPC 的处理、计票和状态转换。
*   **`replication.go`**: 处理日志复制逻辑，包括 AppendEntries RPC 的发送与处理、CommitIndex 的推进。
*   **`snapshot.go`**: 处理快照的生成、安装和日志截断。

### 2. 传输层 (`transport/`)
负责节点之间的网络通信。
*   定义了 `Transport` 接口，使得底层网络实现可以替换。
*   **`tcp/`**: 基于 Go 标准库 `net/rpc` 实现的 TCP 传输层。
*   解决了 Raft 实例与 Transport 实例之间的循环依赖问题（先创建 Transport，再注册 Raft）。

### 3. 存储层 (`storage/`)
负责持久化 Raft 的状态和日志。
*   定义了 `Storage` 接口（用于存储 Raft 元数据和日志）和 `StateMachine` 接口（用于应用层状态机）。
*   **`inmemory/`**: 提供了基于内存的参考实现，用于演示和单元测试。

### 4. 入口 (`cmd/`)
*   使用 `Cobra` 库构建了命令行入口，负责解析参数、初始化各个组件并启动服务。

## 🚀 快速开始

### 环境要求
*   Go 1.24+
*   Make (可选)

### 编译

使用 `Makefile` 提供的命令进行编译：

```bash
make build
```

这将生成一个名为 `go-raft` 的二进制文件。

### 运行集群

为了演示 Raft 集群的工作原理，我们可以在本地启动一个由 3 个节点组成的集群。

假设集群配置如下：
*   Node 1: `127.0.0.1:8001`
*   Node 2: `127.0.0.1:8002`
*   Node 3: `127.0.0.1:8003`

请打开三个终端窗口，分别运行以下命令：

**终端 1 (启动节点 1):**
```bash
./go-raft --id 1 --peers "1=127.0.0.1:8001,2=127.0.0.1:8002,3=127.0.0.1:8003" --data raft-data
```

**终端 2 (启动节点 2):**
```bash
./go-raft --id 2 --peers "1=127.0.0.1:8001,2=127.0.0.1:8002,3=127.0.0.1:8003" --data raft-data
```

**终端 3 (启动节点 3):**
```bash
./go-raft --id 3 --peers "1=127.0.0.1:8001,2=127.0.0.1:8002,3=127.0.0.1:8003" --data raft-data
```

### 观察输出

启动后，你将在终端中看到如下日志信息：

1.  **选举过程**: 节点启动后会发起选举，你会看到 `[Election]` 相关的日志，最终某个节点会成为 Leader。
2.  **心跳维持**: Leader 会定期发送心跳，Follower 会重置选举超时。
3.  **日志提交**: 目前演示程序中包含了一个模拟的日志提交通道，你会看到 `Node X committed entry` 的日志，表明集群正在正常工作并达成共识。

### 命令行参数说明

*   `--id`: 当前节点的唯一整数 ID。
*   `--peers`: 集群所有节点的列表，格式为 `ID=IP:PORT`，用逗号分隔。
*   `--data`: 存放 Raft 数据（日志、快照）的目录路径。

## 🧪 测试

运行所有单元测试：

```bash
make test
```

查看测试覆盖率：

```bash
make cover
```

## 🤝 贡献

欢迎提交 Issue 和 Pull Request 来改进代码或增加新功能。
