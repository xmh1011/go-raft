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
    *   目前提供 `InMemory` 存储实现（用于测试和演示）以及 `SimpleFile` 存储实现。
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
*   **`grpc/`**: 基于 gRPC 实现的传输层。
*   **`inmemory/`**: 基于内存的传输实现，主要用于集成测试。

### 3. 存储层 (`storage/`)
负责持久化 Raft 的状态和日志。
*   定义了 `Storage` 接口（用于存储 Raft 元数据和日志）和 `StateMachine` 接口（用于应用层状态机）。
*   **`inmemory/`**: 提供了基于内存的参考实现，用于演示和单元测试。
*   **`simplefile/`**: 提供了基于文件的简单持久化实现。

### 4. 入口 (`cmd/`)
*   使用 `Cobra` 库构建了命令行入口，负责解析参数、初始化各个组件并启动服务。
*   **`server/`**: Raft 服务端入口。
*   **`client/`**: Raft 客户端工具入口。

## 🚀 快速开始

### 环境要求
*   Go 1.24+
*   Make (可选)
*   Protoc (如果需要重新生成 gRPC 代码)

### 编译

使用 `Makefile` 提供的命令进行编译：

```bash
make build
```

这将生成两个二进制文件：
*   `raft-server`: Raft 服务端程序
*   `raft-client`: Raft 客户端工具

### 运行集群

为了演示 Raft 集群的工作原理，我们可以在本地启动一个由 3 个节点组成的集群。

你可以使用 `make cluster` 命令一键启动 3 个节点的集群（后台运行），这将使用默认配置（gRPC + InMemory）。

```bash
make cluster
```

或者手动在三个终端窗口中分别运行。以下是几种常见的运行模式配置示例。

#### 1. 单节点测试 (In-Memory 模式)

这是最简单的运行方式，适合快速体验和调试核心逻辑。

*   **传输层 (`inmemory`)**: 仅在进程内有效，无法跨进程通信，因此只能启动单节点。
*   **存储层 (`inmemory`)**: 数据保存在内存，重启即失。

**启动命令：**

```bash
./raft-server --id 1 --peers "1=local-test" --transport inmemory --storage inmemory
```

*   `--peers "1=local-test"`: 这里给出了 `--peers` 的示例地址。对于 `inmemory` 传输，地址可以是任意字符串，只要能唯一标识节点即可。

#### 2. 本地三节点集群 (TCP 传输 + 文件持久化)

这种模式模拟了真实的生产环境。节点之间通过 TCP 网络通信，数据持久化到磁盘文件。

*   **传输层 (`tcp`)**: 使用 Go 标准库 `net/rpc`。
*   **存储层 (`simplefile`)**: 使用简单的文件存储。

**启动命令：**

**终端 1 (启动节点 1):**
```bash
./raft-server --id 1 --peers "1=127.0.0.1:8001,2=127.0.0.1:8002,3=127.0.0.1:8003" --transport tcp --storage simplefile --data raft-data-1
```

**终端 2 (启动节点 2):**
```bash
./raft-server --id 2 --peers "1=127.0.0.1:8001,2=127.0.0.1:8002,3=127.0.0.1:8003" --transport tcp --storage simplefile --data raft-data-2
```

**终端 3 (启动节点 3):**
```bash
./raft-server --id 3 --peers "1=127.0.0.1:8001,2=127.0.0.1:8002,3=127.0.0.1:8003" --transport tcp --storage simplefile --data raft-data-3
```

*   `--peers`: 必须包含集群中所有节点的 ID 和地址，格式为 `ID=IP:PORT`，多个节点用逗号分隔。所有节点的该参数必须保持一致。
*   `--data`: 为每个节点指定不同的数据目录（如 `raft-data-1`），以避免在本地运行时发生文件冲突。

#### 3. 本地三节点集群 (gRPC 传输 + 内存存储)

使用 gRPC 进行通信，适合测试网络交互，但数据不持久化。

*   **传输层 (`grpc`)**: 使用 gRPC 协议。
*   **存储层 (`inmemory`)**: 数据保存在内存。

**启动命令：**

**终端 1 (启动节点 1):**
```bash
./raft-server --id 1 --peers "1=127.0.0.1:50051,2=127.0.0.1:50052,3=127.0.0.1:50053" --transport grpc --storage inmemory
```

**终端 2 (启动节点 2):**
```bash
./raft-server --id 2 --peers "1=127.0.0.1:50051,2=127.0.0.1:50052,3=127.0.0.1:50053" --transport grpc --storage inmemory
```

**终端 3 (启动节点 3):**
```bash
./raft-server --id 3 --peers "1=127.0.0.1:50051,2=127.0.0.1:50052,3=127.0.0.1:50053" --transport grpc --storage inmemory
```

### 配置详解

你可以通过命令行参数灵活配置 `raft-server` 的行为。

#### 1. 传输层 (Transport)

通过 `--transport` 参数指定节点间的通信方式。

*   **`grpc` (默认)**: 使用 gRPC 进行通信。这是推荐的生产环境配置，支持流式传输和更丰富的功能。
*   **`tcp`**: 使用 Go 标准库 `net/rpc` over TCP。这是一种轻量级的替代方案。
*   **`inmemory`**: 仅用于进程内测试（如集成测试）。**注意**：在 `raft-server` 独立进程模式下，使用 `inmemory` 将无法与其他节点通信。

#### 2. 存储层 (Storage)

通过 `--storage` 参数指定数据的存储方式。

*   **`inmemory` (默认)**: 所有数据（日志、状态）保存在内存中。进程重启后数据丢失。仅适用于测试或演示。
*   **`simplefile`**: 基于文件的简单持久化存储。使用 `gob` 编码。
    *   **必须**配合 `--data` 参数指定数据目录。
    *   建议为每个节点指定不同的目录，例如 `raft-data-1`, `raft-data-2`，以避免冲突。

### 使用客户端交互

集群启动后，你可以使用 `raft-client` 工具向集群发送命令。

**写入数据 (Set):**
```bash
./raft-client --op set --key mykey --value "hello raft"
```

**读取数据 (Get):**
```bash
./raft-client --op get --key mykey
```

### 停止集群

如果你是使用 `make cluster` 启动的集群，可以使用以下命令停止：

```bash
make stop-cluster
```

### 命令行参数说明

#### raft-server
*   `--id`: 当前节点的唯一整数 ID。
*   `--peers`: 集群所有节点的列表，格式为 `ID=IP:PORT`，用逗号分隔。
*   `--data`: 存放 Raft 数据（日志、快照）的目录路径。
*   `--transport`: 传输层类型，支持 `tcp`, `grpc`, `inmemory` (默认 `grpc`)。
*   `--storage`: 存储层类型，支持 `inmemory`, `simplefile` (默认 `inmemory`)。

#### raft-client
*   `--peers`: 集群所有节点的列表，格式为 `ID=IP:PORT`，用逗号分隔。
*   `--transport`: 传输层类型，支持 `tcp`, `grpc` (默认 `grpc`)。注意：不支持 `inmemory`。
*   `--op`: 操作类型，支持 `get` 或 `set`。
*   `--key`: 操作的键。
*   `--value`: 操作的值（仅 `set` 操作需要）。

## 🧪 测试

运行所有单元测试和集成测试：

```bash
make test
```

查看测试覆盖率：

```bash
make cover
```

## 🤝 贡献

欢迎提交 Issue 和 Pull Request 来改进代码或增加新功能。
