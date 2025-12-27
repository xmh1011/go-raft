package grpc

import (
	"bytes"
	"context"
	"encoding/gob"
	"errors"
	"fmt"
	"log"
	"net"
	"strconv"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/xmh1011/go-raft/param"
	"github.com/xmh1011/go-raft/transport"
	"github.com/xmh1011/go-raft/transport/grpc/pb"
)

// Transport implements transport.Transport using gRPC.
type Transport struct {
	pb.UnimplementedRaftServiceServer
	listener  net.Listener
	localAddr string

	raft       transport.RPCServer
	grpcServer *grpc.Server

	mu        sync.RWMutex
	conns     map[string]*grpc.ClientConn
	clients   map[string]pb.RaftServiceClient
	resolvers map[int]string
}

// NewTransport creates a new gRPC Transport.
func NewTransport(listenAddr string) (*Transport, error) {
	listener, err := net.Listen("tcp", listenAddr)
	if err != nil {
		return nil, err
	}

	return &Transport{
		listener:   listener,
		localAddr:  listener.Addr().String(),
		conns:      make(map[string]*grpc.ClientConn),
		clients:    make(map[string]pb.RaftServiceClient),
		resolvers:  make(map[int]string),
		grpcServer: grpc.NewServer(),
	}, nil
}

// Addr returns the local address.
func (t *Transport) Addr() string {
	return t.localAddr
}

// SetPeers sets the peer resolvers.
func (t *Transport) SetPeers(peers map[int]string) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.resolvers = make(map[int]string)
	for id, addr := range peers {
		t.resolvers[id] = addr
	}

	// Close existing connections to force reconnection with new addresses if needed
	for _, conn := range t.conns {
		conn.Close()
	}
	t.conns = make(map[string]*grpc.ClientConn)
	t.clients = make(map[string]pb.RaftServiceClient)
}

// RegisterRaft registers the Raft RPC server.
func (t *Transport) RegisterRaft(raftInstance transport.RPCServer) {
	t.raft = raftInstance
}

// Start starts the gRPC server.
func (t *Transport) Start() error {
	if t.raft == nil {
		return errors.New("raft instance not registered")
	}

	pb.RegisterRaftServiceServer(t.grpcServer, t)

	go func() {
		if err := t.grpcServer.Serve(t.listener); err != nil {
			log.Printf("[GRPCTransport] Server stopped: %v", err)
		}
	}()

	log.Printf("[GRPCTransport] Service started on %s", t.localAddr)
	return nil
}

// Close stops the gRPC server and closes all connections.
func (t *Transport) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.grpcServer.Stop()

	for _, conn := range t.conns {
		conn.Close()
	}
	t.conns = make(map[string]*grpc.ClientConn)
	t.clients = make(map[string]pb.RaftServiceClient)

	return nil
}

func (t *Transport) getPeerAddress(nodeIDStr string) (string, error) {
	id, err := strconv.Atoi(nodeIDStr)
	if err != nil {
		return "", fmt.Errorf("invalid node id: %s", nodeIDStr)
	}

	t.mu.RLock()
	defer t.mu.RUnlock()
	addr, ok := t.resolvers[id]
	if !ok {
		return "", fmt.Errorf("address not found for node %d", id)
	}
	return addr, nil
}

func (t *Transport) getPeerClient(targetID string) (pb.RaftServiceClient, error) {
	t.mu.RLock()
	client, ok := t.clients[targetID]
	t.mu.RUnlock()
	if ok {
		return client, nil
	}

	addr, err := t.getPeerAddress(targetID)
	if err != nil {
		return nil, err
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	if client, ok := t.clients[targetID]; ok {
		return client, nil
	}

	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}

	client = pb.NewRaftServiceClient(conn)
	t.conns[targetID] = conn
	t.clients[targetID] = client

	return client, nil
}

// --- Helper functions for encoding/decoding ---

func encode(v any) ([]byte, error) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(&v); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func decode(data []byte) (any, error) {
	var v any
	if len(data) == 0 {
		return nil, nil
	}
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&v); err != nil {
		return nil, err
	}
	return v, nil
}

// --- Client side implementation ---

func (t *Transport) SendRequestVote(target string, req *param.RequestVoteArgs, resp *param.RequestVoteReply) error {
	client, err := t.getPeerClient(target)
	if err != nil {
		return err
	}

	pbReq := &pb.RequestVoteRequest{
		Term:         req.Term,
		CandidateId:  int64(req.CandidateId),
		LastLogIndex: req.LastLogIndex,
		LastLogTerm:  req.LastLogTerm,
		PreVote:      req.PreVote,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	pbResp, err := client.RequestVote(ctx, pbReq)
	if err != nil {
		return err
	}

	resp.Term = pbResp.Term
	resp.VoteGranted = pbResp.VoteGranted
	resp.CandidateId = int(pbResp.CandidateId)

	return nil
}

func (t *Transport) SendAppendEntries(target string, req *param.AppendEntriesArgs, resp *param.AppendEntriesReply) error {
	client, err := t.getPeerClient(target)
	if err != nil {
		return err
	}

	pbEntries := make([]*pb.LogEntry, len(req.Entries))
	for i, entry := range req.Entries {
		cmdBytes, err := encode(entry.Command)
		if err != nil {
			return err
		}
		pbEntries[i] = &pb.LogEntry{
			Command: cmdBytes,
			Term:    entry.Term,
			Index:   entry.Index,
		}
	}

	pbReq := &pb.AppendEntriesRequest{
		Term:         req.Term,
		LeaderId:     int64(req.LeaderId),
		PrevLogIndex: req.PrevLogIndex,
		PrevLogTerm:  req.PrevLogTerm,
		Entries:      pbEntries,
		LeaderCommit: req.LeaderCommit,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	pbResp, err := client.AppendEntries(ctx, pbReq)
	if err != nil {
		return err
	}

	resp.Term = pbResp.Term
	resp.Success = pbResp.Success
	resp.ConflictIndex = pbResp.ConflictIndex
	resp.ConflictTerm = pbResp.ConflictTerm

	return nil
}

func (t *Transport) SendInstallSnapshot(target string, req *param.InstallSnapshotArgs, resp *param.InstallSnapshotReply) error {
	client, err := t.getPeerClient(target)
	if err != nil {
		return err
	}

	pbReq := &pb.InstallSnapshotRequest{
		Term:              req.Term,
		LeaderId:          req.LeaderID,
		LastIncludedIndex: req.LastIncludedIndex,
		LastIncludedTerm:  req.LastIncludedTerm,
		Offset:            req.Offset,
		Data:              req.Data,
		Done:              req.Done,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	pbResp, err := client.InstallSnapshot(ctx, pbReq)
	if err != nil {
		return err
	}

	resp.Term = pbResp.Term
	return nil
}

func (t *Transport) SendClientRequest(target string, req *param.ClientArgs, resp *param.ClientReply) error {
	client, err := t.getPeerClient(target)
	if err != nil {
		return err
	}

	cmdBytes, err := encode(req.Command)
	if err != nil {
		return err
	}

	pbReq := &pb.ClientRequestRequest{
		ClientId:    req.ClientID,
		SequenceNum: req.SequenceNum,
		Command:     cmdBytes,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	pbResp, err := client.ClientRequest(ctx, pbReq)
	if err != nil {
		return err
	}

	result, err := decode(pbResp.Result)
	if err != nil {
		return err
	}

	resp.Success = pbResp.Success
	resp.Result = result
	resp.NotLeader = pbResp.NotLeader
	resp.LeaderHint = int(pbResp.LeaderHint)

	return nil
}

// --- Server side implementation ---

func (t *Transport) RequestVote(ctx context.Context, req *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error) {
	args := &param.RequestVoteArgs{
		Term:         req.Term,
		CandidateId:  int(req.CandidateId),
		LastLogIndex: req.LastLogIndex,
		LastLogTerm:  req.LastLogTerm,
		PreVote:      req.PreVote,
	}
	reply := &param.RequestVoteReply{}

	if err := t.raft.RequestVote(args, reply); err != nil {
		return nil, err
	}

	return &pb.RequestVoteResponse{
		Term:        reply.Term,
		VoteGranted: reply.VoteGranted,
		CandidateId: int64(reply.CandidateId),
	}, nil
}

func (t *Transport) AppendEntries(ctx context.Context, req *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
	entries := make([]param.LogEntry, len(req.Entries))
	for i, entry := range req.Entries {
		cmd, err := decode(entry.Command)
		if err != nil {
			return nil, err
		}
		entries[i] = param.LogEntry{
			Command: cmd,
			Term:    entry.Term,
			Index:   entry.Index,
		}
	}

	args := &param.AppendEntriesArgs{
		Term:         req.Term,
		LeaderId:     int(req.LeaderId),
		PrevLogIndex: req.PrevLogIndex,
		PrevLogTerm:  req.PrevLogTerm,
		Entries:      entries,
		LeaderCommit: req.LeaderCommit,
	}
	reply := &param.AppendEntriesReply{}

	if err := t.raft.AppendEntries(args, reply); err != nil {
		return nil, err
	}

	return &pb.AppendEntriesResponse{
		Term:          reply.Term,
		Success:       reply.Success,
		ConflictIndex: reply.ConflictIndex,
		ConflictTerm:  reply.ConflictTerm,
	}, nil
}

func (t *Transport) InstallSnapshot(ctx context.Context, req *pb.InstallSnapshotRequest) (*pb.InstallSnapshotResponse, error) {
	args := &param.InstallSnapshotArgs{
		Term:              req.Term,
		LeaderID:          req.LeaderId,
		LastIncludedIndex: req.LastIncludedIndex,
		LastIncludedTerm:  req.LastIncludedTerm,
		Offset:            req.Offset,
		Data:              req.Data,
		Done:              req.Done,
	}
	reply := &param.InstallSnapshotReply{}

	if err := t.raft.InstallSnapshot(args, reply); err != nil {
		return nil, err
	}

	return &pb.InstallSnapshotResponse{
		Term: reply.Term,
	}, nil
}

func (t *Transport) ClientRequest(ctx context.Context, req *pb.ClientRequestRequest) (*pb.ClientRequestResponse, error) {
	cmd, err := decode(req.Command)
	if err != nil {
		return nil, err
	}

	args := &param.ClientArgs{
		ClientID:    req.ClientId,
		SequenceNum: req.SequenceNum,
		Command:     cmd,
	}
	reply := &param.ClientReply{}

	if err := t.raft.ClientRequest(args, reply); err != nil {
		return nil, err
	}

	resBytes, err := encode(reply.Result)
	if err != nil {
		return nil, err
	}

	return &pb.ClientRequestResponse{
		Success:    reply.Success,
		Result:     resBytes,
		NotLeader:  reply.NotLeader,
		LeaderHint: int64(reply.LeaderHint),
	}, nil
}
