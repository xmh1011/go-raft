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
	"github.com/xmh1011/go-raft/storage"
	"github.com/xmh1011/go-raft/transport"
)

// Config holds the server configuration
type Config struct {
	NodeID        int
	PeersStr      string
	DataDir       string
	TransportType string
	StorageType   string
}

var config Config

func main() {
	var rootCmd = &cobra.Command{
		Use:   "raft-server",
		Short: "A simple Raft server implementation",
		Run:   runServer,
	}

	rootCmd.Flags().IntVar(&config.NodeID, "id", 1, "Node ID")
	rootCmd.Flags().StringVar(&config.PeersStr, "peers", "1=127.0.0.1:8001,2=127.0.0.1:8002,3=127.0.0.1:8003", "Comma-separated list of peer ID=Address pairs")
	rootCmd.Flags().StringVar(&config.DataDir, "data", "raft-data", "Directory to store raft data")
	rootCmd.Flags().StringVar(&config.TransportType, "transport", transport.GrpcTransport, "Transport type: tcp, grpc, inmemory")
	rootCmd.Flags().StringVar(&config.StorageType, "storage", storage.InmemoryStorage, "Storage type: inmemory or simplefile")

	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func runServer(_ *cobra.Command, _ []string) {
	srv, err := NewServer(config)
	if err != nil {
		log.Fatalf("Failed to create server: %v", err)
	}

	if err := srv.Start(); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}

	waitForSignal(srv)
}

// Server represents the Raft server instance
type Server struct {
	config     Config
	raft       *raft.Raft
	transport  transport.Transport
	store      storage.Storage
	commitChan chan param.CommitEntry
}

// NewServer creates a new Server instance
func NewServer(cfg Config) (*Server, error) {
	// 1. Parse peers
	peerMap, peerIDs, myAddr, err := parsePeers(cfg.PeersStr, cfg.NodeID)
	if err != nil {
		return nil, fmt.Errorf("failed to parse peers: %w", err)
	}

	// 2. Initialize storage
	store, stateMachine, err := storage.NewStorage(cfg.StorageType, cfg.DataDir, cfg.NodeID)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize storage: %w", err)
	}

	// 3. Initialize transport
	trans, err := transport.NewTransport(cfg.TransportType, myAddr)
	if err != nil {
		store.Close()
		return nil, fmt.Errorf("failed to initialize transport: %w", err)
	}
	trans.SetPeers(peerMap)

	// 4. Create Raft node
	commitChan := make(chan param.CommitEntry, 100)
	rf := raft.NewRaft(cfg.NodeID, peerIDs, store, stateMachine, trans, commitChan)

	return &Server{
		config:     cfg,
		raft:       rf,
		transport:  trans,
		store:      store,
		commitChan: commitChan,
	}, nil
}

// Start starts the Raft server components
func (s *Server) Start() error {
	// Register Raft to transport
	s.transport.RegisterRaft(s.raft)

	// Start transport service
	go func() {
		log.Printf("Starting %s transport service on %s", s.config.TransportType, s.transport.Addr())
		if err := s.transport.Start(); err != nil {
			log.Fatalf("Failed to start transport service: %v", err)
		}
	}()

	// Start Raft node
	go s.raft.Run()

	// Handle committed entries
	go s.handleCommits()

	log.Printf("Raft node %d started", s.config.NodeID)
	return nil
}

// Stop stops the Raft server
func (s *Server) Stop() {
	log.Println("Shutting down...")
	s.raft.Stop()
	if err := s.transport.Close(); err != nil {
		log.Printf("Failed to close transport: %v", err)
	}
	if s.store != nil {
		if err := s.store.Close(); err != nil {
			log.Printf("Failed to close store: %v", err)
		}
	}
	log.Println("Node stopped")
}

func (s *Server) handleCommits() {
	for entry := range s.commitChan {
		log.Printf("Node %d committed entry: index=%d term=%d command=%v", s.config.NodeID, entry.Index, entry.Term, entry.Command)
	}
}

func waitForSignal(srv *Server) {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan
	srv.Stop()
}

func parsePeers(peersStr string, nodeID int) (map[int]string, []int, string, error) {
	peerMap := make(map[int]string)
	peerIDs := make([]int, 0)
	for _, p := range strings.Split(peersStr, ",") {
		parts := strings.Split(p, "=")
		if len(parts) != 2 {
			return nil, nil, "", fmt.Errorf("invalid peer format: %s", p)
		}
		var pid int
		if _, err := fmt.Sscanf(parts[0], "%d", &pid); err != nil {
			return nil, nil, "", fmt.Errorf("invalid peer ID: %s", parts[0])
		}
		peerMap[pid] = parts[1]
		peerIDs = append(peerIDs, pid)
	}

	myAddr, ok := peerMap[nodeID]
	if !ok {
		return nil, nil, "", fmt.Errorf("my ID %d not found in peers list", nodeID)
	}
	return peerMap, peerIDs, myAddr, nil
}
