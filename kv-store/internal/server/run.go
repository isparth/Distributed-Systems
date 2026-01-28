package server

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/isparth/Distributed-Systems/kv-store/internal/distributedkv"
	"github.com/isparth/Distributed-Systems/kv-store/internal/httpapi"
	"github.com/isparth/Distributed-Systems/kv-store/internal/kvsm"
	"github.com/isparth/Distributed-Systems/kv-store/internal/raft"
	"github.com/isparth/Distributed-Systems/kv-store/internal/raft/storage"
	"github.com/isparth/Distributed-Systems/kv-store/internal/raft/transporthttp"
	"github.com/isparth/Distributed-Systems/kv-store/internal/types"
)

// Run wires together the server components and starts listening.
func Run() error {
	port := flag.Int("port", 8080, "HTTP listen port")
	nodeID := flag.String("id", "node1", "Node ID")
	role := flag.String("role", "follower", "Node role: leader or follower")
	peersFlag := flag.String("peers", "", "Comma-separated list of peer_id=addr pairs (e.g. node2=http://localhost:8081)")
	flag.Parse()

	log.Printf("starting node %s on port %d as %s", *nodeID, *port, *role)

	addr := fmt.Sprintf("http://localhost:%d", *port)

	// Parse peers
	peerMap := make(map[types.NodeID]string)
	var peerIDs []types.NodeID
	if *peersFlag != "" {
		for _, p := range strings.Split(*peersFlag, ",") {
			parts := strings.SplitN(strings.TrimSpace(p), "=", 2)
			if len(parts) != 2 {
				return fmt.Errorf("invalid peer format: %q (expected id=addr)", p)
			}
			id := types.NodeID(parts[0])
			peerMap[id] = parts[1]
			peerIDs = append(peerIDs, id)
		}
	}

	sm := kvsm.New()
	stable := storage.NewMemStableStore()
	if *role == "leader" {
		stable.SetCurrentTerm(1)
	}
	logStore := storage.NewMemLogStore()
	snapStore := storage.NewMemSnapshotStore()

	resolver := transporthttp.NewPeerResolver(peerMap)
	tp := transporthttp.NewHTTPTransport(resolver)

	cfg := raft.Config{
		ID:    types.NodeID(*nodeID),
		Peers: peerIDs,
		Addr:  addr,
		Role:  *role,
	}

	node, err := raft.NewNode(cfg, stable, logStore, snapStore, tp, sm)
	if err != nil {
		return err
	}

	dkv := distributedkv.New(node, sm, distributedkv.Config{})
	apiServer := httpapi.New(dkv)

	// Combine API + Raft HTTP handlers
	mux := http.NewServeMux()
	mux.Handle("/raft/", node.RaftHTTPHandler().Handler())
	mux.Handle("/", apiServer.Handler())

	srv := &http.Server{
		Addr:    fmt.Sprintf(":%d", *port),
		Handler: mux,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := node.Start(ctx); err != nil {
		return err
	}

	errCh := make(chan error, 1)
	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errCh <- err
		}
		close(errCh)
	}()

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
		log.Println("shutting down...")
		node.Stop(context.Background())
		return srv.Shutdown(context.Background())
	}
}
