package main

// import (
// 	"context"
// 	"encoding/json"
// 	"flag"
// 	"fmt"
// 	"log"
// 	"net"
// 	"net/http"
// 	"os"
// 	"os/signal"
// 	"path/filepath"
// 	"strconv"
// 	"strings"
// 	"syscall"
// 	"time"

// 	"go.etcd.io/raft/v3/raftpb"

// 	"djob/raftnode"
// 	"djob/storage"
// )

// var (
// 	cluster string
// 	nodeID  string
// 	kvport  int
// 	join    bool
// 	raftDir string
// 	apiAddr string
// )

// func init() {
// 	flag.StringVar(&cluster, "cluster", "http://127.0.0.1:9021", "comma separated cluster peers")
// 	flag.StringVar(&nodeID, "id", "", "Unique ID of this worker node (required)")
// 	flag.IntVar(&kvport, "port", 9121, "key-value server port")
// 	flag.BoolVar(&join, "join", false, "join an existing cluster")
// 	flag.StringVar(&raftDir, "raft-dir", "/tmp/raft", "Path to Raft data directory")
// 	flag.StringVar(&apiAddr, "api", "127.0.0.1:8080", "HTTP API address for this worker")
// }

// func main() {
// 	flag.Parse()

// 	if nodeID == "" {
// 		log.Fatalf("missing required flag: -id")
// 	}
// 	parsedNodeID, err := strconv.Atoi(nodeID)
// 	if err != nil {
// 		panic(err)
// 	}
// 	raftPath := filepath.Join(raftDir, nodeID)
// 	_ = os.MkdirAll(raftPath, 0700)

// 	proposeC := make(chan []byte)
// 	defer close(proposeC)
// 	confChangeC := make(chan raftpb.ConfChange)
// 	defer close(confChangeC)
// 	var jq *jobqueue
// 	getSnapshot := func() ([]byte, error) { return jq.getSnapshot() }
// 	commitC, errorC, snapshotterReady := raftnode.NewRaftNode(parsedNodeID, strings.Split(cluster, ","), join, getSnapshot, proposeC, confChangeC)
// 	jq = newJobQueue(<-snapshotterReady, proposeC, commitC, errorC)

// 	if err != nil {
// 		log.Fatalf("failed to open job queue: %v", err)
// 	}
// 	defer jobQueue.Close()

// 	if err != nil {
// 		log.Fatalf("failed to start raft node: %v", err)
// 	}
// 	defer rn.Close()

// 	log.Printf("Worker %s started, API at http://%s", nodeID, apiAddr)

// 	mux := http.NewServeMux()
// 	mux.HandleFunc("/enqueue", func(w http.ResponseWriter, r *http.Request) {
// 		if r.Method != http.MethodPost {
// 			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
// 			return
// 		}

// 		var cmd storage.Command
// 		if err := json.NewDecoder(r.Body).Decode(&cmd); err != nil {
// 			http.Error(w, "invalid JSON", http.StatusBadRequest)
// 			return
// 		}

// 		if cmd.Type != storage.CmdAddJob {
// 			http.Error(w, "unsupported command type", http.StatusBadRequest)
// 			return
// 		}

// 		log.Printf("Received job %s for enqueue", cmd.Job.ID)

// 		index, err := rn.ApplyCommand(cmd)
// 		if err != nil {
// 			http.Error(w, fmt.Sprintf("raft apply failed: %v", err), http.StatusInternalServerError)
// 			return
// 		}

// 		log.Printf("Job %s committed at index %d", cmd.Job.ID, index)
// 		w.WriteHeader(http.StatusOK)
// 	})

// 	server := &http.Server{
// 		Addr:    apiAddr,
// 		Handler: mux,
// 		BaseContext: func(l net.Listener) context.Context {
// 			return context.Background()
// 		},
// 	}

// 	go func() {
// 		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
// 			log.Fatalf("API server error: %v", err)
// 		}
// 	}()

// 	// Graceful shutdown
// 	stop := make(chan os.Signal, 1)
// 	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
// 	<-stop

// 	log.Println("Shutting down...")

// 	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
// 	defer cancel()
// 	_ = server.Shutdown(ctx)
// }
