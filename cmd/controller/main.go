package main

// func main() {
// 	id := flag.Uint64("id", 1, "Node ID")
// 	dbPath := flag.String("db", "controller.db", "Path to job queue DB")
// 	walDir := flag.String("wal", "wal-controller", "Path to WAL directory")
// 	peersFlag := flag.String("peers", "1=localhost:7000,2=localhost:7001,3=localhost:7002", "Cluster peers")
// 	flag.Parse()

// 	peers := parsePeers(*peersFlag)

// 	commitC := make(chan *storage.Command)
// 	node, err := raftnode.NewRaftNode(*id, peers, *dbPath, *walDir, commitC)
// 	if err != nil {
// 		log.Fatalf("Failed to start Raft node: %v", err)
// 	}
// 	defer node.Stop()

// 	go func() {
// 		for c := range commitC {
// 			log.Printf("Command committed: %+v", c)
// 		}
// 	}()

// 	err = node.WaitForLeader(10 * time.Second)
// 	if err != nil {
// 		log.Fatalf("Failed to wait for leader: %v", err)
// 	}

// 	if node.LeaderID() == *id {
// 		log.Println("I am the leader!")
// 	}

// 	time.Sleep(2 * time.Second)

// 	job := storage.Job{
// 		ID:   "job-001",
// 		Data: "process this",
// 		TS:   time.Now(),
// 	}

// 	// Propose a command
// 	cmd := storage.Command{
// 		Type: storage.CmdAddJob,
// 		Job:  job,
// 	}

// 	// Use /enque endpoint to publish jobs

// 	// if err := node.ProposeCommand(context.Background(), cmd); err != nil {
// 	// 	log.Fatalf("Failed to propose command: %v", err)
// 	// }

// 	log.Println("Command proposed successfully!")

// 	// Process the commit channel to handle the command after it's applied
// 	go func() {
// 		for cmd := range commitC {
// 			log.Printf("Command %v committed", cmd)
// 		}
// 	}()

// 	// Block the main function to keep the controller running
// 	select {}
// }

// func parsePeers(flagValue string) map[uint64]string {
// 	peers := make(map[uint64]string)
// 	pairs := strings.Split(flagValue, ",")
// 	for _, pair := range pairs {
// 		parts := strings.Split(pair, "=")
// 		if len(parts) == 2 {
// 			var id uint64
// 			_, _ = fmt.Sscanf(parts[0], "%d", &id)
// 			peers[id] = parts[1]
// 		}
// 	}
// 	return peers
// }
