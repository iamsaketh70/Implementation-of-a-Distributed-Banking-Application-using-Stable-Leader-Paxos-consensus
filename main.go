package main

import (
	"bufio"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

var (
	net     *Network
	nodes   []*PaxosNode
	clients map[string]*PaxosClient
)

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: go run . <testfile.csv> [auto]")
		fmt.Println("  auto - run all sets without prompts")
		os.Exit(1)
	}

	testFile := os.Args[1]
	autoMode := len(os.Args) >= 3 && os.Args[2] == "auto"

	testSets, err := ParseTestFile(testFile)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("Loaded %d test sets from %s\n", len(testSets), testFile)

	initSystem()
	defer shutdownSystem()

	scanner := bufio.NewScanner(os.Stdin)

	for i, set := range testSets {
		fmt.Printf("\n========== Test Set %d ==========\n", set.SetNumber)
		fmt.Printf("  Transactions: %d | Live Nodes: %s\n",
			countTx(set), fmtNodes(set.LiveNodes))

		processSet(set)

		if autoMode {
			// Print DB for first alive node
			for j := 0; j < NumNodes; j++ {
				if net.IsAlive(j + 1) {
					nodes[j].PrintDB()
					break
				}
			}
			continue
		}

		// Interactive mode
		isLast := i == len(testSets)-1
		fmt.Println("\nCommands: PrintLog [n], PrintDB [n], PrintStatus <seq>, PrintView, next, exit")
		for {
			fmt.Print("> ")
			if !scanner.Scan() {
				return
			}
			input := strings.TrimSpace(scanner.Text())
			if input == "" {
				continue
			}
			if handleCmd(input, isLast) {
				break
			}
		}
	}

	if autoMode {
		fmt.Println("\n========== All sets complete ==========")
		// Print final state for all alive nodes
		for i := 0; i < NumNodes; i++ {
			nodes[i].PrintDB()
		}
		printView()
	}
}

func initSystem() {
	net = NewNetwork()
	nodes = make([]*PaxosNode, NumNodes)
	for i := 0; i < NumNodes; i++ {
		nodes[i] = NewPaxosNode(i+1, net)
		net.Register(i+1, nodes[i])
	}
	for _, n := range nodes {
		n.Start()
	}
	clients = make(map[string]*PaxosClient)
	for _, c := range AllClientIDs {
		clients[c] = NewPaxosClient(c, net)
	}
	fmt.Println("System initialized: 5 nodes, 10 clients (A-J), each with balance 10")
}

func shutdownSystem() {
	for _, n := range nodes {
		n.Stop()
	}
}

func processSet(set TestSet) {
	alive := make(map[int]bool)
	for _, id := range set.LiveNodes {
		alive[id] = true
	}

	// Update alive status
	for i := 1; i <= NumNodes; i++ {
		was := net.IsAlive(i)
		want := alive[i]
		if want && !was {
			fmt.Printf("  n%d: ONLINE\n", i)
			net.SetAlive(i, true)
			nodes[i-1].SyncLog()
		} else if !want && was {
			fmt.Printf("  n%d: OFFLINE\n", i)
			net.SetAlive(i, false)
		}
	}

	// Split by LF
	batches := splitByLF(set.Transactions)
	for _, b := range batches {
		if b.isLF {
			lid := net.GetCurrentLeaderID()
			if lid > 0 {
				fmt.Printf("  >> LEADER FAILURE: killing n%d <<\n", lid)
				net.SetAlive(lid, false)
				// Wait for backup timers to expire
				time.Sleep(LeaderTimeoutDuration + 500*time.Millisecond)
			} else {
				fmt.Println("  >> LEADER FAILURE requested (no leader found) <<")
				time.Sleep(500 * time.Millisecond)
			}
			continue
		}
		if len(b.txs) == 0 {
			continue
		}

		fmt.Printf("  Processing %d transaction(s)...\n", len(b.txs))

		// Group by sender client
		groups := make(map[string][]Transaction)
		var order []string
		for _, t := range b.txs {
			cid := t.Tx.Sender
			if _, ok := groups[cid]; !ok {
				order = append(order, cid)
			}
			groups[cid] = append(groups[cid], t.Tx)
		}

		var wg sync.WaitGroup
		for _, cid := range order {
			txs := groups[cid]
			cl := clients[cid]
			wg.Add(1)
			go func(c *PaxosClient, ts []Transaction) {
				defer wg.Done()
				for _, tx := range ts {
					ok, err := c.SendTransaction(tx)
					if err != nil {
						fmt.Printf("    %s: ERROR (%v)\n", tx, err)
					} else if ok {
						fmt.Printf("    %s: SUCCESS\n", tx)
					} else {
						fmt.Printf("    %s: FAILED (insufficient balance)\n", tx)
					}
				}
			}(cl, txs)
		}
		wg.Wait()
	}

	// Allow commits to propagate
	time.Sleep(300 * time.Millisecond)
	for i := 0; i < NumNodes; i++ {
		if net.IsAlive(i + 1) {
			nodes[i].tryExecute()
		}
	}
}

type batch struct {
	isLF bool
	txs  []TestTransaction
}

func splitByLF(txs []TestTransaction) []batch {
	var result []batch
	var cur []TestTransaction
	for _, t := range txs {
		if t.IsLF {
			if len(cur) > 0 {
				result = append(result, batch{txs: cur})
				cur = nil
			}
			result = append(result, batch{isLF: true})
		} else {
			cur = append(cur, t)
		}
	}
	if len(cur) > 0 {
		result = append(result, batch{txs: cur})
	}
	return result
}

// ======================== Commands ========================

func handleCmd(input string, isLast bool) bool {
	parts := strings.Fields(input)
	if len(parts) == 0 {
		return false
	}
	cmd := strings.ToLower(parts[0])

	switch cmd {
	case "next":
		if isLast {
			fmt.Println("No more sets.")
			return false
		}
		return true
	case "exit":
		shutdownSystem()
		os.Exit(0)
	case "printlog":
		if len(parts) >= 2 {
			if id, err := strconv.Atoi(parts[1]); err == nil && id >= 1 && id <= NumNodes {
				nodes[id-1].PrintLog()
			} else {
				fmt.Println("Invalid node ID (1-5)")
			}
		} else {
			for i := 0; i < NumNodes; i++ {
				nodes[i].PrintLog()
			}
		}
	case "printdb":
		if len(parts) >= 2 {
			if id, err := strconv.Atoi(parts[1]); err == nil && id >= 1 && id <= NumNodes {
				nodes[id-1].PrintDB()
			} else {
				fmt.Println("Invalid node ID (1-5)")
			}
		} else {
			for i := 0; i < NumNodes; i++ {
				nodes[i].PrintDB()
			}
		}
	case "printstatus":
		if len(parts) < 2 {
			fmt.Println("Usage: PrintStatus <seqNum>")
		} else if seq, err := strconv.Atoi(parts[1]); err == nil {
			printStatus(seq)
		}
	case "printview":
		printView()
	default:
		fmt.Println("Unknown command. Try: PrintLog, PrintDB, PrintStatus, PrintView, next, exit")
	}
	return false
}

func printStatus(seq int) {
	fmt.Printf("\n=== Status for Sequence %d ===\n", seq)
	fmt.Printf("  %-6s %-8s %-20s\n", "Node", "Status", "Transaction")
	for i := 0; i < NumNodes; i++ {
		fmt.Printf("  n%-5d %-8s %-20s\n",
			nodes[i].ID, nodes[i].GetStatus(seq), nodes[i].GetTxString(seq))
	}
}

func printView() {
	fmt.Println("\n=== New-View Messages ===")
	seen := make(map[string]bool)
	type vInfo struct {
		ballot  Ballot
		entries []LogEntry
	}
	var views []vInfo

	for _, node := range nodes {
		node.mu.Lock()
		for _, nv := range node.newViewHistory {
			key := nv.Ballot.String()
			if !seen[key] {
				seen[key] = true
				views = append(views, vInfo{nv.Ballot, nv.AcceptLog})
			}
		}
		node.mu.Unlock()
	}
	if len(views) == 0 {
		fmt.Println("  (none)")
		return
	}
	sort.Slice(views, func(i, j int) bool { return views[j].ballot.GreaterThan(views[i].ballot) })
	for idx, v := range views {
		fmt.Printf("\n  --- New-View #%d (Leader=n%d, Ballot=%s) ---\n", idx+1, v.ballot.NodeID, v.ballot.String())
		if len(v.entries) == 0 {
			fmt.Println("    (empty)")
		}
		for _, e := range v.entries {
			tx := e.Request.Tx.String()
			if e.Request.IsNoop {
				tx = "no-op"
			}
			fmt.Printf("    Seq %d: %s\n", e.SeqNum, tx)
		}
	}
}

func countTx(s TestSet) int {
	c := 0
	for _, t := range s.Transactions {
		if !t.IsLF {
			c++
		}
	}
	return c
}

func fmtNodes(ids []int) string {
	sort.Ints(ids)
	p := make([]string, len(ids))
	for i, id := range ids {
		p[i] = fmt.Sprintf("n%d", id)
	}
	return "[" + strings.Join(p, ",") + "]"
}
