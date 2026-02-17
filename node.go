package main

import (
	"fmt"
	"sort"
	"sync"
	"time"
)

// PaxosNode implements a single Paxos node with banking application state.
type PaxosNode struct {
	mu sync.Mutex

	ID    int
	alive bool

	// Ballot state
	currentBallot   Ballot
	highestPromised Ballot

	// Leader
	isLeader bool
	leaderID int // 0 = unknown

	// Log: seqNum -> *LogEntry
	log        map[int]*LogEntry
	nextSeqNum int

	// Leader tracking: seqNum -> set of nodeIDs that accepted
	acceptedFrom map[int]map[int]bool

	// Execution
	lastExecuted int

	// Bank balances
	balances map[string]int

	// Exactly-once
	lastClientTS     map[string]int64
	lastClientResult map[string]bool

	// New-view history
	newViewHistory []NewViewArgs

	// Execution notify: seqNum -> chan bool
	execNotify map[int]chan bool

	// Timer state
	lastLeaderMsg       time.Time
	timerExpired        bool
	lastPrepareRecv     time.Time
	lastElectionAttempt time.Time
	electionInProgress  bool
	electionFailCount   int

	// Network
	network *Network

	// Lifecycle
	stopCh chan struct{}
	wg     sync.WaitGroup
}

func NewPaxosNode(id int, net *Network) *PaxosNode {
	bal := make(map[string]int)
	for _, c := range AllClientIDs {
		bal[c] = InitBalance
	}
	return &PaxosNode{
		ID:               id,
		currentBallot:    Ballot{0, id},
		log:              make(map[int]*LogEntry),
		nextSeqNum:       1,
		acceptedFrom:     make(map[int]map[int]bool),
		balances:         bal,
		lastClientTS:     make(map[string]int64),
		lastClientResult: make(map[string]bool),
		execNotify:       make(map[int]chan bool),
		timerExpired:     true,
		network:          net,
		stopCh:           make(chan struct{}),
	}
}

func (n *PaxosNode) Start() {
	n.wg.Add(1)
	go n.timerLoop()
}

func (n *PaxosNode) Stop() {
	select {
	case <-n.stopCh:
	default:
		close(n.stopCh)
	}
	n.wg.Wait()
}

// SyncLog copies committed entries from a live peer (recovery catch-up).
func (n *PaxosNode) SyncLog() {
	for pid := 1; pid <= NumNodes; pid++ {
		if pid == n.ID || !n.network.IsAlive(pid) {
			continue
		}
		peer := n.network.GetNode(pid)
		if peer == nil {
			continue
		}

		peer.mu.Lock()
		var entries []LogEntry
		for _, e := range peer.log {
			if e.Status >= StatusCommitted {
				entries = append(entries, *e)
			}
		}
		pLeader := peer.leaderID
		pBallot := peer.highestPromised
		pNext := peer.nextSeqNum
		peer.mu.Unlock()

		n.mu.Lock()
		for _, e := range entries {
			cur := n.log[e.SeqNum]
			if cur == nil {
				n.log[e.SeqNum] = &LogEntry{
					Ballot: e.Ballot, SeqNum: e.SeqNum,
					Request: e.Request, Status: StatusCommitted,
				}
			} else if cur.Status < StatusCommitted {
				cur.Ballot = e.Ballot
				cur.Request = e.Request
				cur.Status = StatusCommitted
			}
		}
		if pLeader > 0 {
			n.leaderID = pLeader
		}
		if pBallot.GreaterThan(n.highestPromised) {
			n.highestPromised = pBallot
		}
		if pNext > n.nextSeqNum {
			n.nextSeqNum = pNext
		}
		n.timerExpired = false
		n.lastLeaderMsg = time.Now()
		n.electionFailCount = 0
		n.mu.Unlock()

		n.tryExecute()
		break
	}
}

// ======================== Timer ========================

func (n *PaxosNode) timerLoop() {
	defer n.wg.Done()
	for {
		select {
		case <-n.stopCh:
			return
		case <-time.After(300 * time.Millisecond):
		}
		n.mu.Lock()
		if !n.alive || n.isLeader || n.leaderID == 0 {
			n.mu.Unlock()
			continue
		}
		if time.Since(n.lastLeaderMsg) > LeaderTimeoutDuration {
			n.timerExpired = true
			backoff := PrepareCooldown * time.Duration(1+n.electionFailCount)
			if backoff > 5*time.Second {
				backoff = 5 * time.Second
			}
			canElect := !n.electionInProgress &&
				time.Since(n.lastPrepareRecv) > PrepareCooldown &&
				time.Since(n.lastElectionAttempt) > backoff
			if canElect {
				n.electionInProgress = true
				n.lastElectionAttempt = time.Now()
				n.mu.Unlock()
				n.startElection()
				continue
			}
		}
		n.mu.Unlock()
	}
}

// ======================== Leader Election ========================

func (n *PaxosNode) startElection() {
	n.mu.Lock()
	if !n.alive {
		n.electionInProgress = false
		n.mu.Unlock()
		return
	}

	newNum := n.highestPromised.Num + 1
	n.currentBallot = Ballot{newNum, n.ID}
	n.highestPromised = n.currentBallot
	ballot := n.currentBallot
	myLog := n.getAcceptLogLocked()
	n.mu.Unlock()

	// Collect promises (self + peers)
	type pRes struct {
		reply PromiseReply
		ok    bool
	}
	ch := make(chan pRes, NumNodes)
	for pid := 1; pid <= NumNodes; pid++ {
		if pid == n.ID {
			continue
		}
		go func(p int) {
			r, err := n.network.SendPrepare(n.ID, p, PrepareArgs{ballot, n.ID})
			ch <- pRes{r, err == nil && r.OK}
		}(pid)
	}

	promises := []PromiseReply{{OK: true, Ballot: ballot, AcceptLog: myLog, NodeID: n.ID}}
	timer := time.After(RPCTimeoutDuration)
	rem := NumNodes - 1
	for rem > 0 {
		select {
		case r := <-ch:
			rem--
			if r.ok {
				promises = append(promises, r.reply)
			}
		case <-timer:
			rem = 0
		}
	}

	if len(promises) < Majority {
		n.mu.Lock()
		n.electionInProgress = false
		n.electionFailCount++
		n.mu.Unlock()
		return
	}

	// Won election
	merged := mergeAcceptLogs(promises, ballot)

	n.mu.Lock()
	if !n.alive {
		n.electionInProgress = false
		n.mu.Unlock()
		return
	}
	n.isLeader = true
	n.leaderID = n.ID
	n.currentBallot = ballot
	n.timerExpired = false
	n.electionInProgress = false
	n.electionFailCount = 0

	for _, e := range merged {
		if e.SeqNum >= n.nextSeqNum {
			n.nextSeqNum = e.SeqNum + 1
		}
		cur := n.log[e.SeqNum]
		if cur == nil || cur.Status < StatusCommitted {
			n.log[e.SeqNum] = &LogEntry{
				Ballot: ballot, SeqNum: e.SeqNum,
				Request: e.Request, Status: StatusAccepted,
			}
			if n.acceptedFrom[e.SeqNum] == nil {
				n.acceptedFrom[e.SeqNum] = make(map[int]bool)
			}
			n.acceptedFrom[e.SeqNum][n.ID] = true
		}
	}

	nv := NewViewArgs{Ballot: ballot, AcceptLog: merged}
	n.newViewHistory = append(n.newViewHistory, nv)
	n.mu.Unlock()

	// Broadcast NEW-VIEW
	var wg2 sync.WaitGroup
	for pid := 1; pid <= NumNodes; pid++ {
		if pid == n.ID {
			continue
		}
		wg2.Add(1)
		go func(p int) {
			defer wg2.Done()
			n.network.SendNewView(n.ID, p, nv)
		}(pid)
	}
	wg2.Wait()

	// Wait and commit NEW-VIEW entries
	if len(merged) > 0 {
		time.Sleep(CommitWait)
		n.commitNewViewEntries(merged, ballot)
	}

	n.tryExecute()
}

// commitNewViewEntries commits NEW-VIEW entries that have majority accepts.
// Retries a few times if needed.
func (n *PaxosNode) commitNewViewEntries(merged []LogEntry, ballot Ballot) {
	for attempt := 0; attempt < 5; attempt++ {
		allDone := true
		var toCommit []CommitArgs
		n.mu.Lock()
		for _, me := range merged {
			entry := n.log[me.SeqNum]
			if entry == nil || entry.Status >= StatusCommitted {
				continue
			}
			if len(n.acceptedFrom[me.SeqNum]) >= Majority {
				entry.Status = StatusCommitted
				toCommit = append(toCommit, CommitArgs{ballot, me.SeqNum, entry.Request})
			} else {
				allDone = false
			}
		}
		n.mu.Unlock()

		// Send COMMITs synchronously outside lock
		for _, ca := range toCommit {
			for p := 1; p <= NumNodes; p++ {
				if p == n.ID {
					continue
				}
				n.network.SendCommit(n.ID, p, ca)
			}
		}
		if allDone {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// ======================== Message Handlers ========================

func (n *PaxosNode) HandlePrepare(args PrepareArgs) (PromiseReply, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if !n.alive {
		return PromiseReply{}, fmt.Errorf("dead")
	}
	n.lastPrepareRecv = time.Now()

	canAccept := (n.timerExpired || n.leaderID == 0) &&
		args.Ballot.GreaterThan(n.highestPromised)

	if canAccept {
		n.highestPromised = args.Ballot
		n.isLeader = false
		n.leaderID = args.SenderID
		return PromiseReply{
			OK: true, Ballot: args.Ballot,
			AcceptLog: n.getAcceptLogLocked(), NodeID: n.ID,
		}, nil
	}
	return PromiseReply{OK: false}, nil
}

func (n *PaxosNode) HandleAccept(args AcceptArgs) (AcceptedReply, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if !n.alive {
		return AcceptedReply{}, fmt.Errorf("dead")
	}

	if args.Ballot.GreaterOrEqual(n.highestPromised) {
		n.highestPromised = args.Ballot
		n.leaderID = args.Ballot.NodeID
		n.lastLeaderMsg = time.Now()
		n.timerExpired = false
		n.electionInProgress = false

		entry := n.log[args.SeqNum]
		if entry == nil {
			entry = &LogEntry{SeqNum: args.SeqNum}
			n.log[args.SeqNum] = entry
		}
		if entry.Status < StatusCommitted {
			entry.Ballot = args.Ballot
			entry.Request = args.Request
			entry.Status = StatusAccepted
		}
		return AcceptedReply{OK: true, Ballot: args.Ballot, SeqNum: args.SeqNum, NodeID: n.ID}, nil
	}
	return AcceptedReply{OK: false}, nil
}

func (n *PaxosNode) HandleCommit(args CommitArgs) {
	n.mu.Lock()
	if !n.alive {
		n.mu.Unlock()
		return
	}
	n.lastLeaderMsg = time.Now()
	n.timerExpired = false
	n.leaderID = args.Ballot.NodeID

	entry := n.log[args.SeqNum]
	if entry == nil {
		entry = &LogEntry{SeqNum: args.SeqNum}
		n.log[args.SeqNum] = entry
	}
	if entry.Status < StatusCommitted {
		entry.Ballot = args.Ballot
		entry.Request = args.Request
		entry.Status = StatusCommitted
	}
	n.mu.Unlock()
	n.tryExecute()
}

func (n *PaxosNode) HandleNewView(leaderID int, args NewViewArgs) {
	n.mu.Lock()
	if !n.alive {
		n.mu.Unlock()
		return
	}

	n.newViewHistory = append(n.newViewHistory, args)
	n.leaderID = args.Ballot.NodeID
	n.highestPromised = args.Ballot
	n.isLeader = false
	n.timerExpired = false
	n.electionInProgress = false
	n.lastLeaderMsg = time.Now()

	for _, e := range args.AcceptLog {
		cur := n.log[e.SeqNum]
		if cur == nil {
			cur = &LogEntry{SeqNum: e.SeqNum}
			n.log[e.SeqNum] = cur
		}
		if cur.Status < StatusCommitted {
			cur.Ballot = args.Ballot
			cur.Request = e.Request
			cur.Status = StatusAccepted
		}
	}

	myID := n.ID
	ballot := args.Ballot
	entries := make([]LogEntry, len(args.AcceptLog))
	copy(entries, args.AcceptLog)
	n.mu.Unlock()

	// Send ACCEPTED back to the new leader
	leader := n.network.GetNode(leaderID)
	if leader != nil && n.network.IsAlive(leaderID) {
		for _, e := range entries {
			leader.recvAccepted(AcceptedReply{
				OK: true, Ballot: ballot,
				SeqNum: e.SeqNum, NodeID: myID,
			})
		}
	}
}

// recvAccepted is called when the leader receives an ACCEPTED message.
// Separated from HandleAccept to avoid confusion.
func (n *PaxosNode) recvAccepted(args AcceptedReply) {
	n.mu.Lock()
	if !n.alive || !n.isLeader || !args.Ballot.GreaterOrEqual(n.currentBallot) {
		n.mu.Unlock()
		return
	}
	if n.acceptedFrom[args.SeqNum] == nil {
		n.acceptedFrom[args.SeqNum] = make(map[int]bool)
	}
	n.acceptedFrom[args.SeqNum][args.NodeID] = true

	shouldCommit := false
	var cReq ClientRequest
	if len(n.acceptedFrom[args.SeqNum]) >= Majority {
		if entry, ok := n.log[args.SeqNum]; ok && entry.Status == StatusAccepted {
			entry.Status = StatusCommitted
			cReq = entry.Request
			shouldCommit = true
		}
	}
	cBallot := n.currentBallot
	n.mu.Unlock()

	if shouldCommit {
		// Send COMMITs synchronously to ensure peers have state before client is notified
		for p := 1; p <= NumNodes; p++ {
			if p == n.ID {
				continue
			}
			n.network.SendCommit(n.ID, p, CommitArgs{cBallot, args.SeqNum, cReq})
		}
		n.tryExecute()
	}
}

// HandleClientRequest is called when a client sends a request to this node.
func (n *PaxosNode) HandleClientRequest(args ClientSubmitArgs) (ClientSubmitReply, error) {
	for attempt := 0; attempt < 5; attempt++ {
		n.mu.Lock()
		if !n.alive {
			n.mu.Unlock()
			return ClientSubmitReply{}, fmt.Errorf("dead")
		}

		// Exactly-once
		if !args.Request.IsNoop {
			if ts, ok := n.lastClientTS[args.Request.ClientID]; ok && args.Request.Timestamp <= ts {
				r := ClientSubmitReply{
					OK: true, Success: n.lastClientResult[args.Request.ClientID],
					LeaderID: n.leaderID, Ballot: n.currentBallot,
				}
				n.mu.Unlock()
				return r, nil
			}
		}

		// === LEADER PATH ===
		if n.isLeader {
			// Pre-check: enough alive nodes?
			n.mu.Unlock()
			if n.network.GetAliveCount() < Majority {
				return ClientSubmitReply{}, fmt.Errorf("not enough nodes")
			}
			n.mu.Lock()
			if !n.alive || !n.isLeader {
				n.mu.Unlock()
				continue
			}

			// Check if request already in log (from NEW-VIEW)
			if !args.Request.IsNoop {
				for _, entry := range n.log {
					if !entry.Request.IsNoop &&
						entry.Request.ClientID == args.Request.ClientID &&
						entry.Request.Timestamp == args.Request.Timestamp {
						// Already in log - wait for execution
						seq := entry.SeqNum
						ch := make(chan bool, 1)
						if entry.Status >= StatusExecuted {
							n.mu.Unlock()
							return ClientSubmitReply{
								OK: true, Success: n.lastClientResult[args.Request.ClientID],
								LeaderID: n.leaderID, Ballot: n.currentBallot,
							}, nil
						}
						n.execNotify[seq] = ch
						n.mu.Unlock()
						return n.waitForExec(ch, seq)
					}
				}
			}

			seq := n.nextSeqNum
			n.nextSeqNum++
			ballot := n.currentBallot

			n.log[seq] = &LogEntry{
				Ballot: ballot, SeqNum: seq,
				Request: args.Request, Status: StatusAccepted,
			}
			if n.acceptedFrom[seq] == nil {
				n.acceptedFrom[seq] = make(map[int]bool)
			}
			n.acceptedFrom[seq][n.ID] = true

			ch := make(chan bool, 1)
			n.execNotify[seq] = ch
			n.mu.Unlock()

			// Broadcast ACCEPT
			acceptCount := 1
			var cmu sync.Mutex
			var wg sync.WaitGroup
			for pid := 1; pid <= NumNodes; pid++ {
				if pid == n.ID {
					continue
				}
				wg.Add(1)
				go func(p int) {
					defer wg.Done()
					r, err := n.network.SendAccept(n.ID, p, AcceptArgs{ballot, seq, args.Request})
					if err == nil && r.OK {
						cmu.Lock()
						acceptCount++
						cmu.Unlock()
						n.recvAccepted(AcceptedReply{OK: true, Ballot: ballot, SeqNum: seq, NodeID: p})
					}
				}(pid)
			}
			wg.Wait()

			cmu.Lock()
			got := acceptCount
			cmu.Unlock()

			if got < Majority {
				// Clean up
				n.mu.Lock()
				delete(n.execNotify, seq)
				if e, ok := n.log[seq]; ok && e.Status < StatusCommitted {
					delete(n.log, seq)
				}
				n.mu.Unlock()
				return ClientSubmitReply{}, fmt.Errorf("not enough accepts (%d/%d)", got, Majority)
			}

			return n.waitForExec(ch, seq)
		}

		// === FORWARD PATH ===
		if n.leaderID > 0 && n.leaderID != n.ID {
			lid := n.leaderID
			n.mu.Unlock()
			reply, err := n.network.SendClientRequest(lid, args)
			if err == nil {
				return reply, nil
			}
			// Leader unreachable - invalidate
			n.mu.Lock()
			if n.leaderID == lid {
				n.leaderID = 0
				n.timerExpired = true
			}
			n.mu.Unlock()
			time.Sleep(300 * time.Millisecond)
			continue
		}

		// === NO LEADER - ELECT ===
		if !n.electionInProgress {
			n.electionInProgress = true
			n.lastElectionAttempt = time.Now()
			n.mu.Unlock()
			n.startElection()
			time.Sleep(200 * time.Millisecond)
			continue
		}
		n.mu.Unlock()
		time.Sleep(ElectionWait)
	}
	return ClientSubmitReply{}, fmt.Errorf("max retries on node %d", n.ID)
}

// waitForExec blocks until a sequence number is executed or times out.
func (n *PaxosNode) waitForExec(ch chan bool, seq int) (ClientSubmitReply, error) {
	deadline := time.After(ExecutionTimeout)
	ticker := time.NewTicker(ExecutionPollInterval)
	defer ticker.Stop()

	for {
		select {
		case success := <-ch:
			n.mu.Lock()
			r := ClientSubmitReply{
				OK: true, Success: success,
				LeaderID: n.leaderID, Ballot: n.currentBallot,
			}
			n.mu.Unlock()
			return r, nil
		case <-ticker.C:
			n.tryExecute()
		case <-deadline:
			n.mu.Lock()
			delete(n.execNotify, seq)
			n.mu.Unlock()
			return ClientSubmitReply{}, fmt.Errorf("exec timeout seq %d", seq)
		}
	}
}

// ======================== Execution ========================

func (n *PaxosNode) tryExecute() {
	n.mu.Lock()
	defer n.mu.Unlock()

	for {
		next := n.lastExecuted + 1
		entry, ok := n.log[next]
		if !ok || entry.Status < StatusCommitted {
			break
		}
		if entry.Status >= StatusExecuted {
			n.lastExecuted = next
			continue
		}

		// Exactly-once check for duplicate requests
		alreadyDone := false
		if !entry.Request.IsNoop {
			if ts, ok := n.lastClientTS[entry.Request.ClientID]; ok && entry.Request.Timestamp <= ts {
				alreadyDone = true
			}
		}

		var success bool
		if alreadyDone {
			success = n.lastClientResult[entry.Request.ClientID]
		} else {
			success = n.execTx(entry)
			if !entry.Request.IsNoop {
				n.lastClientTS[entry.Request.ClientID] = entry.Request.Timestamp
				n.lastClientResult[entry.Request.ClientID] = success
			}
		}

		entry.Status = StatusExecuted
		n.lastExecuted = next

		if ch, ok := n.execNotify[next]; ok {
			select {
			case ch <- success:
			default:
			}
			delete(n.execNotify, next)
		}
	}
}

func (n *PaxosNode) execTx(entry *LogEntry) bool {
	if entry.Request.IsNoop {
		return true
	}
	tx := entry.Request.Tx
	if n.balances[tx.Sender] >= tx.Amount {
		n.balances[tx.Sender] -= tx.Amount
		n.balances[tx.Receiver] += tx.Amount
		return true
	}
	return false
}

// ======================== Helpers ========================

func (n *PaxosNode) getAcceptLogLocked() []LogEntry {
	var result []LogEntry
	for _, e := range n.log {
		if e.Status >= StatusAccepted {
			result = append(result, *e)
		}
	}
	sort.Slice(result, func(i, j int) bool { return result[i].SeqNum < result[j].SeqNum })
	return result
}

func mergeAcceptLogs(promises []PromiseReply, ballot Ballot) []LogEntry {
	best := make(map[int]LogEntry)
	maxSeq := 0
	for _, p := range promises {
		for _, e := range p.AcceptLog {
			if e.SeqNum > maxSeq {
				maxSeq = e.SeqNum
			}
			if cur, ok := best[e.SeqNum]; !ok || e.Ballot.GreaterThan(cur.Ballot) {
				best[e.SeqNum] = e
			}
		}
	}
	if maxSeq == 0 {
		return nil
	}
	var result []LogEntry
	for s := 1; s <= maxSeq; s++ {
		if e, ok := best[s]; ok {
			result = append(result, LogEntry{Ballot: ballot, SeqNum: s, Request: e.Request})
		} else {
			result = append(result, LogEntry{Ballot: ballot, SeqNum: s, Request: ClientRequest{IsNoop: true}})
		}
	}
	return result
}

// ======================== Print Functions ========================

func (n *PaxosNode) PrintLog() {
	n.mu.Lock()
	defer n.mu.Unlock()
	fmt.Printf("\n=== Log for Node n%d (Leader=%v, LeaderID=%d, Ballot=%s) ===\n",
		n.ID, n.isLeader, n.leaderID, n.currentBallot.String())
	if len(n.log) == 0 {
		fmt.Println("  (empty)")
		return
	}
	seqs := make([]int, 0, len(n.log))
	for s := range n.log {
		seqs = append(seqs, s)
	}
	sort.Ints(seqs)
	fmt.Printf("  %-6s %-12s %-20s %-10s\n", "Seq", "Ballot", "Transaction", "Status")
	fmt.Printf("  %-6s %-12s %-20s %-10s\n", "---", "------", "-----------", "------")
	for _, s := range seqs {
		e := n.log[s]
		tx := e.Request.Tx.String()
		if e.Request.IsNoop {
			tx = "no-op"
		}
		fmt.Printf("  %-6d %-12s %-20s %-10s\n", s, e.Ballot.String(), tx, e.Status.Label())
	}
}

func (n *PaxosNode) PrintDB() {
	n.mu.Lock()
	defer n.mu.Unlock()
	fmt.Printf("\n=== DataStore for Node n%d ===\n", n.ID)
	for _, c := range AllClientIDs {
		fmt.Printf("  %s: %d\n", c, n.balances[c])
	}
}

func (n *PaxosNode) GetStatus(seq int) string {
	n.mu.Lock()
	defer n.mu.Unlock()
	if e, ok := n.log[seq]; ok {
		return e.Status.Label()
	}
	return "X"
}

func (n *PaxosNode) GetTxString(seq int) string {
	n.mu.Lock()
	defer n.mu.Unlock()
	if e, ok := n.log[seq]; ok {
		if e.Request.IsNoop {
			return "no-op"
		}
		return e.Request.Tx.String()
	}
	return ""
}
