package main

import (
	"fmt"
	"time"
)

// ======================== Constants ========================

const (
	NumNodes    = 5
	NumClients  = 10
	InitBalance = 10
	Majority    = NumNodes/2 + 1 // 3

	LeaderTimeoutDuration = 1500 * time.Millisecond
	ClientTimeoutDuration = 2000 * time.Millisecond
	RPCTimeoutDuration    = 800 * time.Millisecond
	PrepareCooldown       = 600 * time.Millisecond
	ElectionWait          = 500 * time.Millisecond
	CommitWait            = 300 * time.Millisecond
	ExecutionPollInterval = 50 * time.Millisecond
	ExecutionTimeout      = 8 * time.Second
)

// AllClientIDs lists all 10 client identifiers A-J
var AllClientIDs = []string{"A", "B", "C", "D", "E", "F", "G", "H", "I", "J"}

// ======================== Ballot ========================

// Ballot represents a Paxos ballot number (round, nodeID)
type Ballot struct {
	Num    int
	NodeID int
}

func (b Ballot) String() string {
	return fmt.Sprintf("(%d,%d)", b.Num, b.NodeID)
}

func (b Ballot) IsZero() bool {
	return b.Num == 0 && b.NodeID == 0
}

func (b Ballot) GreaterThan(o Ballot) bool {
	if b.Num != o.Num {
		return b.Num > o.Num
	}
	return b.NodeID > o.NodeID
}

func (b Ballot) GreaterOrEqual(o Ballot) bool {
	return b == o || b.GreaterThan(o)
}

// ======================== Transaction ========================

type Transaction struct {
	Sender   string
	Receiver string
	Amount   int
}

func (t Transaction) String() string {
	if t.Sender == "" && t.Receiver == "" && t.Amount == 0 {
		return "no-op"
	}
	return fmt.Sprintf("(%s,%s,%d)", t.Sender, t.Receiver, t.Amount)
}

// ======================== ClientRequest ========================

type ClientRequest struct {
	Tx        Transaction
	Timestamp int64
	ClientID  string
	IsNoop    bool
}

func (r ClientRequest) String() string {
	if r.IsNoop {
		return "no-op"
	}
	return r.Tx.String()
}

// ======================== Entry Status ========================

type EntryStatus int

const (
	StatusNone      EntryStatus = iota // X
	StatusAccepted                     // A
	StatusCommitted                    // C
	StatusExecuted                     // E
)

func (s EntryStatus) Label() string {
	switch s {
	case StatusAccepted:
		return "A"
	case StatusCommitted:
		return "C"
	case StatusExecuted:
		return "E"
	default:
		return "X"
	}
}

// ======================== Log Entry ========================

type LogEntry struct {
	Ballot  Ballot
	SeqNum  int
	Request ClientRequest
	Status  EntryStatus
}

// ======================== RPC Message Types ========================

type PrepareArgs struct {
	Ballot   Ballot
	SenderID int
}

type PromiseReply struct {
	OK        bool
	Ballot    Ballot
	AcceptLog []LogEntry
	NodeID    int
}

type AcceptArgs struct {
	Ballot  Ballot
	SeqNum  int
	Request ClientRequest
}

type AcceptedReply struct {
	OK     bool
	Ballot Ballot
	SeqNum int
	NodeID int
}

type CommitArgs struct {
	Ballot  Ballot
	SeqNum  int
	Request ClientRequest
}

type NewViewArgs struct {
	Ballot    Ballot
	AcceptLog []LogEntry
}

type ClientSubmitArgs struct {
	Request ClientRequest
}

type ClientSubmitReply struct {
	OK       bool
	Success  bool
	Ballot   Ballot
	LeaderID int
}

// ======================== Test Case Types ========================

type TestTransaction struct {
	Tx   Transaction
	IsLF bool
}

type TestSet struct {
	SetNumber    int
	Transactions []TestTransaction
	LiveNodes    []int
}
