

The primary objective is to ensure that multiple nodes (replicas) reach an agreement on a sequence of transactions, maintaining a consistent state across the system even if some nodes crash.

Core Project Components

1. The Banking Application

Transactions: Clients submit requests to transfer money between accounts in the format (sender, receiver, amount).
State Machine Replication: All replicas must execute transactions in the same total order to ensure the safety of the banking ledger.
Semantics: The system must guarantee exactly-once semantics, using client-assigned timestamps to discard duplicate or old requests.
2. Stable-Leader Paxos Protocol

Unlike Basic Paxos, which requires a leader election for every single value, this implementation uses a "Stable-Leader" approach (inspired by Multi-Paxos).

Efficiency: A single leader is elected to manage multiple transactions, eliminating the overhead of the "prepare" phase for every individual request.
Normal Operations: Once a leader is established, it assigns a sequence number to each request and broadcasts ACCEPT messages. Replicas respond with ACCEPTED, and the leader commits the transaction once a majority ($f+1$) of nodes agree.
Leader Election: If the current leader is suspected of failing (based on configurable timers), a new leader is elected through a PREPARE and PROMISE phase.
New-View Phase: During election, the new leader collects an AcceptLog from a majority to identify any previously accepted but uncommitted transactions, filling gaps with no-op operations if necessary.
3. System Requirements & Constraints

Configuration: The implementation must support 5 nodes (where $f=2$ can fail) and 10 clients.
Communication: Nodes should be independent (processes or threads) and communicate via TCP/UDP sockets or RPCs; shared memory is strictly forbidden.
Fault Tolerance: The system must remain operational and continue processing transactions as long as no more than $f$ nodes are faulty.
Key Required Functions

Your program must include specific terminal-based functions to demonstrate its state:

PrintLog: Displays all messages processed by a specific node.
PrintDB: Shows the current account balances for all clients.
PrintStatus: Outputs the status of a specific transaction (Accepted, Committed, Executed, or No Status) at each node.
PrintView: Lists all NEW-VIEW messages exchanged during leader elections.
