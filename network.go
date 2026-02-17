package main

import (
	"fmt"
	"sync"
)

// Network manages alive/dead status and routes messages between nodes.
type Network struct {
	mu    sync.RWMutex
	nodes map[int]*PaxosNode
	alive map[int]bool
}

func NewNetwork() *Network {
	return &Network{
		nodes: make(map[int]*PaxosNode),
		alive: make(map[int]bool),
	}
}

func (net *Network) Register(id int, node *PaxosNode) {
	net.mu.Lock()
	defer net.mu.Unlock()
	net.nodes[id] = node
	net.alive[id] = false
}

func (net *Network) SetAlive(id int, alive bool) {
	net.mu.Lock()
	net.alive[id] = alive
	node := net.nodes[id]
	net.mu.Unlock()
	if node != nil {
		node.mu.Lock()
		node.alive = alive
		if !alive {
			node.isLeader = false
		}
		node.mu.Unlock()
	}
}

func (net *Network) IsAlive(id int) bool {
	net.mu.RLock()
	defer net.mu.RUnlock()
	return net.alive[id]
}

func (net *Network) GetNode(id int) *PaxosNode {
	net.mu.RLock()
	defer net.mu.RUnlock()
	return net.nodes[id]
}

func (net *Network) GetAliveNodes() []int {
	net.mu.RLock()
	defer net.mu.RUnlock()
	var result []int
	for id, a := range net.alive {
		if a {
			result = append(result, id)
		}
	}
	return result
}

func (net *Network) GetAliveCount() int {
	net.mu.RLock()
	defer net.mu.RUnlock()
	count := 0
	for _, a := range net.alive {
		if a {
			count++
		}
	}
	return count
}

func (net *Network) GetCurrentLeaderID() int {
	net.mu.RLock()
	defer net.mu.RUnlock()
	for id, node := range net.nodes {
		if net.alive[id] {
			node.mu.Lock()
			if node.isLeader {
				lid := node.ID
				node.mu.Unlock()
				return lid
			}
			node.mu.Unlock()
		}
	}
	return 0
}

// ======================== Message Routing ========================

func (net *Network) SendPrepare(from, to int, args PrepareArgs) (PromiseReply, error) {
	if !net.IsAlive(from) || !net.IsAlive(to) {
		return PromiseReply{}, fmt.Errorf("unavailable")
	}
	node := net.GetNode(to)
	if node == nil {
		return PromiseReply{}, fmt.Errorf("not found")
	}
	return node.HandlePrepare(args)
}

func (net *Network) SendAccept(from, to int, args AcceptArgs) (AcceptedReply, error) {
	if !net.IsAlive(from) || !net.IsAlive(to) {
		return AcceptedReply{}, fmt.Errorf("unavailable")
	}
	node := net.GetNode(to)
	if node == nil {
		return AcceptedReply{}, fmt.Errorf("not found")
	}
	return node.HandleAccept(args)
}

func (net *Network) SendCommit(from, to int, args CommitArgs) error {
	if !net.IsAlive(from) || !net.IsAlive(to) {
		return fmt.Errorf("unavailable")
	}
	node := net.GetNode(to)
	if node == nil {
		return fmt.Errorf("not found")
	}
	node.HandleCommit(args)
	return nil
}

func (net *Network) SendNewView(from, to int, args NewViewArgs) error {
	if !net.IsAlive(from) || !net.IsAlive(to) {
		return fmt.Errorf("unavailable")
	}
	node := net.GetNode(to)
	if node == nil {
		return fmt.Errorf("not found")
	}
	node.HandleNewView(from, args)
	return nil
}

func (net *Network) SendClientRequest(to int, args ClientSubmitArgs) (ClientSubmitReply, error) {
	if !net.IsAlive(to) {
		return ClientSubmitReply{}, fmt.Errorf("unavailable")
	}
	node := net.GetNode(to)
	if node == nil {
		return ClientSubmitReply{}, fmt.Errorf("not found")
	}
	return node.HandleClientRequest(args)
}
