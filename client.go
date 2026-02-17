package main

import (
	"fmt"
	"sync"
	"time"
)

// PaxosClient represents a banking client.
type PaxosClient struct {
	mu         sync.Mutex
	ID         string
	timestamp  int64
	leaderHint int
	network    *Network
}

func NewPaxosClient(id string, net *Network) *PaxosClient {
	return &PaxosClient{
		ID:         id,
		leaderHint: 1,
		network:    net,
	}
}

// SendTransaction sends a transaction and returns (success, error).
func (c *PaxosClient) SendTransaction(tx Transaction) (bool, error) {
	c.mu.Lock()
	c.timestamp++
	req := ClientRequest{
		Tx: tx, Timestamp: c.timestamp,
		ClientID: c.ID,
	}
	hint := c.leaderHint
	c.mu.Unlock()

	args := ClientSubmitArgs{Request: req}

	// Phase 1: Try believed leader
	done := make(chan ClientSubmitReply, 1)
	errCh := make(chan error, 1)
	go func() {
		r, err := c.network.SendClientRequest(hint, args)
		if err != nil {
			errCh <- err
		} else {
			done <- r
		}
	}()

	select {
	case r := <-done:
		if r.OK {
			c.mu.Lock()
			if r.LeaderID > 0 {
				c.leaderHint = r.LeaderID
			}
			c.mu.Unlock()
			return r.Success, nil
		}
	case <-errCh:
	case <-time.After(ClientTimeoutDuration):
	}

	// Phase 2: Broadcast
	return c.broadcast(args)
}

func (c *PaxosClient) broadcast(args ClientSubmitArgs) (bool, error) {
	type res struct {
		reply ClientSubmitReply
		err   error
	}
	ch := make(chan res, NumNodes)
	alive := c.network.GetAliveNodes()
	if len(alive) == 0 {
		return false, fmt.Errorf("no alive nodes")
	}

	for _, nid := range alive {
		go func(id int) {
			r, err := c.network.SendClientRequest(id, args)
			ch <- res{r, err}
		}(nid)
	}

	timer := time.After(ClientTimeoutDuration * 2)
	rem := len(alive)
	for rem > 0 {
		select {
		case r := <-ch:
			rem--
			if r.err == nil && r.reply.OK {
				c.mu.Lock()
				if r.reply.LeaderID > 0 {
					c.leaderHint = r.reply.LeaderID
				}
				c.mu.Unlock()
				return r.reply.Success, nil
			}
		case <-timer:
			return false, fmt.Errorf("broadcast timeout for %s", c.ID)
		}
	}
	return false, fmt.Errorf("no node handled request from %s", c.ID)
}
