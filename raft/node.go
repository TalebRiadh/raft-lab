package raft

import (
	"log"
	"math/rand"
	"net/rpc"
	"time"
)

func NewNode(id string, peers []string) *Node {
	n := &Node{
		id:     id,
		peers:  peers,
		state:  Follower,
		stopCh: make(chan struct{}),
	}
	n.resetElectionTimer()
	return n
}

func randomTimeout() time.Duration {
	return time.Duration(150+rand.Intn(150)) * time.Millisecond
}

func (n *Node) resetElectionTimer() {
	n.electionResetAt = time.Now()
	n.electionTimeout = randomTimeout()
}

func (n *Node) Run() {
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-n.stopCh:
			return
		case <-ticker.C:
			n.mu.Lock()
			state := n.state
			elapsed := time.Since(n.electionResetAt)
			timeout := n.electionTimeout
			n.mu.Unlock()

			switch state {
			case Leader:
				n.sendHeartbeats()
			case Follower, Candidate:
				if elapsed >= timeout {
					n.startElection()
				}

			}
		}
	}
}

func (n *Node) startElection() {
	n.mu.Lock()
	n.state = Candidate
	n.currentTerm++
	n.votedFor = n.id
	term := n.currentTerm
	n.resetElectionTimer()
	n.mu.Unlock()

	log.Printf("[%s] becomes a CANDIDATE for the term %d", n.id, term)

	votes := 1
	votesCh := make(chan bool, len(n.peers))
	args := RequestVoteArgs{Term: term, candidateID: n.id}

	for _, peer := range n.peers {
		go func(addr string) {
			var reply RequestVoteReply
			if !n.callRPC(addr, "RPCService.RequestVote", args, &reply) {
				votesCh <- false
				return
			}
			n.mu.Lock()
			if reply.Term > n.currentTerm {
				n.currentTerm = reply.Term
				n.state = Follower
				n.votedFor = ""
			}
			n.mu.Unlock()
			votesCh <- reply.VoteGranted
		}(peer)
	}

	for i := 0; i < len(n.peers); i++ {
		if <-votesCh {
			votes++
		}
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	majority := len(n.peers)/2 + 1
	if n.state == Candidate && n.currentTerm == term && votes >= majority {
		n.state = Leader
		log.Printf("[%s] becomes LEADER for the term %d (votes: %d/%d)", n.id, term, votes, len(n.peers)+1)
	} else {
		log.Printf("[%s] fails to become LEADER (votes: %d/%d requis)", n.id, votes, majority)
	}
}

func (n *Node) sendHeartbeats() {
	n.mu.Lock()
	term := n.currentTerm
	n.mu.Unlock()

	args := AppendEntriesArgs{Term: term, LeaderID: n.id}

	for _, peer := range n.peers {
		go func(addr string) {
			var reply AppendEntriesReply
			if !n.callRPC(addr, "RPCService.AppendEntries", args, &reply) {
				return
			}
			n.mu.Lock()
			if reply.Term > n.currentTerm {
				n.currentTerm = reply.Term
				n.state = Follower
				n.votedFor = ""
			}
			n.mu.Unlock()
		}(peer)
	}
}

func (n *Node) callRPC(addr, method string, args, reply interface{}) bool {
	client, err := rpc.DialHTTP("tcp", addr)
	if err != nil {
		return false // Peer unreachable = normal in DS; we simply ignore it.
	}
	defer client.Close()
	return client.Call(method, args, reply) == nil
}

type RPCService struct {
	node *Node
}

func (s *RPCService) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) error {
	n := s.node
	n.mu.Lock()
	defer n.mu.Unlock()

	reply.Term = n.currentTerm

	if args.Term < n.currentTerm {
		reply.VoteGranted = false
		return nil
	}
	if args.Term > n.currentTerm {
		n.currentTerm = args.Term
		n.state = Follower
		n.votedFor = ""
	}

	if n.votedFor == "" || n.votedFor == args.candidateID {
		n.votedFor = args.candidateID

		reply.VoteGranted = true
		n.resetElectionTimer()
		log.Printf("[%s] vote for %s (term %d)", n.id, args.candidateID, args.Term)
	} else {
		reply.VoteGranted = false
	}

	reply.Term = n.currentTerm
	return nil
}

func (s *RPCService) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) error {
	n := s.node
	n.mu.Lock()
	defer n.mu.Unlock()

	if args.Term < n.currentTerm {
		reply.Success = false
		reply.Term = n.currentTerm
		return nil
	}

	if args.Term > n.currentTerm {
		n.currentTerm = args.Term
		n.votedFor = ""
	}
	n.state = Follower
	n.resetElectionTimer()

	reply.Success = true
	reply.Term = n.currentTerm
	return nil
}
