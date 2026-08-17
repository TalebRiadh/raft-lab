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
		log:    []LogEntry{{Term: 0}}, // sentinel
		kv:     make(map[string]string),
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

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
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
				n.replicateToAllPeers()
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
	lastLogIndex := len(n.log) - 1
	lastLogTerm := n.log[lastLogIndex].Term
	n.resetElectionTimer()
	n.mu.Unlock()

	log.Printf("[%s] becomes a CANDIDATE for the term %d", n.id, term)

	votes := 1
	votesCh := make(chan bool, len(n.peers))
	args := RequestVoteArgs{
		Term:         term,
		CandidateID:  n.id,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}

	for _, peer := range n.peers {
		go func(addr string) {
			var reply RequestVoteReply
			ok := n.callRPC(addr, "RPCService.RequestVote", args, &reply)
			if !ok {
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
		n.leaderID = n.id
		n.nextIndex = make(map[string]int)
		n.matchIndex = make(map[string]int)
		for _, p := range n.peers {
			n.nextIndex[p] = len(n.log)
			n.matchIndex[p] = 0
		}
		log.Printf("[%s] becomes LEADER for the term %d (votes: %d/%d)", n.id, term, votes, len(n.peers)+1)
	} else {
		log.Printf("[%s] fails to become LEADER (votes: %d/%d requis)", n.id, votes, majority)
	}
}

// Start is called by the HTTP API to submit a command.
// Returns (index, term, isLeader).
func (n *Node) Start(op, key, value string) (int, int, bool) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.state != Leader {
		return -1, -1, false
	}

	entry := LogEntry{Term: n.currentTerm, Op: op, Key: key, Value: value}
	n.log = append(n.log, entry)
	index := len(n.log) - 1
	log.Printf("[%s] new entry log[%d]: %s %s=%s", n.id, index, op, key, value)

	return index, n.currentTerm, true
}

func (n *Node) replicateToAllPeers() {
	n.mu.Lock()
	if n.state != Leader {
		n.mu.Unlock()
		return
	}
	term := n.currentTerm
	commitIndex := n.commitIndex
	peers := append([]string{}, n.peers...)
	n.mu.Unlock()

	for _, peer := range peers {
		go n.replicateToPeer(peer, term, commitIndex)
	}
}

func (n *Node) replicateToPeer(peer string, term, leaderCommit int) {
	n.mu.Lock()
	if n.state != Leader || n.currentTerm != term {
		n.mu.Unlock()
		return
	}
	nextIdx := n.nextIndex[peer]
	prevLogIndex := nextIdx - 1
	prevLogTerm := n.log[prevLogIndex].Term
	entries := append([]LogEntry{}, n.log[nextIdx:]...)
	args := AppendEntriesArgs{
		Term:         term,
		LeaderID:     n.id,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: leaderCommit,
	}
	n.mu.Unlock()

	var reply AppendEntriesReply
	if !n.callRPC(peer, "RPCService.AppendEntries", args, &reply) {
		return
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	if n.state != Leader || n.currentTerm != term {
		return
	}

	if reply.Term > n.currentTerm {
		n.currentTerm = reply.Term
		n.state = Follower
		n.votedFor = ""
		return
	}

	if reply.Success {
		n.matchIndex[peer] = prevLogIndex + len(args.Entries)
		n.nextIndex[peer] = n.matchIndex[peer] + 1
		n.tryAdvanceCommitIndex()
	} else {
		if n.nextIndex[peer] > 1 {
			n.nextIndex[peer]--
		}
	}
}

func (n *Node) tryAdvanceCommitIndex() {
	majority := len(n.peers)/2 + 1

	for N := len(n.log) - 1; N > n.commitIndex; N-- {
		if n.log[N].Term != n.currentTerm {
			continue // Raft security: one only knows one's own term.
		}
		count := 1
		for _, peer := range n.peers {
			if n.matchIndex[peer] >= N {
				count++
			}
		}
		if count >= majority {
			n.commitIndex = N
			log.Printf("[%s] commitIndex advances to %d", n.id, N)
			n.applyCommitted()
			break
		}
	}
}

func (n *Node) applyCommitted() {
	for n.lastApplied < n.commitIndex {
		n.lastApplied++
		entry := n.log[n.lastApplied]
		switch entry.Op {
		case "SET":
			n.kv[entry.Key] = entry.Value
		case "DEL":
			delete(n.kv, entry.Key)
		}
		log.Printf("[%s] log apply[%d]: %s %s=%s", n.id, n.lastApplied, entry.Op, entry.Key, entry.Value)
	}
}

func (n *Node) callRPC(addr, method string, args, reply interface{}) bool {
	client, err := rpc.DialHTTP("tcp", addr)
	if err != nil {
		return false // Peer unreachable = normal in DS; we simply ignore it.
	}
	defer func(client *rpc.Client) {
		err := client.Close()
		if err != nil {

		}
	}(client)
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

	myLastIndex := len(n.log) - 1
	myLastTerm := n.log[myLastIndex].Term

	logOk := args.LastLogTerm > myLastTerm ||
		(args.LastLogTerm == myLastTerm && args.LastLogIndex >= myLastIndex)

	if (n.votedFor == "" || n.votedFor == args.CandidateID) && logOk {
		n.votedFor = args.CandidateID
		reply.VoteGranted = true
		n.resetElectionTimer()
		log.Printf("[%s] vote for %s (term %d)", n.id, args.CandidateID, args.Term)
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
	n.leaderID = args.LeaderID
	n.resetElectionTimer()

	// Log Matching Property: my log must have an entry at PrevLogIndex
	// with the same term as the leader's.
	if args.PrevLogIndex >= len(n.log) || n.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
		reply.Term = n.currentTerm
		return nil
	}

	insertAt := args.PrevLogIndex + 1
	for i, entry := range args.Entries {
		idx := insertAt + i
		if idx < len(n.log) {
			if n.log[idx].Term != entry.Term {
				n.log = n.log[:idx] // conflict -> truncate
				n.log = append(n.log, args.Entries[i:]...)
				break
			}
		} else {
			n.log = append(n.log, args.Entries[i:]...)
			break
		}
	}

	if args.LeaderCommit > n.commitIndex {
		n.commitIndex = min(args.LeaderCommit, len(n.log)-1)
		n.applyCommitted()
	}

	reply.Success = true
	reply.Term = n.currentTerm
	return nil
}

func (n *Node) Status() (id string, state State, term, logLen, commitIndex int, leaderID string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.id, n.state, n.currentTerm, len(n.log) - 1, n.commitIndex, n.leaderID
}

func (n *Node) Get(key string) (string, bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	v, ok := n.kv[key]
	return v, ok
}
