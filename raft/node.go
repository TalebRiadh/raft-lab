package raft

import (
	"encoding/json"
	"log"
	"math/rand"
	"net/rpc"
	"time"
)

func NewNode(id string, peers []string) *Node {
	n := &Node{
		id:          id,
		peers:       peers,
		state:       Follower,
		log:         []LogEntry{{Term: 0}}, // sentinel
		kv:          make(map[string]string),
		replicating: make(map[string]bool),
		stopCh:      make(chan struct{}),
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

// snapshotThreshold is intentionally small (normal production value: thousands)
// to easily observe compaction during manual testing.
const snapshotThreshold = 5

// lastLogIndexLocked returns the actual index of the last log entry.
// Must be called with n.mu already held.
func (n *Node) lastLogIndexLocked() int {
	return n.lastIncludedIndex + len(n.log) - 1
}

// getEntry returns the entry at a given actual index. Must be called with
// n.mu already held, and index must be >= n.lastIncludedIndex.
func (n *Node) getEntry(index int) LogEntry {
	return n.log[index-n.lastIncludedIndex]
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
	lastLogIndex := n.lastLogIndexLocked()
	lastLogTerm := n.log[len(n.log)-1].Term
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
			n.nextIndex[p] = n.lastLogIndexLocked() + 1
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
	index := n.lastLogIndexLocked()
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
	var targets []string
	for _, peer := range n.peers {
		if !n.replicating[peer] {
			n.replicating[peer] = true
			targets = append(targets, peer)
		}
	}
	n.mu.Unlock()

	for _, peer := range targets {
		go func(peer string) {
			defer func() {
				n.mu.Lock()
				delete(n.replicating, peer)
				n.mu.Unlock()
			}()
			n.replicateToPeer(peer, term, commitIndex)
		}(peer)
	}
}

func (n *Node) replicateToPeer(peer string, term, leaderCommit int) {
	n.mu.Lock()
	if n.state != Leader || n.currentTerm != term {
		n.mu.Unlock()
		return
	}
	nextIdx := n.nextIndex[peer]

	if nextIdx <= n.lastIncludedIndex {
		n.mu.Unlock()
		n.sendInstallSnapshot(peer, term)
		return
	}

	prevLogIndex := nextIdx - 1
	prevLogTerm := n.log[prevLogIndex-n.lastIncludedIndex].Term
	entries := append([]LogEntry{}, n.log[nextIdx-n.lastIncludedIndex:]...)
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

func (n *Node) sendInstallSnapshot(peer string, term int) {
	n.mu.Lock()
	if n.state != Leader || n.currentTerm != term {
		n.mu.Unlock()
		return
	}

	data, err := json.Marshal(n.kv)
	if err != nil {
		n.mu.Unlock()
		return
	}
	args := InstallSnapshotArgs{
		Term:              term,
		LeaderID:          n.id,
		LastIncludedIndex: n.lastIncludedIndex,
		LastIncludedTerm:  n.lastIncludedTerm,
		Data:              data,
	}
	n.mu.Unlock()

	log.Printf("[%s] sends InstallSnapshot to %s (up to index %d)", n.id, peer, args.LastIncludedIndex)

	var reply InstallSnapshotReply
	if !n.callRPC(peer, "RPCService.InstallSnapshot", args, &reply) {
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

	n.matchIndex[peer] = args.LastIncludedIndex
	n.nextIndex[peer] = args.LastIncludedIndex + 1
}

func (n *Node) tryAdvanceCommitIndex() {
	majority := len(n.peers)/2 + 1

	for N := n.lastLogIndexLocked(); N > n.commitIndex; N-- {
		if n.getEntry(N).Term != n.currentTerm {
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
			n.maybeSnapshot()
			break
		}
	}
}

func (n *Node) applyCommitted() {
	for n.lastApplied < n.commitIndex {
		n.lastApplied++
		entry := n.log[n.lastApplied-n.lastIncludedIndex]
		switch entry.Op {
		case "SET":
			n.kv[entry.Key] = entry.Value
		case "DEL":
			delete(n.kv, entry.Key)
		}
		log.Printf("[%s] log apply[%d]: %s %s=%s", n.id, n.lastApplied, entry.Op, entry.Key, entry.Value)
	}
}

// maybeSnapshot compacts the log if enough committed entries have
// accumulated. Must be called with n.mu held.
func (n *Node) maybeSnapshot() {
	if len(n.log)-1 < snapshotThreshold {
		return
	}
	compactIndex := n.commitIndex
	if compactIndex <= n.lastIncludedIndex {
		return
	}

	newTerm := n.getEntry(compactIndex).Term
	keep := append([]LogEntry{}, n.log[compactIndex-n.lastIncludedIndex:]...)
	keep[0] = LogEntry{Term: newTerm} // new sentinel, without payload

	n.log = keep
	n.lastIncludedIndex = compactIndex
	n.lastIncludedTerm = newTerm

	log.Printf("[%s] snapshot taken up to index %d (compacted log: %d entries remaining in memory)",
		n.id, compactIndex, len(n.log)-1)
}

// rpcTimeout bounds each RPC (dial + call). It must stay well below the
// minimum election timeout so a hung peer cannot stall elections.
const rpcTimeout = 100 * time.Millisecond

func (n *Node) callRPC(addr, method string, args, reply any) bool {
	ch := make(chan bool, 1)
	go func() {
		client, err := rpc.DialHTTP("tcp", addr)
		if err != nil {
			ch <- false // Peer unreachable = normal in DS; we simply ignore it.
			return
		}
		err = client.Call(method, args, reply)
		_ = client.Close()
		ch <- err == nil
	}()

	select {
	case ok := <-ch:
		return ok
	case <-time.After(rpcTimeout):
		return false // reply is abandoned by the caller and never reused
	}
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

	myLastIndex := n.lastLogIndexLocked()
	myLastTerm := n.getEntry(myLastIndex).Term

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

	reply.Term = n.currentTerm

	if args.Term < n.currentTerm {
		reply.Success = false
		return nil
	}

	if args.Term > n.currentTerm {
		n.currentTerm = args.Term
		n.votedFor = ""
	}
	n.state = Follower
	n.leaderID = args.LeaderID
	n.resetElectionTimer()

	// Cas limite : une partie ou la totalité de la requête est déjà résumée
	// dans notre snapshot. Les entrées couvertes par le snapshot sont déjà
	// stockées ; on ne garde que la partie au-delà de celui-ci, rattachée à
	// la sentinelle (index lastIncludedIndex, terme lastIncludedTerm).
	if args.PrevLogIndex < n.lastIncludedIndex {
		if args.PrevLogIndex+len(args.Entries) <= n.lastIncludedIndex {
			reply.Success = true
			reply.Term = n.currentTerm
			return nil
		}
		args.Entries = args.Entries[n.lastIncludedIndex-args.PrevLogIndex:]
		args.PrevLogIndex = n.lastIncludedIndex
		args.PrevLogTerm = n.lastIncludedTerm
	}

	if args.PrevLogIndex > n.lastLogIndexLocked() || n.getEntry(args.PrevLogIndex).Term != args.PrevLogTerm {
		reply.Success = false
		reply.Term = n.currentTerm
		return nil
	}

	insertAt := args.PrevLogIndex + 1 - n.lastIncludedIndex
	for i, entry := range args.Entries {
		idx := insertAt + i
		if idx < len(n.log) {
			if n.log[idx].Term != entry.Term {
				n.log = n.log[:idx]
				n.log = append(n.log, args.Entries[i:]...)
				break
			}
		} else {
			n.log = append(n.log, args.Entries[i:]...)
			break
		}
	}

	if args.LeaderCommit > n.commitIndex {
		n.commitIndex = min(args.LeaderCommit, n.lastLogIndexLocked())
		n.applyCommitted()
		n.maybeSnapshot()
	}

	reply.Success = true
	reply.Term = n.currentTerm
	return nil
}

func (s *RPCService) InstallSnapshot(args InstallSnapshotArgs, reply *InstallSnapshotReply) error {
	n := s.node
	n.mu.Lock()
	defer n.mu.Unlock()

	reply.Term = n.currentTerm

	if args.Term < n.currentTerm {
		return nil
	}
	if args.Term > n.currentTerm {
		n.currentTerm = args.Term
		n.votedFor = ""
	}
	n.state = Follower
	n.leaderID = args.LeaderID
	n.resetElectionTimer()

	// Snapshot déjà connu ou obsolète -> on ignore (idempotence).
	if args.LastIncludedIndex <= n.lastIncludedIndex {
		reply.Term = n.currentTerm
		return nil
	}

	var kv map[string]string
	if err := json.Unmarshal(args.Data, &kv); err != nil {
		return nil
	}

	n.kv = kv
	n.log = []LogEntry{{Term: args.LastIncludedTerm}}
	n.lastIncludedIndex = args.LastIncludedIndex
	n.lastIncludedTerm = args.LastIncludedTerm

	if n.commitIndex < args.LastIncludedIndex {
		n.commitIndex = args.LastIncludedIndex
	}

	if n.lastApplied < args.LastIncludedIndex {
		n.lastApplied = args.LastIncludedIndex
	}

	log.Printf("[%s] snapshot installé depuis %s jusqu'à l'index %d\n", n.id, args.LeaderID, args.LastIncludedIndex)

	reply.Term = n.currentTerm
	return nil
}

func (n *Node) Status() (id string, state State, term, logLen, commitIndex int, snapshotIndex int, leaderID string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.id, n.state, n.currentTerm, n.lastLogIndexLocked(), n.commitIndex, n.lastIncludedIndex, n.leaderID
}

func (n *Node) Get(key string) (string, bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	v, ok := n.kv[key]
	return v, ok
}
