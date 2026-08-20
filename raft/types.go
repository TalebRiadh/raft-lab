package raft

import (
	"sync"
	"time"
)

type State int

const (
	Follower State = iota
	Candidate
	Leader
)

func (s State) String() string {
	switch s {
	case Follower:
		return "FOLLOWER"
	case Candidate:
		return "CANDIDATE"
	case Leader:
		return "LEADER"
	default:
		return "UNKNOWN"
	}
}

type LogEntry struct {
	Term  int
	Op    string // "SET" or "DEL"
	Key   string
	Value string
}

type Node struct {
	mu sync.Mutex

	id    string
	peers []string //addresses host:port of other nodes

	// persistant state
	currentTerm       int
	votedFor          string // "" if no vote is cast during this term
	log               []LogEntry
	lastIncludedIndex int // index of the last entry summarized in the snapshot
	lastIncludedTerm  int // term of this entry

	// volatile state on all nodes
	state       State
	commitIndex int
	lastApplied int
	leaderID    string // redirect client to the right node

	// volatile state only for the leader
	nextIndex   map[string]int
	matchIndex  map[string]int
	replicating map[string]bool // per-peer in-flight replication guard

	// machine à états applicative
	kv map[string]string

	// management of election timing
	electionResetAt time.Time
	electionTimeout time.Duration

	stopCh chan struct{}
}

type RequestVoteArgs struct {
	Term         int
	CandidateID  string
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderID     string
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

// InstallSnapshotArgs RPC: InstallSnapshot (catches up with a follower who has fallen too far behind)
type InstallSnapshotArgs struct {
	Term              int
	LeaderID          string
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte // serialized state machine (JSON from the KV store)
}

type InstallSnapshotReply struct {
	Term int
}
