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

type Node struct {
	mu sync.Mutex

	id    string
	peers []string //addresses host:port of other nodes

	// persistant state
	currentTerm int
	votedFor    string // "" if no vote is cast during this term

	// volatile state
	state State

	// management of election timing
	electionResetAt time.Time
	electionTimeout time.Duration

	stopCh chan struct{}
}

type RequestVoteArgs struct {
	Term        int
	CandidateID string
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term     int
	LeaderID string
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}
