# raft-lab

A minimal [Raft](https://raft.github.io/) consensus implementation written in Go. This is a learning-oriented lab project demonstrating the core Raft mechanics: leader election, log replication, commit advancement, and a replicated key-value store built on top of the replicated log.

## Status

Implemented:

- Follower / Candidate / Leader state transitions
- Randomized election timeouts (150–300 ms)
- Term management and vote granting rules (including log up-to-date checks)
- Heartbeat (`AppendEntries`) to prevent elections and detect higher terms
- Step-down to Follower when a higher term is observed
- **Log replication**: leader appends client commands to its log and replicates them to followers
- Log matching property enforcement, conflict truncation, and `nextIndex` backoff
- Commit index advancement via majority (`matchIndex`) and per-term commit rules
- Application of committed entries to an in-memory KV store (`SET` / `DEL`)
- Log compaction via snapshots (`InstallSnapshot`) when a follower falls behind
- HTTP API for submitting commands, reading values, and inspecting node status

Not yet implemented: membership changes, persistence.

## Project structure

```
raft-lab/
├── cmd/node/          # Executable entry point
│   └── main.go        # Flag parsing and node bootstrap
├── raft/              # Raft library package
│   ├── node.go        # Election, replication, commit/apply logic, RPC handlers
│   ├── server.go      # HTTP API + RPC server bootstrap
│   └── types.go       # State, Node struct, RPC message types, log entries
└── go.mod
```

## Requirements

- Go 1.22.2+

## Usage

Build and run each node on a separate port. Every node must know the addresses of all other nodes.

```bash
# Node 1
go run ./cmd/node -id=node1 -port=8001 -peers=localhost:8002,localhost:8003

# Node 2 (separate terminal)
go run ./cmd/node -id=node2 -port=8002 -peers=localhost:8001,localhost:8003

# Node 3 (separate terminal)
go run ./cmd/node -id=node3 -port=8003 -peers=localhost:8001,localhost:8002
```

### Flags

| Flag     | Description                                        | Required |
| -------- | -------------------------------------------------- | -------- |
| `-id`    | Unique node identifier (e.g., `node1`)             | yes      |
| `-port`  | Listening port (e.g., `8001`)                      | yes      |
| `-peers` | Comma-separated addresses of the other nodes       | no       |

With 3 nodes running, one will log that it becomes `LEADER` shortly after startup (assuming a majority is reachable). Kill the leader to observe a re-election among the survivors.

## HTTP API

Each node exposes an HTTP server (on the same port) with the following endpoints:

| Endpoint     | Method | Description                                                                 |
| ------------ | ------ | --------------------------------------------------------------------------- |
| `/command`   | POST   | Submit a command to the log. JSON body: `{"op": "SET" \| "DEL", "key": ..., "value": ...}`. Returns `{"accepted", "index", "term"}` on the leader, or `421` with `{"error": "not leader", "known_leader": ...}` otherwise. |
| `/get?key=k` | GET    | Read a key from the local applied state (may be briefly stale on followers). Returns `{"key", "value", "found"}`. |
| `/status`    | GET    | Node status: `{"id", "state", "term", "log_length", "commit_index", "snapshot_index", "known_leader"}`. |

```bash
# Submit a command to the leader
curl -X POST localhost:8001/command -d '{"op":"SET","key":"foo","value":"bar"}'

# Read it back from any node once committed
curl 'localhost:8002/get?key=foo'

# Inspect a node
curl localhost:8001/status
```

## How it works

- Each node starts as a **Follower** and waits a randomized timeout (150–300 ms).
- On timeout, it becomes a **Candidate**, increments its term, votes for itself, and requests votes from all peers.
- A Candidate becomes **Leader** upon receiving votes from a majority of the cluster (votes are only granted if the candidate's log is at least as up to date).
- The Leader sends periodic heartbeats (`AppendEntries`); any node seeing a higher term steps down to Follower.
- Commands submitted via `/command` are appended to the leader's log and replicated to followers on each heartbeat tick.
- Followers truncate conflicting entries and reply with the log-matching check result; on failure the leader decrements `nextIndex` and retries.
- The leader advances its `commitIndex` when an entry is replicated to a majority in its own term, then applies committed entries to the KV state machine.
- When the committed log grows past a small compaction threshold (5 entries here for easy observation; thousands in production), a node replaces the committed prefix with an in-memory snapshot of its KV state, tracked by `lastIncludedIndex` / `lastIncludedTerm`.
- A follower whose `nextIndex` falls at or below the leader's snapshot point receives that snapshot via `InstallSnapshot`, then resumes normal log replication.
- Outbound RPCs are time-bounded (100 ms), so an unresponsive peer can never stall elections or replication; replication to each peer is serialized across heartbeat ticks.
- Nodes communicate over Go's `net/rpc` via HTTP on the configured ports.

## Communication protocol

| Method               | Args                                                                         | Reply                                    |
| -------------------- | ---------------------------------------------------------------------------- | ---------------------------------------- |
| `RPCService.RequestVote`    | `RequestVoteArgs{Term, CandidateID, LastLogIndex, LastLogTerm}`       | `RequestVoteReply{Term, VoteGranted}`    |
| `RPCService.AppendEntries`  | `AppendEntriesArgs{Term, LeaderID, PrevLogIndex, PrevLogTerm, Entries, LeaderCommit}` | `AppendEntriesReply{Term, Success}` |
| `RPCService.InstallSnapshot` | `InstallSnapshotArgs{Term, LeaderID, LastIncludedIndex, LastIncludedTerm, Data}` | `InstallSnapshotReply{Term}`        |

## License

None specified.