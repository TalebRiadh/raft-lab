# raft-lab

A minimal [Raft](https://raft.github.io/) consensus implementation written in Go. This is a learning-oriented lab project demonstrating the core Raft mechanics: leader election, term management, and heartbeat-based log-less replication.

## Status

Currently implements the **leader election** portion of Raft only:

- Follower / Candidate / Leader state transitions
- Randomized election timeouts (150–300 ms)
- Term management and vote granting rules
- Heartbeat (`AppendEntries`) to prevent elections and detect higher terms
- Step-down to Follower when a higher term is observed

Not yet implemented: log replication, log compaction/snapshots, membership changes, persistence.

## Project structure

```
raft-lab/
├── cmd/node/          # Executable entry point
│   └── main.go        # Flag parsing and node bootstrap
├── raft/              # Raft library package
│   ├── node.go        # Election logic, heartbeats, RPC handlers
│   ├── server.go      # HTTP-RPC server bootstrap
│   └── types.go       # State, Node struct, RPC message types
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

## How it works

- Each node starts as a **Follower** and waits a randomized timeout (150–300 ms).
- On timeout, it becomes a **Candidate**, increments its term, votes for itself, and requests votes from all peers.
- A Candidate becomes **Leader** upon receiving votes from a majority of the cluster.
- The Leader sends periodic heartbeats (`AppendEntries`); any node seeing a higher term steps down to Follower.
- Nodes communicate over Go's `net/rpc` via HTTP on the configured ports.

## Communication protocol

| Method               | Args                                      | Reply                                     |
| -------------------- | ----------------------------------------- | ----------------------------------------- |
| `RPCService.RequestVote`    | `RequestVoteArgs{Term, candidateID}`      | `RequestVoteReply{Term, VoteGranted}`     |
| `RPCService.AppendEntries`  | `AppendEntriesArgs{Term, LeaderID}`       | `AppendEntriesReply{Term, Success}`       |

## License

None specified.