# Distributed KV Store with Raft Consensus

A production-oriented implementation of a distributed key-value store using the Raft consensus algorithm. This project implements a multi-milestone plan from basic single-node KV storage to a fully replicated, fault-tolerant distributed system.

## Current Status

**Milestone 4: ReadIndex Reads** ✅ Complete

The system supports:
- Distributed replication across multiple nodes
- Automatic leader election with fault tolerance
- Log consistency with conflict detection and repair
- Deterministic state machine with deduplication
- HTTP API for client operations
- Configurable read policies (stale vs consistent ReadIndex reads)

## Architecture

```
┌─────────────────────────────────────────────────┐
│          HTTP API Layer (port 8080+)            │
│    GET/PUT/DELETE /kv/{key} endpoints           │
│    GET /status, /healthz                        │
└────────────────┬────────────────────────────────┘
                 │
┌────────────────▼────────────────────────────────┐
│      DistributedKV Wrapper                      │
│   (leader detection, write redirect)            │
└────────────────┬────────────────────────────────┘
                 │
┌────────────────▼──────────────────────────────────┐
│         Raft Consensus Engine                     │
│  ┌──────────────────────────────────────────────┐ │
│  │ Log Management: append, replicate, apply    │ │
│  │ Leader Election: term voting, heartbeats    │ │
│  │ Commit Advancement: majority quorum rules   │ │
│  └──────────────────────────────────────────────┘ │
└────────────────┬──────────────────────────────────┘
                 │
┌────────────────┴──────────┬───────────────────────┐
│                           │                       │
▼                           ▼                       ▼
KV State Machine    Log Storage (in-memory)   HTTP Transport
• Put/Delete/CAS    • Entries indexed by term   • AppendEntries RPC
• Batch ops        • Conflict detection         • RequestVote RPC
• Deduplication    • DeleteFrom/TermAt          • JSON over HTTP
• Snapshots
```

## Milestones

### ✅ Milestone 0: Single-Node KV Store
- In-memory key-value store with deterministic operations
- Deduplication by (client_id, seq)
- Snapshot and restore functionality
- HTTP API for Put/Get/Delete operations

### ✅ Milestone 1: Fixed-Leader Replication
- Raft log replication from leader to followers
- Write proposals through leader node
- Follower log application and commit tracking
- Automatic follower-to-leader redirect on writes

### ✅ Milestone 2: Leader Election
- Automatic leader election when leader crashes
- Randomized election timeouts (150-300ms, configurable)
- Persistent voting state via StableStore
- Step-down on higher term
- Heartbeat-based leader confirmation

### ✅ Milestone 3: Log Correctness
- Full AppendEntries consistency checks
- Conflict detection with ConflictIndex/ConflictTerm hints
- Leader nextIndex backtracking for efficient log repair
- Committed entries safety (current-term-only commit rule)
- Storage layer validation (DeleteFrom, TermAt error handling)

### ✅ Milestone 4: ReadIndex Reads
- Configurable read policies (stale vs. consistent)
- ReadIndex quorum-based consistent reads with leadership confirmation
- WaitApplied blocking until index is applied to state machine
- Runtime-switchable read policy via SetReadPolicy()

### 🔮 Milestone 5: Snapshots & Compaction (Planned)
- Log snapshots for bounded storage
- InstallSnapshot RPC for efficient catch-up
- Log prefix truncation after snapshot

## Quick Start

### Build

```bash
cd kv-store
go build ./cmd/kvserver
```

### Run Single Node

```bash
./kvserver -id node1 -port 8080
```

Visit `http://localhost:8080/status` to check node health.

### Run 3-Node Cluster

Terminal 1:
```bash
./kvserver -id node1 -port 8080 \
  -peers "node2=http://localhost:8081,node3=http://localhost:8082"
```

Terminal 2:
```bash
./kvserver -id node2 -port 8081 \
  -peers "node1=http://localhost:8080,node3=http://localhost:8082"
```

Terminal 3:
```bash
./kvserver -id node3 -port 8082 \
  -peers "node1=http://localhost:8080,node2=http://localhost:8081"
```

### Test Operations

```bash
# Put a key
curl -X PUT http://localhost:8080/kv/mykey \
  -H "Content-Type: application/json" \
  -d '{"client_id":"cli1","seq":1,"value":"myvalue"}'

# Get a key
curl http://localhost:8080/kv/mykey

# Check node status
curl http://localhost:8080/status

# Batch operations
curl -X POST http://localhost:8080/kv/mput \
  -H "Content-Type: application/json" \
  -d '{"client_id":"cli1","seq":2,"entries":[{"key":"a","value":"1"},{"key":"b","value":"2"}]}'
```

## Design Highlights

### Deterministic State Machine
The KV state machine is fully deterministic:
- No time-based logic
- No external API calls
- Repeatable across restarts via snapshots
- Thread-safe with mutex protection

### Client Deduplication
Operations are deduplicated by (ClientID, Seq):
- Idempotent writes: retrying with same sequence returns cached result
- Prevents double-apply on network retries
- Stored in snapshot for durability

### Raft Safety Properties
- **Election Safety**: At most one leader per term
- **Leader Append-Only**: Leader never overwrites/deletes existing log entries
- **Log Matching**: If logs have entry at same index with same term, they're identical
- **Leader Completeness**: All committed entries are present on leader
- **State Machine Safety**: Only entries from leader's current term are committed

### Conflict Resolution (M3)
When follower log diverges from leader:
1. Follower returns ConflictIndex and ConflictTerm on AppendEntries failure
2. Leader uses hints to efficiently backtrack nextIndex (not just decrement)
3. Leader retries replication with corrected entries
4. Follower truncates conflicting entries and accepts correct ones

## Code Organization

```
internal/
├── types/              # Shared type definitions
├── kvsm/              # KV State Machine
├── raft/
│   ├── raft.go        # Core Raft consensus engine
│   ├── storage/       # Log, stable store, snapshot interfaces
│   └── transporthttp/ # RPC over HTTP
├── distributedkv/     # Raft + KVSM wrapper layer
├── httpapi/           # HTTP request handlers
└── server/            # Node startup and wiring
```

## Testing

```bash
# Run all tests
go test ./...

# Run with verbose output
go test ./... -v

# Run specific test
go test ./internal/raft -run TestRaft_M3_FollowerRejectsOnPrevMismatch

# Run with race detector
go test -race ./...

# Run multiple times for stability
go test -count=5 ./internal/raft
```

All tests pass across 6 core packages with 27+ test cases covering unit, integration, and cluster scenarios.

## API Documentation

See [API.md](./API.md) for complete HTTP API reference including:
- Request/response formats
- Error codes
- Client deduplication
- Leader redirection on writes

## Configuration

### Server Flags

| Flag | Default | Description |
|------|---------|-------------|
| `-port` | 8080 | HTTP listen port |
| `-id` | node1 | Unique node identifier |
| `-peers` | (none) | Comma-separated peer list |

### Timing Configuration
| Parameter | Default | Purpose |
|-----------|---------|---------|
| Election timeout | 150-300ms | Random delay before election starts |
| Heartbeat interval | 50ms | Leader sends heartbeats this frequently |

## Fault Tolerance

The system remains available during:
- **Single node failure**: Remaining N-1 nodes elect a new leader
- **Transient network partition**: Minority partition blocks, majority continues
- **Follower divergence**: Leader automatically repairs log via conflict detection

## Thread Safety

All components are thread-safe:
- KVSM: Protected by mutex
- Raft Node: Protected by mutex with dedicated goroutines
- Storage: Mutex-protected in-memory implementations
- HTTP handlers: Stateless and concurrent-safe

## Known Limitations

- In-memory storage: Data lost on restart (M5 will add snapshots)
- HTTP transport only: No TLS, no authentication
- Single-threaded apply: Could batch for higher throughput
- No member change: Cluster membership is static

## Read Policies (M4)

The system supports two read policies:

| Policy | Consistency | Latency | Use Case |
|--------|-------------|---------|----------|
| **Stale** (default) | Eventually consistent | Fast (local read) | Read-heavy workloads, caching |
| **ReadIndex** | Linearizable | Slower (quorum check) | Financial transactions, critical reads |

### How ReadIndex Works

1. Client calls `Get()` with `ReadPolicyReadIndex` configured
2. Leader confirms leadership via heartbeat quorum (majority response)
3. Leader returns its current `commitIndex` as the read index
4. Client waits until `lastApplied >= readIndex`
5. Read is served from local state machine (guaranteed fresh)

---

**Last Updated**: Milestone 4 (ReadIndex Reads)
