Project: Distributed KV Store with Raft
(Milestones 0–5)
Repo structure (fixed)
cmd/kvserver/main.go
internal/server/run.go
internal/types/types.go
internal/cluster/cluster.go
internal/httpapi/httpapi.go
internal/distributedkv/distributedkv.go // recommended (single file)
internal/kvsm/kvsm.go
internal/opcache/opcache.go
internal/raft/raft.go
internal/raft/storage/storage.go
internal/raft/transporthttp/transporthttp.go
Global conventions (apply to all
milestones)
Data model conventions
●
●
Client dedupe key: (ClientID, Seq) is unique per client operation.
Determinism: KVSM Apply must be deterministic: no time, no randomness, no external
calls.
●
●
Indices: Raft log indices are 1-based (index 0 is a sentinel “no entry”).
Thread safety: All stateful components must be safe under concurrent access:
○
KVSM guarded by mu
○
○
Raft node guarded by mu + dedicated goroutines (applier, timers, replication
workers)
Storage must be thread-safe for tests + concurrent Raft loops.
Error conventions
●
●
types.ApplyResult is client-facing: use ErrCode/ErrMsg for semantic errors
(cas_failed, bad_request, etc.)
Non-leader writes: HTTP should return redirect with leader hint (later: in elections it may
be empty).
Test conventions
●
●
Unit tests should not use real network ports unless necessary.
Transport tests should use httptest.Server.
●
Cluster integration tests should start N nodes on random ports and cleanly stop.
Milestone 0 — Single-node KV state
machine + HTTP API (no Raft)
Goal
Run a single node KV server that supports:
●
Put/Delete/CAS
●
BatchPut/BatchDelete
●
Dedupe by (client_id, seq)
●
Snapshot/Restore of KV and dedupe table
●
HTTP API for these operations
internal/types/types.go
Structs/enums to define
Cluster identity
●
type NodeID string
Policies
●
type ReadPolicy int with:
○
ReadPolicyStale
○
ReadPolicyReadIndex
●
type WriteMode int with:
○
WriteModeSync
○
WriteModeAsync
Operations
●
type OpType int with:
○
OpPut, OpDelete, OpCAS, OpBatchPut, OpBatchDelete
Command
●
type Entry struct { Key, Value string }
●
type Command struct { ClientID string; Seq uint64; Op OpType;
Key, Value, Expected string; Entries []Entry; Keys []string }
Results
●
type ApplyResult struct { Ok bool; Value string; Values
map[string]string; Deleted int; ErrCode string; ErrMsg string }
●
type LeaderHint struct { LeaderID NodeID; LeaderAddr string }
Async
●
type OpID string
●
type OpResult struct { OpID OpID; Result *ApplyResult; Ready bool
}
Required methods
●
None required, but useful:
○
String() on policies/op types
○
Command.ValidateBasic() (optional but helpful for HTTP validation)
Acceptance criteria
●
Code compiles, JSON tags correct, types reusable for later milestones.
internal/kvsm/kvsm.go
Structs
●
KVStateMachine
○
fields:
■
mu sync.Mutex
■
kv map[string]string
■ dedupe map[string]DedupeRecord (key: client_id)
●
DedupeRecord
○
LastSeq uint64
○
LastReply types.ApplyResult
●
Snapshot
○
KV map[string]string
○
Dedupe map[string]DedupeRecord
Methods (must implement)
●
func New() *KVStateMachine
●
func (sm *KVStateMachine) Apply(cmd types.Command)
types.ApplyResult
●
func (sm *KVStateMachine) Get(key string) (string, bool)
●
func (sm *KVStateMachine) MGet(keys []string) map[string]string
●
●
func (sm *KVStateMachine) Snapshot() ([]byte, error) (JSON)
func (sm *KVStateMachine) Restore([]byte) error
●
func (sm *KVStateMachine) LastSeen(clientID string) (uint64,
bool)
Apply semantics (define explicitly)
Deduplication rule
●
If cmd.ClientID !=
○
○
"" and cmd.Seq != 0:
If dedupe[ClientID].LastSeq >= cmd.Seq, return stored LastReply
exactly (no state change).
Else apply normally, store {LastSeq=cmd.Seq, LastReply=result}.
Operations
●
Put: set kv[key]=value, return {Ok:true}
●
Delete: delete if present; return {Ok:true, Deleted:1} if existed else {Ok:true,
Deleted:0}
●
CAS: compare current value (missing key counts as "") with Expected
○
if match: set value, {Ok:true}
○
else: {Ok:false, ErrCode:"cas_failed"}
●
BatchPut: set all entries
●
BatchDelete: delete keys, return Deleted count
Validation behavior
●
If required fields missing: {Ok:false, ErrCode:"bad_request"
,
ErrMsg:"
...
"}
KVSM tests (must implement)
●
TestKVSM_PutGetDelete
●
TestKVSM_CAS_SuccessAndFail
●
TestKVSM_BatchPutBatchDelete
●
TestKVSM_Dedupe_ReplaySameSeqReturnsSameReply
●
TestKVSM_SnapshotRestore_RoundTrip
Flexibility requirements
●
Snapshot format stable JSON.
●
Apply must be deterministic and thread-safe.
●
Avoid returning internal map references (clone when needed).
internal/httpapi/httpapi.go
API endpoints (Milestone 0)
Keep these stable across milestones:
●
GET /healthz
●
GET /kv/{key}
●
PUT /kv/{key} body includes client_id, seq, value
●
DELETE /kv/{key} body includes client_id, seq
●
POST /kv/mget body includes keys
●
POST /kv/mput body includes client_id, seq, entries
●
POST /kv/mdelete body includes client_id, seq, keys
Structs
●
type Server struct { sm *kvsm.KVStateMachine } (later: replace with DKV)
Methods
●
New(sm *kvsm.KVStateMachine) *Server
●
Handler() http.Handler (mux router)
●
Handlers:
○
Healthz
○
GetKey
○
PutKey
○
DeleteKey
○
MGet
○
MPut
○
MDelete
Error mapping rules
●
Validation errors → HTTP 400, JSON body includes ApplyResult or equivalent error
payload.
●
Missing key on GET → HTTP 404 (or 200 with ok=false; choose one and keep
consistent). Recommend:
○
404 with {err_code:"not_found"}.
●
Successful writes → 200 with {ok:true}
HTTP API tests
●
TestHTTPAPI_Healthz
●
TestHTTPAPI_PutGetDelete
●
TestHTTPAPI_BatchEndpoints
Flexibility rules
●
Centralize JSON encode/decode functions:
○
decodeJSON(r, &dst)
○
writeJSON(w, status, payload)
○
writeError(w, status, code, msg, leaderHint?)
internal/server/run.go + cmd/kvserver/main.go
Required behavior
●
Parse config (ports, node id)
●
Create KVSM
●
Create HTTP API server with KVSM
●
Listen and serve
●
On SIGINT/SIGTERM, shut down cleanly
Minimal tests
●
Optional: TestServer_Run_StartStop using context cancellation (can skip for
Milestone 0)
Milestone 1 — Fixed leader replication (no
failures)
Goal
Start N nodes with static membership and a fixed leader:
●
Writes go to leader → append to log → replicate → commit → apply to KVSM on all
nodes
●
Followers redirect client writes to leader
●
Reads remain stale (local KVSM)
internal/raft/storage/storage.go
Interfaces
●
StableStore
○
GetCurrentTerm() (uint64, error)
○
SetCurrentTerm(uint64) error
○
GetVotedFor() (types.NodeID, bool, error)
○
SetVotedFor(types.NodeID) error
●
LogStore
○
LastIndex() (uint64, error)
○
TermAt(index uint64) (uint64, error)
○
Append(entries []LogEntry) error
○
ReadRange(lo, hi uint64) ([]LogEntry, error)
○
DeleteFrom(index uint64) error (must exist; correctness required by
●
M3)
○
SnapshotStore
TruncatePrefix(upto uint64) error (no-op until M5)
○
○
Save(meta SnapshotMeta, data []byte) error (can be stub)
Load() (meta SnapshotMeta, data []byte, ok bool, err error)
(can be stub)
Types
●
LogEntry{Index uint64, Term uint64, Cmd types.Command}
●
SnapshotMeta{LastIncludedIndex, LastIncludedTerm}
Memory implementations
●
MemStableStore (mutex protected)
●
MemLogStore (slice with dummy index 0, mutex protected)
●
MemSnapshotStore (store latest snapshot in memory; ok flag)
Tests
●
TestMemLogStore_AppendReadRangeTermAt
●
TestMemLogStore_DeleteFrom
●
TestMemStableStore_TermVote
Flexibility rules
●
●
●
Storage must be thread-safe.
Log store must not expose internal slices.
Keep storage independent of raft internals (only uses LogEntry).
internal/raft/transporthttp/transporthttp.go
Goal
Implement Raft RPC over HTTP (only AppendEntries needed in M1).
Structs
●
RPC DTOs:
○
AppendEntriesRequest{Term, LeaderID, LeaderAddr,
PrevLogIndex, PrevLogTerm, Entries []LogEntry, LeaderCommit}
○
AppendEntriesResponse{Term, Success bool, ConflictIndex?,
ConflictTerm?} (optional for later)
●
PeerResolver (maps NodeID -> addr)
●
HTTPTransport (client)
●
RaftHTTPServer (server mux)
●
RaftRPCHandler interface:
○
HandleAppendEntries(ctx, req) (resp, error)
Methods
Client:
●
AppendEntries(ctx, to, req)
Server:
●
route POST /raft/append_entries:
○
decode JSON
○
call handler
○
encode response JSON
○
bad JSON → 400
Tests
●
TestTransportHTTP_AppendEntries_RoundTrip
●
TestTransportHTTP_BadJSON_Returns400
Flexibility rules
●
No raft logic in transport layer.
●
Keep routes stable: /raft/...
internal/raft/raft.go (Milestone 1 subset)
Node fields (minimum + future-proof)
●
cfg Config
●
stable StableStore
●
log LogStore
●
snap SnapshotStore
●
tp transport (interface—could be in transporthttp file)
●
sm *kvsm.KVStateMachine
State:
●
mu sync.Mutex
●
role string (leader/follower; candidate added M2)
●
currentTerm uint64
●
leaderHint types.LeaderHint
●
commitIndex uint64
●
lastApplied uint64
Leader replication state (initialize now):
●
matchIndex map[NodeID]uint64
●
nextIndex map[NodeID]uint64 (used in M3)
Goroutines:
●
applierDone chan struct{}
●
ctx cancel logic
Methods (must implement in M1)
●
NewNode(cfg, stable, log, snap, transport, sm) (*Node, error)
●
●
Start(ctx) error:
○
○
start applier loop
if leader, optionally start heartbeat loop (can be disabled M1)
Stop(ctx) error
●
IsLeader() bool
●
LeaderHint() types.LeaderHint
●
●
Status() Status (expose fields used for debugging)
Propose(ctx, cmd) (types.ApplyResult, error):
○
Only valid if leader; else return error (HTTP will redirect)
○
Append log entry locally at lastIndex+1
○
Send AppendEntries to followers with:
■ PrevLogIndex/Term
■ Entries payload
■ LeaderCommit = current commitIndex
○
○
○
Wait for majority success
Advance commitIndex to the new entry index
Wait until applied (optional for M1; recommended to return after apply)
RPC handler:
●
HandleAppendEntries(ctx, req) (resp, error):
○
○
○
○
follower appends entries sequentially (no conflicts assumed in M1)
update leaderHint from req
update commitIndex = min(req.LeaderCommit, lastLogIndex)
respond success
Applier loop:
●
while running:
○
if lastApplied < commitIndex:
■ read log entries (lastApplied+1 .. commitIndex)
■ apply each to sm.Apply(cmd)
■ increment lastApplied
Tests
Unit-ish:
●
TestRaft_M1_LeaderPropose_AppendsAndCommits
●
TestRaft_M1_FollowerHandleAppendEntries_AppendsAndApplies
Integration:
●
TestCluster_M1_3Nodes_WriteToLeader_ReplicatesToFollowers
●
TestCluster_M1_WriteToFollower_ReturnsRedirect
Flexibility rules
●
Log entries should always have Term even in M1.
●
nextIndex and matchIndex should exist even if simplified.
internal/distributedkv/distributedkv.go
(Milestone 1 behavior)
Structs
●
Config{ReadPolicy, WriteMode, AsyncResultTTLSeconds}
●
DistributedKV{ node RaftNodeIface, sm *kvsm.KVStateMachine, cache
*opcache.Cache, cfg Config }
Where RaftNodeIface includes:
●
Propose(ctx, cmd) (ApplyResult, error)
●
IsLeader() bool
●
LeaderHint() LeaderHint
Methods
●
IsLeader() bool
●
LeaderHint() LeaderHint
Writes (sync only):
●
Put/Delete/CAS/MPut/MDelete:
○
set cmd.Op appropriately
○
call node.Propose(ctx, cmd)
Reads (stale):
●
Get(key) uses sm.Get
●
MGet(keys) uses sm.MGet
Tests
●
TestDKV_M1_WritesCallPropose
●
TestDKV_M1_ReadsAreStaleFromSM
Flexibility
●
DistributedKV depends on a small interface, not concrete *raft.Node (easy testing +
future refactor).
internal/httpapi/httpapi.go (Milestone 1 changes)
●
Replace sm *kvsm.KVStateMachine with dkv
*distributedkv.DistributedKV.
●
Write endpoints:
○
if !dkv.IsLeader() → return 307 with JSON body containing LeaderHint
●
Read endpoints still serve locally (stale) via dkv
Tests:
●
Update M0 HTTP tests to cover redirect:
○
write to follower returns 307 + leader hint
○
write to leader returns 200
Milestone 2 — Elections (leader can die)
Goal
If leader crashes, remaining nodes elect a new leader and serve writes.
Transport
●
Add RequestVote DTOs + endpoint:
○
POST /raft/request_vote
○
client method RequestVote
Test:
●
TestTransportHTTP_RequestVote_RoundTrip
Raft changes (raft.go)
Add:
●
candidate role
●
election timer with randomized timeout
●
persistent voting via StableStore:
○
on term increment: set currentTerm and clear votedFor in raft logic
●
HandleRequestVote
●
extend HandleAppendEntries to step down on higher term
●
leader heartbeat loop (AppendEntries with zero entries)
Tests:
●
TestRaft_M2_Election_HappensAfterLeaderStops
●
TestRaft_M2_HigherTermForcesStepDown
●
Integration:
○
TestCluster_M2_LeaderCrash_NewLeaderElected
○
TestCluster_M2_WriteAfterFailover_Succeeds
Flexibility:
●
All timing configurable and fast in tests.
●
Inject deterministic randomness.
Milestone 3 — Log correctness (conflicts
+ repair)
Goal
Correct Raft log matching:
●
follower rejects mismatching prev log
●
leader repairs follower logs with nextIndex backtracking
●
commit rule safety (current term only)
Storage:
●
DeleteFrom must be correct
●
TermAt must error on missing
Raft:
●
●
●
Implement full AppendEntries consistency checks
Implement nextIndex/matchIndex update and backtracking
Implement commit advancement rule (current term)
Tests:
●
TestMemLogStore_DeleteFrom_TruncatesSuffix
●
TestMemLogStore_TermAt_ErrorsOnMissing
●
TestRaft_M3_FollowerRejectsOnPrevMismatch
●
TestRaft_M3_LeaderBacktracksNextIndexAndRepairsFollower
●
Integration:
○
TestRaft_M3_CommittedEntriesNotLostAfterLeaderChange
Flexibility:
●
Keep replication in helper functions (replicateToPeer) even within same file.
Milestone 4 — ReadIndex reads
Goal
Configurable reads:
●
stale: local SM
●
readindex: ensure applied through leader’s committed index
Raft:
●
GetReadIndex(ctx) and WaitApplied(ctx, index)
●
simplest approach: leader-local read barrier + “recent quorum heartbeat” check
DistributedKV:
●
if ReadPolicyReadIndex:
○
idx := node.GetReadIndex()
○
node.WaitApplied(idx)
○
read from sm
Tests:
●
TestRaft_M4_WaitAppliedBlocksUntilApplied
●
TestDKV_M4_ReadPolicySwitch
●
Integration:
○
TestDKV_M4_ReadIndex_ReadWaitsForCommitAndApply
Flexibility:
●
HTTP should not know how ReadIndex works; it just calls dkv.Get/MGet.
Milestone 5 — Snapshots +
InstallSnapshot + compaction
Goal
Bounded logs and efficient catch-up.
KVSM:
●
●
snapshot must include kv + dedupe
restore overwrites both safely
Storage:
●
●
implement snapshot store Save/Load for real
implement TruncatePrefix for log (or implement log base index offset)
Transport:
●
add InstallSnapshot RPC + base64 encoding
Raft:
●
MaybeSnapshot + CreateSnapshot(lastIncludedIndex)
●
follower HandleInstallSnapshot
●
●
leader detects lagging follower and sends snapshot
restart path loads snapshot and restores SM before applying remaining log tail
Tests:
●
TestKVSM_M5_SnapshotIncludesDedupe
●
TestKVSM_M5_RestoreResetsStateCorrectly
●
TestSnapshotStore_SaveLoad
●
TestLogStore_TruncatePrefix
●
TestTransportHTTP_InstallSnapshot_Base64RoundTrip
●
TestRaft_M5_CreateSnapshot_TruncatesLogPrefix
●
TestRaft_M5_FollowerInstallSnapshot_RestoresState
●
Integration:
○
TestCluster_M5_LaggingFollowerCatchesUpViaSnapshot
Flexibility:
●
●
Snapshot bytes owned by KVSM; raft treats as opaque.
Snapshot metadata stored separately and drives log truncation.
Lightweight integration test harness
(recommended)
Implement in internal/server/run_test.go or a dedicated *
_test.go file.
Capabilities:
●
●
●
start N nodes on random ports
stop node i
restart node i
●
●
helper: waitForLeader(nodes) (poll status endpoint or raft.Status)
helper client: put/get/mput/mget against a specific node
Keep it minimal—just enough to run cluster tests.
Final flexibility rules (strict)
1. Raft core depends only on:
○
storage interfaces
○
transport interface (even if defined in transporthttp.go)
○
kvsm.Apply/Snapshot/Restore
2. HTTP API depends only on DistributedKV (never call raft directly).
3. KVSM is deterministic and snapshots are stable JSON.
4. Config toggles control features:
○
ReadPolicy selects stale vs readindex
○
SnapshotThresholdEntries=0 disables snapshots cleanly
5. Use small interfaces for testability:
○
DistributedKV depends on RaftNodeIface
○
Raft depends on TransportIface