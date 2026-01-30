# HTTP API Reference

Complete documentation of the Distributed KV Store HTTP API.

## Base URL

```
http://localhost:8080
```

Default port is 8080. Use `-port` flag to change:
```bash
./kvserver -port 9000
```

## Global Behavior

### Client Deduplication

All write operations support deduplication by (ClientID, Seq):

```json
{
  "client_id": "unique-client-identifier",
  "seq": 1
}
```

- **client_id**: Unique identifier for the client (e.g., UUID, process ID)
- **seq**: Monotonically increasing sequence number per client

**Benefits**:
- Idempotent writes: retrying with same (client_id, seq) returns cached result
- Exactly-once semantics even with network retries
- No risk of double-applying operations

**Example**: Client sends Put with seq=1, crashes after 100ms, retries. Raft applies entry once; retry gets same result.

### Leader Redirection

Write operations require the leader node. If you write to a follower:

**Response**: HTTP 307 (Temporary Redirect)
```json
{
  "error": "not_leader",
  "leader_hint": {
    "leader_id": "node1",
    "leader_addr": "http://localhost:8080"
  }
}
```

**Recommended client behavior**:
1. Try write to any node
2. If 307 response, cache leader_addr
3. Retry write to leader
4. On subsequent writes, try leader first before trying others

### Error Response Format

Failed operations return:

```json
{
  "ok": false,
  "err_code": "error_code",
  "err_msg": "Human-readable error message"
}
```

**Common error codes**:
- `not_found`: Key doesn't exist (GET)
- `cas_failed`: CAS condition didn't match
- `bad_request`: Missing required fields
- `not_leader`: Write to follower (HTTP 307)

## Health & Status

### Health Check

```
GET /healthz
```

Returns node liveness status.

**Response**:
```json
{
  "status": "ok"
}
```

**Status**: 200 OK (always healthy if responding)

---

### Node Status

```
GET /status
```

Returns detailed node state for debugging and monitoring.

**Response**:
```json
{
  "id": "node1",
  "role": "leader",
  "term": 5,
  "commit_index": 10,
  "last_applied": 10,
  "last_index": 10,
  "leader_hint": {
    "leader_id": "node1",
    "leader_addr": "http://localhost:8080"
  }
}
```

**Fields**:
- `id`: Node ID (from -id flag)
- `role`: Current role: "leader", "follower", or "candidate"
- `term`: Current Raft term (increments on elections)
- `commit_index`: Highest log index known to be committed
- `last_applied`: Highest log index applied to state machine
- `last_index`: Highest log index stored locally
- `leader_hint`: Current leader (empty if unknown/during election)

**Use cases**:
- Check if node is leader before writing
- Monitor replication progress (last_applied vs commit_index)
- Track election activity (term increasing)
- Find leader address for redirection

---

## Key-Value Operations

### Get Single Key

```
GET /kv/{key}
```

Retrieves value for a single key. Behavior depends on the configured read policy.

**Path Parameters**:
- `key`: Key name (URL-encoded)

**Response** (success):
```json
{
  "ok": true,
  "value": "the-stored-value"
}
```

**Response** (not found):
```json
{
  "ok": false,
  "err_code": "not_found",
  "err_msg": "key not found"
}
```

**Response** (ReadIndex failed - M4):
```json
{
  "ok": false,
  "err_code": "read_index_failed",
  "err_msg": "not leader"
}
```

**Status**: 200 OK (success), 404 (not found), 503 (read index failed)

**Read Policies (M4)**:
- **Stale (default)**: Returns immediately from local state machine. Fast but may be stale.
- **ReadIndex**: Confirms leadership via quorum, waits for commit index to be applied, then reads. Linearizable but slower.

**Notes**:
- With stale policy: Returns potentially stale data (may lag behind leader)
- With ReadIndex policy: Returns fresh data but requires leader and quorum check
- Fast for stale reads; no Raft replication required
- Safe for read-heavy workloads with stale policy

**Example**:
```bash
curl http://localhost:8080/kv/username
```

---

### Get Multiple Keys

```
POST /kv/mget
```

Retrieves values for multiple keys in a single operation. Behavior depends on read policy.

**Request Body**:
```json
{
  "keys": ["key1", "key2", "key3"]
}
```

**Response**:
```json
{
  "ok": true,
  "values": {
    "key1": "value1",
    "key2": "value2",
    "key3": null
  }
}
```

**Response** (ReadIndex failed - M4):
```json
{
  "ok": false,
  "err_code": "read_index_failed",
  "err_msg": "not leader"
}
```

**Status**: 200 OK, 503 (read index failed with ReadIndex policy)

**Notes**:
- Missing keys are omitted from response
- With stale policy: Reads from local state machine (fast, potentially stale)
- With ReadIndex policy: Confirms leadership, waits for apply, then reads (consistent)
- Atomic from local state machine perspective

**Example**:
```bash
curl -X POST http://localhost:8080/kv/mget \
  -H "Content-Type: application/json" \
  -d '{"keys":["a","b","c"]}'
```

---

### Put Single Key

```
PUT /kv/{key}
```

Sets the value for a key. Only valid on the leader.

**Path Parameters**:
- `key`: Key name (URL-encoded)

**Request Body**:
```json
{
  "client_id": "unique-client-id",
  "seq": 1,
  "value": "new-value"
}
```

**Required Fields**:
- `value`: The value to store
- `client_id`: Unique client identifier (for deduplication)
- `seq`: Sequence number (for deduplication)

**Response** (success):
```json
{
  "ok": true
}
```

**Response** (not leader):
```json
{
  "error": "not_leader",
  "leader_hint": {
    "leader_id": "node1",
    "leader_addr": "http://localhost:8080"
  }
}
```

**Status**: 200 OK (success), 307 (redirect to leader)

**Semantics**:
- Replicated to majority of nodes before response
- Applied to state machine
- Deduplicated: retrying with same (client_id, seq) returns cached result
- Atomic: either fully applied or not at all

**Example**:
```bash
curl -X PUT http://localhost:8080/kv/username \
  -H "Content-Type: application/json" \
  -d '{"client_id":"cli1","seq":1,"value":"alice"}'
```

---

### Put Multiple Keys

```
POST /kv/mput
```

Sets multiple key-value pairs atomically. Only valid on the leader.

**Request Body**:
```json
{
  "client_id": "unique-client-id",
  "seq": 2,
  "entries": [
    {"key": "user:1:name", "value": "Alice"},
    {"key": "user:1:email", "value": "alice@example.com"},
    {"key": "user:1:status", "value": "active"}
  ]
}
```

**Required Fields**:
- `entries`: Array of {key, value} pairs
- `client_id`: Unique client identifier
- `seq`: Sequence number

**Response** (success):
```json
{
  "ok": true
}
```

**Response** (not leader):
```json
{
  "error": "not_leader",
  "leader_hint": {
    "leader_id": "node1",
    "leader_addr": "http://localhost:8080"
  }
}
```

**Status**: 200 OK (success), 307 (redirect to leader)

**Semantics**:
- All keys set atomically (all or nothing)
- Single Raft log entry
- Replicated and applied before response
- Deduplicated

**Example**:
```bash
curl -X POST http://localhost:8080/kv/mput \
  -H "Content-Type: application/json" \
  -d '{
    "client_id": "cli1",
    "seq": 2,
    "entries": [
      {"key": "a", "value": "1"},
      {"key": "b", "value": "2"}
    ]
  }'
```

---

### Delete Single Key

```
DELETE /kv/{key}
```

Deletes a key. Only valid on the leader.

**Path Parameters**:
- `key`: Key name (URL-encoded)

**Request Body**:
```json
{
  "client_id": "unique-client-id",
  "seq": 3
}
```

**Required Fields**:
- `client_id`: Unique client identifier (for deduplication)
- `seq`: Sequence number (for deduplication)

**Response** (success):
```json
{
  "ok": true,
  "deleted": 1
}
```

Or if key didn't exist:
```json
{
  "ok": true,
  "deleted": 0
}
```

**Response** (not leader):
```json
{
  "error": "not_leader",
  "leader_hint": {
    "leader_id": "node1",
    "leader_addr": "http://localhost:8080"
  }
}
```

**Status**: 200 OK (success), 307 (redirect to leader)

**Notes**:
- `deleted` field indicates if key existed
- Idempotent: deleting again returns deleted=0
- Replicated and atomic

**Example**:
```bash
curl -X DELETE http://localhost:8080/kv/username \
  -H "Content-Type: application/json" \
  -d '{"client_id":"cli1","seq":3}'
```

---

### Delete Multiple Keys

```
POST /kv/mdelete
```

Deletes multiple keys atomically. Only valid on the leader.

**Request Body**:
```json
{
  "client_id": "unique-client-id",
  "seq": 4,
  "keys": ["key1", "key2", "key3"]
}
```

**Required Fields**:
- `keys`: Array of key names
- `client_id`: Unique client identifier
- `seq`: Sequence number

**Response** (success):
```json
{
  "ok": true,
  "deleted": 2
}
```

**Status**: 200 OK (success), 307 (redirect to leader)

**Notes**:
- `deleted` field is count of keys that existed
- Atomic: all keys deleted together in single log entry
- Deduplicated by (client_id, seq)

**Example**:
```bash
curl -X POST http://localhost:8080/kv/mdelete \
  -H "Content-Type: application/json" \
  -d '{
    "client_id": "cli1",
    "seq": 4,
    "keys": ["a", "b", "c"]
  }'
```

---

### Compare-And-Set (CAS)

```
PUT /kv/{key}
```

Atomically set value only if current value matches expected. Only valid on the leader.

**Path Parameters**:
- `key`: Key name (URL-encoded)

**Request Body**:
```json
{
  "client_id": "unique-client-id",
  "seq": 5,
  "value": "new-value",
  "expected": "old-value"
}
```

**Required Fields**:
- `value`: New value to set if condition matches
- `expected`: Expected current value
- `client_id`: Unique client identifier
- `seq`: Sequence number

**Response** (success, condition matched):
```json
{
  "ok": true
}
```

**Response** (failure, condition didn't match):
```json
{
  "ok": false,
  "err_code": "cas_failed",
  "err_msg": "current value doesn't match expected"
}
```

**Response** (not leader):
```json
{
  "error": "not_leader",
  "leader_hint": {
    "leader_id": "node1",
    "leader_addr": "http://localhost:8080"
  }
}
```

**Status**: 200 OK (both success and cas_failed), 307 (redirect to leader)

**Semantics**:
- Atomic: read-compare-write is atomic in Raft log
- Safe for optimistic locking
- Returns ok=false (not error) if condition fails
- Deduplicated

**Use Cases**:
- Updating configuration with version check
- Optimistic locking without separate locks
- Preventing lost updates in concurrent scenarios

**Example**:
```bash
# Try to update balance only if current value is "100"
curl -X PUT http://localhost:8080/kv/balance \
  -H "Content-Type: application/json" \
  -d '{
    "client_id": "cli1",
    "seq": 5,
    "expected": "100",
    "value": "150"
  }'
```

---

## Monitoring & Debugging

### Check Node Leadership

```bash
curl http://localhost:8080/status | jq '.role'
```

Returns: `"leader"` or `"follower"` or `"candidate"`

### Monitor Replication Progress

```bash
curl http://localhost:8080/status | jq '.commit_index, .last_applied'
```

- If `commit_index == last_applied`: All committed entries are applied (healthy)
- If `commit_index > last_applied`: Entries waiting to be applied (normal temporarily)
- If `last_applied > commit_index`: Should never happen (bug)

### Find Current Leader

```bash
curl http://localhost:8080/status | jq '.leader_hint.leader_addr'
```

Returns: `"http://localhost:8080"` or `"http://localhost:8081"` etc.

---

## Error Handling Guide

### Network Errors

**Timeout**: Retry with exponential backoff. Consider:
- Node is temporarily overloaded
- Network is slow
- Replication is in progress

**Connection refused**: Try other nodes:
```bash
for node in node1 node2 node3; do
  curl http://localhost:808X/kv/key && break
done
```

### HTTP Errors

**307 Temporary Redirect**: Write operation on non-leader
- Use leader_hint to retry on correct node
- Cache leader address; it won't change until crash/election

**400 Bad Request**: Invalid request format
- Check request body JSON is valid
- Ensure required fields present (client_id, seq, value)
- Check key is URL-encoded if contains special chars

**404 Not Found**: Key doesn't exist on GET
- Normal; use err_code to distinguish from errors

### Application Errors

**cas_failed**: CAS condition didn't match
- Retry with fresh read to get current value
- Implement backoff to avoid busy loop

**bad_request**: Validation error
- Check err_msg for details
- Common: missing required fields, empty key

---

## Best Practices

### Client Implementation

```go
type Client struct {
    leaderAddr string
    clientID   string
    seq        uint64
    nodes      []string // fallback nodes
}

func (c *Client) Put(ctx context.Context, key, value string) error {
    c.seq++
    
    // Try leader first
    if err := c.putToNode(c.leaderAddr, key, value); err == nil {
        return nil
    }
    
    // Try other nodes to find leader
    for _, node := range c.nodes {
        err := c.putToNode(node, key, value)
        if err == nil {
            c.leaderAddr = node // cache new leader
            return nil
        }
        if isNotLeader(err) {
            leader := extractLeaderHint(err)
            c.leaderAddr = leader
            return c.putToNode(leader, key, value)
        }
    }
    return ErrNoLeader
}
```

### Retry Strategy

1. **Immediate retry** (idempotent): Same seq on failure = safe to retry
2. **Exponential backoff** (307): Wait increasing time before trying leader
3. **Circuit breaker** (repeated failures): Stop retrying after N failures

### Deduplication

Always use distinct (client_id, seq) pairs:

```go
// ✅ Good: each operation has unique seq
client.Put("key1", "val1") // seq=1
client.Put("key2", "val2") // seq=2
client.Put("key1", "val3") // seq=3

// ❌ Bad: seq never increments
client.Put("key1", "val1") // seq=0
client.Put("key2", "val2") // seq=0
client.Put("key3", "val3") // seq=0
```

---

## Examples

### Simple Counter Increment

```bash
# Read current value
curl http://localhost:8080/kv/counter

# Increment using CAS
curl -X PUT http://localhost:8080/kv/counter \
  -H "Content-Type: application/json" \
  -d '{
    "client_id": "app1",
    "seq": 1,
    "expected": "42",
    "value": "43"
  }'
```

### Batch User Profile Update

```bash
curl -X POST http://localhost:8080/kv/mput \
  -H "Content-Type: application/json" \
  -d '{
    "client_id": "app1",
    "seq": 2,
    "entries": [
      {"key": "user:123:name", "value": "Alice"},
      {"key": "user:123:email", "value": "alice@example.com"},
      {"key": "user:123:last_login", "value": "2024-01-29"}
    ]
  }'
```

### Finding Leader and Retrying

```bash
# Get status to find leader
STATUS=$(curl http://localhost:8080/status)
LEADER=$(echo $STATUS | jq -r '.leader_hint.leader_addr')

# Retry PUT on leader
curl -X PUT $LEADER/kv/key \
  -H "Content-Type: application/json" \
  -d '{"client_id":"app1","seq":3,"value":"data"}'
```

---

**Last Updated**: Milestone 4 (ReadIndex Reads)
