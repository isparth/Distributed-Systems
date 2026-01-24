package types

type NodeID string

//enum of read policy
//Stale is the follower node gives the data it has
//Readindex colsults the leader before goving the data
type ReadPolicy int
const (
	ReadPolicyStale ReadPolicy = iota
	ReadPolicyReadIndex
)

//enum of write mode

//Sync: wait for the write to be applied
//Async: return immediately after the write is appended to the log
type WriteMode int
const (
	WriteModeSync WriteMode = iota
	WriteModeAsync
)

//Enums for Write operations
type OpType int
const (
	OpPut OpType = iota
	OpDelete
	OpCAS
	OpBatchPut
	OpBatchDelete
)

type Entry struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type Command struct {
	ClientID string `json:"client_id"`
	Seq      uint64 `json:"seq"`
	Op       OpType `json:"op"`

	// Single-key ops
	Key      string `json:"key,omitempty"`
	Value    string `json:"value,omitempty"`
	Expected string `json:"expected,omitempty"`

	// Batch ops
	Entries []Entry  `json:"entries,omitempty"`
	Keys    []string `json:"keys,omitempty"`
}

type ApplyResult struct {
	Ok      bool              `json:"ok,omitempty"`
	Value   string            `json:"value,omitempty"`
	Values  map[string]string `json:"values,omitempty"`
	Deleted int               `json:"deleted,omitempty"`

	// Optional error signaling for clients (still JSON)
	ErrCode string `json:"err_code,omitempty"`
	ErrMsg  string `json:"err_msg,omitempty"`
}

//information about the leader
type LeaderHint struct {
	LeaderID   NodeID `json:"leader_id,omitempty"`
	LeaderAddr string `json:"leader_addr,omitempty"`
}

// Used by async write mode
type OpID string

type OpResult struct {
	OpID   OpID        `json:"op_id"`
	Result *ApplyResult `json:"result,omitempty"`
	Ready  bool        `json:"ready"`
}