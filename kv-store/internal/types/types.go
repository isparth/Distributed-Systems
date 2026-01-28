package types

// NodeID identifies a node in the cluster.
type NodeID string

// ReadPolicy controls how reads are served.
type ReadPolicy int

const (
	ReadPolicyStale     ReadPolicy = iota
	ReadPolicyReadIndex
)

func (r ReadPolicy) String() string {
	switch r {
	case ReadPolicyStale:
		return "stale"
	case ReadPolicyReadIndex:
		return "read_index"
	default:
		return "unknown"
	}
}

// WriteMode controls how writes are acknowledged.
type WriteMode int

const (
	WriteModeSync  WriteMode = iota
	WriteModeAsync
)

func (w WriteMode) String() string {
	switch w {
	case WriteModeSync:
		return "sync"
	case WriteModeAsync:
		return "async"
	default:
		return "unknown"
	}
}

// OpType identifies the operation type.
type OpType int

const (
	OpPut OpType = iota
	OpDelete
	OpCAS
	OpBatchPut
	OpBatchDelete
)

func (o OpType) String() string {
	switch o {
	case OpPut:
		return "put"
	case OpDelete:
		return "delete"
	case OpCAS:
		return "cas"
	case OpBatchPut:
		return "batch_put"
	case OpBatchDelete:
		return "batch_delete"
	default:
		return "unknown"
	}
}

// Entry is a key-value pair used in batch operations.
type Entry struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// Command represents an operation to be applied to the state machine.
type Command struct {
	ClientID string  `json:"client_id"`
	Seq      uint64  `json:"seq"`
	Op       OpType  `json:"op"`
	Key      string  `json:"key,omitempty"`
	Value    string  `json:"value,omitempty"`
	Expected string  `json:"expected,omitempty"`
	Entries  []Entry `json:"entries,omitempty"`
	Keys     []string `json:"keys,omitempty"`
}

// ApplyResult is the result of applying a command.
type ApplyResult struct {
	Ok      bool              `json:"ok"`
	Value   string            `json:"value,omitempty"`
	Values  map[string]string `json:"values,omitempty"`
	Deleted int               `json:"deleted,omitempty"`
	ErrCode string            `json:"err_code,omitempty"`
	ErrMsg  string            `json:"err_msg,omitempty"`
}

// LeaderHint tells clients where the leader is.
type LeaderHint struct {
	LeaderID   NodeID `json:"leader_id,omitempty"`
	LeaderAddr string `json:"leader_addr,omitempty"`
}

// NodeStatus holds status info about a Raft node.
type NodeStatus struct {
	ID          NodeID     `json:"id"`
	Role        string     `json:"role"`
	Term        uint64     `json:"term"`
	CommitIndex uint64     `json:"commit_index"`
	LastApplied uint64     `json:"last_applied"`
	LastIndex   uint64     `json:"last_index"`
	LeaderHint  LeaderHint `json:"leader_hint"`
}

// OpID identifies an async operation.
type OpID string

// OpResult is the result of an async operation.
type OpResult struct {
	OpID   OpID         `json:"op_id"`
	Result *ApplyResult `json:"result,omitempty"`
	Ready  bool         `json:"ready"`
}
