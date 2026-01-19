package Replication


type Role int

const (
	Follower Role = iota
	Leader
)

struct RaftNode {
	ID        string
	Term      int
	VotedFor  string
	Log       []LogEntry
	CommitIdx int
	LastApplied int
	role        Role
}

struct LogEntry {
	Term    int
	Command string
}