package Replication

struct RaftNode {
	ID        string
	Term      int
	VotedFor  string
	Log       []LogEntry
	CommitIdx int
	LastApplied int
}

struct LogEntry {
	Term    int
	Command string
}