
# KV Store

Lightweight in-memory distributed key-value store with HTTP API. 


**Contents:**
- **Command:** `cmd/kvserve` — service entrypoint
- **HTTP API:** `internal/httpapi` — routes, handlers, middleware
- **Store:** `internal/kv` — in-memory model, snapshot persistence, tests
- **Snapshot file:** `snapshot.gob` — gob-encoded persisted snapshot used by the store

**Goals**
- Provide a simple, testable key-value HTTP service useful for distributed-systems experiments and demos.
- Persist store state to a local snapshot file to survive restarts. 

## Features

- Basic KV operations (get, put, delete) via HTTP
- Health endpoint
- Simple UI/static handler.
- Snapshot persistence (gob format) stored to `snapshot.gob`
- Unit tests for core store logic (`internal/kv/store_test.go`)
- Various Modes for Persistence. 

1) No persistence

2) Snapshot (Takes snapshot of the memory depending on the specified rules)

3) Strong WAL (synchonous WAL, writes the write operations to the WAL: This is fully persistent) 

4) Weak Wal (Asynchrous WAL writes the wirte operations asynchonously to the WAL).

## Project layout

- `cmd/kvserve/` — server main; starts HTTP server and wiring
- `internal/httpapi/` — routing, handlers, middleware, JSON responders
	- `handlers_kv.go` — key-value handlers
	- `handlers_health.go` — health endpoint
	- `handlers_ui.go` — UI/static handler
	- `routes.go`, `server.go`, `middleware.go` — router and server setup
- `internal/kv/` — core store implementation and errors
	- `model.go` — data model and types
	- `store.go` — store operations and snapshot persistence
	- `store_test.go` — unit tests

## Build & run

Build and run with the Go toolchain.

To run directly:

```bash
go run ./cmd/kvserve
```

To build a binary:

```bash
go build -o kvserve ./cmd/kvserve
./kvserve
```

If the binary supports flags (check `--help`), you can see available options:

```bash
go run ./cmd/kvserve --help
```

By default the server listens on a configurable address/port; check `cmd/kvserve` or `internal/httpapi/server.go` for exact defaults and flags.

## Configuration

This project uses simple local configuration. Common options you may find or add:

- `PORT` or `ADDR` — bind address for the HTTP server
- `SNAPSHOT_PATH` — path to the gob snapshot file (default: `snapshot.gob`)

If a config file or environment variables are available, they are read in the server bootstrap; review `cmd/kvserve/main.go`.

## HTTP API

The server exposes a small HTTP JSON API. The exact routes are implemented in `internal/httpapi/handlers_kv.go` and `routes.go`. Typical endpoints:

- Health check
	- `GET /health` — returns 200 and a JSON health payload

- Key-Value operations (example paths — confirm exact paths in `routes.go`)
	- `GET /v1/kv/{key}` — retrieve a value for `key`
		- Success: `200` with `{ "value": "..." }`
		- Not found: `404`
	- `PUT /v1/kv/{key}` — set value for `key`
		- Body: JSON `{ "value": "..." }` or plain text depending on implementation
		- Success: `200` or `201`
	- `DELETE /v1/kv/{key}` — remove a key
		- Success: `200` or `204`

Example curl calls (adjust paths if your server uses a different prefix):

```bash
# Put a key
curl -X PUT http://localhost:8080/v1/kv/foo -H "Content-Type: application/json" -d '{"value":"bar"}'

# Get a key
curl http://localhost:8080/v1/kv/foo

# Delete a key
curl -X DELETE http://localhost:8080/v1/kv/foo
```

## Persistence / Snapshots

The store persists state to `snapshot.gob` using Go's `encoding/gob` format. Snapshot behavior (periodic, on write, on shutdown) is implemented in `internal/kv/store.go`.

Important notes:

- `snapshot.gob` is a binary file — do not edit by hand.
- If you want to reset the store, stop the server and remove `snapshot.gob`.

## Tests

Run unit tests with:

```bash
go test ./...
```

Or run just the store tests:

```bash
go test ./internal/kv
```



