# r4code

A Rust port of [g0code](../g0code) / [T3 Code](https://github.com/pingdotgg/t3code), targeting [Rune](https://github.com/anomalyco/rune) for the native desktop GUI framework and sharing the TypeScript/Next.js Tailscale-first control plane maintained in `/Users/jack/workspace/t3code-control-plane` (`../t3code-control-plane`).

## Source pin

The orchestration port is based on **t3code `v0.1.7`**, commit:

```text
322f6544dfad0a3c8a0c8cbecf2c7f7874b14268
```

The pinned source is upstream `https://github.com/pingdotgg/t3code` (mirrored through the Go port in `g0code`). Important upstream references at that revision:

- `docs/orchestration-v2/`
- `packages/contracts/src/orchestrationV2.ts`
- `packages/provider-core/src/server/IdAllocator.ts`
- `packages/provider-core/src/server/ProviderAdapter.ts`
- `apps/server/src/orchestration-v2/Orchestrator.ts`
- `apps/server/src/orchestration-v2/ProjectionStore.ts`
- `apps/server/src/orchestration-v2/ProviderEventIngestor.ts`

## Architecture Overview

1. **Orchestration Core (`src/orchestration/`)**: A UI-independent Rust domain module with zero GUI or provider SDK coupling. A Rune-based desktop application or MCP host can expose commands, snapshots, and event streams while provider and SQLite adapters satisfy the trait seams.
2. **SQLite Durability & Leased Outbox (`src/orchestration/sqlite/`)**: Atomic event, receipt, and provider-effect persistence backed by `rusqlite`, plus an at-least-once leased `EffectWorker` with heartbeat lease renewal and stale-worker protection.
3. **Local CLI Provider Adapters (`src/orchestration/cli/`)**: Native multi-turn adapters for Codex (`codex app-server`), Claude Code (`claude` streaming Agent SDK query protocol), Pi (`pi --mode rpc` with blocking permission extension), Agy (`agy acp`), and Fx (`fx acp`). Includes fixture-backed parity tests against recorded `t3code v0.1.7` transcripts.
4. **Standalone MCP Server (`src/orchestration/mcp/` & `src/bin/r4code-mcp.rs`)**: Stdio JSON-RPC 2.0 MCP server exposing provider capabilities, thread creation/read/list/send/wait/interrupt, atomic parent/child task delegation, and live runtime-request responses.
5. **Shared Control Plane (`/Users/jack/workspace/t3code-control-plane`)**: The remote device registry and Tailscale-first control plane remain shared in TypeScript (`/Users/jack/workspace/t3code-control-plane/README.md`) rather than duplicated in Rust.
   - **Control-plane client/verifier (`src/controlplane/`)**: Rust implementation of the shared protocol v1 (`t3code-control-plane/protocol/PROTOCOL.md`): device Ed25519 identity, pairing and heartbeat client, GUI client-authorization/device/connect-token client, JWKS cache, and the device-side connect-token verifier/authorizer, checked against the shared `test-vectors.json`. The device daemon (heartbeat loop and an authenticated network listener for the MCP surface) is not wired up yet; `r4code-mcp` is still stdio-only.
6. **Desktop GUI Target ([Rune](https://github.com/anomalyco/rune))**: The next layer will bind `Orchestrator`, `SqliteStore`, and `cli::Executor` directly to a Rune GUI shell.

## Implemented in this slice

- Typed app-owned IDs and t3code-compatible deterministic ID derivation/escaping (`ids.rs`).
- Threads, runs, attempts, execution nodes, subagents, provider sessions/threads/turns, messages, turn items, runtime requests, plans, checkpoints, transfers, and handoffs (`types.rs`, `event.rs`).
- Atomic event batches, monotonically increasing cursors, command receipts, duplicate replay, rejected receipts, and command-ID conflict checks (`command.rs`, `store.rs`, `orchestrator.rs`).
- Deterministic projection folding and full replay from stored events (`projection.rs`).
- Thread creation/archive/delete, message dispatch, queued-run cancellation/release, and queue promotion (`message.rs`, `queue.rs`, `control.rs`).
- Provider normalization/ingestion with raw-event deduplication and optional active-attempt guards (`provider.rs`).
- Root-only run completion: child/subagent terminals update their own graph nodes without settling the parent run (`provider.rs`, `delegate.rs`).
- Interrupt requests as post-commit effects; acknowledgement does not mark a run terminal (`control.rs`).
- Live-only runtime-request responses as post-commit effects (`control.rs`).
- Lazy thread forks with native-fork selection when capabilities/refs allow it, otherwise an auditable portable handoff (`fork.rs`).
- Provider switching with durable full/delta context handoff artifacts (`message.rs`).
- Concurrency-safe in-memory `MemoryStore` adapter for development and replay tests (`store.rs`).
- Durable SQLite events, receipts, and leased effect outbox with restart/recovery tests (`sqlite/mod.rs`).
- Local CLI adapters for Codex, Claude Code, Pi, Agy (ACP), and Fx (ACP), with fixture-backed parity tests (`cli/`).
- Standalone MCP server (`src/bin/r4code-mcp.rs` and `src/orchestration/mcp/mod.rs`).
- Control-plane protocol v1 client and connect-token verifier (`src/controlplane/`, see [`src/controlplane/README.md`](src/controlplane/README.md)); library only, not yet used by a daemon or GUI.

### Public seams

- `Orchestrator::dispatch`: accepts a `Command` and returns committed events, a cursor, and new provider effects.
- `Orchestrator::ingest`: normalizes provider lifecycle/entity events into the same durable log.
- `Orchestrator::snapshot` / `list` / `events`: snapshot-plus-cursor reads and event replay.
- `Store` / `OutboxStore`: atomic persistence and leased effect outbox traits.
- `ProviderResolver`: maps provider instances to drivers and capabilities.
- `apply` / `replay`: deterministic projection verification.

## MCP server

A standalone stdio MCP server binary is provided at `src/bin/r4code-mcp.rs`:

```sh
cargo build --bin r4code-mcp
./target/debug/r4code-mcp -workspace /path/to/project -provider codex
```

See [`src/orchestration/mcp/README.md`](src/orchestration/mcp/README.md) for tools and protocol verification.

## Development

```sh
cargo test
cargo build --bin r4code-mcp
```
