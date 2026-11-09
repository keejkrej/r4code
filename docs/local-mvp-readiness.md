# Local Mac MVP readiness

## Verdict

**Ready for local orchestration, CLI provider execution, SQLite persistence, and stdio MCP integration on macOS.** The Rust port (`r4code`) implements full wire-format and behavioral parity with `g0code`'s orchestration core, SQLite store + leased effect outbox, local CLI adapters (Codex, Claude Code, Pi, Agy, Fx), and standalone MCP server (`r4code-mcp`).

The native desktop shell will be built on [Rune](https://github.com/anomalyco/rune), while the remote device control plane is shared at `/Users/jack/workspace/t3code-control-plane`.

## Working locally

| Capability | Status | Evidence/limits |
| --- | --- | --- |
| Orchestration core (`src/orchestration`) | Ready | 14 unit tests covering deterministic IDs, receipts, replay, queues, forks, handoffs, delegation, and interrupts |
| Provider/model selection | Working native catalogs | Concrete default model, reasoning/effort, service tier, thinking, and fast mode where supported |
| Approval-required, auto-accept-edits, auto, full-access modes | Explicit runtime ceilings | `full-access` remains explicit by design |
| Standalone MCP server | Ready for local use | `src/bin/r4code-mcp.rs`; stdio MCP initialize/list/call/delegate/wait/approve workflow verified |
| Codex native integration | Working coding path | Persistent `codex app-server`, thread resume, native tool events/approvals/interrupt |
| Pi native integration | Working coding path | Persistent RPC, native tools, blocking permission extension, abort, settled usage |
| Agy/Fx native integration | Working coding path | Managed ACP, tool lifecycle, permission options, resume/load, session cancel |
| Claude native integration | Working protocol | Streaming SDK query (`stream-json` input and output), control requests, and usage-limit handling |
| Durable state (`src/orchestration/sqlite`) | Ready for current model | SQLite events, receipts, and multi-thread commits; close/reopen recovery tests |
| Pending provider effects | Ready for current model | Leased at-least-once outbox, heartbeat renewal, stale-worker protection |
| Delegation | Working local path | Active-parent child creation, atomic lineage, result transfer, status/cancel tools |
| Tool calls and approvals | Working tested paths | Command/file/dynamic tool nodes and items, native accept/decline responses, denied-write regression |
| Remote control plane | Shared (`/Users/jack/workspace/t3code-control-plane`) | Shares the TypeScript Tailscale-first control plane across `g0code` and `r4code` |
| Rune desktop GUI | Next slice | To be built on `https://github.com/anomalyco/rune` |

## Run the MCP interface

```sh
cargo run --bin r4code-mcp -- -workspace /path/to/project -provider codex
```

The default SQLite state path is `~/.r4code/orchestration.sqlite` (or pass `-state :memory:` for ephemeral sessions).
