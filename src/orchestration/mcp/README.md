# Orchestrator MCP server

`r4code-mcp` exposes the live Rust orchestrator as a standalone stdio MCP server. It is intended for direct integration testing, debugging, and use by other agent CLIs.

## Run

```sh
cargo build --bin r4code-mcp
./target/debug/r4code-mcp -workspace /path/to/project -provider codex
```

Options:

- `-project local`: project ID inherited by top-level MCP threads.
- `-workspace <path>`: checkout inherited by new threads.
- `-provider codex|claude|pi|agy|fx`: default provider.
- `-model default`: use the provider CLI's configured model, or pass an explicit model.
- `-parent-thread <id>`: optionally pin delegated-task tools to a parent thread. Without it, local testing accepts `parentThreadId` in `delegate_task`, `task_status`, and `task_cancel`.
- `-state <path>`: SQLite state path. Defaults to `~/.r4code/orchestration.sqlite`; use `-state :memory:` for an ephemeral server.

Logs and asynchronous effect failures go to stderr. stdout carries only MCP frames. Events, command receipts, and pending provider effects are durable by default. A leased outbox worker executes effects and recovers expired claims after restart.

Example MCP client configuration:

```json
{
  "mcpServers": {
    "r4code": {
      "command": "/path/to/r4code/target/debug/r4code-mcp",
      "args": ["-workspace", "/path/to/project", "-provider", "codex"]
    }
  }
}
```

## Tools

- `orchestrator_capabilities`
- `create_threads`
- `delegate_task`, `task_status`, `task_cancel`
- `t3_thread_list`, `t3_thread_read`, `t3_thread_send`, `t3_thread_wait`, `t3_thread_interrupt`
- `t3_pending_request`

Calls produce structured JSON results and typed `OrchestratorMcpFailure` values. `clientRequestId` provides stable command/thread/message identity for retries. Wait timeouts do not cancel provider work.

Delegation dispatches the real `delegated_task.request` command. Parent/child graph changes commit atomically, children receive only the supplied task and optional role instruction, and child completion publishes a durable subagent-result transfer.

## Verification

`test_mcp_orchestrator_workflow` acts as an MCP JSON-RPC client and covers initialization, tool discovery, active-parent delegation, child result publication, lineage-aware listing, reads, sends, waits, idempotent retries, and pending-request responses:

```sh
cargo test orchestration::mcp::tests
```
