# Local CLI adapters

This module (`r4code::orchestration::cli`) connects local coding-agent CLIs to `Orchestrator` using post-commit effects and normalized provider events.

```text
Orchestrator::dispatch
  -> committed run graph + provider.turn.start effect
  -> cli::Executor::execute
  -> native CLI frames
  -> Orchestrator::ingest (guarded by run + active attempt)
  -> terminal root event
  -> completed/failed/interrupted run projection
```

The adapters support native coding tools and supervised runtime approvals in addition to text turns:
- **Codex (`codex.rs`)**: Persistent `codex app-server` JSON-RPC session.
- **Claude Code (`claude.rs`)**: Streaming Agent SDK query protocol (`--output-format stream-json --input-format stream-json`).
- **Pi (`pi.rs`)**: Persistent `pi --mode rpc` session with a dynamically generated blocking permission extension.
- **Agy / Fx (`acp.rs`)**: Managed ACP sessions over stdio (`agy acp`, `fx acp`, or generic `acp`).

## Fixture-backed parity

`orchestration/cli/testdata/t3code-v0.1.7/` contains scenario inputs, original assertion modules, and native recordings copied from `t3code` commit `322f6544dfad0a3c8a0c8cbecf2c7f7874b14268`.

`ReplayAdapter` and `SequenceAdapter` replace only process transport. Tests drive the same adapter frame handlers, executor, orchestrator, store, and projections as live turns. Native fake peers additionally consume the fixtures' `expect_outbound` operations, failing if Codex does not use `app-server`, Pi does not use RPC, Claude does not use streaming query input, or ACP diverges from the recorded request sequence.

| Pinned scenario | Providers | Ported oracle behavior |
| --- | --- | --- |
| `simple` | Codex, Claude, Pi, Fx, generic ACP | One completed run; root/assistant nodes; user/assistant messages and items; settled assistant text; semantic graph integrity; contiguous event sequences |
| `simple` Pi details | Pi | Exact recorded reasoning content, settled token usage/context window, and native user-entry turn correlation |
| `queued_turn` | Codex, Claude, Fx, generic ACP | Two completed runs; queued intake; queued-to-running-to-completed transitions; preserved `queued_turn` intent; conversation role order |
| `turn_interrupt` | Codex, Claude, Fx, generic ACP | Interrupted run/attempt/root; idle provider thread; linked request/result turn items |

Run all CLI adapter tests with:

```sh
cargo test orchestration::cli::tests
```
