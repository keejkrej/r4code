pub mod acp;
pub mod adapter;
pub mod claude;
pub mod codex;
pub mod discovery;
pub mod models;
pub mod pi;
pub mod process;
pub mod replay;
pub mod rpc;
pub mod tools;

#[cfg(test)]
mod tests;

pub use acp::ACPAdapter;
pub use adapter::{emit_assistant, prompt_text, stable_id, Adapter, EmitFn, Executor, RunRequest};
pub use claude::ClaudeAdapter;
pub use codex::CodexAdapter;
pub use discovery::{known_provider_directories, native_environment, resolve_provider_binary};
pub use models::{default_model, Model, ModelOption};
pub use pi::PiAdapter;
pub use process::{BoundedStderr, ProcessError, DEFAULT_STDERR_LIMIT};
pub use replay::{ReplayAdapter, SequenceAdapter};
pub use rpc::{
    from_slice_lenient, from_value_lenient, normalize_rpc_id, strip_nulls, FrameHandler,
    RequestHandler, RpcClient, RpcClientOptions, RpcRequest, RpcResponse,
};
pub use tools::{emit_tool_call, RequestBroker, ToolCall};
