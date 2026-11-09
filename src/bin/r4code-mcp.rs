use std::collections::BTreeMap;
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use r4code::orchestration::cli::Executor;
use r4code::orchestration::mcp::{Config, Provider, Server};
use r4code::orchestration::orchestrator::Orchestrator;
use r4code::orchestration::outbox::{EffectWorker, ExecutionContext};
use r4code::orchestration::sqlite::SqliteStore;
use r4code::orchestration::types::ModelSelection;

fn main() {
    let mut workspace = std::env::current_dir()
        .map(|p| p.to_string_lossy().into_owned())
        .unwrap_or_else(|_| ".".to_string());
    let mut project = "local".to_string();
    let mut provider = "codex".to_string();
    let mut model = "default".to_string();
    let mut state_path = std::env::var("HOME")
        .map(|h| {
            PathBuf::from(h)
                .join(".r4code")
                .join("orchestration.sqlite")
                .to_string_lossy()
                .into_owned()
        })
        .unwrap_or_else(|_| ":memory:".to_string());
    let mut parent_thread = String::new();

    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        let (flag, inline_val) = if let Some((k, v)) = arg.split_once('=') {
            (k, Some(v.to_string()))
        } else {
            (arg.as_str(), None)
        };
        let mut take_val = || -> String {
            inline_val
                .clone()
                .or_else(|| args.next())
                .unwrap_or_else(|| {
                    eprintln!("missing value for {}", flag);
                    std::process::exit(2);
                })
        };
        match flag {
            "-workspace" | "--workspace" => workspace = take_val(),
            "-project" | "--project" => project = take_val(),
            "-provider" | "--provider" => provider = take_val(),
            "-model" | "--model" => model = take_val(),
            "-state" | "--state" => state_path = take_val(),
            "-parent-thread" | "--parent-thread" => parent_thread = take_val(),
            "-h" | "--help" => {
                eprintln!(
                    "Usage: r4code-mcp [options]\n\
                     \n\
                     Options:\n\
                       -workspace <dir>        workspace inherited by new threads\n\
                       -project <id>           project ID inherited by new threads (default \"local\")\n\
                       -provider <id>          default provider: codex, claude, pi, agy, or fx (default \"codex\")\n\
                       -model <id>             default model; default uses the CLI's configured model\n\
                       -state <path>           SQLite state path, or :memory: for ephemeral state\n\
                       -parent-thread <id>     optional parent thread used to scope delegated tasks"
                );
                return;
            }
            other => {
                eprintln!("unknown flag: {}", other);
                std::process::exit(2);
            }
        }
    }

    let store = match durable_store(&state_path) {
        Ok(s) => Arc::new(s),
        Err(err) => {
            eprintln!("{}", err);
            std::process::exit(1);
        }
    };

    let o = Arc::new(Orchestrator::new(store.clone()));
    let executor = Executor::new(o.clone());
    executor.register_defaults();

    let providers = vec![
        Provider {
            instance_id: "codex".to_string(),
            driver: "codex".to_string(),
            models: Vec::new(),
            available: true,
            runtime_modes: Vec::new(),
        },
        Provider {
            instance_id: "claude".to_string(),
            driver: "claudeAgent".to_string(),
            models: Vec::new(),
            available: true,
            runtime_modes: Vec::new(),
        },
        Provider {
            instance_id: "pi".to_string(),
            driver: "pi".to_string(),
            models: Vec::new(),
            available: true,
            runtime_modes: Vec::new(),
        },
        Provider {
            instance_id: "agy".to_string(),
            driver: "agy".to_string(),
            models: Vec::new(),
            available: true,
            runtime_modes: Vec::new(),
        },
        Provider {
            instance_id: "fx".to_string(),
            driver: "fx".to_string(),
            models: Vec::new(),
            available: true,
            runtime_modes: Vec::new(),
        },
    ];

    let server = Server::new(
        o,
        None,
        Config {
            providers,
            default_project_id: project,
            default_worktree_path: Some(workspace.clone()),
            default_target: ModelSelection {
                instance_id: provider.clone(),
                model,
                options: BTreeMap::new(),
            },
            parent_thread_id: parent_thread,
            on_effect_error: Some(Arc::new(|effect, err| {
                eprintln!("effect {} ({}) failed: {}", effect.id, effect.kind, err);
            })),
        },
    );

    eprintln!(
        "r4code MCP listening on stdio; provider={} workspace={} state={}",
        provider, workspace, state_path
    );

    let ctx = ExecutionContext::new();
    let worker_id = format!("r4code-{}", std::process::id());
    let mut worker = EffectWorker::new(store, Arc::new(executor.clone()), worker_id);
    worker.lease = Duration::from_secs(60);
    worker.poll = Duration::from_millis(50);

    let worker_ctx = ctx.clone();
    thread::spawn(move || {
        if let Err(err) = worker.run(&worker_ctx) {
            if !worker_ctx.is_cancelled() {
                eprintln!("effect worker failed: {}", err);
            }
        }
    });

    let stdin = std::io::stdin();
    let stdout = std::io::stdout();
    let res = server.serve(&ctx, stdin, stdout);
    ctx.cancel();
    let _ = executor.close();
    if let Err(err) = res {
        eprintln!("{}", err);
        std::process::exit(1);
    }
}

fn durable_store(
    path: &str,
) -> Result<SqliteStore, r4code::orchestration::errors::Error> {
    if path == ":memory:" {
        return SqliteStore::open(path);
    }
    if let Some(parent) = std::path::Path::new(path).parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent)
                .map_err(|e| r4code::orchestration::errors::Error::internal(e.to_string()))?;
        }
    }
    SqliteStore::open(path)
}
