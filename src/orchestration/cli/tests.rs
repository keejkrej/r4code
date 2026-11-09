use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::{BufRead, BufReader, BufWriter, Read, Write};
use std::path::PathBuf;
use std::sync::{mpsc, Arc};
use std::thread;
use std::time::{Duration, Instant};

use serde::Deserialize;
use serde_json::Value;
use tempfile::TempDir;

use crate::orchestration::cli::*;
use crate::orchestration::command::*;
use crate::orchestration::event::*;
use crate::orchestration::ids::*;
use crate::orchestration::orchestrator::Orchestrator;
use crate::orchestration::outbox::{EffectExecutor, ExecutionContext};
use crate::orchestration::projection::replay;
use crate::orchestration::store::{EventFilter, MemoryStore, Store};
use crate::orchestration::types::*;

#[derive(Deserialize)]
struct TranscriptEntry {
    #[serde(rename = "type")]
    entry_type: String,
    #[serde(default)]
    label: String,
    #[serde(default)]
    frame: Value,
}

fn fixture_path(scenario: &str, name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("orchestration")
        .join("cli")
        .join("testdata")
        .join("t3code-v0.1.7")
        .join(scenario)
        .join(format!("{}_transcript.ndjson", name))
}

fn read_transcript(name: &str, include_outbound: bool) -> Vec<Vec<u8>> {
    read_scenario_transcript("simple", name, include_outbound)
}

fn read_scenario_transcript(scenario: &str, name: &str, include_outbound: bool) -> Vec<Vec<u8>> {
    let path = fixture_path(scenario, name);
    let file = File::open(&path).unwrap_or_else(|e| panic!("open {:?}: {}", path, e));
    let reader = BufReader::new(file);
    let mut frames = Vec::new();
    for line in reader.lines() {
        let line = line.expect("read line");
        if line.trim().is_empty() {
            continue;
        }
        let entry: TranscriptEntry =
            serde_json::from_str(&line).unwrap_or_else(|e| panic!("{:?}: {}", path, e));
        if entry.entry_type == "emit_inbound"
            || (include_outbound && entry.entry_type == "expect_outbound")
        {
            frames.push(serde_json::to_vec(&entry.frame).expect("encode frame"));
        }
    }
    frames
}

fn dispatch(o: &Orchestrator, command: Command) -> DispatchResult {
    o.dispatch(command)
        .unwrap_or_else(|e| panic!("dispatch failed: {}", e))
}

#[test]
fn test_t3code_simple_fixture_parity() {
    struct Case {
        name: &'static str,
        instance_id: &'static str,
        adapter: Arc<dyn Adapter>,
        acp_transport: bool,
    }
    let cases = vec![
        Case {
            name: "codex",
            instance_id: "codex",
            adapter: Arc::new(CodexAdapter::new()),
            acp_transport: false,
        },
        Case {
            name: "claude",
            instance_id: "claudeAgent",
            adapter: Arc::new(ClaudeAdapter::new()),
            acp_transport: false,
        },
        Case {
            name: "pi",
            instance_id: "pi",
            adapter: Arc::new(PiAdapter::new()),
            acp_transport: false,
        },
        Case {
            name: "fx",
            instance_id: "fx",
            adapter: Arc::new(ACPAdapter::new("fx")),
            acp_transport: true,
        },
        Case {
            name: "registry",
            instance_id: "acpRegistry",
            adapter: Arc::new(ACPAdapter::new("acpRegistry")),
            acp_transport: true,
        },
    ];

    for case in cases {
        let ctx = ExecutionContext::background();
        let store = Arc::new(MemoryStore::new());
        let o = Arc::new(Orchestrator::new(store.clone()));
        let executor = Executor::new(o.clone());
        let frames = read_transcript(case.name, case.acp_transport);
        executor.register(
            case.instance_id,
            ReplayAdapter {
                adapter: case.adapter,
                frames,
            },
        );
        let thread_id = format!("thread-{}", case.name);
        let temp_dir = TempDir::new().unwrap();
        let workspace = temp_dir.path().to_string_lossy().into_owned();
        dispatch(
            &o,
            Command {
                command_id: "create".to_string(),
                thread_id: thread_id.clone(),
                command_type: COMMAND_THREAD_CREATE.to_string(),
                payload: CommandPayload::CreateThread(CreateThread {
                    project_id: "project".to_string(),
                    title: format!("Replay {}", case.name),
                    model_selection: ModelSelection {
                        instance_id: case.instance_id.to_string(),
                        model: "default".to_string(),
                        options: BTreeMap::new(),
                    },
                    worktree_path: Some(workspace),
                    created_by: ACTOR_USER.to_string(),
                    creation_source: CREATION_WEB.to_string(),
                    ..Default::default()
                }),
            },
        );
        let result = dispatch(
            &o,
            Command {
                command_id: "message".to_string(),
                thread_id: thread_id.clone(),
                command_type: COMMAND_MESSAGE_DISPATCH.to_string(),
                payload: CommandPayload::DispatchMessage(DispatchMessage {
                    message_id: "user-message".to_string(),
                    text: "Respond with the following text: fixture simple ok".to_string(),
                    dispatch_mode: DispatchMode {
                        mode_type: DISPATCH_START_IMMEDIATELY.to_string(),
                        ..Default::default()
                    },
                    created_by: ACTOR_USER.to_string(),
                    creation_source: CREATION_WEB.to_string(),
                    ..Default::default()
                }),
            },
        );
        assert_eq!(result.effects.len(), 1, "case {}", case.name);
        executor
            .execute(&ctx, &result.effects[0])
            .unwrap_or_else(|e| panic!("replay {} failed: {}", case.name, e));
        assert_simple_projection_parity(&o, store.as_ref(), &thread_id);
        if case.name == "pi" {
            assert_pi_simple_details(&o, &thread_id);
        }
    }
}

#[test]
fn test_t3code_turn_interrupt_fixture_parity() {
    struct Case {
        name: &'static str,
        instance_id: &'static str,
        adapter: Arc<dyn Adapter>,
    }
    let cases = vec![
        Case {
            name: "codex",
            instance_id: "codex",
            adapter: Arc::new(CodexAdapter::new()),
        },
        Case {
            name: "claude",
            instance_id: "claudeAgent",
            adapter: Arc::new(ClaudeAdapter::new()),
        },
        Case {
            name: "fx",
            instance_id: "fx",
            adapter: Arc::new(ACPAdapter::new("fx")),
        },
        Case {
            name: "registry",
            instance_id: "acpRegistry",
            adapter: Arc::new(ACPAdapter::new("acpRegistry")),
        },
    ];

    for case in cases {
        let ctx = ExecutionContext::background();
        let store = Arc::new(MemoryStore::new());
        let o = Arc::new(Orchestrator::new(store));
        let executor = Executor::new(o.clone());
        executor.register(
            case.instance_id,
            ReplayAdapter {
                adapter: case.adapter,
                frames: read_scenario_transcript("turn_interrupt", case.name, true),
            },
        );
        let thread_id = format!("interrupt-{}", case.name);
        let temp_dir = TempDir::new().unwrap();
        let workspace = temp_dir.path().to_string_lossy().into_owned();
        dispatch(
            &o,
            Command {
                command_id: "create".to_string(),
                thread_id: thread_id.clone(),
                command_type: COMMAND_THREAD_CREATE.to_string(),
                payload: CommandPayload::CreateThread(CreateThread {
                    project_id: "project".to_string(),
                    title: format!("Interrupt {}", case.name),
                    model_selection: ModelSelection {
                        instance_id: case.instance_id.to_string(),
                        model: "default".to_string(),
                        options: BTreeMap::new(),
                    },
                    worktree_path: Some(workspace),
                    ..Default::default()
                }),
            },
        );
        let started = dispatch(
            &o,
            Command {
                command_id: "message".to_string(),
                thread_id: thread_id.clone(),
                command_type: COMMAND_MESSAGE_DISPATCH.to_string(),
                payload: CommandPayload::DispatchMessage(DispatchMessage {
                    message_id: "user-message".to_string(),
                    text: "Do not answer immediately. First run the local shell command `sleep 30`, then respond with exactly: interrupt fixture should not finish naturally.".to_string(),
                    dispatch_mode: DispatchMode {
                        mode_type: DISPATCH_START_IMMEDIATELY.to_string(),
                        ..Default::default()
                    },
                    ..Default::default()
                }),
            },
        );
        let snapshot = o.snapshot(&thread_id).unwrap();
        let run = snapshot.projection.runs[0].clone();
        dispatch(
            &o,
            Command {
                command_id: "interrupt".to_string(),
                thread_id: thread_id.clone(),
                command_type: COMMAND_RUN_INTERRUPT.to_string(),
                payload: CommandPayload::InterruptRun(InterruptRun {
                    run_id: run.id.clone(),
                    ..Default::default()
                }),
            },
        );
        executor
            .execute(&ctx, &started.effects[0])
            .unwrap_or_else(|e| panic!("interrupt replay {} failed: {}", case.name, e));
        let snapshot = o.snapshot(&thread_id).unwrap();
        let projection = snapshot.projection;
        assert_eq!(projection.runs[0].status, RUN_INTERRUPTED);
        assert_eq!(projection.attempts[0].status, ATTEMPT_INTERRUPTED);
        assert_eq!(projection.nodes[0].status, NODE_INTERRUPTED);
        assert_eq!(
            projection.provider_threads[0].status,
            PROVIDER_THREAD_IDLE
        );

        let request_item = projection
            .turn_items
            .iter()
            .find(|i| i.item_type == "run_interrupt_request")
            .expect("run_interrupt_request item");
        let result_item = projection
            .turn_items
            .iter()
            .find(|i| i.item_type == "run_interrupt_result")
            .expect("run_interrupt_result item");
        assert_eq!(request_item.status, TURN_ITEM_COMPLETED);
        assert_eq!(result_item.status, TURN_ITEM_INTERRUPTED);
        assert_eq!(
            result_item.parent_item_id.as_deref(),
            Some(request_item.id.as_str())
        );
        assert_semantic_projection_integrity(&projection);
    }
}

#[test]
fn test_t3code_queued_turn_fixture_parity() {
    struct Case {
        name: &'static str,
        instance_id: &'static str,
        adapter: Arc<dyn Adapter>,
    }
    let cases = vec![
        Case {
            name: "codex",
            instance_id: "codex",
            adapter: Arc::new(CodexAdapter::new()),
        },
        Case {
            name: "claude",
            instance_id: "claudeAgent",
            adapter: Arc::new(ClaudeAdapter::new()),
        },
        Case {
            name: "fx",
            instance_id: "fx",
            adapter: Arc::new(ACPAdapter::new("fx")),
        },
        Case {
            name: "registry",
            instance_id: "acpRegistry",
            adapter: Arc::new(ACPAdapter::new("acpRegistry")),
        },
    ];

    for case in cases {
        let ctx = ExecutionContext::background();
        let store = Arc::new(MemoryStore::new());
        let o = Arc::new(Orchestrator::new(store.clone()));
        let executor = Executor::new(o.clone());
        let turns = split_queued_transcript(case.name);
        executor.register(
            case.instance_id,
            SequenceAdapter::new_arc(case.adapter, turns),
        );
        let thread_id = format!("queued-{}", case.name);
        let temp_dir = TempDir::new().unwrap();
        let workspace = temp_dir.path().to_string_lossy().into_owned();
        dispatch(
            &o,
            Command {
                command_id: "create".to_string(),
                thread_id: thread_id.clone(),
                command_type: COMMAND_THREAD_CREATE.to_string(),
                payload: CommandPayload::CreateThread(CreateThread {
                    project_id: "project".to_string(),
                    title: format!("Queued {}", case.name),
                    model_selection: ModelSelection {
                        instance_id: case.instance_id.to_string(),
                        model: "default".to_string(),
                        options: BTreeMap::new(),
                    },
                    worktree_path: Some(workspace),
                    ..Default::default()
                }),
            },
        );
        let first = dispatch(
            &o,
            Command {
                command_id: "first".to_string(),
                thread_id: thread_id.clone(),
                command_type: COMMAND_MESSAGE_DISPATCH.to_string(),
                payload: CommandPayload::DispatchMessage(DispatchMessage {
                    message_id: "first-message".to_string(),
                    text: "Respond with exactly: first fixture turn complete".to_string(),
                    dispatch_mode: DispatchMode {
                        mode_type: DISPATCH_START_IMMEDIATELY.to_string(),
                        ..Default::default()
                    },
                    ..Default::default()
                }),
            },
        );
        dispatch(
            &o,
            Command {
                command_id: "second".to_string(),
                thread_id: thread_id.clone(),
                command_type: COMMAND_MESSAGE_DISPATCH.to_string(),
                payload: CommandPayload::DispatchMessage(DispatchMessage {
                    message_id: "second-message".to_string(),
                    text: "Respond with exactly: second fixture turn complete".to_string(),
                    dispatch_mode: DispatchMode {
                        mode_type: DISPATCH_QUEUE_AFTER_ACTIVE.to_string(),
                        ..Default::default()
                    },
                    ..Default::default()
                }),
            },
        );
        let snapshot = o.snapshot(&thread_id).unwrap();
        assert_eq!(snapshot.projection.runs.len(), 2);
        assert_eq!(snapshot.projection.runs[1].status, RUN_QUEUED);

        executor
            .execute(&ctx, &first.effects[0])
            .unwrap_or_else(|e| panic!("queued replay {} failed: {}", case.name, e));

        let snapshot = o.snapshot(&thread_id).unwrap();
        let projection = snapshot.projection;
        assert_eq!(projection.runs.len(), 2);
        for (idx, run) in projection.runs.iter().enumerate() {
            assert_eq!(run.ordinal, (idx + 1) as i64);
            assert_eq!(run.status, RUN_COMPLETED);
        }
        let roles: Vec<&str> = projection
            .messages
            .iter()
            .map(|m| m.role.as_str())
            .collect();
        assert_eq!(
            roles,
            vec![
                MESSAGE_USER,
                MESSAGE_ASSISTANT,
                MESSAGE_USER,
                MESSAGE_ASSISTANT,
            ]
        );
        assert_assistant_text(&projection.messages, "first fixture turn complete");
        assert_assistant_text(&projection.messages, "second fixture turn complete");
        for item in &projection.turn_items {
            if item.item_type == "user_message"
                && item.run_id.as_deref() == Some(projection.runs[1].id.as_str())
            {
                assert_eq!(
                    item.data.get("inputIntent"),
                    Some(&Value::String("queued_turn".to_string()))
                );
            }
        }
        let events = store
            .read(&EventFilter {
                thread_id: Some(thread_id.clone()),
                ..Default::default()
            })
            .unwrap();
        let mut run2_statuses = Vec::new();
        for ev in events {
            if ev.event.run_id.as_deref() != Some(projection.runs[1].id.as_str()) {
                continue;
            }
            if ev.event.event_type == EVENT_RUN_CREATED || ev.event.event_type == EVENT_RUN_UPDATED
            {
                if let EventPayload::Run(r) = ev.event.payload {
                    run2_statuses.push(r.status);
                }
            }
        }
        assert!(run2_statuses.len() >= 3);
        assert_eq!(run2_statuses[0], RUN_QUEUED);
        assert!(run2_statuses.contains(&RUN_RUNNING.to_string()));
        assert_eq!(*run2_statuses.last().unwrap(), RUN_COMPLETED);
        assert_semantic_projection_integrity(&projection);
    }
}

fn split_queued_transcript(name: &str) -> Vec<Vec<Vec<u8>>> {
    let path = fixture_path("queued_turn", name);
    let file = File::open(&path).unwrap();
    let reader = BufReader::new(file);
    let mut turns: Vec<Vec<Vec<u8>>> = vec![Vec::new()];
    let mut trigger_count = 0;
    for line in reader.lines() {
        let line = line.unwrap();
        if line.trim().is_empty() {
            continue;
        }
        let entry: TranscriptEntry = serde_json::from_str(&line).unwrap();
        if entry.entry_type != "emit_inbound" && entry.entry_type != "expect_outbound" {
            continue;
        }
        let mut trigger = false;
        if entry.entry_type == "expect_outbound" {
            match name {
                "codex" => trigger = entry.label.starts_with("turn/start"),
                "claude" => trigger = entry.label.starts_with("prompt.offer:"),
                _ => {
                    let kind = entry
                        .frame
                        .get("kind")
                        .and_then(|v| v.as_str())
                        .unwrap_or_default();
                    let method = entry
                        .frame
                        .get("method")
                        .and_then(|v| v.as_str())
                        .unwrap_or_default();
                    trigger = kind == "request" && method == "session/prompt";
                }
            }
        }
        if trigger {
            trigger_count += 1;
            if trigger_count == 2 {
                turns.push(Vec::new());
            }
        }
        turns
            .last_mut()
            .unwrap()
            .push(serde_json::to_vec(&entry.frame).unwrap());
    }
    assert_eq!(turns.len(), 2, "{} split into {} turns", name, turns.len());
    turns
}

#[test]
fn test_native_transport_matches_t3code_simple_fixture() {
    let cases = vec![
        ("codex", "codex", "gpt-6-luna"),
        ("pi", "pi", "openrouter/deepseek/deepseek-v4-flash"),
        ("fx", "fx", "default"),
    ];

    for (name, instance_id, model) in cases {
        let fixture = fixture_path("simple", name);
        let temp_dir = TempDir::new().unwrap();
        let binary = native_helper_binary(temp_dir.path(), &fixture, name);
        let ctx = ExecutionContext::background();
        let store = Arc::new(MemoryStore::new());
        let o = Arc::new(Orchestrator::new(store.clone()));
        let executor = Executor::new(o.clone());

        let mut options = BTreeMap::new();
        match name {
            "codex" => {
                executor.register(
                    instance_id,
                    CodexAdapter {
                        binary,
                        ..Default::default()
                    },
                );
            }
            "pi" => {
                let session_dir = TempDir::new().unwrap();
                executor.register(
                    instance_id,
                    PiAdapter {
                        binary,
                        session_dir: session_dir.path().to_string_lossy().into_owned(),
                        ..Default::default()
                    },
                );
            }
            "fx" => {
                options.insert(
                    "reasoningEffort".to_string(),
                    Value::String("medium".to_string()),
                );
                executor.register(
                    instance_id,
                    ACPAdapter {
                        binary,
                        args: vec!["acp".to_string()],
                        driver_kind: "fx".to_string(),
                        mode: "ask".to_string(),
                        ..Default::default()
                    },
                );
            }
            _ => unreachable!(),
        }

        let thread_id = format!("native-{}", name);
        let workspace_dir = TempDir::new().unwrap();
        let workspace = workspace_dir.path().to_string_lossy().into_owned();
        dispatch(
            &o,
            Command {
                command_id: "create".to_string(),
                thread_id: thread_id.clone(),
                command_type: COMMAND_THREAD_CREATE.to_string(),
                payload: CommandPayload::CreateThread(CreateThread {
                    project_id: "project".to_string(),
                    title: "Replay fixture: simple".to_string(),
                    model_selection: ModelSelection {
                        instance_id: instance_id.to_string(),
                        model: model.to_string(),
                        options,
                    },
                    worktree_path: Some(workspace),
                    runtime_mode: RUNTIME_FULL_ACCESS.to_string(),
                    interaction_mode: INTERACTION_DEFAULT.to_string(),
                    ..Default::default()
                }),
            },
        );
        let result = dispatch(
            &o,
            Command {
                command_id: "message".to_string(),
                thread_id: thread_id.clone(),
                command_type: COMMAND_MESSAGE_DISPATCH.to_string(),
                payload: CommandPayload::DispatchMessage(DispatchMessage {
                    message_id: "user-message".to_string(),
                    text: "Respond with the following text: fixture simple ok".to_string(),
                    dispatch_mode: DispatchMode {
                        mode_type: DISPATCH_START_IMMEDIATELY.to_string(),
                        ..Default::default()
                    },
                    ..Default::default()
                }),
            },
        );
        executor
            .execute(&ctx, &result.effects[0])
            .unwrap_or_else(|e| panic!("native transport replay {}: {}", name, e));
        assert_simple_projection_parity(&o, store.as_ref(), &thread_id);
        executor.close().unwrap();
    }
}

#[test]
fn test_claude_uses_streaming_sdk_query_transport() {
    let fixture = fixture_path("simple", "claude");
    let temp_dir = TempDir::new().unwrap();
    let binary = native_helper_binary(temp_dir.path(), &fixture, "claude");
    let ctx = ExecutionContext::background();
    let store = Arc::new(MemoryStore::new());
    let o = Arc::new(Orchestrator::new(store.clone()));
    let executor = Executor::new(o.clone());
    executor.register(
        "claudeAgent",
        ClaudeAdapter {
            binary,
            ..Default::default()
        },
    );
    let thread_id = "native-claude".to_string();
    let workspace_dir = TempDir::new().unwrap();
    let workspace = workspace_dir.path().to_string_lossy().into_owned();
    dispatch(
        &o,
        Command {
            command_id: "create".to_string(),
            thread_id: thread_id.clone(),
            command_type: COMMAND_THREAD_CREATE.to_string(),
            payload: CommandPayload::CreateThread(CreateThread {
                project_id: "project".to_string(),
                title: "Replay fixture: simple".to_string(),
                model_selection: ModelSelection {
                    instance_id: "claudeAgent".to_string(),
                    model: "default".to_string(),
                    options: BTreeMap::new(),
                },
                worktree_path: Some(workspace),
                ..Default::default()
            }),
        },
    );
    let result = dispatch(
        &o,
        Command {
            command_id: "message".to_string(),
            thread_id: thread_id.clone(),
            command_type: COMMAND_MESSAGE_DISPATCH.to_string(),
            payload: CommandPayload::DispatchMessage(DispatchMessage {
                message_id: "user-message".to_string(),
                text: "Respond with the following text: fixture simple ok".to_string(),
                dispatch_mode: DispatchMode {
                    mode_type: DISPATCH_START_IMMEDIATELY.to_string(),
                    ..Default::default()
                },
                ..Default::default()
            }),
        },
    );
    executor
        .execute(&ctx, &result.effects[0])
        .unwrap_or_else(|e| panic!("Claude streaming query replay: {}", e));
    assert_simple_projection_parity(&o, store.as_ref(), &thread_id);
    executor.close().unwrap();
}

#[test]
fn test_native_provider_discovery_with_packaged_path() {
    let home = TempDir::new().unwrap();
    let bin_dir = home.path().join(".local").join("bin");
    fs::create_dir_all(&bin_dir).unwrap();
    let target = bin_dir.join("codex");
    fs::write(&target, "#!/bin/sh\nexit 0\n").unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut perms = fs::metadata(&target).unwrap().permissions();
        perms.set_mode(0o700);
        fs::set_permissions(&target, perms).unwrap();
    }
    let old_home = std::env::var("HOME").ok();
    let old_path = std::env::var("PATH").ok();
    unsafe {
        std::env::set_var("HOME", home.path());
        std::env::set_var("PATH", "/usr/bin:/bin");
    }

    let resolved = resolve_provider_binary("codex");
    let env = native_environment(&[]);

    unsafe {
        if let Some(h) = old_home {
            std::env::set_var("HOME", h);
        }
        if let Some(p) = old_path {
            std::env::set_var("PATH", p);
        }
    }

    let resolved = resolved.expect("resolve codex");
    assert_eq!(resolved, target.to_string_lossy());
    let path_val = env
        .iter()
        .find(|(k, _)| k == "PATH")
        .map(|(_, v)| v.as_str())
        .unwrap_or_default();
    assert!(
        path_val.contains(&bin_dir.to_string_lossy().into_owned()),
        "native PATH {:?} missing {:?}",
        path_val,
        bin_dir
    );
}

#[test]
fn test_codex_native_command_approval_round_trip() {
    run_native_approval_fixture(
        "tool_call_read_only_on_request",
        "codex",
        "accept",
        false,
    );
}

#[test]
fn test_fx_native_denied_write_round_trip() {
    run_native_approval_fixture("tool_call_denied_write", "fx", "decline", true);
}

fn run_native_approval_fixture(
    scenario: &str,
    provider: &str,
    decision: &str,
    expect_denied: bool,
) {
    let ctx = ExecutionContext::background();
    let store = Arc::new(MemoryStore::new());
    let o = Arc::new(Orchestrator::new(store));
    let executor = Executor::new(o.clone());
    let fixture = fixture_path(scenario, provider);
    let temp_dir = TempDir::new().unwrap();
    let binary = native_helper_binary(temp_dir.path(), &fixture, provider);

    let mut options = BTreeMap::new();
    if provider == "fx" {
        options.insert(
            "reasoningEffort".to_string(),
            Value::String("medium".to_string()),
        );
        executor.register(
            provider,
            ACPAdapter {
                binary,
                args: vec!["acp".to_string()],
                driver_kind: "fx".to_string(),
                mode: "ask".to_string(),
                ..Default::default()
            },
        );
    } else {
        executor.register(
            provider,
            CodexAdapter {
                binary,
                ..Default::default()
            },
        );
    }

    let thread_id = format!("tool-{}", provider);
    let workspace_dir = TempDir::new().unwrap();
    let workspace = workspace_dir.path().to_string_lossy().into_owned();
    dispatch(
        &o,
        Command {
            command_id: "create".to_string(),
            thread_id: thread_id.clone(),
            command_type: COMMAND_THREAD_CREATE.to_string(),
            payload: CommandPayload::CreateThread(CreateThread {
                project_id: "project".to_string(),
                title: "Tool fixture".to_string(),
                worktree_path: Some(workspace),
                runtime_mode: RUNTIME_AUTO_ACCEPT_EDITS.to_string(),
                interaction_mode: INTERACTION_DEFAULT.to_string(),
                model_selection: ModelSelection {
                    instance_id: provider.to_string(),
                    model: "default".to_string(),
                    options,
                },
                ..Default::default()
            }),
        },
    );
    let started = dispatch(
        &o,
        Command {
            command_id: "start".to_string(),
            thread_id: thread_id.clone(),
            command_type: COMMAND_MESSAGE_DISPATCH.to_string(),
            payload: CommandPayload::DispatchMessage(DispatchMessage {
                message_id: "user".to_string(),
                text: "Run the recorded tool scenario".to_string(),
                dispatch_mode: DispatchMode {
                    mode_type: DISPATCH_START_IMMEDIATELY.to_string(),
                    ..Default::default()
                },
                ..Default::default()
            }),
        },
    );

    let (turn_done_tx, turn_done_rx) = mpsc::channel();
    let exec_bg = executor.clone();
    let ctx_bg = ctx.clone();
    let start_effect = started.effects[0].clone();
    thread::spawn(move || {
        let _ = turn_done_tx.send(exec_bg.execute(&ctx_bg, &start_effect));
    });

    let request = wait_for_pending_request(&o, &thread_id, &turn_done_rx);
    let response = dispatch(
        &o,
        Command {
            command_id: "respond".to_string(),
            thread_id: thread_id.clone(),
            command_type: COMMAND_RUNTIME_REQUEST_RESPOND.to_string(),
            payload: CommandPayload::RuntimeRequestResponse(RuntimeRequestResponse {
                request_id: request.id.clone(),
                decision: Some(decision.to_string()),
                ..Default::default()
            }),
        },
    );
    assert_eq!(response.effects.len(), 1);
    executor.execute(&ctx, &response.effects[0]).unwrap();

    let turn_res = turn_done_rx
        .recv_timeout(Duration::from_secs(10))
        .expect("native turn did not finish after approval");
    turn_res.expect("turn failed");

    let snapshot = o.snapshot(&thread_id).unwrap();
    let resolved = snapshot
        .projection
        .runtime_request(&request.id)
        .expect("resolved request");
    assert_eq!(resolved.status, REQUEST_RESOLVED);
    assert_eq!(snapshot.projection.runs[0].status, RUN_COMPLETED);

    let mut found_tool = false;
    for item in &snapshot.projection.turn_items {
        if item.item_type == "command_execution"
            || item.item_type == "file_change"
            || item.item_type == "dynamic_tool"
        {
            found_tool = true;
            if expect_denied {
                assert_eq!(item.status, TURN_ITEM_FAILED);
            }
        }
        if item.request_id == request.id && expect_denied {
            assert_eq!(item.status, TURN_ITEM_DECLINED);
        }
    }
    assert!(found_tool, "tool call missing");
    executor.close().unwrap();
}

fn wait_for_pending_request(
    o: &Orchestrator,
    thread_id: &str,
    turn_done_rx: &mpsc::Receiver<Result<(), crate::orchestration::errors::Error>>,
) -> RuntimeRequest {
    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline {
        let snapshot = o.snapshot(thread_id).unwrap();
        for req in &snapshot.projection.runtime_requests {
            if req.status == REQUEST_PENDING {
                return req.clone();
            }
        }
        if let Ok(res) = turn_done_rx.try_recv() {
            panic!(
                "turn ended before a pending request on {}: {:?}",
                thread_id, res
            );
        }
        thread::sleep(Duration::from_millis(10));
    }
    panic!("no pending request on {}", thread_id);
}

fn native_helper_binary(
    dir: &std::path::Path,
    fixture: &std::path::Path,
    provider: &str,
) -> String {
    let path = dir.join("native-peer.sh");
    let exe = std::env::current_exe().expect("current test exe");
    let content = format!(
        "#!/bin/sh\n\
         export R4CODE_NATIVE_PEER=1\n\
         export R4CODE_NATIVE_FIXTURE={}\n\
         export R4CODE_NATIVE_PROVIDER={}\n\
         i=0\n\
         for arg in \"$@\"; do\n\
           eval \"export R4CODE_NATIVE_ARG_$i=\\\"\\$arg\\\"\"\n\
           i=$((i + 1))\n\
         done\n\
         export R4CODE_NATIVE_ARGC=$i\n\
         exec 3>&1\n\
         exec {} --exact orchestration::cli::tests::test_native_transport_helper --nocapture >/dev/null\n",
        shell_quote(&fixture.to_string_lossy()),
        shell_quote(provider),
        shell_quote(&exe.to_string_lossy()),
    );
    fs::write(&path, content).unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut perms = fs::metadata(&path).unwrap().permissions();
        perms.set_mode(0o700);
        fs::set_permissions(&path, perms).unwrap();
    }
    path.to_string_lossy().into_owned()
}

fn shell_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\\''"))
}

#[test]
fn test_native_transport_helper() {
    if std::env::var("R4CODE_NATIVE_PEER").as_deref() != Ok("1") {
        return;
    }
    let fixture = std::env::var("R4CODE_NATIVE_FIXTURE").unwrap_or_default();
    let provider = std::env::var("R4CODE_NATIVE_PROVIDER").unwrap_or_default();
    let argc: usize = std::env::var("R4CODE_NATIVE_ARGC")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    let mut launch_args = Vec::with_capacity(argc);
    for i in 0..argc {
        launch_args.push(std::env::var(format!("R4CODE_NATIVE_ARG_{}", i)).unwrap_or_default());
    }
    let out_file = unsafe {
        use std::os::unix::io::FromRawFd;
        File::from_raw_fd(3)
    };
    if let Err(err) = run_native_peer(&fixture, &provider, &launch_args, out_file) {
        eprintln!("{}", err);
        std::process::exit(2);
    }
    std::process::exit(0);
}

fn run_native_peer(
    fixture: &str,
    provider: &str,
    launch_args: &[String],
    out_file: File,
) -> Result<(), String> {
    match provider {
        "codex" => {
            if launch_args.first().map(|s| s.as_str()) != Some("app-server") {
                return Err(format!(
                    "Codex launch args = {:?}, want app-server",
                    launch_args
                ));
            }
        }
        "pi" => {
            if !has_adjacent_arg(launch_args, "--mode", "rpc") {
                return Err(format!("Pi launch args = {:?}, want --mode rpc", launch_args));
            }
        }
        "claude" => {
            if !has_adjacent_arg(launch_args, "--input-format", "stream-json")
                || !has_adjacent_arg(launch_args, "--output-format", "stream-json")
            {
                return Err(format!(
                    "Claude launch args = {:?}, want streaming SDK query",
                    launch_args
                ));
            }
            return run_claude_native_peer(fixture, out_file);
        }
        _ => {
            if launch_args.first().map(|s| s.as_str()) != Some("acp") {
                return Err(format!("ACP launch args = {:?}, want acp", launch_args));
            }
        }
    }
    run_recorded_native_peer(fixture, out_file)
}

fn run_recorded_native_peer(fixture: &str, out_file: File) -> Result<(), String> {
    let entries = read_transcript_entries(fixture);
    let mut writer = BufWriter::new(out_file);
    let stdin = std::io::stdin();
    let mut reader = BufReader::new(stdin.lock());
    let mut request_ids: std::collections::HashMap<String, Value> =
        std::collections::HashMap::new();
    let mut index = 0;

    while index < entries.len() {
        while index < entries.len() && entries[index].entry_type != "expect_outbound" {
            index += 1;
        }
        if index >= entries.len() {
            break;
        }
        let expected = &entries[index];
        if expected.label == "process_start" {
            index += 1;
            continue;
        }
        let actual = read_native_record(&mut reader)
            .map_err(|e| format!("read {}: {}", expected.label, e))?;
        if let Some(method) = actual.get("method").and_then(|v| v.as_str()) {
            if actual.get("kind").and_then(|v| v.as_str()) != Some("response") {
                if let Some(id_val) = actual.get("id") {
                    request_ids.insert(method.to_string(), id_val.clone());
                }
            }
        }
        assert_same_native_operation(&expected.frame, &actual)
            .map_err(|e| format!("{}: {}", expected.label, e))?;
        index += 1;
        while index < entries.len() && entries[index].entry_type == "emit_inbound" {
            let mut frame = entries[index]
                .frame
                .as_object()
                .cloned()
                .expect("frame object");
            if frame.get("kind").and_then(|v| v.as_str()) == Some("request")
                && frame.get("id").is_none_or(|v| v.is_null())
            {
                frame.insert(
                    "id".to_string(),
                    Value::String(format!("server-{}", index)),
                );
            }
            if native_frame_is_response(&frame) {
                if let Some(id_val) = actual.get("id") {
                    frame.insert("id".to_string(), id_val.clone());
                }
                if let Some(method) = frame.get("method").and_then(|v| v.as_str()) {
                    if let Some(saved_id) = request_ids.get(method) {
                        frame.insert("id".to_string(), saved_id.clone());
                    }
                }
            }
            write_native_record(&mut writer, &Value::Object(frame))?;
            index += 1;
        }
    }
    let mut sink = Vec::new();
    let _ = reader.read_to_end(&mut sink);
    Ok(())
}

fn run_claude_native_peer(fixture: &str, out_file: File) -> Result<(), String> {
    let stdin = std::io::stdin();
    let mut reader = BufReader::new(stdin.lock());
    let record = read_native_record(&mut reader)?;
    if record.get("type").and_then(|v| v.as_str()) != Some("user") {
        return Err(format!(
            "first Claude query input type = {:?}, want user",
            record.get("type")
        ));
    }
    let message = record
        .get("message")
        .and_then(|v| v.as_object())
        .ok_or_else(|| format!("Claude query input missing message: {:?}", record))?;
    if message.get("role").and_then(|v| v.as_str()) != Some("user")
        || message.get("content").and_then(|v| v.as_str())
            != Some("Respond with the following text: fixture simple ok")
    {
        return Err(format!("Claude query input mismatch: {:?}", record));
    }
    let mut writer = BufWriter::new(out_file);
    for entry in read_transcript_entries(fixture) {
        if entry.entry_type != "emit_inbound" {
            continue;
        }
        write_native_record(&mut writer, &entry.frame)?;
    }
    let mut sink = Vec::new();
    let _ = reader.read_to_end(&mut sink);
    Ok(())
}

fn read_transcript_entries(path: &str) -> Vec<TranscriptEntry> {
    let file = File::open(path).expect("open fixture");
    let reader = BufReader::new(file);
    reader
        .lines()
        .map(|l| serde_json::from_str::<TranscriptEntry>(&l.unwrap()).unwrap())
        .collect()
}

fn read_native_record<R: BufRead>(reader: &mut R) -> Result<serde_json::Map<String, Value>, String> {
    let mut line = String::new();
    let n = reader.read_line(&mut line).map_err(|e| e.to_string())?;
    if n == 0 {
        return Err("EOF".to_string());
    }
    let val: Value = serde_json::from_str(&line).map_err(|e| e.to_string())?;
    val.as_object()
        .cloned()
        .ok_or_else(|| "not a JSON object".to_string())
}

fn write_native_record<W: Write>(writer: &mut W, record: &Value) -> Result<(), String> {
    let mut bytes = serde_json::to_vec(record).map_err(|e| e.to_string())?;
    bytes.push(b'\n');
    writer.write_all(&bytes).map_err(|e| e.to_string())?;
    writer.flush().map_err(|e| e.to_string())
}

fn assert_same_native_operation(
    expected_val: &Value,
    actual: &serde_json::Map<String, Value>,
) -> Result<(), String> {
    let Some(expected) = expected_val.as_object() else {
        return Ok(());
    };
    if expected.get("kind").and_then(|v| v.as_str()) == Some("response") {
        return Ok(());
    }
    for key in ["method", "type"] {
        if let Some(exp_v) = expected.get(key) {
            if !exp_v.is_null() && actual.get(key) != Some(exp_v) {
                return Err(format!(
                    "{} = {:?}, want {:?}; actual={:?}",
                    key,
                    actual.get(key),
                    exp_v,
                    actual
                ));
            }
        }
    }
    Ok(())
}

fn native_frame_is_response(frame: &serde_json::Map<String, Value>) -> bool {
    frame.get("kind").and_then(|v| v.as_str()) == Some("response")
        || frame.get("type").and_then(|v| v.as_str()) == Some("response")
        || (frame.get("result").is_some_and(|v| !v.is_null())
            && frame.get("method").and_then(|v| v.as_str()) != Some("session/update"))
}

fn has_adjacent_arg(args: &[String], flag: &str, value: &str) -> bool {
    args.windows(2).any(|w| w[0] == flag && w[1] == value)
}

fn assert_simple_projection_parity(o: &Orchestrator, store: &dyn Store, thread_id: &ThreadID) {
    let snapshot = o.snapshot(thread_id).unwrap();
    let projection = snapshot.projection;
    assert_eq!(projection.runs.len(), 1);
    assert_eq!(projection.runs[0].ordinal, 1);
    assert_eq!(projection.runs[0].status, RUN_COMPLETED);
    assert!(!projection.provider_threads.is_empty());
    assert!(!projection.provider_turns.is_empty());
    assert!(projection.nodes.iter().any(|n| n.kind == NODE_ROOT_TURN));
    assert!(projection
        .nodes
        .iter()
        .any(|n| n.kind == NODE_ASSISTANT_MESSAGE));
    assert!(projection.messages.iter().any(|m| m.role == MESSAGE_USER));
    assert!(projection
        .messages
        .iter()
        .any(|m| m.role == MESSAGE_ASSISTANT));
    assert!(projection
        .turn_items
        .iter()
        .any(|i| i.item_type == "user_message"));
    assert!(projection
        .turn_items
        .iter()
        .any(|i| i.item_type == "assistant_message"));
    assert_assistant_text(&projection.messages, "fixture simple ok");
    assert_semantic_projection_integrity(&projection);

    let events: Vec<StoredEvent> = store
        .read(&EventFilter {
            thread_id: Some(thread_id.clone()),
            ..Default::default()
        })
        .unwrap();
    for (idx, ev) in events.iter().enumerate() {
        assert_eq!(ev.sequence, (idx + 1) as u64);
    }
    let replayed = replay(&events).unwrap();
    let replay_snap = replayed.get(thread_id).unwrap();
    assert_eq!(replay_snap.runs.len(), 1);
    assert_eq!(replay_snap.runs[0].status, RUN_COMPLETED);
}

fn assert_assistant_text(messages: &[ConversationMessage], expected: &str) {
    let found = messages
        .iter()
        .any(|m| m.role == MESSAGE_ASSISTANT && m.text == expected && !m.streaming);
    assert!(
        found,
        "missing settled assistant text {:?} in {:?}",
        expected, messages
    );
}

fn assert_pi_simple_details(o: &Orchestrator, thread_id: &ThreadID) {
    let snapshot = o.snapshot(thread_id).unwrap();
    let reasoning =
        "The user wants me to respond with a specific text: \"fixture simple ok\". Let me do that.";
    let found_reasoning = snapshot
        .projection
        .turn_items
        .iter()
        .any(|i| i.item_type == "reasoning" && i.text == reasoning);
    assert!(found_reasoning, "Pi reasoning mismatch");
    let turn = &snapshot.projection.provider_turns[0];
    assert_eq!(
        turn.native_turn_ref
            .as_ref()
            .and_then(|r| r.native_id.as_deref()),
        Some("0b4c51f4")
    );
    let usage = turn.token_usage.as_ref().expect("Pi settled usage");
    assert_eq!(usage.used_tokens, 1814);
    assert_eq!(usage.max_tokens, Some(1048576));
    assert_eq!(usage.output_tokens, 28);
}

fn assert_semantic_projection_integrity(projection: &Projection) {
    let mut root_count = 0;
    for run in &projection.runs {
        let root_id = run.root_node_id.as_ref().expect("run root node");
        let root = projection.node(root_id).expect("root node exists");
        assert_eq!(root.kind, NODE_ROOT_TURN);
        assert!(root.counts_for_run);
    }
    for node in &projection.nodes {
        if node.kind == NODE_ROOT_TURN {
            root_count += 1;
            assert!(node.counts_for_run);
        }
    }
    assert!(root_count >= projection.runs.len());
    for turn in &projection.provider_turns {
        assert!(projection.node(&turn.node_id).is_some());
    }
    let mut prev_ordinal = 0i64;
    for item in &projection.turn_items {
        assert!(
            item.ordinal > prev_ordinal,
            "turn items not strictly ordered"
        );
        prev_ordinal = item.ordinal;
        if let Some(ref nid) = item.node_id {
            assert!(projection.node(nid).is_some());
        }
        if let Some(ref ptid) = item.provider_turn_id {
            assert!(projection.provider_turn(ptid).is_some());
        }
    }
    for msg in &projection.messages {
        if let Some(ref rid) = msg.run_id {
            assert!(projection.run(rid).is_some());
        }
        if let Some(ref nid) = msg.node_id {
            assert!(projection.node(nid).is_some());
        }
    }
}
