use std::fmt::Display;

pub type CommandID = String;
pub type EventID = String;
pub type RawEventID = String;
pub type ProjectID = String;
pub type ThreadID = String;
pub type MessageID = String;
pub type RunID = String;
pub type RunAttemptID = String;
pub type NodeID = String;
pub type ProviderSessionID = String;
pub type ProviderThreadID = String;
pub type ProviderTurnID = String;
pub type RuntimeRequestID = String;
pub type TurnItemID = String;
pub type CheckpointScopeID = String;
pub type CheckpointID = String;
pub type ContextHandoffID = String;
pub type ContextTransferID = String;
pub type PlanID = String;
pub type ProviderDriverKind = String;
pub type ProviderInstanceID = String;

/// Matches JavaScript's `encodeURIComponent`, which t3code uses for
/// deterministic orchestration IDs.
pub fn encode_id_part(value: &str) -> String {
    const HEX_DIGITS: &[u8; 16] = b"0123456789ABCDEF";
    let mut out = String::with_capacity(value.len());
    for &b in value.as_bytes() {
        let is_unreserved = b.is_ascii_lowercase()
            || b.is_ascii_uppercase()
            || b.is_ascii_digit()
            || matches!(b, b'-' | b'_' | b'.' | b'!' | b'~' | b'*' | b'\'' | b'(' | b')');
        if is_unreserved {
            out.push(b as char);
        } else {
            out.push('%');
            out.push(HEX_DIGITS[(b >> 4) as usize] as char);
            out.push(HEX_DIGITS[(b & 0x0f) as usize] as char);
        }
    }
    out
}

pub fn join_id(prefix: &str, parts: &[&dyn Display]) -> String {
    let mut out = String::from(prefix);
    for part in parts {
        out.push(':');
        out.push_str(&encode_id_part(&part.to_string()));
    }
    out
}

pub fn derive_run_id(thread_id: &str, ordinal: i64) -> RunID {
    join_id("run", &[&"thread", &thread_id, &"ordinal", &ordinal])
}

pub fn derive_run_attempt_id(run_id: &str, ordinal: i64) -> RunAttemptID {
    join_id("run-attempt", &[&"run", &run_id, &"attempt", &ordinal])
}

pub fn derive_root_node_id(run_id: &str) -> NodeID {
    join_id("node", &[&"run", &run_id, &"root"])
}

pub fn derive_delegated_task_node_id(command_id: &str) -> NodeID {
    join_id("node", &[&"delegated-task", &command_id])
}

pub fn derive_delegated_task_thread_id(command_id: &str) -> ThreadID {
    join_id("thread", &[&"delegated-task", &command_id])
}

pub fn derive_delegated_task_message_id(command_id: &str) -> MessageID {
    join_id("message", &[&"delegated-task", &command_id])
}

pub fn derive_delegated_task_turn_item_id(command_id: &str) -> TurnItemID {
    join_id("turn-item", &[&"delegated-task", &command_id])
}

pub fn derive_provider_thread_id(
    driver: &str,
    instance_id: &str,
    native_id: &str,
) -> ProviderThreadID {
    if instance_id.is_empty() {
        join_id(
            "provider-thread",
            &[&"provider", &driver, &"native-thread", &native_id],
        )
    } else {
        join_id(
            "provider-thread",
            &[
                &"provider",
                &driver,
                &"provider-instance",
                &instance_id,
                &"native-thread",
                &native_id,
            ],
        )
    }
}

pub fn derive_provider_turn_id(driver: &str, native_id: &str) -> ProviderTurnID {
    join_id(
        "provider-turn",
        &[&"provider", &driver, &"native-turn", &native_id],
    )
}

pub fn derive_node_from_provider_item_id(driver: &str, native_id: &str) -> NodeID {
    join_id("node", &[&"provider", &driver, &"native-item", &native_id])
}

pub fn derive_message_from_provider_item_id(driver: &str, native_id: &str) -> MessageID {
    join_id(
        "message",
        &[&"provider", &driver, &"native-item", &native_id],
    )
}

pub fn derive_turn_item_from_provider_item_id(driver: &str, native_id: &str) -> TurnItemID {
    join_id(
        "turn-item",
        &[&"provider", &driver, &"native-item", &native_id],
    )
}

pub fn derive_user_turn_item_id(message_id: &str) -> TurnItemID {
    join_id("turn-item", &[&"message", &message_id])
}

pub fn derive_run_signal_turn_item_id(run_id: &str, signal: &str) -> TurnItemID {
    join_id("turn-item", &[&"run", &run_id, &"signal", &signal])
}

pub fn derive_checkpoint_scope_id(thread_id: &str, name: &str) -> CheckpointScopeID {
    join_id(
        "checkpoint-scope",
        &[&"thread", &thread_id, &"name", &name],
    )
}

pub fn new_uuid_v4() -> Result<String, String> {
    let mut bytes = [0u8; 16];
    getrandom::fill(&mut bytes).map_err(|e| format!("getrandom failed: {e}"))?;
    bytes[6] = (bytes[6] & 0x0f) | 0x40;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    let encoded = hex::encode(bytes);
    Ok(format!(
        "{}-{}-{}-{}-{}",
        &encoded[0..8],
        &encoded[8..12],
        &encoded[12..16],
        &encoded[16..20],
        &encoded[20..32]
    ))
}

pub fn random_id(prefix: &str, parts: &[&str]) -> Result<String, String> {
    let uuid = new_uuid_v4().map_err(|e| format!("allocate {prefix} ID: {e}"))?;
    let mut out = String::from(prefix);
    for part in parts {
        out.push(':');
        out.push_str(&encode_id_part(part));
    }
    out.push(':');
    out.push_str(&encode_id_part(&uuid));
    Ok(out)
}
