use std::collections::HashMap;
use std::io::{BufRead, BufReader, Read, Write};
use std::process::{Child, ChildStdin, Command, Stdio};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use std::thread;
use std::time::Duration;

use serde::de::DeserializeOwned;
use serde::Deserialize;
use serde_json::{json, Value};

use crate::orchestration::errors::Error;
use crate::orchestration::outbox::ExecutionContext;

use super::discovery::{native_environment, resolve_provider_binary};
use super::process::{BoundedStderr, ProcessError, DEFAULT_STDERR_LIMIT};

#[derive(Debug, Clone)]
pub struct RpcRequest {
    pub id: Value,
    pub request_id: Value,
    pub req_type: String,
    pub method: String,
    pub params: Value,
    pub raw: Vec<u8>,
}

pub type FrameHandler = Arc<dyn Fn(&[u8]) + Send + Sync + 'static>;
pub type RequestHandler = Arc<dyn Fn(RpcRequest) -> Result<Value, Error> + Send + Sync + 'static>;

#[derive(Default)]
pub struct RpcClientOptions {
    pub binary: String,
    pub args: Vec<String>,
    pub dir: String,
    pub env: Vec<String>,
    pub id_prefix: String,
    pub on_notification: Option<FrameHandler>,
    pub on_response: Option<FrameHandler>,
    pub on_request: Option<RequestHandler>,
}

#[derive(Debug)]
pub struct RpcResponse {
    pub id: Value,
    pub result: Option<Value>,
    pub err: Option<Error>,
    pub raw: Vec<u8>,
}

struct SharedState {
    pending: Mutex<HashMap<String, mpsc::Sender<RpcResponse>>>,
    closed: AtomicBool,
    close_err: Mutex<Option<Error>>,
    on_notification: Mutex<Option<FrameHandler>>,
    on_response: Mutex<Option<FrameHandler>>,
    on_request: Mutex<Option<RequestHandler>>,
}

pub struct RpcClient {
    binary: String,
    args: Vec<String>,
    id_prefix: String,
    next_id: AtomicU64,
    stdin: Arc<Mutex<Option<ChildStdin>>>,
    child: Mutex<Option<Child>>,
    stderr: BoundedStderr,
    state: Arc<SharedState>,
}

impl RpcClient {
    pub fn start(opts: RpcClientOptions) -> Result<Arc<Self>, Error> {
        let resolved_binary = resolve_provider_binary(&opts.binary)?;
        let mut cmd = Command::new(&resolved_binary);
        cmd.args(&opts.args)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        if !opts.dir.is_empty() {
            cmd.current_dir(&opts.dir);
        }
        cmd.env_clear();
        for (k, v) in native_environment(&opts.env) {
            cmd.env(k, v);
        }

        let mut child = cmd.spawn().map_err(|e| {
            ProcessError {
                binary: opts.binary.clone(),
                args: opts.args.clone(),
                cause: e.to_string(),
                stderr: String::new(),
            }
            .into_error()
        })?;

        let stdin = child.stdin.take().ok_or_else(|| {
            Error::provider(format!("provider process {}: missing stdin", opts.binary))
        })?;
        let stdout = child.stdout.take().ok_or_else(|| {
            Error::provider(format!("provider process {}: missing stdout", opts.binary))
        })?;
        let stderr_pipe = child.stderr.take().ok_or_else(|| {
            Error::provider(format!("provider process {}: missing stderr", opts.binary))
        })?;

        let stderr = BoundedStderr::new(DEFAULT_STDERR_LIMIT);
        let stderr_clone = stderr.clone();
        thread::spawn(move || {
            let mut reader = stderr_pipe;
            let mut buf = [0u8; 4096];
            while let Ok(n) = reader.read(&mut buf) {
                if n == 0 {
                    break;
                }
                stderr_clone.write_bytes(&buf[..n]);
            }
        });

        let client = Arc::new(Self {
            binary: opts.binary,
            args: opts.args,
            id_prefix: opts.id_prefix,
            next_id: AtomicU64::new(0),
            stdin: Arc::new(Mutex::new(Some(stdin))),
            child: Mutex::new(Some(child)),
            stderr,
            state: Arc::new(SharedState {
                pending: Mutex::new(HashMap::new()),
                closed: AtomicBool::new(false),
                close_err: Mutex::new(None),
                on_notification: Mutex::new(opts.on_notification),
                on_response: Mutex::new(opts.on_response),
                on_request: Mutex::new(opts.on_request),
            }),
        });

        let client_reader = Arc::clone(&client);
        thread::spawn(move || {
            client_reader.read_loop(stdout);
        });

        Ok(client)
    }

    pub fn set_on_notification(&self, handler: Option<FrameHandler>) {
        if let Ok(mut guard) = self.state.on_notification.lock() {
            *guard = handler;
        }
    }

    pub fn set_on_response(&self, handler: Option<FrameHandler>) {
        if let Ok(mut guard) = self.state.on_response.lock() {
            *guard = handler;
        }
    }

    pub fn set_on_request(&self, handler: Option<RequestHandler>) {
        if let Ok(mut guard) = self.state.on_request.lock() {
            *guard = handler;
        }
    }

    pub fn is_closed(&self) -> bool {
        self.state.closed.load(Ordering::SeqCst)
    }

    fn process_error(&self, cause: impl Into<String>) -> Error {
        ProcessError {
            binary: self.binary.clone(),
            args: self.args.clone(),
            cause: cause.into(),
            stderr: self.stderr.as_string(),
        }
        .into_error()
    }

    fn read_loop(&self, stdout: std::process::ChildStdout) {
        let mut reader = BufReader::new(stdout);
        let mut line_buf = Vec::new();
        loop {
            line_buf.clear();
            let bytes_read = match reader.read_until(b'\n', &mut line_buf) {
                Ok(n) => n,
                Err(e) => {
                    self.mark_closed(Some(self.process_error(e.to_string())));
                    return;
                }
            };
            if bytes_read == 0 {
                break;
            }
            let trimmed = trim_ascii_whitespace(&line_buf);
            if trimmed.is_empty() {
                continue;
            }
            let raw = trimmed.to_vec();
            self.route(&raw);
        }
        self.mark_closed(Some(self.process_error("EOF")));
    }

    fn route(&self, raw: &[u8]) {
        #[derive(Deserialize, Default)]
        struct Frame {
            #[serde(default)]
            id: Value,
            #[serde(default)]
            method: String,
            #[serde(default)]
            params: Value,
            #[serde(default)]
            result: Option<Value>,
            #[serde(default)]
            error: Option<RpcErrorObj>,
            #[serde(rename = "type", default)]
            frame_type: String,
            #[serde(default)]
            kind: String,
            #[serde(default)]
            request_id: Value,
            #[serde(default)]
            response: FrameResponseInner,
        }
        #[derive(Deserialize, Default)]
        struct RpcErrorObj {
            #[serde(default)]
            code: i64,
            #[serde(default)]
            message: String,
        }
        #[derive(Deserialize, Default)]
        struct FrameResponseInner {
            #[serde(default)]
            request_id: Value,
        }

        let Ok(frame) = from_slice_lenient::<Frame>(raw) else {
            return;
        };

        let has_id = !frame.id.is_null();
        let has_req_id = !frame.request_id.is_null();
        let is_response = (frame.result.is_some() && has_id)
            || frame.error.is_some()
            || frame.frame_type == "response"
            || frame.kind == "response"
            || frame.frame_type == "control_response";

        if is_response {
            let id = if has_id {
                frame.id
            } else if has_req_id {
                frame.request_id
            } else {
                frame.response.request_id
            };
            let key = normalize_rpc_id(&id);
            let tx = self.state.pending.lock().unwrap().remove(&key);
            if let Some(tx) = tx {
                let err = frame.error.map(|e| {
                    Error::provider(format!("JSON-RPC error {}: {}", e.code, e.message))
                });
                let _ = tx.send(RpcResponse {
                    id,
                    result: frame.result,
                    err,
                    raw: raw.to_vec(),
                });
            } else {
                let on_resp = self.state.on_response.lock().unwrap().clone();
                if let Some(handler) = on_resp {
                    handler(raw);
                }
            }
            return;
        }

        if (!frame.method.is_empty() && has_id && frame.frame_type != "extension_ui_request")
            || (frame.frame_type == "control_request" && has_req_id)
        {
            let request_id = if frame.frame_type == "control_request" {
                frame.request_id.clone()
            } else {
                frame.id.clone()
            };
            let req = RpcRequest {
                id: request_id.clone(),
                request_id: frame.request_id.clone(),
                req_type: frame.frame_type.clone(),
                method: frame.method.clone(),
                params: frame.params,
                raw: raw.to_vec(),
            };
            let on_req = self.state.on_request.lock().unwrap().clone();
            let res = if let Some(handler) = on_req {
                handler(req)
            } else {
                Err(Error::provider("client callback not supported"))
            };
            let response = if frame.frame_type == "control_request" {
                match res {
                    Ok(result) => json!({
                        "type": "control_response",
                        "response": {
                            "request_id": request_id,
                            "subtype": "success",
                            "result": result,
                        }
                    }),
                    Err(err) => json!({
                        "type": "control_response",
                        "response": {
                            "request_id": request_id,
                            "subtype": "error",
                            "error": err.to_string(),
                        }
                    }),
                }
            } else {
                match res {
                    Ok(result) => json!({
                        "jsonrpc": "2.0",
                        "id": frame.id,
                        "result": result,
                    }),
                    Err(err) => json!({
                        "jsonrpc": "2.0",
                        "id": frame.id,
                        "error": {
                            "code": -32000,
                            "message": err.to_string(),
                        }
                    }),
                }
            };
            let _ = self.write_record(&response);
            return;
        }

        if !frame.method.is_empty() || !frame.frame_type.is_empty() {
            let on_notif = self.state.on_notification.lock().unwrap().clone();
            if let Some(handler) = on_notif {
                handler(raw);
            }
        }
    }

    fn mark_closed(&self, err: Option<Error>) {
        self.state.closed.store(true, Ordering::SeqCst);
        if let Ok(mut close_err) = self.state.close_err.lock() {
            if close_err.is_none() {
                *close_err = err;
            }
        }
        let pending_map = {
            let mut guard = self.state.pending.lock().unwrap();
            std::mem::take(&mut *guard)
        };
        let err_msg = format!("process transport closed: {}", self.stderr.as_string());
        for (_, tx) in pending_map {
            let _ = tx.send(RpcResponse {
                id: Value::Null,
                result: None,
                err: Some(Error::provider(err_msg.clone())),
                raw: Vec::new(),
            });
        }
    }

    pub fn next_request_id(&self) -> (Value, String) {
        let n = self.next_id.fetch_add(1, Ordering::SeqCst) + 1;
        if !self.id_prefix.is_empty() {
            let id = format!("{}{}", self.id_prefix, n);
            (Value::String(id.clone()), id)
        } else {
            let val = Value::Number(serde_json::Number::from(n));
            (val, n.to_string())
        }
    }

    pub fn call<T: DeserializeOwned>(
        &self,
        ctx: &ExecutionContext,
        method: &str,
        params: Value,
    ) -> Result<T, Error> {
        let mut rec = serde_json::Map::new();
        rec.insert("jsonrpc".to_string(), Value::String("2.0".to_string()));
        rec.insert("method".to_string(), Value::String(method.to_string()));
        rec.insert("params".to_string(), params);
        self.request_record(ctx, rec)
    }

    pub fn call_unit(
        &self,
        ctx: &ExecutionContext,
        method: &str,
        params: Value,
    ) -> Result<(), Error> {
        let mut rec = serde_json::Map::new();
        rec.insert("jsonrpc".to_string(), Value::String("2.0".to_string()));
        rec.insert("method".to_string(), Value::String(method.to_string()));
        rec.insert("params".to_string(), params);
        let _ = self.request_record_raw(ctx, rec)?;
        Ok(())
    }

    pub fn request_record<T: DeserializeOwned>(
        &self,
        ctx: &ExecutionContext,
        record: serde_json::Map<String, Value>,
    ) -> Result<T, Error> {
        let resp = self.request_record_raw(ctx, record)?;
        if let Some(res_val) = resp.result {
            from_value_lenient(res_val).map_err(|e| Error::provider(e.to_string()))
        } else {
            from_slice_lenient(&resp.raw).map_err(|e| Error::provider(e.to_string()))
        }
    }

    pub fn request_record_raw(
        &self,
        ctx: &ExecutionContext,
        mut record: serde_json::Map<String, Value>,
    ) -> Result<RpcResponse, Error> {
        let (id, key) = self.next_request_id();
        record.insert("id".to_string(), id);
        let (tx, rx) = mpsc::channel();
        {
            if self.state.closed.load(Ordering::SeqCst) {
                return Err(self.closed_error());
            }
            self.state.pending.lock().unwrap().insert(key.clone(), tx);
        }
        if let Err(err) = self.write_record(&Value::Object(record)) {
            self.state.pending.lock().unwrap().remove(&key);
            return Err(err);
        }
        self.wait_response(ctx, &key, rx)
    }

    pub fn notify(&self, method: &str, params: Option<Value>) -> Result<(), Error> {
        let mut msg = serde_json::Map::new();
        msg.insert("jsonrpc".to_string(), Value::String("2.0".to_string()));
        msg.insert("method".to_string(), Value::String(method.to_string()));
        if let Some(p) = params {
            msg.insert("params".to_string(), p);
        }
        self.write_record(&Value::Object(msg))
    }

    pub fn send_record(&self, record: &Value) -> Result<(), Error> {
        self.write_record(record)
    }

    pub fn control_request(&self, ctx: &ExecutionContext, request: Value) -> Result<(), Error> {
        let (id, key) = self.next_request_id();
        let (tx, rx) = mpsc::channel();
        {
            if self.state.closed.load(Ordering::SeqCst) {
                return Err(self.closed_error());
            }
            self.state.pending.lock().unwrap().insert(key.clone(), tx);
        }
        let msg = json!({
            "type": "control_request",
            "request_id": id,
            "request": request,
        });
        if let Err(err) = self.write_record(&msg) {
            self.state.pending.lock().unwrap().remove(&key);
            return Err(err);
        }
        let _ = self.wait_response(ctx, &key, rx)?;
        Ok(())
    }

    pub fn write_record(&self, value: &Value) -> Result<(), Error> {
        let mut encoded = serde_json::to_vec(value).map_err(|e| Error::provider(e.to_string()))?;
        encoded.push(b'\n');
        let mut guard = self.stdin.lock().unwrap();
        let Some(ref mut stdin) = *guard else {
            return Err(self.process_error("stdin closed"));
        };
        stdin
            .write_all(&encoded)
            .and_then(|_| stdin.flush())
            .map_err(|e| self.process_error(e.to_string()))
    }

    fn wait_response(
        &self,
        ctx: &ExecutionContext,
        key: &str,
        rx: mpsc::Receiver<RpcResponse>,
    ) -> Result<RpcResponse, Error> {
        loop {
            if ctx.is_cancelled() {
                self.state.pending.lock().unwrap().remove(key);
                return Err(Error::provider("execution context cancelled"));
            }
            match rx.recv_timeout(Duration::from_millis(20)) {
                Ok(resp) => {
                    if let Some(err) = resp.err {
                        return Err(err);
                    }
                    return Ok(resp);
                }
                Err(mpsc::RecvTimeoutError::Timeout) => {
                    if self.state.closed.load(Ordering::SeqCst) {
                        if let Ok(resp) = rx.try_recv() {
                            if let Some(err) = resp.err {
                                return Err(err);
                            }
                            return Ok(resp);
                        }
                        return Err(self.closed_error());
                    }
                }
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    return Err(self.closed_error());
                }
            }
        }
    }

    pub fn closed_error(&self) -> Error {
        if let Ok(guard) = self.state.close_err.lock() {
            if let Some(ref err) = *guard {
                return err.clone();
            }
        }
        self.process_error("rpc client closed")
    }

    pub fn close(&self) {
        {
            let mut stdin_guard = self.stdin.lock().unwrap();
            *stdin_guard = None;
        }
        let mut child_guard = self.child.lock().unwrap();
        let Some(ref mut child) = *child_guard else {
            return;
        };
        let deadline = std::time::Instant::now() + Duration::from_secs(1);
        loop {
            match child.try_wait() {
                Ok(Some(_)) => {
                    *child_guard = None;
                    return;
                }
                Ok(None) => {
                    if std::time::Instant::now() >= deadline {
                        let _ = child.kill();
                        let _ = child.wait();
                        *child_guard = None;
                        return;
                    }
                    thread::sleep(Duration::from_millis(15));
                }
                Err(_) => {
                    *child_guard = None;
                    return;
                }
            }
        }
    }
}

pub fn normalize_rpc_id(id: &Value) -> String {
    match id {
        Value::String(s) => s.clone(),
        Value::Number(n) => n.to_string(),
        Value::Null => String::new(),
        other => other.to_string(),
    }
}

pub fn strip_nulls(val: &mut Value) {
    match val {
        Value::Object(map) => {
            map.retain(|_, v| !v.is_null());
            for v in map.values_mut() {
                strip_nulls(v);
            }
        }
        Value::Array(arr) => {
            for v in arr {
                strip_nulls(v);
            }
        }
        _ => {}
    }
}

pub fn from_value_lenient<T: DeserializeOwned>(mut val: Value) -> Result<T, serde_json::Error> {
    strip_nulls(&mut val);
    serde_json::from_value(val)
}

pub fn from_slice_lenient<T: DeserializeOwned>(raw: &[u8]) -> Result<T, serde_json::Error> {
    let val: Value = serde_json::from_slice(raw)?;
    from_value_lenient(val)
}

fn trim_ascii_whitespace(mut bytes: &[u8]) -> &[u8] {
    while let Some(&first) = bytes.first() {
        if first.is_ascii_whitespace() {
            bytes = &bytes[1..];
        } else {
            break;
        }
    }
    while let Some(&last) = bytes.last() {
        if last.is_ascii_whitespace() {
            bytes = &bytes[..bytes.len() - 1];
        } else {
            break;
        }
    }
    bytes
}
