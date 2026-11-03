//! Plain-mode JSON-RPC to the local herdr-core, and core endpoint discovery.

use std::io::{self, BufRead, BufReader, Read, Write};
use std::os::unix::net::UnixStream;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use serde_json::{json, Value};

pub(crate) const DEFAULT_CALL_TIMEOUT: Duration = Duration::from_secs(10);
const CORE_START_TIMEOUT: Duration = Duration::from_secs(10);
const MAX_PLAIN_LINE: usize = 16 * 1024 * 1024;

static NEXT_ID: AtomicU64 = AtomicU64::new(1);

pub(crate) fn next_request_id() -> u64 {
    NEXT_ID.fetch_add(1, Ordering::Relaxed)
}

#[derive(Debug)]
pub(crate) enum HcError {
    Io(io::Error),
    Rpc { kind: String, message: String },
    Protocol(String),
}

impl HcError {
    #[cfg(test)]
    pub(crate) fn kind(&self) -> Option<&str> {
        match self {
            Self::Rpc { kind, .. } => Some(kind),
            _ => None,
        }
    }
}

impl std::fmt::Display for HcError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(err) => write!(f, "herdr-core: {err}"),
            Self::Rpc { kind, message } => write!(f, "herdr-core {kind}: {message}"),
            Self::Protocol(message) => write!(f, "herdr-core protocol: {message}"),
        }
    }
}

impl std::error::Error for HcError {}

impl From<io::Error> for HcError {
    fn from(err: io::Error) -> Self {
        Self::Io(err)
    }
}

impl From<HcError> for io::Error {
    fn from(err: HcError) -> Self {
        match err {
            HcError::Io(err) => err,
            other => io::Error::other(other.to_string()),
        }
    }
}

/// Mirrors herdr-core's endpoint rules (docs/security.md, "State and endpoints").
pub(crate) fn socket_path() -> PathBuf {
    if let Some(path) = std::env::var_os("HC_SOCKET").filter(|v| !v.is_empty()) {
        return PathBuf::from(path);
    }
    let session = session_name();
    let file = format!("{session}.sock");
    if let Some(dir) = std::env::var_os("XDG_RUNTIME_DIR").filter(|v| !v.is_empty()) {
        return PathBuf::from(dir).join("herdr-core").join(file);
    }
    // SAFETY: getuid has no preconditions and cannot fail.
    let uid = unsafe { libc::getuid() };
    let run_user = PathBuf::from(format!("/run/user/{uid}"));
    if run_user.is_dir() {
        return run_user.join("herdr-core").join(file);
    }
    state_home().join("herdr-core").join(&session).join(file)
}

pub(crate) fn session_name() -> String {
    std::env::var("HC_SESSION")
        .ok()
        .filter(|v| !v.is_empty())
        .unwrap_or_else(|| "default".to_string())
}

fn state_home() -> PathBuf {
    if let Some(dir) = std::env::var_os("XDG_STATE_HOME").filter(|v| !v.is_empty()) {
        return PathBuf::from(dir);
    }
    let home = std::env::var_os("HOME")
        .map(PathBuf::from)
        .unwrap_or_default();
    home.join(".local").join("state")
}

/// Connects to the core, starting it with `hc server start` when nothing answers.
pub(crate) fn connect() -> Result<UnixStream, HcError> {
    let path = socket_path();
    match UnixStream::connect(&path) {
        Ok(stream) => Ok(stream),
        Err(err)
            if matches!(
                err.kind(),
                io::ErrorKind::NotFound | io::ErrorKind::ConnectionRefused
            ) =>
        {
            start_core()?;
            let deadline = Instant::now() + CORE_START_TIMEOUT;
            loop {
                match UnixStream::connect(&path) {
                    Ok(stream) => return Ok(stream),
                    Err(err) if Instant::now() >= deadline => return Err(err.into()),
                    Err(_) => std::thread::sleep(Duration::from_millis(50)),
                }
            }
        }
        Err(err) => Err(err.into()),
    }
}

fn hc_binary() -> PathBuf {
    if let Some(path) = std::env::var_os("HERDR_HC_BIN").filter(|v| !v.is_empty()) {
        return PathBuf::from(path);
    }
    for name in ["hc", "herdr-core"] {
        if let Some(path) = find_on_path(name) {
            return path;
        }
    }
    let home = std::env::var_os("HOME")
        .map(PathBuf::from)
        .unwrap_or_default();
    home.join(".local").join("bin").join("herdr-core")
}

fn find_on_path(name: &str) -> Option<PathBuf> {
    std::env::split_paths(&std::env::var_os("PATH")?)
        .map(|dir| dir.join(name))
        .find(|candidate| candidate.is_file())
}

fn start_core() -> Result<(), HcError> {
    let binary = hc_binary();
    let output = std::process::Command::new(&binary)
        .args(["server", "start"])
        .stdin(std::process::Stdio::null())
        .output()
        .map_err(|err| {
            HcError::Io(io::Error::new(
                err.kind(),
                format!("could not run {} server start: {err}", binary.display()),
            ))
        })?;
    if output.status.success() {
        return Ok(());
    }
    Err(HcError::Protocol(format!(
        "{} server start failed: {}",
        binary.display(),
        String::from_utf8_lossy(&output.stderr).trim()
    )))
}

/// One request on a fresh plain-mode connection.
pub(crate) fn call(method: &str, params: Value, timeout: Duration) -> Result<Value, HcError> {
    let stream = connect()?;
    stream.set_read_timeout(Some(timeout))?;
    stream.set_write_timeout(Some(timeout))?;
    let id = next_request_id();
    let mut request = serde_json::to_vec(&request(id, method, params))
        .map_err(|err| HcError::Protocol(err.to_string()))?;
    request.push(b'\n');
    (&stream).write_all(&request)?;
    let mut reader = BufReader::new(stream.take(MAX_PLAIN_LINE as u64));
    let mut line = Vec::new();
    loop {
        line.clear();
        if reader.read_until(b'\n', &mut line)? == 0 {
            return Err(HcError::Protocol(format!(
                "connection closed before {method} replied"
            )));
        }
        let message: Value =
            serde_json::from_slice(&line).map_err(|err| HcError::Protocol(err.to_string()))?;
        if message.get("id").and_then(Value::as_u64) == Some(id) {
            return response_result(message);
        }
    }
}

pub(crate) fn request(id: u64, method: &str, params: Value) -> Value {
    json!({"jsonrpc": "2.0", "id": id, "method": method, "params": params})
}

pub(crate) fn response_result(mut message: Value) -> Result<Value, HcError> {
    if let Some(error) = message.get_mut("error").map(Value::take) {
        let message = error
            .get("message")
            .and_then(Value::as_str)
            .unwrap_or("request failed")
            .to_string();
        let kind = error
            .pointer("/data/kind")
            .and_then(Value::as_str)
            .unwrap_or("error")
            .to_string();
        return Err(HcError::Rpc { kind, message });
    }
    message
        .get_mut("result")
        .map(Value::take)
        .ok_or_else(|| HcError::Protocol("response without result".into()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rpc_errors_keep_their_stable_kind() {
        let err = response_result(json!({
            "jsonrpc": "2.0", "id": 1,
            "error": {"code": -32000, "message": "no terminal t9", "data": {"kind": "not_found"}}
        }))
        .unwrap_err();
        assert_eq!(err.kind(), Some("not_found"));
        assert_eq!(err.to_string(), "herdr-core not_found: no terminal t9");
    }

    #[test]
    fn results_are_returned_whole() {
        let result =
            response_result(json!({"jsonrpc": "2.0", "id": 1, "result": {"live": 2}})).unwrap();
        assert_eq!(result, json!({"live": 2}));
    }
}
