//! Optional pane backend that runs pane programs in herdr-core (hc) terminals.

pub(crate) mod frame;
pub(crate) mod pane_io;
pub(crate) mod rpc;

use std::sync::atomic::{AtomicBool, Ordering};

use portable_pty::CommandBuilder;
use serde_json::{json, Map, Value};

pub(crate) use pane_io::{HcAttach, HcPaneIo, HcPaneIoConfig, HcStreamEnd};
pub(crate) use rpc::HcError;

/// Env set by core for each terminal; passing ours would mislabel the child.
const CORE_OWNED_ENV: &[&str] = &["HC_TERMINAL_ID"];
pub(crate) const OWNER_LABEL: &str = "herdr.owner";
pub(crate) const PANE_LABEL: &str = "herdr.pane";

static PANE_BACKEND_HC: AtomicBool = AtomicBool::new(false);

pub(crate) fn set_pane_backend_enabled(enabled: bool) {
    PANE_BACKEND_HC.store(enabled, Ordering::Release);
}

pub(crate) fn pane_backend_enabled() -> bool {
    PANE_BACKEND_HC.load(Ordering::Acquire)
}

#[derive(Debug, Clone)]
pub(crate) struct RunTerminal {
    pub address: String,
    pub pid: Option<u32>,
}

/// Starts `cmd` in a new hc terminal; the caller attaches to the returned address.
pub(crate) fn run_terminal(
    cmd: &CommandBuilder,
    labels: &[(&str, String)],
) -> Result<RunTerminal, HcError> {
    let result = rpc::call(
        "terminal.run",
        run_params(cmd, labels),
        rpc::DEFAULT_CALL_TIMEOUT,
    )?;
    let address = result
        .get("address")
        .and_then(Value::as_str)
        .ok_or_else(|| HcError::Protocol("terminal.run reply without an address".into()))?
        .to_string();
    let pid = result
        .get("pid")
        .and_then(Value::as_u64)
        .and_then(|pid| u32::try_from(pid).ok());
    Ok(RunTerminal { address, pid })
}

/// Whether a saved terminal still runs a program we can reattach to.
pub(crate) fn terminal_is_live(address: &str) -> bool {
    match rpc::call(
        "terminal.get",
        json!({"terminal": address}),
        rpc::DEFAULT_CALL_TIMEOUT,
    ) {
        Ok(record) => record.get("state").and_then(Value::as_str) == Some("live"),
        Err(err) => {
            tracing::debug!(terminal = %address, err = %err, "saved herdr-core terminal unavailable");
            false
        }
    }
}

/// Ends the program; used when a pane is closed, not when herdr detaches.
pub(crate) fn kill_terminal(address: &str) -> Result<(), HcError> {
    rpc::call(
        "terminal.kill",
        json!({"terminal": address}),
        rpc::DEFAULT_CALL_TIMEOUT,
    )
    .map(|_| ())
}

fn run_params(cmd: &CommandBuilder, labels: &[(&str, String)]) -> Value {
    let argv: Vec<String> = if cmd.is_default_prog() {
        Vec::new()
    } else {
        cmd.get_argv()
            .iter()
            .map(|arg| arg.to_string_lossy().into_owned())
            .collect()
    };
    let env: Map<String, Value> = cmd
        .iter_full_env_as_str()
        .filter(|(key, _)| !CORE_OWNED_ENV.contains(key))
        .map(|(key, value)| (key.to_string(), Value::String(value.to_string())))
        .collect();
    let labels: Map<String, Value> = labels
        .iter()
        .map(|(key, value)| (key.to_string(), Value::String(value.clone())))
        .collect();
    let mut params = json!({
        "argv": argv,
        "env": env,
        "labels": labels,
        "no_parent": true,
        "no_notify": true,
    });
    if let Some(cwd) = cmd.get_cwd() {
        params["cwd"] = Value::String(cwd.to_string_lossy().into_owned());
    }
    params
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn run_params_carry_argv_cwd_env_and_labels_but_not_core_identity() {
        let mut cmd = CommandBuilder::new("claude");
        cmd.arg("--resume");
        cmd.cwd("/srv/app");
        cmd.env("HERDR_PANE_ID", "w1:p2");
        cmd.env("HC_TERMINAL_ID", "t9");
        let params = run_params(&cmd, &[(OWNER_LABEL, "default".into())]);
        assert_eq!(params["argv"], json!(["claude", "--resume"]));
        assert_eq!(params["cwd"], json!("/srv/app"));
        assert_eq!(params["env"]["HERDR_PANE_ID"], json!("w1:p2"));
        assert!(params["env"].get("HC_TERMINAL_ID").is_none());
        assert_eq!(params["labels"][OWNER_LABEL], json!("default"));
        assert_eq!(params["no_parent"], json!(true));
    }

    #[test]
    fn default_program_asks_core_for_the_machine_shell() {
        let cmd = CommandBuilder::new_default_prog();
        assert_eq!(run_params(&cmd, &[])["argv"], json!([]));
    }
}
