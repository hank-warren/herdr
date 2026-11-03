//! One pane's attach stream to a herdr-core terminal. Mirrors the PTY actor:
//! a reader thread feeds output to `on_read`, a writer thread serializes input,
//! query responses, credits and resizes onto the stream.

use std::io::{self, Read, Write};
use std::net::Shutdown;
use std::os::unix::net::UnixStream;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{mpsc as std_mpsc, Arc, Mutex};
use std::time::{Duration, Instant};

use bytes::Bytes;
use serde_json::{json, Value};
use tokio::sync::mpsc;
use tracing::{debug, warn};

use super::frame::{self, FrameKind, CONTROL_CHANNEL};
use super::rpc::{self, HcError};
use crate::pty::actor::PtyReadResult;

const OUTGOING_BUFFER: usize = 1024;
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);
const WRITE_TIMEOUT: Duration = Duration::from_secs(5);
const REATTACH_WINDOW: Duration = Duration::from_secs(30);
const MAX_HELLO_LINE: usize = 1024 * 1024;

pub(crate) type ReadCallback = Box<dyn FnMut(&[u8]) -> PtyReadResult + Send + 'static>;
pub(crate) type EndCallback = Box<dyn FnOnce(HcStreamEnd) + Send + 'static>;

/// Why a pane's terminal stopped streaming, as core reported it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct HcStreamEnd {
    /// Core's reason word (`exited`, `killed`, `server_stop`, ...), or
    /// `connection_lost` when reattaching gave up.
    pub reason: String,
    pub exit_code: Option<i32>,
}

impl HcStreamEnd {
    fn from_close_payload(payload: &[u8]) -> Self {
        let value: Value = serde_json::from_slice(payload).unwrap_or(Value::Null);
        Self::from_record(&value)
    }

    fn from_record(value: &Value) -> Self {
        Self {
            reason: value
                .get("reason")
                .and_then(Value::as_str)
                .unwrap_or("exited")
                .to_string(),
            exit_code: value
                .get("exit_code")
                .and_then(Value::as_i64)
                .and_then(|code| i32::try_from(code).ok()),
        }
    }

    fn connection_lost() -> Self {
        Self {
            reason: "connection_lost".into(),
            exit_code: None,
        }
    }

    /// The program itself ended (as opposed to core or the link going away).
    pub(crate) fn program_ended(&self) -> bool {
        matches!(self.reason.as_str(), "exited" | "killed" | "signaled")
    }
}

#[derive(Debug, Clone)]
pub(crate) struct HcAttach {
    pub terminal: String,
    pub rows: u16,
    pub cols: u16,
    pub history: u32,
}

pub(crate) struct HcPaneIoConfig {
    pub pane_id: u32,
    pub attach: HcAttach,
    pub on_read: ReadCallback,
    pub on_end: EndCallback,
}

enum Outgoing {
    Frame(FrameKind, Bytes),
    Submit {
        text: Bytes,
        enter: Bytes,
        delay: Duration,
        deadline: Option<Instant>,
        reply: std_mpsc::Sender<io::Result<()>>,
    },
}

/// The live link: replaced in place when the core restarts and we reattach.
struct Link {
    stream: Mutex<Option<UnixStream>>,
    channel: AtomicU32,
}

impl Link {
    fn write(&self, kind: FrameKind, payload: &[u8]) -> io::Result<()> {
        let channel = self.channel.load(Ordering::Acquire);
        let guard = self
            .stream
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let Some(stream) = guard.as_ref() else {
            return Err(io::ErrorKind::NotConnected.into());
        };
        let mut stream = stream;
        frame::write_frame(&mut stream, channel, kind, payload)
    }

    fn replace(&self, stream: UnixStream, channel: u32) {
        let mut guard = self
            .stream
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        self.channel.store(channel, Ordering::Release);
        *guard = Some(stream);
    }

    fn shutdown(&self) {
        let guard = self
            .stream
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(stream) = guard.as_ref() {
            let channel = self.channel.load(Ordering::Acquire);
            let mut writer = stream;
            let _ = frame::write_frame(&mut writer, channel, FrameKind::Close, &[]);
            let _ = stream.shutdown(Shutdown::Both);
        }
    }
}

pub(crate) struct HcPaneIo {
    terminal: String,
    pid: Arc<AtomicU32>,
    outgoing: std_mpsc::SyncSender<Outgoing>,
    accepting: Arc<AtomicBool>,
    closing: Arc<AtomicBool>,
    link: Arc<Link>,
    cell_px: Mutex<(u32, u32)>,
}

/// What the attach reply told us about the terminal.
pub(crate) struct AttachedTerminal {
    pub pid: Option<u32>,
}

impl HcPaneIo {
    pub(crate) fn spawn(config: HcPaneIoConfig) -> Result<(Self, AttachedTerminal), HcError> {
        let HcPaneIoConfig {
            pane_id,
            attach,
            on_read,
            on_end,
        } = config;
        let opened = open_stream(&attach, false)?;
        let attached = AttachedTerminal {
            pid: terminal_pid(&opened.terminal),
        };
        let pid = Arc::new(AtomicU32::new(attached.pid.unwrap_or(0)));
        let reader = io::BufReader::with_capacity(frame::READ_BUFFER, opened.stream.try_clone()?);
        let link = Arc::new(Link {
            stream: Mutex::new(Some(opened.stream)),
            channel: AtomicU32::new(opened.channel),
        });
        let (outgoing, outgoing_rx) = std_mpsc::sync_channel(OUTGOING_BUFFER);
        let accepting = Arc::new(AtomicBool::new(true));
        let closing = Arc::new(AtomicBool::new(false));

        {
            let link = link.clone();
            crate::thread_spawn::spawn_named(&format!("hc-write-{pane_id}"), move || {
                run_writer(&link, outgoing_rx)
            })?;
        }
        {
            let link = link.clone();
            let outgoing = outgoing.clone();
            let closing = closing.clone();
            let accepting = accepting.clone();
            let pid = pid.clone();
            crate::thread_spawn::spawn_named(&format!("hc-read-{pane_id}"), move || {
                let end = run_reader(ReaderContext {
                    pane_id,
                    attach,
                    reader,
                    link,
                    outgoing,
                    closing: closing.clone(),
                    pid,
                    on_read,
                });
                accepting.store(false, Ordering::Release);
                if let Some(end) = end {
                    if !closing.load(Ordering::Acquire) {
                        on_end(end);
                    }
                }
            })?;
        }

        Ok((
            Self {
                terminal: opened.address,
                pid,
                outgoing,
                accepting,
                closing,
                link,
                cell_px: Mutex::new((0, 0)),
            },
            attached,
        ))
    }

    pub(crate) fn terminal(&self) -> &str {
        &self.terminal
    }

    pub(crate) fn pid(&self) -> Option<u32> {
        Some(self.pid.load(Ordering::Acquire)).filter(|pid| *pid != 0)
    }

    pub(crate) fn foreground_process_group_id(&self) -> Option<u32> {
        crate::platform::foreground_process_group_id(self.pid()?)
    }

    pub(crate) fn try_write_user_input(
        &self,
        bytes: Bytes,
    ) -> Result<(), mpsc::error::TrySendError<Bytes>> {
        if !self.accepting.load(Ordering::Acquire) {
            return Err(mpsc::error::TrySendError::Closed(bytes));
        }
        match self
            .outgoing
            .try_send(Outgoing::Frame(FrameKind::Data, bytes))
        {
            Ok(()) => Ok(()),
            Err(std_mpsc::TrySendError::Full(Outgoing::Frame(_, bytes))) => {
                Err(mpsc::error::TrySendError::Full(bytes))
            }
            Err(std_mpsc::TrySendError::Disconnected(Outgoing::Frame(_, bytes))) => {
                Err(mpsc::error::TrySendError::Closed(bytes))
            }
            Err(_) => Err(mpsc::error::TrySendError::Closed(Bytes::new())),
        }
    }

    pub(crate) fn write_terminal_response(&self, response: impl FnOnce() -> Option<Bytes>) {
        let Some(bytes) = response().filter(|bytes| !bytes.is_empty()) else {
            return;
        };
        // Responses must not be dropped; block briefly rather than lose ordering.
        let _ = self.outgoing.send(Outgoing::Frame(FrameKind::Data, bytes));
    }

    pub(crate) fn queue_user_input_submission(
        &self,
        text: Bytes,
        enter: Bytes,
        delay: Duration,
        deadline: Option<Instant>,
    ) -> io::Result<std_mpsc::Receiver<io::Result<()>>> {
        if !self.accepting.load(Ordering::Acquire) {
            return Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "herdr-core stream closed",
            ));
        }
        let (reply, reply_rx) = std_mpsc::channel();
        self.outgoing
            .try_send(Outgoing::Submit {
                text,
                enter,
                delay,
                deadline,
                reply,
            })
            .map_err(|err| match err {
                std_mpsc::TrySendError::Full(_) => {
                    io::Error::new(io::ErrorKind::WouldBlock, "herdr-core input queue is full")
                }
                std_mpsc::TrySendError::Disconnected(_) => {
                    io::Error::new(io::ErrorKind::BrokenPipe, "herdr-core stream closed")
                }
            })?;
        Ok(reply_rx)
    }

    pub(crate) fn resize(
        &self,
        rows: u16,
        cols: u16,
        cell_width_px: u32,
        cell_height_px: u32,
        terminal_responses: Vec<Bytes>,
    ) {
        let mut payload = [0u8; 4];
        payload[..2].copy_from_slice(&cols.to_le_bytes());
        payload[2..].copy_from_slice(&rows.to_le_bytes());
        let _ = self.outgoing.send(Outgoing::Frame(
            FrameKind::Resize,
            Bytes::copy_from_slice(&payload),
        ));
        let cell = (cell_width_px, cell_height_px);
        let cell_changed = {
            let mut last = self
                .cell_px
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let changed = cell.0 > 0 && cell.1 > 0 && *last != cell;
            if changed {
                *last = cell;
            }
            changed
        };
        if cell_changed {
            let facts = json!({"cell": [cell.0, cell.1]}).to_string();
            let _ = self
                .outgoing
                .send(Outgoing::Frame(FrameKind::Facts, Bytes::from(facts)));
        }
        for response in terminal_responses {
            let _ = self
                .outgoing
                .send(Outgoing::Frame(FrameKind::Data, response));
        }
    }

    /// Detach without ending the program.
    pub(crate) fn shutdown(&self) {
        self.accepting.store(false, Ordering::Release);
        if !self.closing.swap(true, Ordering::AcqRel) {
            self.link.shutdown();
        }
    }
}

impl Drop for HcPaneIo {
    fn drop(&mut self) {
        self.shutdown();
    }
}

fn run_writer(link: &Link, outgoing: std_mpsc::Receiver<Outgoing>) {
    while let Ok(item) = outgoing.recv() {
        match item {
            Outgoing::Frame(kind, payload) => {
                if let Err(err) = link.write(kind, &payload) {
                    debug!(err = %err, "hc stream write failed");
                }
            }
            Outgoing::Submit {
                text,
                enter,
                delay,
                deadline,
                reply,
            } => {
                if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
                    let _ = reply.send(Err(io::Error::new(
                        io::ErrorKind::TimedOut,
                        "input submission expired before it was written",
                    )));
                    continue;
                }
                let result = link.write(FrameKind::Data, &text).and_then(|()| {
                    std::thread::sleep(delay);
                    link.write(FrameKind::Data, &enter)
                });
                let _ = reply.send(result);
            }
        }
    }
}

/// Matches the PTY actor's read size, so herdr parses hc output in the same chunks.
const MAX_PARSE_BATCH: usize = 64 * 1024;

struct ReaderContext {
    pane_id: u32,
    attach: HcAttach,
    reader: io::BufReader<UnixStream>,
    link: Arc<Link>,
    outgoing: std_mpsc::SyncSender<Outgoing>,
    closing: Arc<AtomicBool>,
    pid: Arc<AtomicU32>,
    on_read: ReadCallback,
}

/// Returns how the stream ended, or `None` when we detached on purpose.
fn run_reader(mut ctx: ReaderContext) -> Option<HcStreamEnd> {
    loop {
        let channel = ctx.link.channel.load(Ordering::Acquire);
        let frame = match frame::read_frame(&mut ctx.reader) {
            Ok(Some(frame)) => frame,
            Ok(None) | Err(_) if ctx.closing.load(Ordering::Acquire) => return None,
            Ok(None) | Err(_) => match reattach(&mut ctx) {
                Ok(()) => continue,
                Err(end) => return Some(end),
            },
        };
        if frame.channel != channel {
            // Channel 0 carries no replies we wait for after attach.
            continue;
        }
        match frame.kind {
            FrameKind::Data => {
                let mut batch = frame.payload;
                frame::coalesce_buffered_data(
                    &mut ctx.reader,
                    channel,
                    &mut batch,
                    MAX_PARSE_BATCH,
                );
                let len = batch.len();
                let result = (ctx.on_read)(&batch);
                for response in result.terminal_responses {
                    let _ = ctx
                        .outgoing
                        .send(Outgoing::Frame(FrameKind::Data, response));
                }
                let credit = u32::try_from(len).unwrap_or(u32::MAX);
                let _ = ctx.outgoing.send(Outgoing::Frame(
                    FrameKind::Credit,
                    Bytes::copy_from_slice(&credit.to_le_bytes()),
                ));
            }
            FrameKind::Close => {
                if ctx.closing.load(Ordering::Acquire) {
                    return None;
                }
                let end = HcStreamEnd::from_close_payload(&frame.payload);
                if end.program_ended() {
                    return Some(end);
                }
                // Core stopping or restarting: the program may live on in the next core.
                match reattach(&mut ctx) {
                    Ok(()) => continue,
                    Err(lost) => {
                        return Some(if lost.reason == "connection_lost" {
                            end
                        } else {
                            lost
                        })
                    }
                }
            }
            FrameKind::Resize | FrameKind::Credit | FrameKind::Focus | FrameKind::Facts => {}
        }
    }
}

fn reattach(ctx: &mut ReaderContext) -> Result<(), HcStreamEnd> {
    let deadline = Instant::now() + REATTACH_WINDOW;
    let mut delay = Duration::from_millis(100);
    loop {
        if ctx.closing.load(Ordering::Acquire) {
            return Err(HcStreamEnd::connection_lost());
        }
        match open_stream(&ctx.attach, true) {
            Ok(opened) => {
                let reader = opened.stream.try_clone().map_err(|err| {
                    warn!(pane = ctx.pane_id, err = %err, "hc reattach clone failed");
                    HcStreamEnd::connection_lost()
                })?;
                if let Some(pid) = terminal_pid(&opened.terminal) {
                    ctx.pid.store(pid, Ordering::Release);
                }
                ctx.link.replace(opened.stream, opened.channel);
                ctx.reader = io::BufReader::with_capacity(frame::READ_BUFFER, reader);
                debug!(pane = ctx.pane_id, terminal = %ctx.attach.terminal, "hc stream reattached");
                return Ok(());
            }
            Err(HcError::Rpc { kind, message }) => {
                debug!(pane = ctx.pane_id, %kind, %message, "hc reattach refused");
                return Err(finished_record(&ctx.attach.terminal));
            }
            Err(err) if Instant::now() >= deadline => {
                warn!(pane = ctx.pane_id, err = %err, "hc reattach gave up");
                return Err(HcStreamEnd::connection_lost());
            }
            Err(_) => {
                std::thread::sleep(delay);
                delay = (delay * 2).min(Duration::from_secs(2));
            }
        }
    }
}

fn finished_record(terminal: &str) -> HcStreamEnd {
    rpc::call(
        "terminal.get",
        json!({"terminal": terminal}),
        rpc::DEFAULT_CALL_TIMEOUT,
    )
    .map(|record| HcStreamEnd::from_record(&record))
    .unwrap_or_else(|_| HcStreamEnd::connection_lost())
}

fn terminal_pid(terminal: &Value) -> Option<u32> {
    terminal
        .get("pid")
        .and_then(Value::as_u64)
        .and_then(|pid| u32::try_from(pid).ok())
        .filter(|pid| *pid != 0)
}

struct OpenedStream {
    stream: UnixStream,
    channel: u32,
    address: String,
    terminal: Value,
}

fn open_stream(attach: &HcAttach, resumed: bool) -> Result<OpenedStream, HcError> {
    let stream = rpc::connect()?;
    stream.set_read_timeout(Some(HANDSHAKE_TIMEOUT))?;
    stream.set_write_timeout(Some(WRITE_TIMEOUT))?;

    let hello_id = rpc::next_request_id();
    let mut hello = serde_json::to_vec(&rpc::request(hello_id, "hello", json!({"framed": true})))
        .map_err(|err| HcError::Protocol(err.to_string()))?;
    hello.push(b'\n');
    (&stream).write_all(&hello)?;
    let hello_reply = read_plain_line(&stream)?;
    let hello_reply: Value =
        serde_json::from_slice(&hello_reply).map_err(|err| HcError::Protocol(err.to_string()))?;
    let hello_result = rpc::response_result(hello_reply)?;
    if hello_result.get("framed").and_then(Value::as_bool) != Some(true) {
        return Err(HcError::Protocol(
            "core did not switch to framed mode".into(),
        ));
    }

    let attach_id = rpc::next_request_id();
    let request = rpc::request(
        attach_id,
        "terminal.attach",
        json!({
            "terminal": attach.terminal,
            "cols": attach.cols.max(1),
            "rows": attach.rows.max(1),
            "history": if resumed { 0 } else { attach.history },
            "resumed": resumed,
        }),
    );
    let request = serde_json::to_vec(&request).map_err(|err| HcError::Protocol(err.to_string()))?;
    frame::write_frame(&mut &stream, CONTROL_CHANNEL, FrameKind::Data, &request)?;

    let result = loop {
        let Some(reply) = frame::read_frame(&mut &stream)? else {
            return Err(HcError::Protocol(
                "core closed the stream during attach".into(),
            ));
        };
        if reply.channel != CONTROL_CHANNEL || reply.kind != FrameKind::Data {
            continue;
        }
        let message: Value = serde_json::from_slice(&reply.payload)
            .map_err(|err| HcError::Protocol(err.to_string()))?;
        if message.get("id").and_then(Value::as_u64) == Some(attach_id) {
            break rpc::response_result(message)?;
        }
    };
    stream.set_read_timeout(None)?;

    let channel = result
        .get("channel")
        .and_then(Value::as_u64)
        .and_then(|channel| u32::try_from(channel).ok())
        .ok_or_else(|| HcError::Protocol("attach reply without a channel".into()))?;
    let terminal = result.get("terminal").cloned().unwrap_or(Value::Null);
    let address = terminal
        .get("address")
        .and_then(Value::as_str)
        .unwrap_or(&attach.terminal)
        .to_string();
    Ok(OpenedStream {
        stream,
        channel,
        address,
        terminal,
    })
}

/// Reads the plain-mode hello reply byte by byte so no framed bytes are consumed.
fn read_plain_line(mut stream: &UnixStream) -> Result<Vec<u8>, HcError> {
    let mut line = Vec::with_capacity(256);
    let mut byte = [0u8; 1];
    loop {
        match stream.read(&mut byte) {
            Ok(0) => {
                return Err(HcError::Protocol(
                    "core closed the connection during hello".into(),
                ))
            }
            Ok(_) if byte[0] == b'\n' => return Ok(line),
            Ok(_) => {
                if line.len() >= MAX_HELLO_LINE {
                    return Err(HcError::Protocol("hello reply too long".into()));
                }
                line.push(byte[0]);
            }
            Err(err) if err.kind() == io::ErrorKind::Interrupted => {}
            Err(err) => return Err(err.into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn close_reasons_distinguish_program_end_from_core_going_away() {
        let exited =
            HcStreamEnd::from_close_payload(br#"{"reason":"exited","exit_code":3,"signal":null}"#);
        assert_eq!(exited.exit_code, Some(3));
        assert!(exited.program_ended());
        assert!(!HcStreamEnd::from_close_payload(br#"{"reason":"server_stop"}"#).program_ended());
        assert!(
            !HcStreamEnd::from_close_payload(br#"{"reason":"stopped_by_restart"}"#).program_ended()
        );
        assert!(HcStreamEnd::from_close_payload(b"").program_ended());
    }
}
