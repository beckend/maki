//! What the process-level tests need: a provider the binary can talk to on
//! loopback, an isolated home, and the binary itself.
//!
//! Each test binary compiles this module whole and uses part of it, so the
//! items one binary does not reach are allowed to stay unused here.
#![allow(dead_code)]

use std::fs;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, Command, Stdio};
use std::sync::mpsc::{Receiver, Sender, channel};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use serde_json::{Value, json};
use tempfile::TempDir;

/// The model the mock answers for. A built-in provider, so the binary resolves
/// it without a catalog download.
pub const MOCK_MODEL: &str = "anthropic/claude-sonnet-4-20250514";
/// The provider slug whose origin the mock takes over.
pub const MOCK_SLUG: &str = "anthropic";
/// The env var the anthropic provider reads its key from.
pub const MOCK_KEY_ENV: &str = "ANTHROPIC_API_KEY";
/// A key the mock never checks, so no real credential is involved.
pub const MOCK_KEY: &str = "not-a-credential";
/// How long the binary may take before a test gives up on it.
pub const DEADLINE: Duration = Duration::from_secs(30);
/// The init.lua that turns the native runs on.
pub const RUNS_INIT: &str = "maki.setup({ agent = { runs = { enabled = true } } })\n";

const BIND_FAILED: &str = "cannot bind the loopback provider";
const CACHE_DIR: &str = "cache";
const CONFIG_DIR: &str = "config";
const CONTENT_LENGTH: &str = "content-length";
const DATA_DIR: &str = "data";
const HOME: &str = "HOME";
const JOURNAL: &str = "journal.ndjson";
const LOOPBACK: &str = "127.0.0.1:0";
const LOG_EXTENSION: &str = "log";
const PATH: &str = "PATH";
const POLL: Duration = Duration::from_millis(20);
const RUNS_DIR: &str = "runs";
const SESSION_EXTENSION: &str = "jsonl";
const SESSIONS_DIR: &str = "sessions";
const STATE_DIR: &str = "state";
const TEMP_DIR: &str = "tmp";
const TEMPDIR_FAILED: &str = "cannot create the sandbox";
const TMP_DIR: &str = "TMPDIR";
/// The variables that name the directories a user keeps, and the directory
/// each one gets under the home of the sandbox.
const USER_DIRS: &[(&str, &str)] = &[
    ("XDG_CONFIG_HOME", CONFIG_DIR),
    ("XDG_STATE_HOME", STATE_DIR),
    ("XDG_DATA_HOME", DATA_DIR),
    ("XDG_CACHE_HOME", CACHE_DIR),
];

/// A provider on loopback that answers every request from one script and
/// keeps what the client sent.
pub struct MockProvider {
    base_url: String,
    requests: Arc<Mutex<Vec<Value>>>,
}

impl MockProvider {
    /// Serves `answer` for every request. The answer gets the index of the
    /// request and the body the client sent.
    pub fn serve(answer: impl Fn(usize, &Value) -> String + Send + Sync + 'static) -> Self {
        let listener = TcpListener::bind(LOOPBACK).expect(BIND_FAILED);
        let base_url = format!("http://{}", listener.local_addr().expect(BIND_FAILED));
        let requests = Arc::new(Mutex::new(Vec::new()));
        let log = Arc::clone(&requests);
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(mut stream) = stream else { return };
                let Some(body) = read_body(&mut stream) else {
                    continue;
                };
                let parsed: Value = serde_json::from_slice(&body).unwrap_or(Value::Null);
                let index = {
                    let mut guard = log.lock().unwrap_or_else(|e| e.into_inner());
                    guard.push(parsed.clone());
                    guard.len() - 1
                };
                write_sse(&mut stream, &answer(index, &parsed));
            }
        });
        Self { base_url, requests }
    }

    /// The origin the provider answers on.
    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    /// The bodies the client sent, in order.
    pub fn requests(&self) -> Vec<Value> {
        self.requests
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }
}

/// The body of one HTTP request, read off the wire as it came.
fn read_body(stream: &mut TcpStream) -> Option<Vec<u8>> {
    let mut reader = BufReader::new(stream);
    let mut length = 0usize;
    loop {
        let mut line = String::new();
        reader.read_line(&mut line).ok()?;
        let line = line.trim_end();
        if line.is_empty() {
            break;
        }
        if let Some((name, value)) = line.split_once(':')
            && name.eq_ignore_ascii_case(CONTENT_LENGTH)
        {
            length = value.trim().parse().unwrap_or(0);
        }
    }
    let mut body = vec![0u8; length];
    reader.read_exact(&mut body).ok()?;
    Some(body)
}

/// One SSE stream, with the length the client needs to read it.
fn write_sse(stream: &mut TcpStream, body: &str) {
    let response = format!(
        "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
        body.len()
    );
    let _ = stream.write_all(response.as_bytes());
    let _ = stream.flush();
}

/// One Anthropic SSE frame.
fn frame(event: &str, data: &Value) -> String {
    format!("event: {event}\ndata: {data}\n\n")
}

/// The stream that opens one tool call.
pub fn tool_use_sse(id: &str, name: &str, input: &Value) -> String {
    let mut out = frame(
        "message_start",
        &json!({"type":"message_start","message":{"usage":{"input_tokens":8,"output_tokens":1}}}),
    );
    out.push_str(&frame(
        "content_block_start",
        &json!({"type":"content_block_start","index":0,"content_block":{"type":"tool_use","id":id,"name":name}}),
    ));
    out.push_str(&frame(
        "content_block_delta",
        &json!({"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":input.to_string()}}),
    ));
    out.push_str(&frame(
        "content_block_stop",
        &json!({"type":"content_block_stop","index":0}),
    ));
    out.push_str(&frame(
        "message_delta",
        &json!({"type":"message_delta","delta":{"stop_reason":"tool_use"},"usage":{"output_tokens":5}}),
    ));
    out.push_str(&frame("message_stop", &json!({"type":"message_stop"})));
    out
}

/// The stream that ends the turn with text.
pub fn text_sse(text: &str) -> String {
    let mut out = frame(
        "message_start",
        &json!({"type":"message_start","message":{"usage":{"input_tokens":8,"output_tokens":1}}}),
    );
    out.push_str(&frame(
        "content_block_start",
        &json!({"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}),
    ));
    out.push_str(&frame(
        "content_block_delta",
        &json!({"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":text}}),
    ));
    out.push_str(&frame(
        "content_block_stop",
        &json!({"type":"content_block_stop","index":0}),
    ));
    out.push_str(&frame(
        "message_delta",
        &json!({"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":5}}),
    ));
    out.push_str(&frame("message_stop", &json!({"type":"message_stop"})));
    out
}

/// A home of its own: the child gets the directories below through its own
/// environment, so two tests never share a state dir, the child never sees
/// the machine's own directories or credentials, and the process the test
/// runs in keeps the environment it had.
pub struct Sandbox {
    root: TempDir,
}

impl Sandbox {
    /// The environment the spawned binary gets: the directories of this
    /// sandbox, the mock's origin and the mock's key, and nothing else.
    pub fn child_env(&self, provider: &MockProvider) -> Vec<(String, String)> {
        let mut env = vec![
            (
                HOME.to_owned(),
                self.root.path().to_string_lossy().into_owned(),
            ),
            (
                TMP_DIR.to_owned(),
                self.dir(TEMP_DIR).to_string_lossy().into_owned(),
            ),
        ];
        for (var, dir) in USER_DIRS {
            env.push((
                (*var).to_owned(),
                self.dir(dir).to_string_lossy().into_owned(),
            ));
        }
        env.push((
            maki_config::providers::base_url_env_var(MOCK_SLUG),
            provider.base_url().to_owned(),
        ));
        env.push((MOCK_KEY_ENV.to_owned(), MOCK_KEY.to_owned()));
        env
    }

    /// The state directory the binary keeps its sessions and its runs in.
    pub fn state_dir(&self) -> PathBuf {
        self.dir(STATE_DIR)
    }

    /// The directories holding a session's runs: `<state>/runs/<session>`.
    pub fn run_dirs(&self) -> Vec<PathBuf> {
        let mut out = Vec::new();
        let Ok(apps) = fs::read_dir(self.state_dir()) else {
            return out;
        };
        for app in apps.flatten() {
            let runs = app.path().join(RUNS_DIR);
            let Ok(sessions) = fs::read_dir(&runs) else {
                continue;
            };
            for session in sessions.flatten() {
                out.push(session.path());
            }
        }
        out
    }

    /// The log of every run under the state directory of the sandbox.
    pub fn run_logs(&self) -> Vec<PathBuf> {
        let mut out = Vec::new();
        for dir in self.run_dirs() {
            let Ok(entries) = fs::read_dir(&dir) else {
                continue;
            };
            for entry in entries.flatten() {
                let path = entry.path();
                if path.extension().is_some_and(|ext| ext == LOG_EXTENSION) {
                    out.push(path);
                }
            }
        }
        out
    }

    /// The transcripts of every session under the state directory of the
    /// sandbox.
    pub fn session_files(&self) -> Vec<PathBuf> {
        let mut out = Vec::new();
        let Ok(apps) = fs::read_dir(self.state_dir()) else {
            return out;
        };
        for app in apps.flatten() {
            let sessions = app.path().join(SESSIONS_DIR);
            let Ok(entries) = fs::read_dir(&sessions) else {
                continue;
            };
            for entry in entries.flatten() {
                let path = entry.path();
                if path.extension().is_some_and(|ext| ext == SESSION_EXTENSION) {
                    out.push(path);
                }
            }
        }
        out
    }

    /// The journal of every session under the state directory of the sandbox.
    pub fn run_journals(&self) -> Vec<PathBuf> {
        let mut out = Vec::new();
        for dir in self.run_dirs() {
            let journal = dir.join(JOURNAL);
            if journal.exists() {
                out.push(journal);
            }
        }
        out
    }

    fn dir(&self, dir: &str) -> PathBuf {
        self.root.path().join(dir)
    }
}

/// A sandbox of its own: a home, and the directories under it.
pub fn sandbox() -> Sandbox {
    let root = TempDir::new().expect(TEMPDIR_FAILED);
    for (_, dir) in USER_DIRS {
        fs::create_dir_all(root.path().join(dir)).expect(TEMPDIR_FAILED);
    }
    fs::create_dir_all(root.path().join(TEMP_DIR)).expect(TEMPDIR_FAILED);
    Sandbox { root }
}

/// A directory the binary can run in, and the project config that turns the
/// native runs on.
pub fn project() -> TempDir {
    let dir = TempDir::new().expect("the project dir");
    let config = dir.path().join(".maki");
    fs::create_dir_all(&config).expect("the project config dir");
    fs::write(config.join("init.lua"), RUNS_INIT).expect("the project init.lua");
    dir
}

/// Waits for `condition` to hold, polling until the deadline.
pub fn wait_for(mut condition: impl FnMut() -> bool) -> bool {
    let start = Instant::now();
    while start.elapsed() < DEADLINE {
        if condition() {
            return true;
        }
        std::thread::sleep(POLL);
    }
    condition()
}

/// What the binary wrote and how it ended.
pub struct Outcome {
    /// The exit code, or `None` when the process was still running at the
    /// deadline.
    pub code: Option<i32>,
    pub stdout: String,
    pub stderr: String,
}

/// The spawned binary: its stdin, and the lines it writes on stdout.
pub struct Binary {
    child: Child,
    stdin: Option<ChildStdin>,
    stdout: Arc<Mutex<String>>,
    stderr: Option<JoinHandle<String>>,
    lines: Receiver<String>,
    reader: Option<JoinHandle<()>>,
}

impl Binary {
    /// Starts the binary with `args`, in `cwd`, and with `env` as its whole
    /// environment.
    pub fn spawn(args: &[String], cwd: &Path, env: &[(String, String)]) -> Self {
        Self::start(args, cwd, env, false)
    }

    /// Starts the binary with its stdin open, for a server that reads from it.
    pub fn with_stdin(args: &[String], cwd: &Path, env: &[(String, String)]) -> Self {
        Self::start(args, cwd, env, true)
    }

    fn start(args: &[String], cwd: &Path, env: &[(String, String)], stdin: bool) -> Self {
        let mut command = Command::new(env!("CARGO_BIN_EXE_maki"));
        command.args(args).current_dir(cwd);
        // The child gets the environment the test hands it and nothing of the
        // machine the test runs on: no credential, no directory, no setting
        // of its own. The path to the tools stays, since a run is a shell
        // command.
        command.env_clear();
        if let Some(path) = std::env::var_os(PATH) {
            command.env(PATH, path);
        }
        for (key, value) in env {
            command.env(key, value);
        }
        command.stdout(Stdio::piped()).stderr(Stdio::piped());
        command.stdin(if stdin { Stdio::piped() } else { Stdio::null() });
        let mut child = command.spawn().expect("the binary starts");
        let stdin = child.stdin.take();
        let stdout = child.stdout.take().expect("the stdout pipe");
        let stderr = child.stderr.take().expect("the stderr pipe");
        let (sender, lines) = channel();
        let all = Arc::new(Mutex::new(String::new()));
        let kept = Arc::clone(&all);
        let reader = std::thread::spawn(move || read_lines(stdout, sender, kept));
        Self {
            child,
            stdin,
            stdout: all,
            stderr: Some(drain(stderr)),
            lines,
            reader: Some(reader),
        }
    }

    /// Writes one ndjson request to the binary.
    pub fn write_line(&mut self, line: &str) {
        let stdin = self.stdin.as_mut().expect("the binary reads stdin");
        stdin
            .write_all(line.as_bytes())
            .expect("the request goes out");
        stdin.write_all(b"\n").expect("the request goes out");
        stdin.flush().expect("the request goes out");
    }

    /// Closes the binary's stdin, which is how a server learns the client is
    /// done.
    pub fn close_stdin(&mut self) {
        self.stdin = None;
    }

    /// The next line the binary wrote, or `None` when none came in time.
    pub fn next_line(&self) -> Option<String> {
        self.lines.recv_timeout(DEADLINE).ok()
    }

    /// The response to the request with this id, read off the wire.
    pub fn response(&self, id: i64) -> Value {
        while let Some(line) = self.next_line() {
            let Ok(value) = serde_json::from_str::<Value>(&line) else {
                continue;
            };
            if value.get("id").and_then(Value::as_i64) == Some(id) {
                return value;
            }
        }
        panic!("no response to the request {id}");
    }

    /// Waits for the binary to end and collects what it wrote.
    pub fn finish(mut self) -> Outcome {
        let code = wait_for_exit(&mut self.child);
        if code.is_none() {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
        self.collect(code)
    }

    /// Kills the binary the way a crash would and collects what it wrote.
    pub fn kill(mut self) -> Outcome {
        let _ = self.child.kill();
        let _ = self.child.wait();
        self.collect(None)
    }

    fn collect(&mut self, code: Option<i32>) -> Outcome {
        if let Some(reader) = self.reader.take() {
            let _ = reader.join();
        }
        let stdout = self
            .stdout
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        let stderr = self
            .stderr
            .take()
            .and_then(|handle| handle.join().ok())
            .unwrap_or_default();
        Outcome {
            code,
            stdout,
            stderr,
        }
    }
}

impl Drop for Binary {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// Waits for the process to end, or gives up at the deadline.
fn wait_for_exit(child: &mut Child) -> Option<i32> {
    let start = Instant::now();
    while start.elapsed() < DEADLINE {
        match child.try_wait() {
            Ok(Some(status)) => return Some(status.code().unwrap_or(-1)),
            Ok(None) => std::thread::sleep(POLL),
            Err(_) => return None,
        }
    }
    None
}

/// Reads everything a pipe holds, line by line.
fn read_lines(reader: impl Read, sender: Sender<String>, all: Arc<Mutex<String>>) {
    let mut reader = BufReader::new(reader);
    let mut line = String::new();
    loop {
        line.clear();
        match reader.read_line(&mut line) {
            Ok(0) | Err(_) => return,
            Ok(_) => {
                let text = line.clone();
                all.lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .push_str(&text);
                if sender.send(text).is_err() {
                    return;
                }
            }
        }
    }
}

/// Reads everything a pipe holds, as one string.
fn drain(mut reader: impl Read + Send + 'static) -> JoinHandle<String> {
    std::thread::spawn(move || {
        let mut out = String::new();
        let _ = reader.read_to_string(&mut out);
        out
    })
}
