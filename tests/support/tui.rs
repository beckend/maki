//! The binary under a pty: the TUI draws on a terminal and reads its keys
//! from one, so a test that drives it has to give it one.

use std::fs::File;
use std::io::{Read, Write};
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::os::unix::process::CommandExt;
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;

/// The rows the child draws on. A viewport of nothing renders nothing.
const ROWS: u16 = 40;
/// The columns the child draws on.
const COLUMNS: u16 = 120;
/// How many bytes one read takes off the pty.
const CHUNK: usize = 8192;
/// The path to the tools the child runs, kept out of the machine's own
/// environment, like in [`super::Binary`].
const PATH: &str = "PATH";

/// The binary under a pty: what it drew, and the keys a user would type.
pub struct Tui {
    child: Child,
    master: File,
    drawn: Arc<Mutex<Vec<u8>>>,
    stderr: Arc<Mutex<String>>,
    reader: Option<JoinHandle<()>>,
    errors: Option<JoinHandle<()>>,
}

impl Tui {
    /// Starts the binary under a pty, in `cwd`, with `env` as its whole
    /// environment. `None` when the machine has no pty to give: a sandbox
    /// without `/dev/ptmx` cannot show a TUI, and the test says so instead
    /// of failing.
    pub fn spawn(args: &[String], cwd: &Path, env: &[(String, String)]) -> Option<Self> {
        let (master, slave) = open_pty()?;
        let mut command = Command::new(env!("CARGO_BIN_EXE_maki"));
        command.args(args).current_dir(cwd);
        command.env_clear();
        if let Some(path) = std::env::var_os(PATH) {
            command.env(PATH, path);
        }
        for (key, value) in env {
            command.env(key, value);
        }
        command.stdout(Stdio::from(slave.try_clone().ok()?));
        command.stderr(Stdio::piped());
        command.stdin(Stdio::from(slave));
        unsafe {
            // The child gets a session of its own, so it never sits in the
            // test runner's process group and never takes its terminal.
            command.pre_exec(|| {
                if libc::setsid() < 0 {
                    return Err(std::io::Error::last_os_error());
                }
                Ok(())
            });
        }
        let mut child = command.spawn().ok()?;
        let stderr = Arc::new(Mutex::new(String::new()));
        let errors = child.stderr.take().map(|pipe| read_errors(pipe, &stderr));
        let drawn = Arc::new(Mutex::new(Vec::new()));
        let reader = read_in_background(&master, &drawn);
        Some(Self {
            child,
            master: master.into(),
            drawn,
            stderr,
            reader: Some(reader),
            errors,
        })
    }

    /// Writes the keys a user would type.
    pub fn write(&mut self, keys: &[u8]) {
        let _ = self.master.write_all(keys);
        let _ = self.master.flush();
    }

    /// Everything the child drew, as far as it is text.
    pub fn drawn(&self) -> String {
        String::from_utf8_lossy(&self.bytes()).into_owned()
    }

    /// Everything the child drew, as it came off the terminal.
    pub fn bytes(&self) -> Vec<u8> {
        self.drawn.lock().unwrap_or_else(|e| e.into_inner()).clone()
    }

    /// Where the child has drawn to, so a test can look at what came after.
    pub fn mark(&self) -> usize {
        self.drawn.lock().unwrap_or_else(|e| e.into_inner()).len()
    }

    /// What the child drew after the byte it had reached when the mark was
    /// taken.
    pub fn drawn_since(&self, mark: usize) -> String {
        let bytes = self.bytes();
        String::from_utf8_lossy(&bytes[mark.min(bytes.len())..]).into_owned()
    }

    /// Waits for the child to draw `needle`, polling until the deadline.
    pub fn wait_for(&self, needle: &str) -> bool {
        super::wait_for(|| self.drawn().contains(needle))
    }

    /// Waits for the child to draw `needle` after the byte it had reached
    /// when the mark was taken, polling until the deadline.
    pub fn wait_for_since(&self, mark: usize, needle: &str) -> bool {
        super::wait_for(|| self.drawn_since(mark).contains(needle))
    }

    /// What the child wrote on stderr so far.
    pub fn stderr(&self) -> String {
        self.stderr
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    /// Whether the child is still running.
    pub fn alive(&mut self) -> bool {
        matches!(self.child.try_wait(), Ok(None))
    }
}

impl Drop for Tui {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        if let Some(reader) = self.reader.take() {
            let _ = reader.join();
        }
        if let Some(errors) = self.errors.take() {
            let _ = errors.join();
        }
    }
}

/// A pty pair, or nothing when the machine has none.
fn open_pty() -> Option<(OwnedFd, OwnedFd)> {
    let mut master: libc::c_int = -1;
    let mut slave: libc::c_int = -1;
    let size = libc::winsize {
        ws_row: ROWS,
        ws_col: COLUMNS,
        ws_xpixel: 0,
        ws_ypixel: 0,
    };
    let opened = unsafe {
        libc::openpty(
            &mut master,
            &mut slave,
            std::ptr::null_mut(),
            std::ptr::null_mut(),
            &size,
        )
    };
    if opened != 0 {
        return None;
    }
    Some(unsafe { (OwnedFd::from_raw_fd(master), OwnedFd::from_raw_fd(slave)) })
}

/// Reads the pty from the start, in a thread: a pty nobody drains fills up
/// and blocks the child that writes to it.
fn read_in_background(master: &OwnedFd, drawn: &Arc<Mutex<Vec<u8>>>) -> JoinHandle<()> {
    let read_end = unsafe { File::from_raw_fd(libc::dup(master.as_raw_fd())) };
    let kept = Arc::clone(drawn);
    std::thread::spawn(move || {
        let mut read_end = read_end;
        let mut buf = [0u8; CHUNK];
        loop {
            match read_end.read(&mut buf) {
                Ok(0) | Err(_) => return,
                Ok(n) => kept
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .extend_from_slice(&buf[..n]),
            }
        }
    })
}

/// Reads the child's errors, so a pipe nobody drains never fills up and
/// blocks the child that writes to it.
fn read_errors(
    mut pipe: impl Read + Send + 'static,
    errors: &Arc<Mutex<String>>,
) -> JoinHandle<()> {
    let kept = Arc::clone(errors);
    std::thread::spawn(move || {
        let mut out = String::new();
        let _ = pipe.read_to_string(&mut out);
        kept.lock()
            .unwrap_or_else(|e| e.into_inner())
            .push_str(&out);
    })
}
