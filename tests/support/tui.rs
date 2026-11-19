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

    /// Waits for the child to draw `needle` on the screen, polling until
    /// the deadline.
    ///
    /// The search is row by row: the terminal writes only the cells that
    /// change, so a word can come in pieces, and the pieces are one word
    /// on the screen. A word that is drawn across two rows is not a word
    /// on the screen, so it does not match.
    pub fn wait_for(&self, needle: &str) -> bool {
        super::wait_for(|| self.shows(needle))
    }

    /// Waits for the terminal title to carry `needle`, polling until the
    /// deadline.
    ///
    /// The title is one escape sequence that the program writes whole, so
    /// it is not split the way the cells of the screen are.
    pub fn wait_for_title(&self, needle: &str) -> bool {
        super::wait_for(|| self.drawn().contains(needle))
    }

    /// Whether the screen holds `needle` on one of its rows.
    pub fn shows(&self, needle: &str) -> bool {
        self.screen().iter().any(|row| row.contains(needle))
    }

    /// The screen the child has drawn, as rows of text.
    pub fn screen(&self) -> Vec<String> {
        Screen::of(&self.bytes()).text()
    }

    /// Waits for the child to draw `needle` on the screen after the byte
    /// it had reached when the mark was taken, polling until the
    /// deadline.
    ///
    /// Only a row that changed after the mark counts: the word was drawn
    /// after it, not before.
    pub fn wait_for_since(&self, mark: usize, needle: &str) -> bool {
        let before = self.screen_at(mark);
        super::wait_for(|| {
            self.screen()
                .iter()
                .zip(before.iter())
                .any(|(row, old)| row != old && row.contains(needle))
        })
    }

    /// Waits for the terminal title to carry `needle` after the byte it
    /// had reached when the mark was taken, polling until the deadline.
    pub fn wait_for_title_since(&self, mark: usize, needle: &str) -> bool {
        super::wait_for(|| self.drawn_since(mark).contains(needle))
    }

    /// The screen the child had drawn when it had reached the byte the
    /// mark was taken at.
    fn screen_at(&self, mark: usize) -> Vec<String> {
        let bytes = self.bytes();
        Screen::of(&bytes[..mark.min(bytes.len())]).text()
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

/// The screen the bytes draw, as rows of text.
pub fn screen_of(bytes: &[u8]) -> Vec<String> {
    Screen::of(bytes).text()
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

/// The screen the child drew, as rows of text.
///
/// The terminal writes only the cells that change, so a word can come in
/// pieces: the diff skips a cell whose character did not change and moves
/// the cursor over it. The grid puts the pieces back together, so a search
/// finds the word the user sees and not the pieces the terminal sent.
struct Screen {
    rows: Vec<Vec<char>>,
    row: usize,
    col: usize,
}

impl Screen {
    /// The screen the bytes draw.
    fn of(bytes: &[u8]) -> Self {
        let mut screen = Self {
            rows: vec![vec![' '; COLUMNS as usize]; ROWS as usize],
            row: 0,
            col: 0,
        };
        let text: Vec<char> = String::from_utf8_lossy(bytes).chars().collect();
        let mut index = 0;
        while index < text.len() {
            let c = text[index];
            index += 1;
            match c {
                '\u{1b}' => index = screen.escape(&text, index),
                '\r' => screen.col = 0,
                '\n' => screen.row = (screen.row + 1).min(ROWS as usize - 1),
                '\t' => screen.col = (screen.col + 8 - screen.col % 8).min(COLUMNS as usize - 1),
                c if (c as u32) < 0x20 => {}
                c => screen.put(c),
            }
        }
        screen
    }

    /// The rows of the screen, as text.
    fn text(&self) -> Vec<String> {
        self.rows
            .iter()
            .map(|row| row.iter().collect::<String>().trim_end().to_owned())
            .collect()
    }

    /// An escape sequence, at the character after the escape. It returns
    /// the index of the first character after it.
    ///
    /// A control sequence moves the cursor and a title does not: only the
    /// cursor decides where the next character goes.
    fn escape(&mut self, text: &[char], index: usize) -> usize {
        match text.get(index) {
            Some('[') => self.control(text, index + 1),
            Some(']') => self.title(text, index + 1),
            Some('(') | Some(')') | Some('#') | Some('%') => index + 2,
            _ => index,
        }
    }

    /// A control sequence: its parameters and the byte that ends it.
    fn control(&mut self, text: &[char], index: usize) -> usize {
        let mut params = String::new();
        let mut index = index;
        while let Some(&c) = text.get(index) {
            index += 1;
            if ('\u{40}'..='\u{7e}').contains(&c) {
                self.cursor(&params, c);
                return index;
            }
            params.push(c);
        }
        index
    }

    /// What the control sequence does to the cursor.
    fn cursor(&mut self, params: &str, last: char) {
        let numbers: Vec<usize> = params
            .trim_start_matches(['?', '>'])
            .split(';')
            .map(|number| number.parse().unwrap_or(0))
            .collect();
        let first = numbers.first().copied().unwrap_or(0);
        let second = numbers.get(1).copied().unwrap_or(0);
        match last {
            'H' | 'f' => {
                self.row = (first.max(1) - 1).min(ROWS as usize - 1);
                self.col = (second.max(1) - 1).min(COLUMNS as usize - 1);
            }
            'A' => self.row = self.row.saturating_sub(first.max(1)),
            'B' => self.row = (self.row + first.max(1)).min(ROWS as usize - 1),
            'C' => self.col = (self.col + first.max(1)).min(COLUMNS as usize - 1),
            'D' => self.col = self.col.saturating_sub(first.max(1)),
            'J' => self.erase_display(first),
            'K' => self.erase_line(first),
            _ => {}
        }
    }

    /// The erase of the screen: everything, or from the cursor down.
    fn erase_display(&mut self, mode: usize) {
        let (from, to) = match mode {
            0 => (self.row, ROWS as usize),
            1 => (0, self.row + 1),
            _ => (0, ROWS as usize),
        };
        for row in from..to.min(ROWS as usize) {
            self.rows[row].fill(' ');
        }
    }

    /// The erase of the line: the line, or from the cursor to its end.
    fn erase_line(&mut self, mode: usize) {
        let (from, to) = match mode {
            0 => (self.col, COLUMNS as usize),
            1 => (0, self.col + 1),
            _ => (0, COLUMNS as usize),
        };
        for col in from..to.min(COLUMNS as usize) {
            self.rows[self.row][col] = ' ';
        }
    }

    /// A title: the text the terminal shows in its title bar. It is not on
    /// the screen, and it ends at the bell or at the string terminator.
    fn title(&mut self, text: &[char], index: usize) -> usize {
        let mut index = index;
        while let Some(&c) = text.get(index) {
            index += 1;
            if c == '\u{7}' {
                return index;
            }
            if c == '\u{1b}' {
                return index + 1;
            }
        }
        index
    }

    /// A character on the screen.
    fn put(&mut self, c: char) {
        if self.col >= COLUMNS as usize {
            return;
        }
        self.rows[self.row][self.col] = c;
        self.col += 1;
    }
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
