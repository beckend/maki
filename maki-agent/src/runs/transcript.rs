//! Per-turn transcripts of agent runs (design 7.4): one JSON message per line
//! in `<journal dir>/transcripts/<handle>.ndjson`, appended at each turn end,
//! so a crash loses at most the turn in flight and a cold revive can rebuild
//! the history.

use std::fs::{self, OpenOptions};
use std::io::{self, BufRead, BufReader, Write};
use std::path::Path;

use maki_providers::Message;

fn private_options() -> OpenOptions {
    let mut options = OpenOptions::new();
    options.create(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    options
}

fn encode(messages: &[Message]) -> io::Result<Vec<u8>> {
    let mut out = Vec::new();
    for message in messages {
        serde_json::to_writer(&mut out, message).map_err(io::Error::other)?;
        out.push(b'\n');
    }
    Ok(out)
}

fn ensure_parent(path: &Path) -> io::Result<()> {
    match path.parent() {
        Some(dir) => fs::create_dir_all(dir),
        None => Ok(()),
    }
}

/// Adds `messages` to the end of the file.
pub(super) fn append(path: &Path, messages: &[Message]) -> io::Result<()> {
    if messages.is_empty() {
        return Ok(());
    }
    ensure_parent(path)?;
    let data = encode(messages)?;
    let mut file = private_options().append(true).open(path)?;
    // A torn last line (a crash mid-write) must not swallow the next record.
    if file.metadata()?.len() > 0 && !ends_with_newline(path)? {
        file.write_all(b"\n")?;
    }
    file.write_all(&data)
}

fn ends_with_newline(path: &Path) -> io::Result<bool> {
    use std::io::{Read, Seek, SeekFrom};
    let mut file = fs::File::open(path)?;
    file.seek(SeekFrom::End(-1))?;
    let mut last = [0u8; 1];
    file.read_exact(&mut last)?;
    Ok(last[0] == b'\n')
}

/// Replaces the file: the history got shorter than what was saved (a repair
/// removed messages), so appending would leave stale ones.
pub(super) fn rewrite(path: &Path, messages: &[Message]) -> io::Result<()> {
    ensure_parent(path)?;
    let tmp = path.with_extension("ndjson.tmp");
    let mut file = private_options().write(true).truncate(true).open(&tmp)?;
    file.write_all(&encode(messages)?)?;
    file.sync_data()?;
    fs::rename(tmp, path)
}

/// The saved messages, `None` when there is no transcript. A torn or corrupt
/// line ends the read: what came before it is still a usable history.
pub(super) fn load(path: &Path) -> Option<Vec<Message>> {
    let file = fs::File::open(path).ok()?;
    let mut out = Vec::new();
    for line in BufReader::new(file).split(b'\n') {
        let Ok(line) = line else { break };
        if line.is_empty() {
            continue;
        }
        match serde_json::from_slice::<Message>(&line) {
            Ok(message) => out.push(message),
            Err(_) => break,
        }
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    use maki_providers::{ContentBlock, Role};

    use super::*;

    fn text(role: Role, s: &str) -> Message {
        Message {
            role,
            content: vec![ContentBlock::Text { text: s.into() }],
            ..Default::default()
        }
    }

    #[test]
    fn append_then_load_round_trips() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t").join("ag-a.ndjson");
        assert!(load(&path).is_none());
        append(&path, &[text(Role::User, "one")]).unwrap();
        append(&path, &[text(Role::Assistant, "two")]).unwrap();
        let got = load(&path).unwrap();
        assert_eq!(got.len(), 2);
        assert_eq!(got[1], text(Role::Assistant, "two"));
    }

    #[test]
    fn torn_tail_is_skipped_and_not_glued_to_the_next_line() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("ag-a.ndjson");
        append(&path, &[text(Role::User, "one")]).unwrap();
        let mut file = OpenOptions::new().append(true).open(&path).unwrap();
        file.write_all(b"{\"role\":\"ass").unwrap();
        assert_eq!(load(&path).unwrap().len(), 1);
        append(&path, &[text(Role::User, "three")]).unwrap();
        let got = load(&path).unwrap();
        assert_eq!(got.len(), 1, "a corrupt line ends the read");
    }

    #[test]
    fn rewrite_replaces_the_content() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("ag-a.ndjson");
        append(&path, &[text(Role::User, "a"), text(Role::User, "b")]).unwrap();
        rewrite(&path, &[text(Role::User, "c")]).unwrap();
        assert_eq!(load(&path).unwrap(), vec![text(Role::User, "c")]);
    }
}
