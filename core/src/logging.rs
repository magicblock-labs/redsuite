use std::{
    borrow::Cow,
    fs::File,
    io::{self, Read, Seek, SeekFrom},
    path::Path,
};

#[path = "../../shared/logging.rs"]
mod subscriber;
pub use subscriber::init;

/// Strips terminal control sequences from one line of process output.
pub fn strip_ansi(line: &str) -> Cow<'_, str> {
    if !line.contains('\u{1b}') {
        return Cow::Borrowed(line);
    }
    let mut clean = String::with_capacity(line.len());
    let mut chars = line.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch != '\u{1b}' {
            clean.push(ch);
        } else if chars.next_if_eq(&'[').is_some() {
            for ch in chars.by_ref() {
                if ('@'..='~').contains(&ch) {
                    break;
                }
            }
        }
    }
    Cow::Owned(clean)
}

pub(crate) fn read_log_tail(path: &Path) -> io::Result<String> {
    const MAX_BYTES: u64 = 1024 * 1024;
    let mut file = File::open(path)?;
    let start = file.metadata()?.len().saturating_sub(MAX_BYTES);
    file.seek(SeekFrom::Start(start))?;
    let mut bytes = Vec::new();
    file.take(MAX_BYTES).read_to_end(&mut bytes)?;
    // Drop an incomplete first line when the byte cap cuts through it.
    let first = if start == 0 {
        0
    } else {
        bytes.iter().position(|byte| *byte == b'\n').map_or(bytes.len(), |i| i + 1)
    };
    Ok(String::from_utf8_lossy(&bytes[first..]).into_owned())
}
