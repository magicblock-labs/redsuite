use std::{
    borrow::Cow,
    collections::{BTreeMap, VecDeque},
    fs::File,
    io::{self, Read, Seek, SeekFrom},
    path::Path,
};

use tracing::{error, warn, Level};

use crate::resources::LaunchRecord;

const MAX_BYTES: u64 = 1024 * 1024;
const MAX_LINES: usize = 20;
const MAX_LINE_BYTES: usize = 2048;

/// Recent context for a failure; shared logs may include earlier scenarios.
pub(super) fn emit(shared: Option<&Path>, launches: &[LaunchRecord]) {
    let mut sources = BTreeMap::new();
    if let Some(path) = shared {
        let label = path.file_stem().unwrap_or_default().to_string_lossy();
        sources.insert(path, label.trim_start_matches("er-").to_owned());
    }
    for launch in launches {
        sources.entry(Path::new(&launch.log)).or_insert_with(|| launch.label.clone());
    }
    for (path, node) in sources {
        let mut entries = VecDeque::new();
        for source in [path.with_extension("log.prev"), path.to_owned()] {
            let content = match tail(&source) {
                Ok(content) => content,
                Err(error) => {
                    if error.kind() != io::ErrorKind::NotFound {
                        warn!(%node, log = %source.display(), %error,
                            "cannot read recent validator logs");
                    }
                    continue;
                }
            };
            for line in content.lines() {
                let text = strip_ansi(line);
                let Some(level) = severity(&text) else { continue };
                let mut text = text.into_owned();
                if text.len() > MAX_LINE_BYTES {
                    text.truncate(text.floor_char_boundary(MAX_LINE_BYTES));
                    text.push_str(" ... [truncated]");
                }
                if entries.len() == MAX_LINES {
                    entries.pop_front();
                }
                entries.push_back((source.clone(), level, text));
            }
        }
        if entries.is_empty() {
            warn!(%node, log = %path.display(), "no recent validator WARN/ERROR entries available");
        }
        for (source, level, text) in entries {
            if level == Level::ERROR {
                error!(%node, log = %source.display(), validator_line = %text,
                    "recent validator logs");
            } else {
                warn!(%node, log = %source.display(), validator_line = %text,
                    "recent validator logs");
            }
        }
    }
}

fn tail(path: &Path) -> io::Result<String> {
    let mut file = File::open(path)?;
    let start = file.metadata()?.len().saturating_sub(MAX_BYTES);
    file.seek(SeekFrom::Start(start))?;
    let mut bytes = Vec::new();
    file.take(MAX_BYTES).read_to_end(&mut bytes)?;
    // The byte limit may cut through a line; discard that first fragment.
    let first = if start == 0 {
        0
    } else {
        bytes.iter().position(|byte| *byte == b'\n').map_or(bytes.len(), |i| i + 1)
    };
    Ok(String::from_utf8_lossy(&bytes[first..]).into_owned())
}

fn severity(line: &str) -> Option<Level> {
    let mut fields = line.split_whitespace();
    let first = fields.next()?;
    let level = if first.as_bytes().first().is_some_and(u8::is_ascii_digit) && first.contains('T') {
        fields.next()?
    } else {
        first
    };
    match level {
        "WARN" => Some(Level::WARN),
        "ERROR" => Some(Level::ERROR),
        _ => None,
    }
}

fn strip_ansi(line: &str) -> Cow<'_, str> {
    if !line.contains('\u{1b}') {
        return Cow::Borrowed(line);
    }
    let mut clean = String::with_capacity(line.len());
    let mut chars = line.chars();
    while let Some(ch) = chars.next() {
        if ch == '\u{1b}' && chars.next() == Some('[') {
            for ch in chars.by_ref() {
                if ('@'..='~').contains(&ch) {
                    break;
                }
            }
        } else {
            clean.push(ch);
        }
    }
    Cow::Owned(clean)
}
