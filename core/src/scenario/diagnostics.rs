use std::{
    collections::{BTreeMap, VecDeque},
    io,
    path::Path,
};

use tracing::{debug, enabled, info, Level};

use crate::{
    logging::{read_log_tail, strip_ansi},
    resources::LaunchRecord,
};

const MAX_LINES: usize = 8;
const MAX_LINE_BYTES: usize = 2048;

/// Recent context for a failure; shared logs may include earlier scenarios.
pub(super) fn emit(shared: Option<&Path>, launches: &[LaunchRecord]) {
    let mut sources = BTreeMap::new();
    if let Some(path) = shared {
        let label = path.file_stem().unwrap_or_default().to_string_lossy();
        sources.insert(path, (label.trim_start_matches("er-").to_owned(), None));
    }
    for launch in launches {
        sources.insert(Path::new(&launch.log), (launch.label.clone(), Some(launch)));
    }
    for (path, (node, launch)) in sources {
        let version = launch.map(|launch| launch.bin_version.as_str()).filter(|v| *v != "unknown");
        info!(%node, log = %path.display(), version, "validator log");
        if !enabled!(Level::DEBUG) {
            continue;
        }
        let mut entries = VecDeque::new();
        let previous = path.with_extension("log.prev");
        for (previous_boot, source) in [(true, previous.as_path()), (false, path)] {
            let content = match read_log_tail(source) {
                Ok(content) => content,
                Err(error) => {
                    if error.kind() != io::ErrorKind::NotFound {
                        debug!(%node, log = %source.display(), %error,
                            "cannot read recent validator logs");
                    }
                    continue;
                }
            };
            for line in content.lines() {
                let text = strip_ansi(line);
                if !is_diagnostic(&text) {
                    continue;
                }
                let mut text = text.into_owned();
                if text.len() > MAX_LINE_BYTES {
                    text.truncate(text.floor_char_boundary(MAX_LINE_BYTES));
                    text.push_str(" ... [truncated]");
                }
                if entries.len() == MAX_LINES {
                    entries.pop_front();
                }
                entries.push_back((previous_boot, text));
            }
        }
        for (previous_boot, text) in entries {
            debug!(%node, previous_boot = previous_boot.then_some(true), validator_line = %text,
                "recent validator log");
        }
    }
}

fn is_diagnostic(line: &str) -> bool {
    let mut fields = line.trim_start_matches('[').split_whitespace();
    let Some(first) = fields.next() else { return false };
    let level = if first.as_bytes().first().is_some_and(u8::is_ascii_digit) && first.contains('T') {
        fields.next()
    } else {
        Some(first)
    };
    matches!(level, Some("WARN" | "ERROR"))
}
