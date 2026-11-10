use std::{
    borrow::Cow,
    collections::{BTreeMap, VecDeque},
    fs::{File, Metadata},
    io::{self, Read, Seek, SeekFrom},
    os::unix::fs::MetadataExt,
    path::{Path, PathBuf},
};

use tracing::{error, warn, Level};

use crate::resources::LaunchRecord;

const MAX_BYTES: u64 = 1024 * 1024;
const MAX_LINES: usize = 20;
const MAX_LINE_BYTES: usize = 2048;

/// Checkpoints existing logs without taking ownership of their validators.
pub(super) struct ValidatorLogs {
    checkpoints: [Option<Checkpoint>; 2],
    shared: Option<PathBuf>,
}

struct Checkpoint {
    identity: (u64, u64),
    offset: u64,
    // Keep the inode alive if a restart replaces the file during this scenario.
    _file: File,
}

struct Entry {
    source: PathBuf,
    level: Level,
    text: String,
}

#[derive(Default)]
struct Excerpt {
    entries: VecDeque<Entry>,
    omitted: usize,
    truncated: bool,
    found: bool,
}

impl ValidatorLogs {
    pub(super) fn new(shared: Option<PathBuf>) -> Self {
        let checkpoints = shared.as_ref().map_or([None, None], |path| {
            [path.with_extension("log.prev"), path.to_owned()].map(|source| {
                let file = File::open(source).ok()?;
                let meta = file.metadata().ok()?;
                Some(Checkpoint {
                    identity: identity(&meta),
                    offset: meta.len(),
                    _file: file,
                })
            })
        });
        Self { checkpoints, shared }
    }

    pub(super) fn emit(&self, launches: &[LaunchRecord]) {
        let mut sources = BTreeMap::new();
        if let Some(path) = &self.shared {
            let label = path.file_stem().unwrap_or_default().to_string_lossy();
            sources.insert(
                path.as_path(),
                (label.trim_start_matches("er-").to_owned(), true),
            );
        }
        for launch in launches {
            let source = sources
                .entry(Path::new(&launch.log))
                .or_insert_with(|| (launch.label.clone(), false));
            // For a newly launched validator, .prev belongs to this scenario
            // only after that validator has restarted at least once.
            source.1 |= launch.relaunches != 0;
        }
        for (path, (node, previous_boot)) in sources {
            self.excerpt(path, previous_boot).emit(&node, path);
        }
    }

    fn excerpt(&self, path: &Path, previous_boot: bool) -> Excerpt {
        let mut excerpt = Excerpt::default();
        for source in
            [previous_boot.then(|| path.with_extension("log.prev")), Some(path.to_owned())]
                .into_iter()
                .flatten()
        {
            match self.read(&source, &mut excerpt) {
                Ok(()) => {}
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                Err(error) => {
                    warn!(log = %source.display(), %error, "cannot read validator diagnostics");
                }
            }
        }
        excerpt
    }

    fn read(&self, path: &Path, excerpt: &mut Excerpt) -> io::Result<()> {
        let mut file = File::open(path)?;
        let meta = file.metadata()?;
        excerpt.found = true;
        let offset = self
            .checkpoints
            .iter()
            .flatten()
            .rfind(|checkpoint| checkpoint.identity == identity(&meta))
            .map_or(0, |checkpoint| checkpoint.offset);
        // A truncated file belongs to a new boot, so read it from the start.
        let offset = if offset <= meta.len() { offset } else { 0 };
        if offset == meta.len() {
            return Ok(());
        }
        let start = offset.max(meta.len().saturating_sub(MAX_BYTES));
        excerpt.truncated |= start > offset;
        let mut boundary = [b'\n'];
        if start != 0 {
            file.seek(SeekFrom::Start(start - 1))?;
            file.read_exact(&mut boundary)?;
        }
        let mut bytes = Vec::new();
        file.take(meta.len() - start).read_to_end(&mut bytes)?;
        let bytes = if boundary[0] != b'\n' {
            &bytes[bytes.iter().position(|byte| *byte == b'\n').map_or(bytes.len(), |i| i + 1)..]
        } else {
            &bytes[..]
        };
        for line in String::from_utf8_lossy(bytes).lines() {
            let text = strip_ansi(line);
            let Some(level) = severity(&text) else { continue };
            let mut text = text.into_owned();
            if text.len() > MAX_LINE_BYTES {
                text.truncate(text.floor_char_boundary(MAX_LINE_BYTES));
                text.push_str(" ... [truncated]");
            }
            if excerpt.entries.len() == MAX_LINES {
                excerpt.entries.pop_front();
                excerpt.omitted += 1;
            }
            excerpt.entries.push_back(Entry {
                source: path.to_owned(),
                level,
                text,
            });
        }
        Ok(())
    }
}

impl Excerpt {
    fn emit(self, node: &str, path: &Path) {
        if self.truncated || self.omitted != 0 {
            warn!(%node, log = %path.display(), omitted = self.omitted,
                "validator diagnostics truncated; full logs retained");
        }
        if self.entries.is_empty() {
            let message = if self.found {
                "no validator WARN/ERROR entries captured during this scenario"
            } else {
                "validator log unavailable"
            };
            warn!(%node, log = %path.display(), "{message}");
        }
        for entry in self.entries {
            if entry.level == Level::ERROR {
                error!(%node, log = %entry.source.display(), validator_line = %entry.text,
                    "validator log");
            } else {
                warn!(%node, log = %entry.source.display(), validator_line = %entry.text,
                    "validator log");
            }
        }
    }
}

fn identity(meta: &Metadata) -> (u64, u64) {
    (meta.dev(), meta.ino())
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
