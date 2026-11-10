use std::{
    collections::{BTreeMap, HashMap, VecDeque},
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
    offsets: HashMap<(u64, u64), Checkpoint>,
    shared: Option<PathBuf>,
}

struct Checkpoint {
    offset: u64,
    // Keep the inode alive if a restart replaces the file during this scenario.
    _file: File,
}

#[derive(Debug)]
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
        let mut offsets = HashMap::new();
        if let Some(path) = &shared {
            for source in [path.with_extension("log.prev"), path.to_owned()] {
                if let Ok(file) = File::open(source) {
                    if let Ok(meta) = file.metadata() {
                        offsets.insert(
                            identity(&meta),
                            Checkpoint { offset: meta.len(), _file: file },
                        );
                    }
                }
            }
        }
        Self { offsets, shared }
    }

    pub(super) fn emit(&self, launches: &[LaunchRecord]) {
        let mut sources = BTreeMap::new();
        if let Some(path) = &self.shared {
            let label = path.file_stem().unwrap_or_default().to_string_lossy();
            sources.insert(
                path.clone(),
                (label.trim_start_matches("er-").to_owned(), true),
            );
        }
        for launch in launches {
            let source = sources
                .entry(PathBuf::from(&launch.log))
                .or_insert_with(|| (launch.label.clone(), false));
            // For a newly launched validator, .prev belongs to this scenario
            // only after that validator has restarted at least once.
            source.1 |= launch.relaunches != 0;
        }
        for (path, (node, previous_boot)) in sources {
            let excerpt = self.excerpt(&path, previous_boot);
            if excerpt.truncated || excerpt.omitted != 0 {
                warn!(%node, log = %path.display(), omitted = excerpt.omitted,
                    "validator diagnostics truncated; full logs retained");
            }
            if excerpt.entries.is_empty() {
                if excerpt.found {
                    warn!(%node, log = %path.display(),
                        "no validator WARN/ERROR entries captured during this scenario");
                } else {
                    warn!(%node, log = %path.display(), "validator log unavailable");
                }
            }
            for entry in excerpt.entries {
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
        let offset = self.offsets.get(&identity(&meta)).map_or(0, |checkpoint| checkpoint.offset);
        // A truncated file belongs to a new boot, so read it from the start.
        let offset = if offset <= meta.len() { offset } else { 0 };
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
            let mut text = strip_ansi(line);
            let Some(level) = severity(&text) else { continue };
            if text.len() > MAX_LINE_BYTES {
                let mut end = MAX_LINE_BYTES;
                while !text.is_char_boundary(end) {
                    end -= 1;
                }
                text.truncate(end);
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

fn strip_ansi(line: &str) -> String {
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
    clean
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        fs,
        io::Write,
        sync::{Arc, Mutex},
        time::SystemTime,
    };

    #[derive(Clone)]
    struct Output(Arc<Mutex<Vec<u8>>>);

    impl Write for Output {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            self.0.lock().unwrap().write(bytes)
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    struct Log {
        dir: PathBuf,
        path: PathBuf,
    }

    impl Log {
        fn new() -> Self {
            let stamp =
                SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_nanos();
            let dir =
                std::env::temp_dir().join(format!("redsuite-logs-{}-{stamp}", std::process::id()));
            fs::create_dir(&dir).unwrap();
            let path = dir.join("er-test.log");
            fs::write(&path, "").unwrap();
            Self { dir, path }
        }

        fn append(&self, text: &str) {
            fs::OpenOptions::new()
                .append(true)
                .open(&self.path)
                .unwrap()
                .write_all(text.as_bytes())
                .unwrap();
        }

        fn checkpoint(&self) -> ValidatorLogs {
            ValidatorLogs::new(Some(self.path.clone()))
        }
    }

    impl Drop for Log {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.dir);
        }
    }

    #[test]
    fn passing_scenario_is_quiet_and_failure_prints_shared_validator_diagnostics() {
        let log = Log::new();
        let logs = log.checkpoint();
        log.append("WARN validator warning\nERROR validator error\n");
        let bytes = Arc::new(Mutex::new(Vec::new()));
        let output = Output(bytes.clone());
        let subscriber = tracing_subscriber::fmt()
            .without_time()
            .with_ansi(false)
            .with_max_level(Level::INFO)
            .with_writer(move || output.clone())
            .finish();
        let _guard = tracing::subscriber::set_default(subscriber);
        let mut record = super::super::RunRecord::new("redhat/check".to_owned());
        record.conclude(false, 42, &logs);
        let success = String::from_utf8(bytes.lock().unwrap().clone()).unwrap();
        assert_eq!(success.lines().count(), 1);
        assert!(success.contains("passed"));
        assert!(!success.contains("validator warning"));

        bytes.lock().unwrap().clear();
        record.outcome = Err(anyhow::anyhow!("probe failed"));
        record.conclude(false, 42, &logs);
        let failure = String::from_utf8(bytes.lock().unwrap().clone()).unwrap();
        assert!(failure.contains("seed=42"));
        assert!(failure.contains("probe failed"));
        assert!(failure.contains("WARN") && failure.contains("validator warning"));
        assert!(failure.contains("ERROR") && failure.contains("validator error"));
        assert!(failure.contains(&log.path.display().to_string()));
    }

    #[test]
    fn captures_only_new_warning_and_error_events() {
        let log = Log::new();
        log.append("2026-11-10T09:00:00Z WARN old warning\n");
        let logs = log.checkpoint();
        log.append(concat!(
            "2026-11-10T09:01:00Z INFO message mentions ERROR\n",
            "DEBUG message mentions WARN\n",
            "2026-11-10T09:01:01Z WARN new warning\n",
            "\x1b[2m2026-11-10T09:01:02Z\x1b[0m \x1b[31mERROR\x1b[0m new error\n",
        ));
        let excerpt = logs.excerpt(&log.path, true);
        assert_eq!(excerpt.entries.len(), 2);
        assert_eq!(excerpt.entries[0].level, Level::WARN);
        assert!(excerpt.entries[0].text.ends_with("new warning"));
        assert_eq!(excerpt.entries[1].level, Level::ERROR);
        assert!(excerpt.entries[1].text.ends_with("new error"));
        assert!(!excerpt.entries[1].text.contains('\x1b'));
    }

    #[test]
    fn rotation_retains_this_scenarios_warnings_from_both_boots() {
        let log = Log::new();
        log.append("WARN previous scenario\n");
        let logs = log.checkpoint();
        log.append("WARN before restart\n");
        fs::rename(&log.path, log.path.with_extension("log.prev")).unwrap();
        fs::write(&log.path, "ERROR after restart\n").unwrap();
        let excerpt = logs.excerpt(&log.path, true);
        assert_eq!(excerpt.entries.len(), 2);
        assert_eq!(excerpt.entries[0].text, "WARN before restart");
        assert_eq!(excerpt.entries[1].text, "ERROR after restart");
    }

    #[test]
    fn new_private_log_excludes_the_previous_runs_rotated_log() {
        let log = Log::new();
        log.append("WARN previous run\n");
        let logs = ValidatorLogs::new(None);
        fs::rename(&log.path, log.path.with_extension("log.prev")).unwrap();
        fs::write(&log.path, "WARN current run\n").unwrap();
        let excerpt = logs.excerpt(&log.path, false);
        assert_eq!(excerpt.entries.len(), 1);
        assert_eq!(excerpt.entries[0].text, "WARN current run");
    }

    #[test]
    fn bounds_tail_reads_and_warning_output() {
        let log = Log::new();
        let logs = log.checkpoint();
        let mut bytes = vec![b'x'; MAX_BYTES as usize + 128];
        bytes.push(b'\n');
        for index in 0..MAX_LINES + 5 {
            writeln!(&mut bytes, "WARN warning {index}").unwrap();
        }
        fs::write(&log.path, bytes).unwrap();
        let excerpt = logs.excerpt(&log.path, true);
        assert!(excerpt.truncated);
        assert_eq!(excerpt.omitted, 5);
        assert_eq!(excerpt.entries.len(), MAX_LINES);
        assert_eq!(excerpt.entries[0].text, "WARN warning 5");
    }

    #[test]
    fn ignores_a_warning_started_before_the_checkpoint() {
        let log = Log::new();
        log.append("WARN previous scenario, unfinished");
        let logs = log.checkpoint();
        log.append(" until now\nERROR current scenario\n");
        let excerpt = logs.excerpt(&log.path, true);
        assert_eq!(excerpt.entries.len(), 1);
        assert_eq!(excerpt.entries[0].text, "ERROR current scenario");
    }

    #[test]
    fn bounds_long_unicode_error_lines() {
        let log = Log::new();
        let logs = log.checkpoint();
        log.append(&format!("ERROR {}\n", "€".repeat(MAX_LINE_BYTES)));
        let excerpt = logs.excerpt(&log.path, true);
        let entry = &excerpt.entries[0];
        assert_eq!(entry.level, Level::ERROR);
        assert!(entry.text.ends_with(" ... [truncated]"));
        assert!(entry.text.len() <= MAX_LINE_BYTES + " ... [truncated]".len());
    }
}
