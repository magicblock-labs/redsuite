use std::{collections::HashMap, time::Instant};

use crate::Result;

const CLOCK_TICKS_PER_SEC: f64 = 100.0;

pub fn proc_running(pid: u32) -> bool {
    let Ok(stat) = std::fs::read_to_string(format!("/proc/{pid}/stat")) else {
        return false;
    };
    // `pid (comm) S …` — comm may contain anything, so find the last ')'.
    let state = stat
        .rfind(')')
        .and_then(|paren_at| stat[paren_at + 1..].trim_start().chars().next());
    !matches!(state, Some('Z') | None)
}

pub struct CpuSample {
    taken: Instant,
    process_ticks: u64,
    thread_ticks: HashMap<u64, u64>,
}

fn stat_cpu_ticks(path: &str) -> Result<u64> {
    let stat =
        std::fs::read_to_string(path).map_err(|e| format!("{path}: {e}"))?;
    let after_comm = stat
        .rsplit_once(')')
        .ok_or_else(|| format!("{path}: no comm field"))?
        .1;
    let fields: Vec<&str> = after_comm.split_whitespace().collect();
    let utime: u64 = fields
        .get(11)
        .ok_or_else(|| format!("{path}: short stat line"))?
        .parse()
        .map_err(|e| format!("{path}: utime: {e}"))?;
    let stime: u64 = fields
        .get(12)
        .ok_or_else(|| format!("{path}: short stat line"))?
        .parse()
        .map_err(|e| format!("{path}: stime: {e}"))?;
    Ok(utime + stime)
}

pub fn cpu_sample(pid: u32) -> Result<CpuSample> {
    let process_ticks = stat_cpu_ticks(&format!("/proc/{pid}/stat"))?;
    let mut thread_ticks = HashMap::new();
    let tasks = std::fs::read_dir(format!("/proc/{pid}/task"))
        .map_err(|e| format!("/proc/{pid}/task: {e}"))?;
    for task in tasks {
        let task = task?;
        let Ok(tid) = task.file_name().to_string_lossy().parse::<u64>() else {
            continue;
        };
        if let Ok(ticks) =
            stat_cpu_ticks(&format!("/proc/{pid}/task/{tid}/stat"))
        {
            thread_ticks.insert(tid, ticks);
        }
    }
    Ok(CpuSample {
        taken: Instant::now(),
        process_ticks,
        thread_ticks,
    })
}

impl CpuSample {
    fn wall_since(&self, earlier: &CpuSample) -> f64 {
        self.taken
            .duration_since(earlier.taken)
            .as_secs_f64()
            .max(1e-9)
    }

    pub fn cores_since(&self, earlier: &CpuSample) -> f64 {
        let ticks = self.process_ticks.saturating_sub(earlier.process_ticks);
        ticks as f64 / CLOCK_TICKS_PER_SEC / self.wall_since(earlier)
    }

    pub fn thread_cores_since(&self, earlier: &CpuSample) -> Vec<f64> {
        let wall = self.wall_since(earlier);
        let mut cores: Vec<f64> = self
            .thread_ticks
            .iter()
            .map(|(tid, ticks)| {
                let before =
                    earlier.thread_ticks.get(tid).copied().unwrap_or(0);
                ticks.saturating_sub(before) as f64 / CLOCK_TICKS_PER_SEC / wall
            })
            .collect();
        cores.sort_by(|left, right| right.total_cmp(left));
        cores
    }
}

pub fn fd_count(pid: u32) -> Result<usize> {
    let entries = std::fs::read_dir(format!("/proc/{pid}/fd"))
        .map_err(|e| format!("/proc/{pid}/fd: {e}"))?;
    Ok(entries.count())
}

pub fn dir_size_bytes(dir: &std::path::Path) -> Result<u64> {
    let mut total = 0;
    let entries = std::fs::read_dir(dir)
        .map_err(|e| format!("{}: {e}", dir.display()))?;
    for entry in entries {
        let entry = entry?;
        let metadata = entry.metadata()?;
        if metadata.is_dir() {
            total += dir_size_bytes(&entry.path())?;
        } else {
            total += metadata.len();
        }
    }
    Ok(total)
}

pub fn rss_kb(pid: u32) -> Result<u64> {
    let status = std::fs::read_to_string(format!("/proc/{pid}/status"))
        .map_err(|e| format!("/proc/{pid}/status: {e}"))?;
    for line in status.lines() {
        if let Some(rest) = line.strip_prefix("VmRSS:") {
            let kb = rest
                .trim()
                .trim_end_matches(" kB")
                .trim()
                .parse::<u64>()
                .map_err(|e| format!("bad VmRSS line `{line}`: {e}"))?;
            return Ok(kb);
        }
    }
    Err(format!("no VmRSS in /proc/{pid}/status").into())
}
