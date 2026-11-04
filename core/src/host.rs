use std::{collections::HashMap, fs, fs::ReadDir, path::Path, time::Instant};

use anyhow::{anyhow, Context};
#[cfg(target_os = "macos")]
use tracing::warn;

use crate::Result;

#[cfg(target_os = "linux")]
const CLOCK_TICKS_PER_SEC: f64 = 100.0;
#[cfg(target_os = "macos")]
const NANOS_PER_SEC: f64 = 1e9;
#[cfg(target_os = "macos")]
const SZOMB: u32 = 5;

#[cfg(target_os = "linux")]
pub fn proc_running(pid: u32) -> bool {
    let Ok(stat) = fs::read_to_string(format!("/proc/{pid}/stat")) else {
        return false;
    };
    // `pid (comm) S …` — comm may contain anything, so find the last ')'.
    let state = stat
        .rfind(')')
        .and_then(|paren_at| stat[paren_at + 1..].trim_start().chars().next());
    !matches!(state, Some('Z') | None)
}

#[cfg(target_os = "macos")]
pub fn proc_running(pid: u32) -> bool {
    use libproc::{bsd_info::BSDInfo, proc_pid::pidinfo};
    if pid == 0 {
        return false;
    }
    match pidinfo::<BSDInfo>(pid as i32, 0) {
        Ok(info) => info.pbi_status != SZOMB,
        Err(error) => {
            if !error.contains("No such process") {
                warn!(pid, error = %format_args!("{error:#}"), "process liveness query failed");
            }
            false
        }
    }
}

pub struct CpuSample {
    taken: Instant,
    process_secs: f64,
    thread_secs: HashMap<u64, f64>,
}

#[cfg(target_os = "linux")]
fn stat_cpu_secs(path: &str) -> Result<f64> {
    let stat = fs::read_to_string(path).with_context(|| path.to_owned())?;
    let after_comm = stat.rsplit_once(')').with_context(|| format!("{path}: no comm field"))?.1;
    let fields: Vec<&str> = after_comm.split_whitespace().collect();
    let utime: u64 = fields
        .get(11)
        .with_context(|| format!("{path}: short stat line"))?
        .parse()
        .with_context(|| format!("{path}: utime"))?;
    let stime: u64 = fields
        .get(12)
        .with_context(|| format!("{path}: short stat line"))?
        .parse()
        .with_context(|| format!("{path}: stime"))?;
    Ok((utime + stime) as f64 / CLOCK_TICKS_PER_SEC)
}

#[cfg(target_os = "linux")]
pub fn cpu_sample(pid: u32) -> Result<CpuSample> {
    let process_secs = stat_cpu_secs(&format!("/proc/{pid}/stat"))?;
    let mut thread_secs = HashMap::new();
    let tasks =
        fs::read_dir(format!("/proc/{pid}/task")).with_context(|| format!("/proc/{pid}/task"))?;
    for task in tasks {
        let task = task?;
        let Ok(tid) = task.file_name().to_string_lossy().parse::<u64>() else {
            continue;
        };
        if let Ok(secs) = stat_cpu_secs(&format!("/proc/{pid}/task/{tid}/stat")) {
            thread_secs.insert(tid, secs);
        }
    }
    Ok(CpuSample {
        taken: Instant::now(),
        process_secs,
        thread_secs,
    })
}

#[cfg(target_os = "macos")]
pub fn cpu_sample(pid: u32) -> Result<CpuSample> {
    use libproc::{
        proc_pid::{listpidinfo, pidinfo, ListThreads},
        task_info::TaskInfo,
        thread_info::ThreadInfo,
    };
    let pid = pid as i32;
    let task = pidinfo::<TaskInfo>(pid, 0)
        .map_err(|error| anyhow!("task info for pid {pid}: {error:#}"))?;
    let thread_limit = task.pti_threadnum.max(1) as usize;
    let threads = listpidinfo::<ListThreads>(pid, thread_limit)
        .map_err(|error| anyhow!("thread list for pid {pid}: {error:#}"))?;
    let mut thread_secs = HashMap::new();
    for thread in threads {
        if let Ok(info) = pidinfo::<ThreadInfo>(pid, thread) {
            let nanos = info.pth_user_time + info.pth_system_time;
            thread_secs.insert(thread, nanos as f64 / NANOS_PER_SEC);
        }
    }
    let nanos = task.pti_total_user + task.pti_total_system;
    Ok(CpuSample {
        taken: Instant::now(),
        process_secs: nanos as f64 / NANOS_PER_SEC,
        thread_secs,
    })
}

impl CpuSample {
    fn wall_since(&self, earlier: &CpuSample) -> f64 {
        self.taken.duration_since(earlier.taken).as_secs_f64().max(1e-9)
    }

    pub fn cores_since(&self, earlier: &CpuSample) -> f64 {
        (self.process_secs - earlier.process_secs).max(0.0) / self.wall_since(earlier)
    }

    pub fn thread_cores_since(&self, earlier: &CpuSample) -> Vec<f64> {
        let wall = self.wall_since(earlier);
        let mut cores: Vec<f64> = self
            .thread_secs
            .iter()
            .map(|(tid, secs)| {
                let before = earlier.thread_secs.get(tid).copied().unwrap_or(0.0);
                (secs - before).max(0.0) / wall
            })
            .collect();
        cores.sort_by(|left, right| right.total_cmp(left));
        cores
    }
}

#[cfg(target_os = "linux")]
pub fn fd_count(pid: u32) -> Result<usize> {
    let entries =
        fs::read_dir(format!("/proc/{pid}/fd")).with_context(|| format!("/proc/{pid}/fd"))?;
    Ok(entries.count())
}

#[cfg(target_os = "macos")]
pub fn fd_count(pid: u32) -> Result<usize> {
    use libproc::{
        bsd_info::BSDInfo,
        file_info::ListFDs,
        proc_pid::{listpidinfo, pidinfo},
    };
    let info = pidinfo::<BSDInfo>(pid as i32, 0)
        .map_err(|error| anyhow!("bsd info for pid {pid}: {error:#}"))?;
    let table_size = (info.pbi_nfiles as usize).max(1);
    let open = listpidinfo::<ListFDs>(pid as i32, table_size)
        .map_err(|error| anyhow!("fd list for pid {pid}: {error:#}"))?;
    Ok(open.len())
}

pub fn dir_size_bytes(dir: &Path) -> Result<u64> {
    let entries = fs::read_dir(dir).with_context(|| format!("{}", dir.display()))?;
    walk_size(entries)
}

fn walk_size(entries: ReadDir) -> Result<u64> {
    use std::io::ErrorKind::NotFound;
    let mut total = 0;
    for entry in entries {
        let entry = match entry {
            Ok(entry) => entry,
            Err(error) if error.kind() == NotFound => continue,
            Err(error) => return Err(error.into()),
        };
        let metadata = match entry.metadata() {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == NotFound => continue,
            Err(error) => return Err(error.into()),
        };
        if metadata.is_dir() {
            total += match fs::read_dir(entry.path()) {
                Ok(inner) => walk_size(inner)?,
                Err(error) if error.kind() == NotFound => 0,
                Err(error) => return Err(error.into()),
            };
        } else {
            total += metadata.len();
        }
    }
    Ok(total)
}

#[cfg(target_os = "linux")]
pub fn rss_kb(pid: u32) -> Result<u64> {
    let status = fs::read_to_string(format!("/proc/{pid}/status"))
        .with_context(|| format!("/proc/{pid}/status"))?;
    for line in status.lines() {
        if let Some(rest) = line.strip_prefix("VmRSS:") {
            let kb = rest
                .trim()
                .trim_end_matches(" kB")
                .trim()
                .parse::<u64>()
                .with_context(|| format!("bad VmRSS line `{line}`"))?;
            return Ok(kb);
        }
    }
    Err(anyhow!("no VmRSS in /proc/{pid}/status"))
}

#[cfg(target_os = "macos")]
pub fn rss_kb(pid: u32) -> Result<u64> {
    use libproc::{proc_pid::pidinfo, task_info::TaskInfo};
    let task = pidinfo::<TaskInfo>(pid as i32, 0)
        .map_err(|error| anyhow!("task info for pid {pid}: {error:#}"))?;
    Ok(task.pti_resident_size / 1024)
}
