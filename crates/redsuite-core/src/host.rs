use crate::Result;

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
