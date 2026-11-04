use anyhow::{ensure, Context};
use redsuite_core::{programs::CellHeader, Result};
use std::{fs::File, os::unix::fs::FileExt, path::Path};
use tracing::warn;

pub(super) struct PayloadFault {
    file: File,
    changes: Vec<(u64, u8, u8)>,
    restored: bool,
}

impl PayloadFault {
    pub(super) fn inject(directory: &Path, image: &[u8], bit: u8) -> Result<Self> {
        ensure!(
            image.len() >= CellHeader::SIZE && bit < 8,
            "invalid payload fault"
        );
        CellHeader::try_decode(image)?;
        let current = directory.join("CURRENT");
        let path = if current.is_dir() {
            current.join("storage.db")
        } else {
            directory.join("storage.db")
        };
        let file = File::options()
            .read(true)
            .write(true)
            .open(&path)
            .with_context(|| format!("opening owned account storage {}", path.display()))?;
        let mut version = [0; 8];
        file.read_exact_at(&mut version, 0)?;
        ensure!(u64::from_le_bytes(version) == 1, "payload fault requires accountsdb format 1; route another format to accountsdb's owning tests");
        let len = file.metadata()?.len();
        ensure!(
            len <= 1024 * 1024 * 1024,
            "fixture account store exceeds fault scan limit"
        );
        let chunk = 1024 * 1024;
        let mut buffer = vec![0; chunk + image.len() - 1];
        let mut matches = Vec::new();
        let mut start = 256u64;
        while start < len {
            let size = (len - start).min(buffer.len() as u64) as usize;
            file.read_exact_at(&mut buffer[..size], start)?;
            for (index, value) in buffer[..size].windows(image.len()).take(chunk).enumerate() {
                if value == image {
                    matches.push(start + index as u64);
                }
            }
            start += chunk as u64;
        }
        ensure!(
            (1..=2).contains(&matches.len()),
            "payload fault not exercised: expected one or two exact cell images, found {}",
            matches.len()
        );
        let byte = image[CellHeader::VALUE_OFFSET];
        let changes = matches
            .into_iter()
            .map(|offset| {
                (
                    offset + CellHeader::VALUE_OFFSET as u64,
                    byte,
                    byte ^ (1 << bit),
                )
            })
            .collect();
        let fault = Self { file, changes, restored: false };
        for &(offset, original, replacement) in &fault.changes {
            let mut actual = [0];
            fault.file.read_exact_at(&mut actual, offset)?;
            ensure!(
                actual[0] == original,
                "cell moved while preparing the storage fault"
            );
            fault.file.write_all_at(&[replacement], offset)?;
        }
        fault.file.sync_data()?;
        Ok(fault)
    }

    pub(super) fn restore(&mut self) -> Result<()> {
        if self.restored {
            return Ok(());
        }
        for &(offset, original, replacement) in &self.changes {
            let mut actual = [0];
            self.file.read_exact_at(&mut actual, offset)?;
            ensure!(actual[0] == replacement || actual[0] == original,
                "account payload changed during fault; refusing to overwrite a new value at {offset}");
            self.file.write_all_at(&[original], offset)?;
        }
        self.file.sync_data()?;
        self.restored = true;
        Ok(())
    }
}

impl Drop for PayloadFault {
    fn drop(&mut self) {
        if let Err(error) = self.restore() {
            warn!(%error, "restoring owned fixture account payload failed");
        }
    }
}
