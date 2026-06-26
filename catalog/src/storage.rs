//! Utilities for managing and monitoring local log storage.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytesize::ByteSize;
#[cfg(not(test))]
use libc;
use serde::{Deserialize, Serialize};
use tokio::task::spawn_blocking;
use tokio::time::sleep;
use tracing::info;

const MONITOR: bool = true;
const MAX_WRITES: usize = 10_000;
const MAX_DURATION: Duration = Duration::from_secs(10);
const MIN_AVAILABLE: ByteSize = ByteSize::gb(1);

/// Filesystem size statistics for a mount point.
pub(crate) struct MountStat {
    /// Total capacity (f_blocks * f_frsize).
    pub total: ByteSize,
    /// Free blocks including root-reserved (f_bfree * f_frsize).
    pub free: ByteSize,
    /// Free blocks available to unprivileged users (f_bavail * f_frsize).
    pub avail: ByteSize,
}

/// Call `statvfs(2)` on the mount containing `path` and return size fields.
///
/// Uses `f_frsize` (the fundamental block size that `f_blocks`/`f_bfree`/
/// `f_bavail` are counted in) rather than `f_bsize` (preferred I/O size).
/// On some filesystems these differ, causing `f_blocks * f_bsize` to
/// overcount by a factor of `f_bsize / f_frsize`.
#[cfg(not(test))]
pub(crate) async fn path_mount_stat(path: PathBuf) -> anyhow::Result<MountStat> {
    spawn_blocking(move || {
        use std::ffi::CString;
        let cpath = CString::new(path.to_str().ok_or_else(|| {
            anyhow::anyhow!("path contains invalid UTF-8")
        })?)?;
        let mut info: libc::statvfs = unsafe { std::mem::zeroed() };
        let rc = unsafe { libc::statvfs(cpath.as_ptr(), &mut info) };
        if rc != 0 {
            return Err(std::io::Error::last_os_error().into());
        }
        let frsize = info.f_frsize;
        Ok(MountStat {
            total: ByteSize::b(info.f_blocks * frsize),
            free:  ByteSize::b(info.f_bfree  * frsize),
            avail: ByteSize::b(info.f_bavail  * frsize),
        })
    })
    .await?
}

/// Periodically monitors the disk space available for logs
/// and switches its state between read-only and writable
/// based on configured thresholds.
#[derive(Clone, Debug)]
pub(crate) struct DiskMonitor {
    readonly: Arc<AtomicBool>,
    write_count: Arc<AtomicUsize>,
    last_write: Arc<AtomicU64>,
    epoch: Instant,
}

impl Default for DiskMonitor {
    fn default() -> Self {
        Self::new()
    }
}

impl DiskMonitor {
    /// Returns a newly initialized instance.
    pub(crate) fn new() -> Self {
        let readonly = AtomicBool::new(false);
        let write_count = AtomicUsize::new(0);
        let last_write = AtomicU64::new(0);

        Self {
            readonly: Arc::new(readonly),
            write_count: Arc::new(write_count),
            last_write: Arc::new(last_write),
            epoch: Instant::now(),
        }
    }

    /// Returns true if the disk status is currently read-only.
    pub(crate) fn is_readonly(&self) -> bool {
        self.readonly.load(Ordering::SeqCst)
    }

    /// Records a single log write operation.
    pub(crate) fn record_write(&self) {
        self.write_count.fetch_add(1, Ordering::SeqCst);
        let now = self.epoch.elapsed().as_secs();
        self.last_write.store(now, Ordering::SeqCst);
    }

    /// Loops indefinitely while checking for available disk space on the configured path.
    pub(crate) async fn run(&self, root: impl AsRef<Path>, config: &Config) -> anyhow::Result<()> {
        let root = root.as_ref();

        let path = root.to_path_buf();
        let path = spawn_blocking(|| canonical_mount(path)).await??;

        info!("storage monitor starting for {root:?} (mount point: {path:?})");

        loop {
            let deadline = self
                .epoch
                .elapsed()
                .saturating_sub(config.max_duration)
                .as_secs();
            if self.write_count.load(Ordering::SeqCst) >= config.max_writes
                || self.last_write.load(Ordering::SeqCst) < deadline
            {
                let avail = path_mount_stat(path.clone()).await?.avail;

                self.readonly
                    .store(avail < config.min_available, Ordering::SeqCst);
                self.write_count.store(0, Ordering::SeqCst);
            }

            sleep(Duration::from_secs(1)).await;
        }
    }
}

/// Climbs the directory tree looking for a moint point we can monitor
#[cfg(unix)]
fn find_mount_point(path: impl AsRef<Path>) -> anyhow::Result<PathBuf> {
    use std::os::unix::fs::MetadataExt;
    let path = path.as_ref().to_path_buf();

    let dev = std::fs::metadata(path.as_path())?.dev();
    let mut last = path.as_path();

    for parent in path.ancestors() {
        if std::fs::metadata(parent)?.dev() != dev {
            return Ok(last.to_path_buf());
        } else {
            last = parent;
        }
    }

    Ok(last.to_path_buf())
}

/// Punt
#[cfg(not(unix))]
fn find_mount_point(path: impl AsRef<Path>) -> anyhow::Result<PathBuf> {
    Ok(std::fs::canonicalize(path)?)
}

#[cfg(not(test))]
fn canonical_mount(path: impl AsRef<Path>) -> anyhow::Result<PathBuf> {
    let path = std::fs::canonicalize(path)?;
    find_mount_point(path)
}

#[cfg(test)]
fn canonical_mount(path: impl AsRef<Path>) -> anyhow::Result<PathBuf> {
    Ok(path.as_ref().to_path_buf())
}

/// Stores configuration properties used for storage monitoring.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default)]
pub struct Config {
    /// Enable disk monitoring.
    pub monitor: bool,

    /// Maximum number of log writes before checking for available storage.
    pub max_writes: usize,

    /// Maximum duration to wait between storage availability checks.
    #[serde(with = "humantime_serde")]
    pub max_duration: Duration,

    /// Minimum bytes available for storage before turning read-only.
    pub min_available: ByteSize,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            monitor: MONITOR,
            max_writes: MAX_WRITES,
            max_duration: MAX_DURATION,
            min_available: MIN_AVAILABLE,
        }
    }
}

/// Test stub: parses avail bytes from the last path component (numeric suffix).
#[cfg(test)]
pub(crate) async fn path_mount_stat(path: PathBuf) -> anyhow::Result<MountStat> {
    let avail = path
        .file_name()
        .and_then(|s| s.to_str())
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(0);
    Ok(MountStat {
        total: ByteSize::b(avail),
        free: ByteSize::b(avail),
        avail: ByteSize::b(avail),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::timeout;

    #[tokio::test]
    async fn can_find_mount_point() {
        assert_eq!(Path::new("/"), find_mount_point("/bin").unwrap());
        assert_eq!(Path::new("/"), find_mount_point("/").unwrap());
        assert_eq!(Path::new("/dev"), find_mount_point("/dev/null").unwrap());
    }

    #[tokio::test]
    async fn monitor_available_disk_space() {
        let monitor = DiskMonitor::default();
        let fixture = monitor.clone();
        let config = Config {
            monitor: true,
            max_writes: 1,
            max_duration: Duration::from_secs(20),
            min_available: ByteSize::b(1024),
        };

        tokio::spawn(async move { monitor.run("/temp/1024", &config).await });

        sleep(Duration::from_secs(1)).await;
        assert!(!fixture.is_readonly());

        fixture.record_write();

        sleep(Duration::from_secs(1)).await;
        assert!(!fixture.is_readonly());
    }

    #[tokio::test]
    async fn monitor_insufficient_disk_space() {
        let monitor = DiskMonitor::default();
        let fixture = monitor.clone();
        let config = Config {
            monitor: true,
            max_writes: 1,
            max_duration: Duration::ZERO,
            min_available: ByteSize::b(1024),
        };

        let _ = timeout(Duration::from_secs(2), monitor.run("/temp/1023", &config)).await;
        assert!(fixture.is_readonly());
    }
}
