//! Utilities for managing and monitoring local log storage.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytesize::ByteSize;
use serde::{Deserialize, Serialize};
#[cfg(not(test))]
use systemstat::{Platform, System};
use tokio::task::spawn_blocking;
use tokio::time::sleep;
use tracing::info;

const MONITOR: bool = true;
const MAX_WRITES: usize = 10_000;
const MAX_DURATION: Duration = Duration::from_secs(10);
const MIN_AVAILABLE: ByteSize = ByteSize::gb(1);

pub(crate) async fn path_mount_stat(path: PathBuf) -> anyhow::Result<systemstat::Filesystem> {
    let stat = System::new();
    spawn_blocking(move || {
        let fs = stat.mount_at(path)?;
        Ok::<_, anyhow::Error>(fs)
    })
    .await?
}

/// Periodically monitors the disk space available for logs
/// and switches its state between read-only and writable
/// based on configured thresholds.
///
/// Exposes two signals derived from the same filesystem polling loop:
/// - [`Self::is_readonly`] — set when measured available bytes drops below
///   `min_available`. Gates write acceptance.
/// - [`Self::available_bytes`] — the most recently observed available bytes,
///   if any poll has completed. Consumers (e.g. the emergency reconcile
///   trigger) run in their own tasks so a long-running operation cannot
///   delay the read-only gate.
#[derive(Clone, Debug)]
pub(crate) struct DiskMonitor {
    readonly: Arc<AtomicBool>,
    /// Most recently observed filesystem `avail`. `u64::MAX` is used as a
    /// "no reading yet" sentinel so consumers don't false-trigger before the
    /// first poll.
    available_bytes: Arc<AtomicU64>,
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
        let available_bytes = AtomicU64::new(u64::MAX);
        let write_count = AtomicUsize::new(0);
        let last_write = AtomicU64::new(0);

        Self {
            readonly: Arc::new(readonly),
            available_bytes: Arc::new(available_bytes),
            write_count: Arc::new(write_count),
            last_write: Arc::new(last_write),
            epoch: Instant::now(),
        }
    }

    /// Returns true if the disk status is currently read-only.
    pub(crate) fn is_readonly(&self) -> bool {
        self.readonly.load(Ordering::SeqCst)
    }

    /// Returns the most recently observed available bytes on the monitored
    /// mount point. `None` if no poll has completed yet.
    pub(crate) fn available_bytes(&self) -> Option<ByteSize> {
        match self.available_bytes.load(Ordering::SeqCst) {
            u64::MAX => None,
            n => Some(ByteSize::b(n)),
        }
    }

    /// Test-only: directly set the cached available-bytes reading so that
    /// `available_bytes()` returns a deterministic value without running
    /// the polling loop.
    #[cfg(test)]
    pub(crate) fn set_available_bytes_for_test(&self, avail: ByteSize) {
        self.available_bytes.store(avail.as_u64(), Ordering::SeqCst);
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

                self.available_bytes
                    .store(avail.as_u64(), Ordering::SeqCst);
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

#[cfg(test)]
struct System;

#[cfg(test)]
impl System {
    fn new() -> Self {
        Self
    }

    /// Parses a free space value from the supplied path, and if found includes it
    /// in a mock fs stats struct
    fn mount_at(&self, path: impl AsRef<Path>) -> std::io::Result<systemstat::Filesystem> {
        let mut fs = systemstat::Filesystem {
            files: Default::default(),
            files_total: Default::default(),
            files_avail: Default::default(),
            free: Default::default(),
            avail: Default::default(),
            total: Default::default(),
            name_max: Default::default(),
            fs_type: Default::default(),
            fs_mounted_from: Default::default(),
            fs_mounted_on: Default::default(),
        };

        let suffix = path.as_ref().file_name().unwrap_or_default();
        if let Some(avail) = suffix.to_str().and_then(|path| path.parse::<u64>().ok()) {
            fs.avail = ByteSize::b(avail);
        }

        Ok(fs)
    }
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
        assert_eq!(fixture.available_bytes(), Some(ByteSize::b(1024)));
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
