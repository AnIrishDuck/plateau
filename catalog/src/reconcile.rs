//! Reconciliation job for verifying consistency between the manifest and files on disk.
//!
//! This module provides functionality to:
//! - Verify all files on disk are tracked in the manifest
//! - Verify file sizes on disk match sizes in the manifest
//! - Detect files that don't belong to any segment in their directory

use std::collections::BTreeSet;
use std::iter;
use std::mem;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::fs;

use anyhow::Result;
use bytesize::ByteSize;
use futures::stream::StreamExt;
use plateau_data::segment::Segment;
use serde::{Deserialize, Serialize};
use tracing::{debug, info, warn};

use crate::catalog::Catalog;
use crate::data::RecordIndex;
use crate::manifest::{PartitionId, SegmentData};
use crate::partition::Partition;
use crate::slog::Slog;
use crate::topic::Topic;

/// Configuration for the reconciliation job
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct ReconcileConfig {
    /// Maximum units of work to process in a single run
    pub limit: Option<usize>,
    /// Ratio for controlling how much idle time the reconciler takes
    /// If zero, we are never idle, if one, we idle for as long as we work, 10 we idle 10x the work
    /// time, etc.
    pub idle_ratio: f64,
    /// Whether to track individual file paths or just count them
    pub track_files: bool,
    /// Set of fixes to apply during reconciliation
    #[serde(default)]
    pub fixes: BTreeSet<ReconcileFix>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub enum ReconcileFix {
    /// Workaround for inability to pass an empty collection via [config]
    Noop,
    UpdateManifestSizes,
    // TODO: RemoveOrphans,
    // TODO: RemoveUntrackedSegments
}

/// Configuration for the emergency reconciliation task.
///
/// The emergency reconcile level lies between the standard retention threshold
/// (`max_bytes - headroom`) and the absolute maximum (`max_bytes`, near where
/// the storage monitor would override into read-only). It is computed as:
///
/// ```text
/// emergency_level = (total_byte_limit + headroom) * threshold_fraction
///                 = retain.max_bytes * threshold_fraction
/// ```
///
/// A `threshold_fraction` near 1.0 (the default) places the emergency level
/// just below `max_bytes`. Lower values trigger emergency reconciliation
/// sooner.
///
/// The emergency loop runs in its own dedicated task and polls
/// `polling_interval`; because there is a single task, only one emergency
/// reconciliation can be in flight at a time.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct EmergencyReconcileConfig {
    /// How often to check whether the catalog has exceeded the emergency level.
    #[serde(with = "humantime_serde")]
    pub polling_interval: Duration,
    /// Fraction of `retain.max_bytes` at which to trigger emergency
    /// reconciliation. Clamped to `[0.0, 1.0]` when evaluated.
    pub threshold_fraction: f64,
    /// Reconcile job configuration to use when emergency reconciliation runs.
    pub reconcile: ReconcileConfig,
}

impl Default for EmergencyReconcileConfig {
    fn default() -> Self {
        Self {
            polling_interval: Duration::from_secs(60),
            threshold_fraction: 0.95,
            reconcile: ReconcileConfig::default(),
        }
    }
}

/// A reconciliation job that incrementally validates consistency between
/// the manifest and files on disk.
#[derive(Debug)]
pub struct ReconcileJob {
    /// Catalog to reconcile
    catalog: Arc<Catalog>,
    /// Current position in the reconciliation process
    state: ReconcileState,
    /// Configuration for the reconciliation job
    config: ReconcileConfig,
}

/// The current state of a reconciliation job
#[derive(Clone, Debug, Default)]
struct ReconcileState {
    /// Current topic being processed
    current_topic_index: usize,
    /// Current partition being processed within the current topic
    current_partition_index: usize,
    /// All topics in the catalog
    topics: Option<Vec<String>>,
    /// Accumulator for all segments in the current topic
    topic_segments: BTreeSet<PathBuf>,
    /// Statistics from the reconciliation
    stats: ReconcileStats,
}

impl ReconcileState {
    pub fn new(track_files: bool) -> Self {
        Self {
            stats: if track_files {
                ReconcileStats::with_path_tracking()
            } else {
                ReconcileStats::default()
            },
            ..Default::default()
        }
    }
}

#[derive(Debug, Clone)]
pub enum PathStats {
    Paths(Vec<PathBuf>),
    Counter(usize),
}

impl Default for PathStats {
    fn default() -> Self {
        Self::Counter(0)
    }
}

impl PathStats {
    pub fn empty_paths() -> Self {
        Self::Paths(Vec::new())
    }

    pub fn len(&self) -> usize {
        match self {
            Self::Paths(paths) => paths.len(),
            Self::Counter(count) => *count,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

// File tracking statistics to track untracked, checked, and missing files
#[derive(Debug, Clone, Default)]
pub struct FileStats {
    pub paths: PathStats,
    pub total_bytes: usize,
}

impl FileStats {
    pub fn display_bytes(&self) -> ByteSize {
        ByteSize(self.total_bytes as u64)
    }

    pub fn empty_paths() -> Self {
        Self {
            paths: PathStats::Paths(Vec::new()),
            total_bytes: 0,
        }
    }

    pub fn add_path(&mut self, path: PathBuf, bytes: usize) {
        match &mut self.paths {
            PathStats::Paths(paths) => paths.push(path),
            PathStats::Counter(count) => *count += 1,
        }
        self.total_bytes += bytes;
    }

    pub fn add_paths(&mut self, paths: Vec<PathBuf>, bytes: usize) {
        match &mut self.paths {
            PathStats::Paths(existing_paths) => existing_paths.extend(paths),
            PathStats::Counter(count) => *count += paths.len(),
        }
        self.total_bytes += bytes;
    }

    pub fn len(&self) -> usize {
        match &self.paths {
            PathStats::Paths(paths) => paths.len(),
            PathStats::Counter(count) => *count,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Statistics collected during reconciliation
#[derive(Debug, Clone, Default)]
pub struct ReconcileStats {
    /// Number of files checked
    pub files_checked: FileStats,
    /// Number of untracked files found
    pub untracked_files: FileStats,
    /// Number of size mismatches found
    pub size_mismatches: FileStats,
    /// Number of missing files found
    pub missing_files: FileStats,
    /// Total expected byte count
    pub expected_size: ByteSize,
    /// Total actual byte count
    pub actual_size: ByteSize,
}

impl ReconcileStats {
    pub fn with_path_tracking() -> Self {
        Self {
            files_checked: FileStats::empty_paths(),
            untracked_files: FileStats::empty_paths(),
            size_mismatches: FileStats::empty_paths(),
            missing_files: FileStats::empty_paths(),
            ..Default::default()
        }
    }
}

impl ReconcileJob {
    /// Create a new reconciliation job for the given catalog with default configuration
    pub fn new(catalog: Arc<Catalog>) -> Self {
        Self::with_config(catalog, ReconcileConfig::default())
    }

    /// Create a new reconciliation job for the given catalog with custom configuration
    pub fn with_config(catalog: Arc<Catalog>, config: ReconcileConfig) -> Self {
        Self {
            catalog,
            state: ReconcileState::new(config.track_files),
            config,
        }
    }

    /// Run a limited reconciliation pass, returning true when complete
    ///
    /// This method will process up to `limit` units of work and return
    /// true when the entire reconciliation is complete, false otherwise.
    /// If limit is None, run until completion.
    pub async fn run(&mut self, limit: Option<usize>) -> Result<bool> {
        // Use the limit from the parameter if provided, otherwise use config
        let effective_limit = limit.or(self.config.limit);

        let mut work_done = 0;
        let max_work = effective_limit.unwrap_or(usize::MAX);

        while work_done < max_work {
            let start_time = Instant::now();
            let done = self.process_next_unit().await?;
            let work_time = start_time.elapsed();

            work_done += 1;

            // If we're done, return true
            if done {
                let stats = self.stats();
                info!("Reconciliation complete: {:?}", stats);
                return Ok(true);
            }

            // Sleep for ratio * work_time to control how much idle time the reconciler takes
            if self.config.idle_ratio > 0.0 {
                let sleep_duration = Duration::from_micros(
                    (work_time.as_micros() as f64 * self.config.idle_ratio) as u64,
                );
                tokio::time::sleep(sleep_duration).await;
            }
        }

        Ok(false)
    }

    /// Process the next unit of work in the reconciliation
    async fn process_next_unit(&mut self) -> Result<bool> {
        // Load topics if we haven't already
        let topics = {
            if let Some(topics) = &self.state.topics {
                topics
            } else {
                self.state.topics = Some(self.catalog.manifest().get_topics().await);
                self.state.topics.as_ref().unwrap()
            }
        };

        // Get current positions
        let (current_topic_index, current_partition_index, topics_len) = {
            (
                self.state.current_topic_index,
                self.state.current_partition_index,
                topics.len(),
            )
        };

        // If we've processed all topics, we're done
        if current_topic_index >= topics_len {
            return Ok(true);
        }

        // Get the current topic name
        let topic_name = topics[current_topic_index].clone();

        debug!("Reconciling topic: {}", topic_name);

        // Get all partitions for this topic
        let partitions = self.catalog.manifest().get_partitions(&topic_name).await;

        // If we've processed all partitions in this topic, move to the next topic
        if current_partition_index >= partitions.len() {
            let topics_len = topics.len();
            let topic_segments = mem::take(&mut self.state.topic_segments);
            self.identify_untracked_files(&topic_name, topic_segments)
                .await?;
            self.state.current_partition_index = 0;
            self.state.current_topic_index += 1;

            // If we've processed all topics, we're done
            if self.state.current_topic_index >= topics_len {
                return Ok(true);
            }

            // Not done yet, but we've completed this unit of work
            return Ok(false);
        }

        // Process the current partition
        let partition_name = partitions[current_partition_index].clone();
        debug!("Reconciling partition: {}/{}", topic_name, partition_name);

        // Process this partition
        let partition_segments = self
            .process_partition_phase(&topic_name, &partition_name)
            .await?;

        self.state.topic_segments.extend(partition_segments);
        self.state.current_partition_index += 1;

        // We've processed one unit of work
        Ok(false)
    }

    async fn identify_untracked_files(
        &mut self,
        topic_name: &str,
        tracked_files: BTreeSet<PathBuf>,
    ) -> Result<()> {
        let root = self.catalog.topic_root();
        let topic_path = Topic::partition_root(root, topic_name);

        // Get all files in the partition directory
        let partition_files = self.list_segment_files(&topic_path).await?;
        let mut partition_bytes = 0;
        for path in &partition_files {
            if let Ok(metadata) = fs::metadata(path).await {
                partition_bytes += metadata.len() as usize;
            }
        }

        // Add the partition files to our stats
        self.state
            .stats
            .files_checked
            .add_paths(partition_files.clone(), partition_bytes);
        debug!(
            "Found {} files in partition directory",
            partition_files.len()
        );

        for file_path in partition_files {
            debug!("Checking file: {:?}", file_path);
            if !tracked_files.contains(&file_path) {
                warn!("Untracked file in topic {:?}: {:?}", topic_name, file_path);
                // Add the untracked path to our stats
                let file_size = fs::metadata(&file_path)
                    .await
                    .map(|m| m.len() as usize)
                    .unwrap_or(0);
                self.state
                    .stats
                    .untracked_files
                    .add_path(file_path.clone(), file_size);
            } else {
                debug!("Found tracked path {:?}", file_path);
            }
        }

        Ok(())
    }

    /// Process the current partition, returning true when complete
    async fn process_partition_phase(
        &mut self,
        topic_name: &str,
        partition_name: &str,
    ) -> Result<BTreeSet<PathBuf>> {
        // Process the entire partition in one go since we don't need incremental resume
        let root = self.catalog.topic_root();
        let partition_id = PartitionId::new(topic_name, partition_name);
        let topic_path = Topic::partition_root(root, topic_name);

        debug!(
            "Processing partition {}/{} with path {:?}",
            topic_name, partition_name, topic_path
        );

        // Create sets to track files
        let mut tracked_files = BTreeSet::new();

        // Fetch all segments for this partition
        let segments_stream = self.catalog.manifest().stream_segments(
            &partition_id,
            RecordIndex(0),
            crate::data::index::Ordering::Forward,
        );

        let segments: Vec<SegmentData> = segments_stream.collect().await;
        if let Some((start, end)) = segments.first().zip(segments.last()) {
            debug!(
                "Fetched {} segments: {} ..= {}",
                segments.len(),
                start.index.0,
                end.index.0
            );
        } else {
            debug!("Found no segments")
        }

        // Validate each segment
        for segment in segments {
            debug!("Validating segment: {:?}", segment.index);

            let partition_id = PartitionId {
                topic: topic_name.into(),
                partition: partition_name.into(),
            };

            let slog_name = Partition::slog_name(&partition_id);
            let segment_file_name = format!("{}-{}", slog_name, segment.index.0);
            let segment_path = Slog::segment_path(&topic_path, &slog_name, segment.index);

            debug!("Checking segment file: {} at {:?}", slog_name, segment_path);

            // Mark this file as tracked (we may want to consider .arrows extension depending on actual requirements)
            tracked_files.insert(segment_path.clone());

            // Check if the file exists
            if !segment_path.exists() {
                warn!("Missing file {:?}", segment_path);
                // Add the missing path to our stats
                self.state
                    .stats
                    .missing_files
                    .add_path(segment_path.clone(), segment.size);
            } else {
                // Check file size including recovery files
                let mut total_actual_size = 0;

                // Check main segment file
                match fs::metadata(&segment_path).await {
                    Ok(metadata) => {
                        total_actual_size += metadata.len() as usize;
                        debug!(
                            "Segment {} file size: {}",
                            segment_file_name,
                            metadata.len()
                        );
                    }
                    Err(e) => {
                        warn!(
                            "Error getting metadata for segment {}: {:?}",
                            segment_file_name, e
                        );
                    }
                }

                // Check for associated parts and add their size
                let segment_file = Segment::at(segment_path);
                for part_path in segment_file
                    .parts()
                    .chain(iter::once(segment_file.cache_path()))
                {
                    if part_path.exists() {
                        tracked_files.insert(part_path.clone());
                        if part_path != segment_file.cache_path() {
                            match fs::metadata(&part_path).await {
                                Ok(metadata) => {
                                    total_actual_size += metadata.len() as usize;
                                    debug!("Part {:?} size: {}", part_path, metadata.len());
                                }
                                Err(e) => {
                                    warn!(
                                        "Error getting metadata for part {:?}: {:?}",
                                        part_path, e
                                    );
                                }
                            }
                        }
                    } else {
                        debug!("Part {:?} does not exist", part_path);
                    }
                }

                let expected_size = ByteSize(segment.size as u64);
                let actual_size = ByteSize(total_actual_size as u64);
                self.state.stats.expected_size =
                    ByteSize(self.state.stats.expected_size.as_u64() + expected_size.as_u64());
                self.state.stats.actual_size =
                    ByteSize(self.state.stats.actual_size.as_u64() + actual_size.as_u64());

                // Compare total size with expected size
                debug!(
                    "Comparing sizes - total_actual_size={}, segment.size={}, diff={}",
                    total_actual_size,
                    segment.size,
                    total_actual_size.abs_diff(segment.size)
                );
                if total_actual_size.abs_diff(segment.size) > 0 {
                    warn!(
                        "Size mismatch for segment {}. Expected {}, actual {}",
                        segment_file_name, expected_size, actual_size
                    );
                    // Add the mismatched path to our stats
                    self.state.stats.size_mismatches.add_path(
                        segment_file.path().clone(),
                        // NOTE: this is probably not ideal as it can "overcount" the total difference
                        total_actual_size.abs_diff(segment.size),
                    );

                    if self
                        .config
                        .fixes
                        .contains(&ReconcileFix::UpdateManifestSizes)
                    {
                        info!("Fixing size mismatch for segment {}", segment_file_name);
                        let mut corrected_segment = segment.clone();
                        corrected_segment.size = total_actual_size;

                        // Update the manifest with the correct size
                        self.catalog
                            .manifest()
                            .update(&partition_id, &corrected_segment)
                            .await;
                    }
                } else {
                    debug!(
                        "Segment {} size ok. Expected: {}, actual: {}",
                        segment_file_name, segment.size, total_actual_size
                    );
                }
            }
        }

        // We've completed processing this partition
        Ok(tracked_files)
    }

    /// List all segment-related files in a partition directory
    async fn list_segment_files(&self, partition_path: &Path) -> Result<Vec<PathBuf>> {
        let mut files = Vec::new();

        if partition_path.exists() {
            let mut entries = fs::read_dir(partition_path).await?;
            while let Some(entry) = entries.next_entry().await? {
                let path = entry.path();

                if path.is_file() {
                    files.push(path);
                }
            }
        }

        Ok(files)
    }

    /// Get current reconciliation statistics
    pub fn stats(&self) -> &ReconcileStats {
        &self.state.stats
    }

    /// Reset the reconciliation job to start from the beginning
    pub async fn reset(&mut self) {
        self.state.current_topic_index = 0;
        self.state.current_partition_index = 0;
        self.state.topics = None;
        self.state.stats = if self.config.track_files {
            ReconcileStats::with_path_tracking()
        } else {
            ReconcileStats::default()
        };
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::Config;
    use crate::data::records::Record;
    use chrono::Utc;
    use std::path::PathBuf;
    use tempfile::TempDir;
    use tokio::fs;
    use tracing::trace;

    async fn create_test_catalog() -> (TempDir, Arc<Catalog>) {
        let dir = TempDir::new().unwrap();
        let root = PathBuf::from(dir.path());
        let config = Config::default();
        let catalog = Catalog::attach(root, config).await.unwrap();
        (dir, Arc::new(catalog))
    }

    // run reconcile with tracking for all of these tests and verify the associated
    // path(s) end up in the path stats.

    #[test_log::test(tokio::test)]
    async fn test_reconcile_empty_catalog() -> Result<()> {
        let (_tmpdir, catalog) = create_test_catalog().await;
        // Create reconciler with summary check disabled for predictable test behavior
        let config = ReconcileConfig {
            track_files: true,
            ..Default::default()
        };
        let mut reconciler = ReconcileJob::with_config(catalog, config);

        // Should complete immediately on an empty catalog
        let done = reconciler.run(Some(100)).await?;
        assert!(done);

        let stats = reconciler.stats();
        assert_eq!(stats.files_checked.len(), 0);
        assert_eq!(stats.untracked_files.len(), 0);
        assert_eq!(stats.size_mismatches.len(), 0);
        assert_eq!(stats.missing_files.len(), 0);

        Ok(())
    }

    #[test_log::test(tokio::test)]
    async fn test_reconcile_with_data() -> Result<()> {
        let (_tmpdir, catalog) = create_test_catalog().await;

        // Add some data to the catalog
        let records: Vec<_> = vec!["abc", "def", "ghi"]
            .into_iter()
            .map(|message| Record {
                time: Utc::now(),
                message: message.bytes().collect(),
            })
            .collect();

        let topic = catalog.get_topic("test-topic").await;
        topic.extend_records("default", &records).await?;
        topic.extend_records("other", &records).await?;
        topic.commit().await?;

        // Force a checkpoint to ensure files are written to disk
        drop(topic);
        catalog.checkpoint().await;

        // Create reconciler with tracking
        let config = ReconcileConfig {
            track_files: true,
            ..Default::default()
        };
        let mut reconciler = ReconcileJob::with_config(catalog.clone(), config);

        // Run reconciliation
        let done = reconciler.run(Some(100)).await?;
        assert!(done);

        // Should have validated some segments
        let stats = reconciler.stats();
        assert!(!stats.files_checked.is_empty());
        assert_eq!(stats.untracked_files.paths.len(), 0);
        assert_eq!(stats.missing_files.paths.len(), 0);

        Ok(())
    }

    #[test_log::test(tokio::test)]
    async fn test_incremental_reconciliation() -> Result<()> {
        let (_tmpdir, catalog) = create_test_catalog().await;

        // Add some data to multiple topics
        let records: Vec<_> = vec!["abc", "def", "ghi"]
            .into_iter()
            .map(|message| Record {
                time: Utc::now(),
                message: message.bytes().collect(),
            })
            .collect();

        for i in 0..3 {
            let topic_name = format!("topic-{}", i);
            let topic = catalog.get_topic(&topic_name).await;
            topic.extend_records("default", &records).await?;
            topic.commit().await?;
        }

        // Create reconciler with tracking
        let config = ReconcileConfig {
            track_files: true,
            ..Default::default()
        };
        let mut reconciler = ReconcileJob::with_config(catalog.clone(), config);

        // Run reconciliation with a small limit
        let done1 = reconciler.run(Some(1)).await?;
        assert!(!done1); // Should not be done after just 1 unit of work

        // Run again with another small limit
        let done2 = reconciler.run(Some(1)).await?;
        assert!(!done2); // Still not done

        // Run with enough limit to finish
        let done3 = reconciler.run(Some(100)).await?;
        assert!(done3); // Should be done now

        // Stats should show work was done
        let stats = reconciler.stats();
        assert_eq!(stats.files_checked.len(), 3);

        Ok(())
    }

    #[test_log::test(tokio::test)]
    async fn test_unlimited_reconciliation() -> Result<()> {
        let (_tmpdir, catalog) = create_test_catalog().await;

        // Add some data
        let records: Vec<_> = vec!["abc", "def"]
            .into_iter()
            .map(|message| Record {
                time: Utc::now(),
                message: message.bytes().collect(),
            })
            .collect();

        let topic = catalog.get_topic("test-topic").await;
        topic.extend_records("default", &records).await?;
        topic.commit().await?;

        // Create reconciler with tracking
        let config = ReconcileConfig {
            track_files: true,
            ..Default::default()
        };
        let mut reconciler = ReconcileJob::with_config(catalog.clone(), config);

        // Run reconciliation with no limit
        let done = reconciler.run(None).await?;
        assert!(done); // Should be done

        // Stats should show work was done
        let stats = reconciler.stats();
        assert_eq!(stats.files_checked.len(), 1);
        assert_eq!(stats.missing_files.len(), 0);
        assert_eq!(stats.size_mismatches.len(), 0);

        Ok(())
    }

    #[test_log::test(tokio::test)]
    async fn test_reconcile_orphan_files() -> Result<()> {
        let (_tmpdir, catalog) = create_test_catalog().await;

        // Add some data to the catalog to create a partition directory
        let records: Vec<_> = vec!["abc", "def"]
            .into_iter()
            .map(|message| Record {
                time: Utc::now(),
                message: message.bytes().collect(),
            })
            .collect();

        let topic = catalog.get_topic("test-topic").await;
        topic.extend_records("default", &records).await?;
        topic.commit().await?;
        drop(topic);

        // Force a checkpoint to ensure files are written to disk
        catalog.checkpoint().await;

        // Debug: Check what directories exist
        let topic_root = catalog.topic_root().join("test-topic");
        // Orphan files are created in the topic directory, not partition subdirectory
        let partition_path = topic_root.clone();
        trace!("Topic root: {:?}", topic_root);
        trace!("Partition path (topic directory): {:?}", partition_path);
        trace!("Partition path exists: {}", partition_path.exists());

        if partition_path.exists() {
            let mut entries = fs::read_dir(&partition_path).await?;
            while let Some(entry) = entries.next_entry().await? {
                trace!("Existing file: {:?}", entry.file_name());
            }
        } else {
            // Create the directory if it doesn't exist
            fs::create_dir_all(&partition_path).await?;
        }

        // Manually create an orphan file in the topic directory (where partition files are stored)
        let orphan_file_path = partition_path.join("orphan-file-123");
        fs::write(&orphan_file_path, "orphan content").await?;

        // Create reconciler with tracking
        let config = ReconcileConfig {
            track_files: true,
            ..Default::default()
        };
        let mut reconciler = ReconcileJob::with_config(catalog.clone(), config);

        // Run reconciliation
        let done = reconciler.run(Some(100)).await?;
        assert!(done);

        // Should have found one untracked file
        let stats = reconciler.stats();
        assert_eq!(stats.untracked_files.len(), 1);
        assert_eq!(stats.files_checked.len(), 2);

        // Verify the orphan file path is recorded when tracking is enabled
        match &stats.untracked_files.paths {
            PathStats::Paths(paths) => {
                assert_eq!(paths.len(), 1);
                assert_eq!(paths[0], orphan_file_path);
            }
            PathStats::Counter(_) => panic!("Expected Paths variant when track_files is enabled"),
        }

        Ok(())
    }

    #[test_log::test(tokio::test)]
    async fn test_reconcile_corrupted_segment_size() -> Result<()> {
        let (_tmpdir, catalog) = create_test_catalog().await;

        // Add some data to create segments
        let records: Vec<_> = vec!["record1", "record2", "record3"]
            .into_iter()
            .map(|message| Record {
                time: Utc::now(),
                message: message.bytes().collect(),
            })
            .collect();

        let topic_name = "corruption-test";
        let partition_name = "partition1";
        let topic = catalog.get_topic(topic_name).await;
        topic.extend_records(partition_name, &records).await?;
        topic.commit().await?;
        drop(topic);

        // Force a checkpoint to ensure files are written to disk
        catalog.checkpoint().await;

        // Partition files are stored in the topic directory, not a separate partition subdirectory
        let topic_root = Topic::partition_root(catalog.topic_root(), topic_name);
        let partition_path = topic_root.clone(); // Use topic root, not partition subdirectory

        if !partition_path.exists() {
            fs::create_dir_all(&partition_path).await?;
        }

        // Get the manifest and partition information
        let manifest = catalog.manifest();
        let partition_id = PartitionId::new(topic_name, partition_name);

        // Get the first segment for this partition
        let segments_stream = manifest.stream_segments(
            &partition_id,
            RecordIndex(0),
            crate::data::index::Ordering::Forward,
        );

        let segments: Vec<_> = segments_stream.collect().await;
        assert!(!segments.is_empty(), "Should have at least one segment");

        let segment_to_corrupt = segments[0].clone();
        let _original_size = segment_to_corrupt.size; // Keep for documentation, not used in test

        // Create the actual segment file manually to test size validation
        // This simulates a scenario where the file exists but has a different size than expected
        let slog_name = Partition::slog_name(&partition_id);
        let segment_file_name = format!("{}-{}", slog_name, segment_to_corrupt.index.0);
        let segment_file_path = partition_path.join(&segment_file_name);

        // Create a file with a different size than what's in the manifest
        // Make the difference much larger than the tolerance (200 bytes) to ensure it's detected
        let corrupted_size = segment_to_corrupt.size + 1000; // This will definitely be different from the real size
        let dummy_content = vec![0u8; corrupted_size];
        fs::write(&segment_file_path, &dummy_content).await?;

        // Create reconciler with tracking
        let config = ReconcileConfig {
            track_files: true,
            ..Default::default()
        };
        let mut reconciler = ReconcileJob::with_config(catalog.clone(), config.clone());

        // Now run reconciliation - it should detect the size mismatch
        let done = reconciler.run(Some(100)).await?;
        assert!(done);

        // We should check that there's at least one segment validated
        let stats = reconciler.stats();
        assert_eq!(stats.files_checked.len(), 1);

        // We should have found exactly one size mismatch
        assert_eq!(stats.size_mismatches.len(), 1);

        // Verify the corrupted file path is recorded when tracking is enabled
        match &stats.size_mismatches.paths {
            PathStats::Paths(paths) => {
                assert_eq!(paths.len(), 1);
                assert_eq!(paths[0], segment_file_path);
            }
            PathStats::Counter(_) => panic!("Expected Paths variant when track_files is enabled"),
        }

        // Now run a fix reconciliation
        let fix_config = ReconcileConfig {
            track_files: true,
            fixes: BTreeSet::from([ReconcileFix::UpdateManifestSizes]),
            ..Default::default()
        };
        let mut fix_reconciler = ReconcileJob::with_config(catalog.clone(), fix_config);

        info!("running a reconciliation fix job");
        // Run reconciliation with fix - it should fix the size mismatch
        let done = fix_reconciler.run(Some(100)).await?;
        assert!(done);

        // After fixing, we should still have validated segments but no size mismatches in stats
        // NOTE: The stats tracking the mismatches that were already found won't be cleared,
        // but the actual size comparison should now match
        let fix_stats = fix_reconciler.stats();
        assert_eq!(fix_stats.files_checked.len(), 1);

        // Now run another reconciliation to verify there are no errors
        let mut verify_reconciler = ReconcileJob::with_config(catalog.clone(), config.clone());
        let done = verify_reconciler.run(Some(100)).await?;
        assert!(done);

        // Verify no size mismatches are found after the fix
        let verify_stats = verify_reconciler.stats();
        assert_eq!(verify_stats.size_mismatches.len(), 0,
                   "Should detect no size mismatches after fix. Got: files_checked.len()={}, size_mismatches.len()={}, missing_files.len()={}",
                   verify_stats.files_checked.len(), verify_stats.size_mismatches.len(), verify_stats.missing_files.len());

        Ok(())
    }
}
