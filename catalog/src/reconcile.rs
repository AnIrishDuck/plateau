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

use crate::slog::SegmentIndex;

use tokio::fs;

use crate::data::segment::Segment;
use anyhow::Result;
use bytesize::ByteSize;
use futures::stream::StreamExt;
use serde::{Deserialize, Serialize};
use tracing::{debug, info, warn};

use crate::catalog::Catalog;
use crate::data::RecordIndex;
use crate::manifest::{PartitionId, SegmentData};
use crate::partition::Partition;
use crate::slog::Slog;
use crate::topic::Topic;

/// Whether a segment is sealed relative to a partition's `sealed_ix` watermark.
///
/// A segment is sealed only when the partition has a watermark (`Some`) and the
/// segment index is at or below it. When the watermark is `None` (no segment has
/// sealed durably yet) every segment is treated as active.
fn is_sealed(sealed_ix: Option<SegmentIndex>, index: SegmentIndex) -> bool {
    sealed_ix.is_some_and(|watermark| index <= watermark)
}

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
    /// Report accumulated during the reconciliation (sealed + active buckets)
    report: ReconcileReport,
}

impl ReconcileState {
    pub fn new(track_files: bool) -> Self {
        Self {
            report: if track_files {
                ReconcileReport::with_path_tracking()
            } else {
                ReconcileReport::default()
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

/// An informational report for the currently *active* (writeable) tail segment
/// of a single partition.
///
/// The active segment is deliberately excluded from the strict sealed-diff
/// pipeline. Writes accumulate in it before the manifest is flushed, so its
/// on-disk size routinely runs ahead of the size recorded in the manifest.
/// Treating that as a "size mismatch" would flag the active segment of every
/// partition on every pass. This bucket exists to surface that drift without
/// ever acting on it — reconciliation fixes never touch the active segment.
///
/// How to read the signals:
/// - `delta != 0` is *expected*: disk is ahead of the manifest between flushes.
/// - `delta < 0` is alert-worthy: the manifest claims more bytes than exist on
///   disk, which points at corruption or an accounting bug.
/// - `delta` growing unboundedly across consecutive scans suggests manifest
///   updates are stuck or falling behind.
#[derive(Debug, Clone)]
pub struct ActiveSegmentReport {
    /// Topic the active segment belongs to.
    pub topic: String,
    /// Partition the active segment belongs to.
    pub partition: String,
    /// Size the manifest currently records for the active segment.
    pub manifest_size: usize,
    /// Size observed on disk (main segment file plus any parts).
    pub disk_size: usize,
    /// `disk_size as i64 - manifest_size as i64`. Signed; negative is an alert
    /// signal (manifest claims more bytes than disk has).
    pub delta: i64,
}

/// The full output of a reconciliation pass, split into two buckets.
///
/// `sealed` is the strict pipeline: every segment at or below a partition's
/// `sealed_ix` watermark is diffed against the manifest and, when configured,
/// fixed. This is the historical [ReconcileStats] behaviour, unchanged.
///
/// `active` is informational only: one entry per partition whose active tail
/// segment was observed during the pass. Reconciliation fixes never touch the
/// active bucket. See [ActiveSegmentReport] for how to interpret its entries.
#[derive(Debug, Clone, Default)]
pub struct ReconcileReport {
    /// Strict sealed-segment diffs (the historical [ReconcileStats] shape).
    pub sealed: ReconcileStats,
    /// Informational per-partition active-segment reports.
    pub active: Vec<ActiveSegmentReport>,
}

impl ReconcileReport {
    fn with_path_tracking() -> Self {
        Self {
            sealed: ReconcileStats::with_path_tracking(),
            active: Vec::new(),
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
                let report = self.report();
                info!("Reconciliation complete: {:?}", report);
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
            .report
            .sealed
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
                    .report
                    .sealed
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

        // Read the sealed watermark once for this partition pass. Segments at or
        // below it are durable and run through the strict sealed-diff pipeline;
        // anything above it (or every segment, if the partition has not sealed
        // one yet) is the currently active tail and goes to the informational
        // active bucket instead. The watermark never moves backward, so this
        // single read is a stable basis for the whole pass.
        let sealed_ix = {
            let topic = self.catalog.get_topic(topic_name).await;
            let partition = topic.get_partition(partition_name).await;
            partition.sealed_ix()
        };

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
                "Fetched {} segments: {} ..= {} (sealed_ix={:?})",
                segments.len(),
                start.index.0,
                end.index.0,
                sealed_ix,
            );
        } else {
            debug!("Found no segments")
        }

        // Validate each segment
        for segment in segments {
            debug!("Validating segment: {:?}", segment.index);

            let slog_name = Partition::slog_name(&partition_id);
            let segment_file_name = format!("{}-{}", slog_name, segment.index.0);
            let segment_path = Slog::segment_path(&topic_path, &slog_name, segment.index);

            debug!("Checking segment file: {} at {:?}", slog_name, segment_path);

            // Mark this file as tracked regardless of bucket so the orphan
            // detection phase does not false-positive on active segment files.
            tracked_files.insert(segment_path.clone());

            // A segment is "sealed" only when the partition has a watermark and
            // this segment's index is at or below it. Everything else is the
            // active tail (this includes the all-segments-active case when the
            // partition has never sealed a segment, i.e. sealed_ix is None).
            if is_sealed(sealed_ix, segment.index) {
                self.process_sealed_segment(
                    &partition_id,
                    &segment,
                    &segment_file_name,
                    segment_path,
                    &mut tracked_files,
                )
                .await;
            } else {
                self.process_active_segment(
                    topic_name,
                    partition_name,
                    &segment,
                    &segment_file_name,
                    segment_path,
                    &mut tracked_files,
                )
                .await;
            }
        }

        // We've completed processing this partition
        Ok(tracked_files)
    }

    /// Strict diff (and optional CAS-fix) for a sealed segment. This preserves
    /// the historical reconciliation behaviour and only ever touches the sealed
    /// bucket of the report.
    async fn process_sealed_segment(
        &mut self,
        partition_id: &PartitionId,
        segment: &SegmentData,
        segment_file_name: &str,
        segment_path: PathBuf,
        tracked_files: &mut BTreeSet<PathBuf>,
    ) {
        // Check if the file exists
        if !segment_path.exists() {
            warn!("Missing file {:?}", segment_path);
            // Add the missing path to our stats
            self.state
                .report
                .sealed
                .missing_files
                .add_path(segment_path.clone(), segment.size);
            return;
        }

        let total_actual_size = self
            .segment_disk_size(&segment_path, segment_file_name, tracked_files)
            .await;

        let expected_size = ByteSize(segment.size as u64);
        let actual_size = ByteSize(total_actual_size as u64);
        self.state.report.sealed.expected_size = ByteSize(
            self.state.report.sealed.expected_size.as_u64() + expected_size.as_u64(),
        );
        self.state.report.sealed.actual_size =
            ByteSize(self.state.report.sealed.actual_size.as_u64() + actual_size.as_u64());

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
            self.state.report.sealed.size_mismatches.add_path(
                segment_path.clone(),
                // NOTE: this is probably not ideal as it can "overcount" the total difference
                total_actual_size.abs_diff(segment.size),
            );

            if self
                .config
                .fixes
                .contains(&ReconcileFix::UpdateManifestSizes)
            {
                // Apply the fix conditionally on the size we observed. If
                // retention removed this segment (or another writer changed
                // it) since we snapshotted the manifest, the update is a
                // no-op rather than re-creating a stale row. This keeps the
                // fix safe to run while writes and retention are in flight.
                let applied = self
                    .catalog
                    .manifest()
                    .update_size_if_unchanged(
                        partition_id,
                        segment.index,
                        segment.size,
                        total_actual_size,
                    )
                    .await;
                if applied {
                    info!("Fixed size mismatch for segment {}", segment_file_name);
                } else {
                    info!(
                        "Skipped stale size fix for segment {} (concurrently removed or changed)",
                        segment_file_name
                    );
                }
            }
        } else {
            debug!(
                "Segment {} size ok. Expected: {}, actual: {}",
                segment_file_name, segment.size, total_actual_size
            );
        }
    }

    /// Record an informational active-segment report. This never contributes a
    /// size mismatch to the sealed bucket and never applies a fix — the active
    /// tail segment is, by definition, not sealed, so its on-disk size legally
    /// runs ahead of the manifest between flushes.
    async fn process_active_segment(
        &mut self,
        topic_name: &str,
        partition_name: &str,
        segment: &SegmentData,
        segment_file_name: &str,
        segment_path: PathBuf,
        tracked_files: &mut BTreeSet<PathBuf>,
    ) {
        let disk_size = if segment_path.exists() {
            self.segment_disk_size(&segment_path, segment_file_name, tracked_files)
                .await
        } else {
            // The active segment may not have been flushed to disk yet. A zero
            // disk size against a non-zero manifest size yields a negative delta,
            // which is exactly the alert signal we want to surface.
            debug!("Active segment file {:?} not present on disk", segment_path);
            0
        };

        let manifest_size = segment.size;
        let delta = disk_size as i64 - manifest_size as i64;
        debug!(
            "Active segment {} for {}/{}: manifest_size={}, disk_size={}, delta={}",
            segment_file_name, topic_name, partition_name, manifest_size, disk_size, delta
        );

        self.state.report.active.push(ActiveSegmentReport {
            topic: topic_name.to_string(),
            partition: partition_name.to_string(),
            manifest_size,
            disk_size,
            delta,
        });
    }

    /// Compute the on-disk size of a segment: the main file plus any parts.
    /// Every part path encountered is inserted into `tracked_files` so the
    /// orphan-detection phase does not flag it. The caller is responsible for
    /// confirming the main segment file exists before calling.
    async fn segment_disk_size(
        &self,
        segment_path: &Path,
        segment_file_name: &str,
        tracked_files: &mut BTreeSet<PathBuf>,
    ) -> usize {
        let mut total_actual_size = 0;

        // Check main segment file
        match fs::metadata(segment_path).await {
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
        let segment_file = Segment::at(segment_path.to_path_buf());
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
                            warn!("Error getting metadata for part {:?}: {:?}", part_path, e);
                        }
                    }
                }
            } else {
                debug!("Part {:?} does not exist", part_path);
            }
        }

        total_actual_size
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

    /// Get the current reconciliation report (sealed + active buckets)
    pub fn report(&self) -> &ReconcileReport {
        &self.state.report
    }

    /// Reset the reconciliation job to start from the beginning
    pub async fn reset(&mut self) {
        self.state.current_topic_index = 0;
        self.state.current_partition_index = 0;
        self.state.topics = None;
        self.state.report = if self.config.track_files {
            ReconcileReport::with_path_tracking()
        } else {
            ReconcileReport::default()
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

    /// Create a catalog whose partitions roll after `max_rows` rows, so tests
    /// can deterministically force a segment to seal.
    async fn create_test_catalog_rolling(max_rows: usize) -> (TempDir, Arc<Catalog>) {
        let dir = TempDir::new().unwrap();
        let root = PathBuf::from(dir.path());
        let mut config = Config::default();
        config.partition.roll.max_rows = max_rows;
        let catalog = Catalog::attach(root, config).await.unwrap();
        (dir, Arc::new(catalog))
    }

    fn test_records(messages: &[&str]) -> Vec<Record> {
        messages
            .iter()
            .map(|message| Record {
                time: Utc::now(),
                message: message.bytes().collect(),
            })
            .collect()
    }

    /// Look up the size the manifest currently records for a single segment.
    async fn manifest_segment_size(
        catalog: &Catalog,
        partition_id: &PartitionId,
        index: SegmentIndex,
    ) -> Option<usize> {
        catalog
            .manifest()
            .get_segment_data(index.to_id(partition_id))
            .await
            .map(|data| data.size)
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

        let stats = &reconciler.report().sealed;
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
        let stats = &reconciler.report().sealed;
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
        let stats = &reconciler.report().sealed;
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
        let stats = &reconciler.report().sealed;
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
        let stats = &reconciler.report().sealed;
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
        let stats = &reconciler.report().sealed;
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
        let fix_stats = &fix_reconciler.report().sealed;
        assert_eq!(fix_stats.files_checked.len(), 1);

        // Now run another reconciliation to verify there are no errors
        let mut verify_reconciler = ReconcileJob::with_config(catalog.clone(), config.clone());
        let done = verify_reconciler.run(Some(100)).await?;
        assert!(done);

        // Verify no size mismatches are found after the fix
        let verify_stats = &verify_reconciler.report().sealed;
        assert_eq!(verify_stats.size_mismatches.len(), 0,
                   "Should detect no size mismatches after fix. Got: files_checked.len()={}, size_mismatches.len()={}, missing_files.len()={}",
                   verify_stats.files_checked.len(), verify_stats.size_mismatches.len(), verify_stats.missing_files.len());

        Ok(())
    }

    /// Collect the active-bucket entries for a single partition.
    fn active_entries<'a>(
        report: &'a ReconcileReport,
        topic: &str,
        partition: &str,
    ) -> Vec<&'a ActiveSegmentReport> {
        report
            .active
            .iter()
            .filter(|a| a.topic == topic && a.partition == partition)
            .collect()
    }

    /// A partition that has data but has never rolled (sealed_ix == None) keeps
    /// its only segment in the active bucket; it must never appear as a sealed
    /// size mismatch.
    #[test_log::test(tokio::test)]
    async fn test_active_segment_not_sealed_mismatch() -> Result<()> {
        // A high roll threshold keeps a single batch in the active segment, so
        // no roll happens and the watermark stays None.
        let (_tmpdir, catalog) = create_test_catalog_rolling(1000).await;

        let topic_name = "active-only";
        let partition_name = "p0";

        let topic = catalog.get_topic(topic_name).await;
        topic
            .extend_records(partition_name, &test_records(&["a", "b", "c"]))
            .await?;
        drop(topic);

        // Make the active segment durable in the manifest without sealing it.
        catalog.checkpoint().await;
        catalog
            .get_topic(topic_name)
            .await
            .ensure_index(partition_name, RecordIndex(3))
            .await?;

        // Sanity: the partition has not sealed a segment yet.
        {
            let topic = catalog.get_topic(topic_name).await;
            let partition = topic.get_partition(partition_name).await;
            assert_eq!(partition.sealed_ix(), None);
        }

        let config = ReconcileConfig {
            track_files: true,
            ..Default::default()
        };
        let mut reconciler = ReconcileJob::with_config(catalog.clone(), config);
        assert!(reconciler.run(Some(100)).await?);

        let report = reconciler.report();
        // The active segment must not surface as a sealed-bucket problem.
        assert_eq!(report.sealed.size_mismatches.len(), 0);
        assert_eq!(report.sealed.missing_files.len(), 0);

        // It must show up exactly once in the active bucket, with real sizes.
        let active = active_entries(report, topic_name, partition_name);
        assert_eq!(active.len(), 1);
        assert!(active[0].manifest_size > 0);
        assert!(active[0].disk_size > 0);

        Ok(())
    }

    /// After a roll, the just-sealed segment runs through the strict sealed
    /// pipeline while the new active segment lands in the active bucket. A
    /// consistent system reports no problems in either bucket.
    #[test_log::test(tokio::test)]
    async fn test_just_rolled_segment_in_sealed_bucket() -> Result<()> {
        // Roll after 2 rows so the second batch seals segment 0.
        let (_tmpdir, catalog) = create_test_catalog_rolling(2).await;
        let topic_name = "rolled";
        let partition_name = "p0";

        let topic = catalog.get_topic(topic_name).await;
        // First batch fills the active segment past the roll threshold.
        topic
            .extend_records(partition_name, &test_records(&["a", "b", "c"]))
            .await?;
        // Second batch rolls (and seals) segment 0, then lands in segment 1.
        // The roll waits for segment 0's seal to be durable.
        topic
            .extend_records(partition_name, &test_records(&["d", "e", "f"]))
            .await?;
        drop(topic);

        // Make the active segment (1) durable so it appears in the manifest,
        // and finalize segment 0's file on disk.
        catalog.checkpoint().await;
        catalog
            .get_topic(topic_name)
            .await
            .ensure_index(partition_name, RecordIndex(6))
            .await?;
        catalog.checkpoint().await;

        // Segment 0 sealed; segment 1 is the active tail.
        {
            let topic = catalog.get_topic(topic_name).await;
            let partition = topic.get_partition(partition_name).await;
            assert_eq!(partition.sealed_ix(), Some(SegmentIndex(0)));
        }

        let config = ReconcileConfig {
            track_files: true,
            ..Default::default()
        };
        let mut reconciler = ReconcileJob::with_config(catalog.clone(), config);
        assert!(reconciler.run(Some(100)).await?);

        let report = reconciler.report();
        // The rolled segment was checked in the sealed bucket.
        assert!(report.sealed.files_checked.len() >= 1);
        // Healthy system: no problems in either bucket.
        assert_eq!(report.sealed.size_mismatches.len(), 0);
        assert_eq!(report.sealed.missing_files.len(), 0);

        // The still-active segment shows up in the active bucket.
        let active = active_entries(report, topic_name, partition_name);
        assert_eq!(active.len(), 1);

        Ok(())
    }

    /// The UpdateManifestSizes fix must never apply to an active segment, even
    /// when its on-disk size is corrupted. The drift is reported (non-zero
    /// delta) but the manifest is left untouched.
    #[test_log::test(tokio::test)]
    async fn test_fix_never_targets_active_segment() -> Result<()> {
        use tokio::io::AsyncWriteExt;

        // High roll threshold keeps the single segment active (sealed_ix None).
        let (_tmpdir, catalog) = create_test_catalog_rolling(1000).await;
        let topic_name = "active-fix";
        let partition_name = "p0";

        let topic = catalog.get_topic(topic_name).await;
        topic
            .extend_records(partition_name, &test_records(&["a", "b", "c"]))
            .await?;
        drop(topic);
        catalog.checkpoint().await;
        catalog
            .get_topic(topic_name)
            .await
            .ensure_index(partition_name, RecordIndex(3))
            .await?;

        let partition_id = PartitionId::new(topic_name, partition_name);
        // Record the manifest's stored size for the (active) segment 0.
        let original_size = manifest_segment_size(&catalog, &partition_id, SegmentIndex(0))
            .await
            .expect("segment 0 should be in the manifest");

        // Corrupt the on-disk size by appending extra bytes to the active
        // segment file before any roll seals it.
        let topic_path = Topic::partition_root(catalog.topic_root(), topic_name);
        let slog_name = Partition::slog_name(&partition_id);
        let segment_path = Slog::segment_path(&topic_path, &slog_name, SegmentIndex(0));
        {
            let mut f = fs::OpenOptions::new().append(true).open(&segment_path).await?;
            f.write_all(&vec![0u8; 1000]).await?;
            f.flush().await?;
        }

        let fix_config = ReconcileConfig {
            track_files: true,
            fixes: BTreeSet::from([ReconcileFix::UpdateManifestSizes]),
            ..Default::default()
        };
        let mut reconciler = ReconcileJob::with_config(catalog.clone(), fix_config);
        assert!(reconciler.run(Some(100)).await?);

        // The fix must not have touched the active segment's manifest size.
        let after_size = manifest_segment_size(&catalog, &partition_id, SegmentIndex(0))
            .await
            .expect("segment 0 should still be in the manifest");
        assert_eq!(
            after_size, original_size,
            "active-segment manifest size must be unchanged by the fix"
        );

        let report = reconciler.report();
        // The corruption is never a sealed-bucket mismatch for an active segment.
        assert_eq!(report.sealed.size_mismatches.len(), 0);

        // The active bucket reports the (positive) drift we introduced.
        let active = active_entries(report, topic_name, partition_name);
        assert_eq!(active.len(), 1);
        assert!(
            active[0].delta > 0,
            "expected positive delta, got {}",
            active[0].delta
        );

        Ok(())
    }
}
