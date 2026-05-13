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
use crate::data::segment::Segment;
use rand::rngs::StdRng;
use rand::seq::{index, IndexedRandom};
use rand::SeedableRng;
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
    /// How to choose which topics/partitions to reconcile in a pass.
    #[serde(default)]
    pub sampling: SamplingStrategy,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub enum ReconcileFix {
    /// Workaround for inability to pass an empty collection via [config]
    Noop,
    UpdateManifestSizes,
    // TODO: RemoveOrphans,
    // TODO: RemoveUntrackedSegments
}

/// How a reconciliation pass selects work.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum SamplingStrategy {
    /// Reconcile every topic and partition in the catalog. This is the
    /// exhaustive default and the only mode that can detect orphan files
    /// (since orphan detection requires visiting every partition in a topic).
    All,
    /// Pick a random subset of topics and partitions to reconcile.
    ///
    /// Topics are sampled without replacement using harmonic weights over
    /// topics ordered newest-first by their most recent segment `time_end`:
    /// `weight(rank) = 1 / (rank + 1)`. This biases sampling strongly toward
    /// recently active topics while still giving older topics a chance to be
    /// audited. Within each chosen topic, `partitions` partitions are sampled
    /// uniformly at random without replacement.
    ///
    /// Orphan-file detection is skipped in this mode because we don't visit
    /// every partition.
    Stochastic {
        /// Number of topics to sample.
        topics: usize,
        /// Number of partitions to sample per chosen topic.
        partitions: usize,
    },
}

impl Default for SamplingStrategy {
    fn default() -> Self {
        Self::All
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
    /// Planned work for this pass: `(topic, partitions to check)`.
    /// `None` until the work has been planned for the current pass.
    work: Option<Vec<(String, Vec<String>)>>,
    /// Accumulator for segment files in the current topic. Only meaningful
    /// when reconciling every partition of the topic (i.e. orphan detection
    /// is enabled).
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
        // Plan the work for this pass if we haven't already.
        if self.state.work.is_none() {
            self.state.work = Some(self.plan_work().await);
        }
        let work = self.state.work.as_ref().unwrap();

        let current_topic_index = self.state.current_topic_index;
        let current_partition_index = self.state.current_partition_index;

        // If we've processed all topics, we're done
        if current_topic_index >= work.len() {
            return Ok(true);
        }

        let (topic_name, partitions) = &work[current_topic_index];
        let topic_name = topic_name.clone();
        let partitions_len = partitions.len();

        debug!("Reconciling topic: {}", topic_name);

        // If we've processed all partitions in this topic, move to the next topic.
        // Orphan detection only runs when we plan to visit every partition.
        if current_partition_index >= partitions_len {
            let work_len = work.len();
            let topic_segments = mem::take(&mut self.state.topic_segments);
            if self.scans_all_partitions() {
                self.identify_untracked_files(&topic_name, topic_segments)
                    .await?;
            }
            self.state.current_partition_index = 0;
            self.state.current_topic_index += 1;

            if self.state.current_topic_index >= work_len {
                return Ok(true);
            }

            return Ok(false);
        }

        let partition_name = partitions[current_partition_index].clone();
        debug!("Reconciling partition: {}/{}", topic_name, partition_name);

        let partition_segments = self
            .process_partition_phase(&topic_name, &partition_name)
            .await?;

        if self.scans_all_partitions() {
            self.state.topic_segments.extend(partition_segments);
        }
        self.state.current_partition_index += 1;

        Ok(false)
    }

    fn scans_all_partitions(&self) -> bool {
        matches!(self.config.sampling, SamplingStrategy::All)
    }

    /// Build the work list for this pass according to the sampling strategy.
    async fn plan_work(&self) -> Vec<(String, Vec<String>)> {
        match &self.config.sampling {
            SamplingStrategy::All => {
                let topics = self.catalog.manifest().get_topics().await;
                let mut work = Vec::with_capacity(topics.len());
                for topic in topics {
                    let partitions = self.catalog.manifest().get_partitions(&topic).await;
                    work.push((topic, partitions));
                }
                work
            }
            SamplingStrategy::Stochastic { topics, partitions } => {
                if *topics == 0 || *partitions == 0 {
                    return Vec::new();
                }

                let ordered = self.catalog.manifest().get_topics_by_recency().await;
                if ordered.is_empty() {
                    return Vec::new();
                }

                // StdRng (not ThreadRng) so the future remains `Send` across
                // `.await` points inside this loop.
                let mut rng = StdRng::from_os_rng();
                let want_topics = (*topics).min(ordered.len());

                // Sample distinct topic ranks without replacement, weighted
                // harmonically so newer topics dominate.
                let topic_indices = match index::sample_weighted(
                    &mut rng,
                    ordered.len(),
                    |rank| 1.0_f64 / (rank as f64 + 1.0),
                    want_topics,
                ) {
                    Ok(ix) => ix,
                    Err(e) => {
                        warn!("stochastic topic sampling failed: {e:?}");
                        return Vec::new();
                    }
                };

                let mut work = Vec::with_capacity(want_topics);
                for ix in topic_indices.iter() {
                    let topic = ordered[ix].clone();
                    let all_parts = self.catalog.manifest().get_partitions(&topic).await;
                    let chosen: Vec<String> = all_parts
                        .choose_multiple(&mut rng, *partitions)
                        .cloned()
                        .collect();
                    work.push((topic, chosen));
                }
                work
            }
        }
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
        self.state.work = None;
        self.state.topic_segments.clear();
        self.state.stats = if self.config.track_files {
            ReconcileStats::with_path_tracking()
        } else {
            ReconcileStats::default()
        };
    }

    /// Execute a single complete pass: reset state, plan work according to the
    /// configured [SamplingStrategy], and run to completion. Intended to be
    /// called periodically (e.g. from the catalog retention loop) for
    /// stochastic reconciliation.
    pub async fn pass(&mut self) -> Result<()> {
        self.reset().await;
        self.run(None).await?;
        Ok(())
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

    /// Helper: populate `catalog` with `topics * partitions_per_topic` partitions
    /// of dummy data, one record per partition. Topics are written in order so
    /// `topic-0` is oldest and `topic-{topics-1}` is newest.
    async fn seed_topics(catalog: &Catalog, topics: usize, partitions_per_topic: usize) {
        for t in 0..topics {
            let topic_name = format!("topic-{t}");
            let topic = catalog.get_topic(&topic_name).await;
            for p in 0..partitions_per_topic {
                let part_name = format!("p-{p}");
                let records = vec![Record {
                    time: Utc::now(),
                    message: format!("{topic_name}/{part_name}").into_bytes(),
                }];
                topic.extend_records(&part_name, &records).await.unwrap();
            }
            topic.commit().await.unwrap();
            // Small delay so MAX(time_end) is monotonically increasing across topics.
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    }

    #[test_log::test(tokio::test)]
    async fn test_stochastic_samples_requested_counts() -> Result<()> {
        let (_tmpdir, catalog) = create_test_catalog().await;
        seed_topics(&catalog, 6, 4).await;
        catalog.checkpoint().await;

        let config = ReconcileConfig {
            track_files: true,
            sampling: SamplingStrategy::Stochastic {
                topics: 3,
                partitions: 2,
            },
            ..Default::default()
        };
        let mut reconciler = ReconcileJob::with_config(catalog.clone(), config);

        reconciler.pass().await?;

        // The plan should contain exactly the requested counts; each chosen
        // topic should be distinct and each partition list distinct within it.
        let plan = reconciler.state.work.as_ref().expect("plan was built");
        assert_eq!(plan.len(), 3, "expected 3 sampled topics");
        let mut seen_topics: BTreeSet<&str> = BTreeSet::new();
        for (topic, parts) in plan {
            assert!(seen_topics.insert(topic.as_str()), "duplicate topic {topic}");
            assert_eq!(parts.len(), 2, "expected 2 partitions for {topic}");
            let unique: BTreeSet<&str> = parts.iter().map(String::as_str).collect();
            assert_eq!(unique.len(), 2, "duplicate partitions for {topic}");
        }

        // Validation must have happened against real segments on disk: sizes
        // accumulate and no missing / size-mismatch findings are produced.
        let stats = reconciler.stats();
        assert!(
            stats.expected_size.as_u64() > 0,
            "expected_size should accumulate from sampled segments"
        );
        assert_eq!(stats.expected_size, stats.actual_size);
        assert_eq!(stats.missing_files.len(), 0);
        assert_eq!(stats.size_mismatches.len(), 0);
        // Stochastic mode deliberately skips the orphan-file scan.
        assert_eq!(stats.untracked_files.len(), 0);

        Ok(())
    }

    #[test_log::test(tokio::test)]
    async fn test_stochastic_biases_toward_newer_topics() -> Result<()> {
        let (_tmpdir, catalog) = create_test_catalog().await;
        let n_topics = 8;
        seed_topics(&catalog, n_topics, 1).await;
        catalog.checkpoint().await;

        // Older half = topic-0..3, newer half = topic-4..7.
        // With harmonic weights `1/(rank+1)` over the newest-first ordering,
        // the top 4 ranks (newer half) get cumulative weight
        // 1 + 1/2 + 1/3 + 1/4 ≈ 2.083, vs. older 1/5+...+1/8 ≈ 0.635 — newer
        // topics should be sampled ~3x as often.
        let mut newer_hits = 0u32;
        let mut older_hits = 0u32;
        let trials = 200;
        for _ in 0..trials {
            let config = ReconcileConfig {
                track_files: true,
                sampling: SamplingStrategy::Stochastic {
                    topics: 1,
                    partitions: 1,
                },
                ..Default::default()
            };
            let reconciler = ReconcileJob::with_config(catalog.clone(), config);
            let work = reconciler.plan_work().await;
            assert_eq!(work.len(), 1);
            let topic_ix: usize = work[0]
                .0
                .strip_prefix("topic-")
                .unwrap()
                .parse()
                .unwrap();
            if topic_ix >= n_topics / 2 {
                newer_hits += 1;
            } else {
                older_hits += 1;
            }
        }
        // Generous bound to avoid flakes: newer half should clearly dominate.
        assert!(
            newer_hits > older_hits * 2,
            "expected newer topics to be sampled at least 2x as often; got newer={newer_hits} older={older_hits}"
        );

        Ok(())
    }

    #[test_log::test(tokio::test)]
    async fn test_stochastic_skips_orphan_check() -> Result<()> {
        // With stochastic sampling we may not visit every partition in a topic,
        // so files belonging to unvisited partitions must NOT be flagged as
        // orphans. The equivalent `All` pass over the same data would also see
        // zero orphans, but it WOULD populate `files_checked` from the topic
        // directory scan — which stochastic mode skips by design.
        let (_tmpdir, catalog) = create_test_catalog().await;
        seed_topics(&catalog, 1, 4).await;
        catalog.checkpoint().await;

        let config = ReconcileConfig {
            track_files: true,
            sampling: SamplingStrategy::Stochastic {
                topics: 1,
                partitions: 1, // only one of the four partitions visited
            },
            ..Default::default()
        };
        let mut reconciler = ReconcileJob::with_config(catalog.clone(), config);
        reconciler.pass().await?;

        let stats = reconciler.stats();
        assert_eq!(stats.untracked_files.len(), 0);
        // No directory scan ⇒ files_checked stays empty, but a real segment
        // was validated which shows up in expected_size.
        assert_eq!(stats.files_checked.len(), 0);
        assert!(stats.expected_size.as_u64() > 0);

        Ok(())
    }

    #[test_log::test(tokio::test)]
    async fn test_stochastic_zero_request_is_noop() -> Result<()> {
        let (_tmpdir, catalog) = create_test_catalog().await;
        seed_topics(&catalog, 3, 2).await;
        catalog.checkpoint().await;

        let config = ReconcileConfig {
            track_files: true,
            sampling: SamplingStrategy::Stochastic {
                topics: 0,
                partitions: 5,
            },
            ..Default::default()
        };
        let mut reconciler = ReconcileJob::with_config(catalog, config);
        reconciler.pass().await?;
        assert_eq!(reconciler.stats().files_checked.len(), 0);

        Ok(())
    }

    #[test_log::test(tokio::test)]
    async fn test_all_strategy_is_default() {
        let config = ReconcileConfig::default();
        assert!(matches!(config.sampling, SamplingStrategy::All));
    }
}
