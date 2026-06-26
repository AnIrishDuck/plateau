//! The catalog indexes all currently attached topics.
//! It is used to route reads and writes to the correct topic / partition.

use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use bytesize::ByteSize;
use chrono::Utc;
use futures::future::join_all;
use metrics::gauge;
use rand::seq::IteratorRandom;
use serde::{Deserialize, Serialize};
use tokio::{
    sync::{watch, Mutex, RwLock, RwLockReadGuard},
    time,
};
use tokio_stream::wrappers::IntervalStream;
use tracing::{debug, error, info, trace, warn};

use crate::data::limit::Retention;
use crate::manifest::Manifest;
use crate::manifest::Scope;
use crate::partition;
use crate::reconcile::ReconcileReport;
use crate::storage::{self, DiskMonitor};
use crate::topic::Topic;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default)]
pub struct Config {
    #[serde(with = "humantime_serde")]
    pub checkpoint_interval: Duration,
    pub retain: Retention,
    pub retention: CatalogRetention,
    #[serde(default = "Catalog::default_headroom")]
    pub headroom: ByteSize,
    pub partition: partition::Config,
    pub storage: storage::Config,
    #[serde(default = "Catalog::default_max_open_topics")]
    pub max_open_topics: usize,
    pub max_partition_bytes: ByteSize,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            checkpoint_interval: Duration::from_millis(1000),
            retain: Default::default(),
            retention: CatalogRetention::default(),
            headroom: Catalog::default_headroom(),
            partition: Default::default(),
            storage: Default::default(),
            max_open_topics: Catalog::default_max_open_topics(),
            max_partition_bytes: ByteSize::mib(3500),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum CatalogRetention {
    Fixed(Retention),
    RootMountTotal,
}

impl CatalogRetention {
    async fn resolve(&self, root: &Path) -> anyhow::Result<Retention> {
        match self {
            Self::Fixed(r) => Ok(r.clone()),
            Self::RootMountTotal => {
                let stat = storage::path_mount_stat(root.to_owned()).await?;
                // `total` includes blocks reserved for root (typically ~5% on
                // ext4), which are never usable by plateau. Use avail + used
                // (i.e. total - reserved) so retention fires before the storage
                // monitor's min_available threshold.
                let used = stat.total.0.saturating_sub(stat.free.0);
                let mount_size = ByteSize(stat.avail.0.saturating_add(used));
                info!(
                    ?root,
                    total = %stat.total,
                    free = %stat.free,
                    avail = %stat.avail,
                    %used,
                    %mount_size,
                    "resolved size for retention"
                );

                Ok(Retention {
                    max_bytes: mount_size,
                    ..Default::default()
                })
            }
        }
    }

    fn prior_default() -> Retention {
        Retention {
            max_bytes: ByteSize::gib(99),
            ..Default::default()
        }
    }
}

impl Default for CatalogRetention {
    fn default() -> Self {
        Self::Fixed(Self::prior_default())
    }
}

#[derive(Debug)]
#[must_use = "close() explicitly to flush writes"]
pub struct Catalog {
    config: Config,
    manifest: Manifest,
    root: PathBuf,
    topic_root: PathBuf,
    state: RwLock<State>,
    disk_monitor: DiskMonitor,
    /// Single-flight guard for fix-enabled reconciliation passes. Held for the
    /// duration of a pass (see [crate::reconcile::ReconcileJob]); a second
    /// fix-enabled pass that cannot acquire it degrades to report-only instead
    /// of overlapping. Report-only passes never take it.
    reconcile_fix_guard: Arc<Mutex<()>>,
    /// The latest completed reconciliation report. Whoever finishes a pass
    /// publishes it here; readers (e.g. the `/info` handler) snapshot the
    /// current value without blocking on an in-flight pass. `None` until the
    /// first pass completes. A [watch] channel also lets future consumers await
    /// new reports, which the continuous reconciliation loop will want.
    reconcile_report: watch::Sender<Option<Arc<ReconcileReport>>>,
}

#[derive(Debug)]
struct State {
    topics: HashMap<String, Topic>,
    last_checkpoint: SystemTime,
}

impl Catalog {
    pub async fn attach(root: PathBuf, mut config: Config) -> anyhow::Result<Self> {
        let manifest = Manifest::current_prior_attach(
            root.join("manifest.sqlite"),
            root.join("manifest.json"),
        )
        .await?;

        let mut topic_root = root.clone();
        topic_root.push("topics");
        if !topic_root.exists() {
            std::fs::create_dir(&topic_root)?;
        }
        Self::migrate_topics(&manifest, &root, &topic_root).await?;

        if config.retain != CatalogRetention::prior_default() {
            warn!(
                "catalog.config.retain is obsolete and has no effect. see catalog.config.retention"
            )
        }

        config.retain = config.retention.resolve(&root).await?;

        Ok(Self::attach_v0(manifest, root, topic_root, config).await)
    }

    async fn attach_v0(
        manifest: Manifest,
        root: PathBuf,
        topic_root: PathBuf,
        config: Config,
    ) -> Self {
        let disk_monitor = DiskMonitor::new();
        Self {
            config,
            manifest,
            root,
            topic_root,
            state: RwLock::new(State {
                topics: HashMap::new(),
                last_checkpoint: SystemTime::now(),
            }),
            disk_monitor,
            reconcile_fix_guard: Arc::new(Mutex::new(())),
            reconcile_report: watch::channel(None).0,
        }
    }

    pub async fn migrate_topics(
        manifest: &Manifest,
        root: &Path,
        topic_root: &Path,
    ) -> anyhow::Result<()> {
        for topic in manifest.get_topics().await {
            let topic = PathBuf::from(topic);
            let src: PathBuf = root.join(&topic);
            let dst: PathBuf = topic_root.join(&topic);

            if src.exists() {
                info!("migrating {src:?} to {dst:?}");
                std::fs::rename(src, dst)?;
            }
        }

        Ok(())
    }

    pub async fn last_checkpoint(&self) -> SystemTime {
        self.state.read().await.last_checkpoint
    }

    pub async fn checkpoint(&self) {
        trace!("begin full catalog checkpoint");
        let start = SystemTime::now();
        let state = self.state.read().await;
        for (_, topic) in state.topics.iter() {
            topic.checkpoint().await
        }
        let end = SystemTime::now();
        if let Ok(duration) = end.duration_since(start) {
            if duration > self.config.checkpoint_interval {
                warn!(
                    "full catalog checkpoint took {:?} (longer than interval ({:?})!)",
                    duration, self.config.checkpoint_interval
                );
            } else {
                trace!("finished full catalog checkpoint in {:?}", duration);
            }
            gauge!("catalog_checkpoint_ms",).set((duration.as_micros() as f64) / 1000.0);
        } else {
            warn!("finished full catalog checkpoint; time skew");
        }

        drop(state);
        self.state.write().await.last_checkpoint = end;
    }

    pub async fn checkpoints(catalog: Arc<Self>) {
        use futures::stream::StreamExt;

        let mut checkpoints = time::interval(catalog.config.checkpoint_interval);
        checkpoints.set_missed_tick_behavior(time::MissedTickBehavior::Delay);
        IntervalStream::new(checkpoints)
            .for_each(|_| async {
                let r = catalog.as_ref();
                r.checkpoint().await;
                r.retain().await;
            })
            .await;
    }

    pub async fn retain(&self) {
        trace!("begin global retention check");
        self.gauge_topics().await;
        self.prune_topics().await;
        while self.over_retention_limit().await {
            // errors in here are effectively unrecoverable as the loop would otherwise spin.
            // additionally, the disk will eventually fill leading to system failure
            let oldest = self
                .manifest
                .get_oldest_segment(None)
                .await
                .expect("no partition to remove");

            let topic = self.get_topic(oldest.topic()).await;
            let partition = topic.get_partition(oldest.partition()).await;
            partition.remove_oldest().await;
        }
        trace!("end global retention check");
    }

    #[tracing::instrument(skip_all, level = "debug")]
    pub async fn gauge_topics(&self) {
        let names = self.list_topics().await;
        for name in names {
            let bytes = self.manifest.get_size(Scope::Topic(&name)).await;
            let labels = [("topic", name)];

            trace!(?labels, %bytes);
            gauge!("topic_bytes", &labels).set(bytes as f64);
        }
    }

    pub async fn prune_topics(&self) {
        let topics = &mut self.state.write().await.topics;
        while topics.len() > self.config.max_open_topics {
            let to_drop = {
                let mut rng = rand::rng();
                topics
                    .keys()
                    .choose(&mut rng)
                    .expect("no topics left")
                    .clone()
            };

            info!(
                "open topic limit hit ({} > {}), dropping \"{}\"",
                topics.len(),
                self.config.max_open_topics,
                to_drop
            );

            if let Some(topic) = topics.remove(&to_drop) {
                topic.close().await;
            }
        }

        let mut bytes = 0;
        let mut ages = BTreeMap::default();
        for (topic_name, topic) in topics.iter() {
            for (partition_name, data) in topic.active_data().await {
                ages.insert(*data.time.end(), (topic_name.clone(), partition_name));
                bytes += data.size;
            }
        }

        let max_bytes = self.config.max_partition_bytes.as_u64() as usize;
        trace!(bytes, max_bytes);
        while bytes > max_bytes {
            let Some((time, (topic_name, partition_name))) = ages.pop_first() else {
                error!("ran out of topics while trying to prune");
                return;
            };

            // XXX - these errors should never happen as we hold the lock and just iterated above
            let Some(topic) = topics.get(&topic_name) else {
                error!("invalid topic {topic_name}");
                continue;
            };

            let age = Utc::now().signed_duration_since(time).to_std();
            info!("closing {topic_name}/{partition_name} (age {age:?}, {bytes} > {max_bytes})");
            let Some(data) = topic.close_partition(&partition_name).await else {
                error!("invalid partition {partition_name}");
                continue;
            };

            bytes -= data.size;
        }
    }

    async fn byte_size(&self) -> ByteSize {
        ByteSize::b(self.manifest.get_size(Scope::Global).await as u64)
    }

    pub fn total_byte_limit(&self) -> ByteSize {
        ByteSize(
            self.config
                .retain
                .max_bytes
                .0
                .saturating_sub(self.config.headroom.0),
        )
    }

    async fn over_retention_limit(&self) -> bool {
        let size = self.byte_size().await;
        let limit = self.total_byte_limit();
        debug!(?limit, "catalog size: {}", size);
        gauge!("stored_size_bytes").set(size.as_u64() as f64);
        let over = size > limit;

        if over {
            info!("over retention limit {} > {}", size, limit);
        }

        over
    }

    pub async fn list_topics(&self) -> Vec<String> {
        let mem_topics = &self.state.read().await.topics;
        let mut topics: Vec<String> = mem_topics.keys().cloned().collect();
        let disk_topics: Vec<String> = self
            .manifest
            .get_topics()
            .await
            .into_iter()
            .filter(|t| !topics.contains(t))
            .collect();
        topics.extend(disk_topics);
        topics
    }

    pub async fn get_topic(&self, name: &str) -> RwLockReadGuard<'_, Topic> {
        let read = self.state.read().await;
        let v = RwLockReadGuard::try_map(read, |m| m.topics.get(name));
        match v {
            Ok(topic) => topic,
            Err(read) => {
                drop(read);
                let mut write = self.state.write().await;
                info!("creating new topic: {}", name);
                let topic = Topic::attach(
                    self.topic_root.clone(),
                    self.manifest.clone(),
                    String::from(name),
                    self.config.partition.clone(),
                )
                .await;
                write.topics.insert(String::from(name), topic);
                let read = write.downgrade();
                RwLockReadGuard::map(read, |m| m.topics.get(name).unwrap())
            }
        }
    }

    /// Returns true if the Catalog is not accepting log writes.
    pub fn is_readonly(&self) -> bool {
        self.disk_monitor.is_readonly()
    }

    /// Records an attempted log write.
    pub fn record_write(&self) {
        self.disk_monitor.record_write()
    }

    // Starts running the disk storage monitor.
    pub async fn monitor_disk_storage(&self) {
        if let Err(e) = self
            .disk_monitor
            .run(&*self.root, &self.config.storage)
            .await
        {
            error!(
                "error while monitoring disk storage capacity for {}: {:?}",
                std::fs::canonicalize(&self.root).unwrap().display(),
                e
            );
            // we want to loop here; otherwise the select() in main exits early
            // this could allow the server to run without the disk watcher, but
            // that seems preferable to not running at all
            std::future::pending().await
        }
    }

    /// Default headroom (in bytes) to reserve for filesystem overhead.
    pub fn default_headroom() -> ByteSize {
        ByteSize::mib(500)
    }

    /// Default number of topics to keep in-memory.
    pub fn default_max_open_topics() -> usize {
        128
    }

    pub async fn close(self) {
        let mut state = self.state.write().await;

        join_all(state.topics.drain().map(|(_, topic)| topic.close())).await;
    }

    /// Close catalog (perform final checkpoint and drop all writers)
    pub async fn close_arc(mut catalog: Arc<Self>) -> bool {
        let now = Instant::now();
        let secs = 30;
        info!("waiting {secs}s for all pending operations to complete");
        let mut exclusive = None;
        for _ in 0..secs {
            // gah, into_inner is 1.70 onward...
            match Arc::try_unwrap(catalog) {
                Ok(c) => {
                    exclusive = Some(c);
                    break;
                }
                Err(arc) => {
                    catalog = arc;
                }
            }
            trace!("outstanding: {}", Arc::strong_count(&catalog));
            time::sleep(Duration::from_secs(1)).await;
        }

        if let Some(exclusive) = exclusive {
            info!("all pending operations completed in {:?}", now.elapsed());
            let now = Instant::now();
            info!("performing final checkpoint");
            exclusive.checkpoint().await;
            info!("final checkpoint complete in {:?}", now.elapsed());

            let now = Instant::now();
            info!("closing catalog");
            exclusive.close().await;
            info!("catalog closed in {:?}", now.elapsed());
            true
        } else {
            warn!("operations still pending; could not close catalog");
            false
        }
    }

    /// Get the topic root path
    pub fn topic_root(&self) -> &Path {
        &self.topic_root
    }

    /// Get a reference to the manifest
    pub fn manifest(&self) -> &Manifest {
        &self.manifest
    }

    /// A handle to the single-flight guard for fix-enabled reconciliation
    /// passes. Reconciliation acquires this lazily (via `try_lock_owned`) and
    /// holds it for the duration of a fix-enabled pass.
    pub fn reconcile_fix_guard(&self) -> Arc<Mutex<()>> {
        self.reconcile_fix_guard.clone()
    }

    /// Snapshot the latest completed reconciliation report, if any pass has
    /// finished. Cheap to call and never blocks on an in-flight pass.
    pub fn latest_reconcile_report(&self) -> Option<Arc<ReconcileReport>> {
        self.reconcile_report.borrow().clone()
    }

    /// A receiver for observing reconciliation reports as they are published.
    /// The current value is `None` until the first pass completes.
    pub fn subscribe_reconcile_report(&self) -> watch::Receiver<Option<Arc<ReconcileReport>>> {
        self.reconcile_report.subscribe()
    }

    /// Publish a completed reconciliation report as the latest snapshot. Uses
    /// `send_replace` so it succeeds even when there are no subscribers.
    pub fn publish_reconcile_report(&self, report: Arc<ReconcileReport>) {
        self.reconcile_report.send_replace(Some(report));
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::data::records::{build_records, Record};
    use crate::data::RecordIndex;
    use anyhow::Result;
    use chrono::{TimeDelta, TimeZone, Timelike, Utc};
    use futures::stream;
    use futures::stream::StreamExt;
    use std::slice;
    use tempfile::{tempdir, TempDir};
    use test_log::test;

    impl Catalog {
        async fn active_topics(&self) -> usize {
            self.state.read().await.topics.len()
        }

        async fn active_partitions(&self) -> usize {
            stream::iter(self.state.read().await.topics.iter())
                .fold(0, |acc, (_, topic)| async move {
                    acc + topic.active_data().await.len()
                })
                .await
        }
    }

    async fn catalog() -> (TempDir, Catalog) {
        catalog_config(Default::default()).await
    }

    async fn catalog_config(config: Config) -> (TempDir, Catalog) {
        let dir = tempdir().unwrap();
        let root = PathBuf::from(dir.path());
        (dir, Catalog::attach(root, config).await.unwrap())
    }

    #[test(tokio::test)]
    async fn test_independence() -> Result<()> {
        let (_root, catalog) = catalog().await;

        let records: Vec<_> = vec!["abc", "def", "ghi", "jkl", "mno", "p"]
            .into_iter()
            .map(|message| Record {
                time: Utc.timestamp_opt(0, 0).unwrap(),
                message: message.bytes().collect(),
            })
            .collect();

        for (ix, record) in records.iter().enumerate() {
            let name = format!("topic-{}", ix % 3);
            catalog
                .get_topic(&name)
                .await
                .extend_records("default", slice::from_ref(record))
                .await?;
        }

        assert_eq!(catalog.list_topics().await.len(), 3);

        for (ix, record) in records.iter().enumerate() {
            let name = format!("topic-{}", ix % 3);
            let topic = catalog.get_topic(&name).await;
            assert_eq!(
                topic
                    .get_record_by_index("default", RecordIndex(ix / 3))
                    .await,
                Some(record.clone())
            );
        }

        Ok(())
    }

    #[test(tokio::test)]
    async fn test_migration() -> Result<()> {
        let config = Config::default();
        let dir = tempdir().unwrap();
        let root = PathBuf::from(dir.path());

        // emulate the v0 attachment process
        // in v0 the manifest was named "manifest.json" and the topic root was
        // also the catalog root
        let manifest = Manifest::attach(root.join("manifest.json")).await;
        let v0 = Catalog::attach_v0(manifest, root.clone(), root.clone(), config.clone()).await;

        let records: Vec<_> = vec!["abc", "def", "ghi", "jkl", "mno", "p"]
            .into_iter()
            .map(|message| Record {
                time: Utc.timestamp_opt(0, 0).unwrap(),
                message: message.bytes().collect(),
            })
            .collect();

        for (ix, record) in records.iter().enumerate() {
            let name = format!("topic-{}", ix % 3);
            let topic = v0.get_topic(&name).await;

            topic
                .extend_records("default", slice::from_ref(record))
                .await?;
        }

        v0.checkpoint().await;
        assert_eq!(v0.list_topics().await.len(), 3);
        assert!(Catalog::close_arc(Arc::new(v0)).await);

        let v1 = Catalog::attach(root, config).await?;
        for (ix, record) in records.iter().enumerate() {
            let name = format!("topic-{}", ix % 3);
            let topic = v1.get_topic(&name).await;
            assert_eq!(
                topic
                    .get_record_by_index("default", RecordIndex(ix / 3))
                    .await,
                Some(record.clone())
            );
        }

        Ok(())
    }

    #[test_log::test(tokio::test)]
    async fn test_retain() -> Result<()> {
        let (_root, mut catalog) = catalog().await;
        catalog.config.retain.max_bytes = ByteSize::b(8000 + catalog.manifest.db_bytes() as u64);
        catalog.config.headroom = ByteSize::b(0);

        let data = "x".to_string().repeat(500);

        let old_size = {
            let oldest_records = build_records((0..10).map(|_| (0, data.clone())));
            let oldest_topic = catalog.get_topic("oldest").await;
            let p = oldest_topic.get_partition("default").await;

            p.extend_records(&oldest_records).await?;
            p.compact().await;
            p.extend_records(&oldest_records).await?;
            p.compact().await;
            p.extend_records(&oldest_records).await?;
            p.byte_size().await
        };

        let records = build_records((0..10).map(|_| (100, data.clone())));
        for ix in 0..5 {
            let name = format!("topic-{ix}");
            let topic = catalog.get_topic(&name).await;
            let partition = topic.get_partition("default").await;
            partition.extend_records(&records).await?;
            partition.compact().await;
            partition.extend_records(&records).await?;
        }

        assert!(catalog.byte_size().await > catalog.total_byte_limit());
        catalog.retain().await;
        assert!(catalog.byte_size().await < catalog.total_byte_limit());
        let topic = catalog.get_topic("oldest").await;
        let partition = topic.get_partition("default").await;
        assert!(partition.byte_size().await < old_size);

        Ok(())
    }

    #[test_log::test(tokio::test)]
    async fn test_max_open_topics() -> Result<()> {
        let (_root, catalog) = catalog_config(Config {
            max_open_topics: 1,
            ..Default::default()
        })
        .await;

        let records: Vec<_> = vec!["abc", "def", "ghi", "jkl", "mno", "p"]
            .into_iter()
            .map(|message| Record {
                time: Utc.timestamp_opt(0, 0).unwrap(),
                message: message.bytes().collect(),
            })
            .collect();

        for (ix, record) in records.iter().enumerate() {
            let name = format!("topic-{}", ix % 3);
            {
                catalog
                    .get_topic(&name)
                    .await
                    .extend_records("default", slice::from_ref(record))
                    .await?;
            }
            catalog.checkpoint().await;
            catalog.retain().await;
            {
                assert!(catalog.state.read().await.topics.len() <= 1);
            }
        }

        for (ix, record) in records.iter().enumerate() {
            let name = format!("topic-{}", ix % 3);
            {
                let topic = catalog.get_topic(&name).await;
                assert_eq!(
                    topic
                        .get_record_by_index("default", RecordIndex(ix / 3))
                        .await,
                    Some(record.clone())
                );
            }
            catalog.prune_topics().await;
            {
                assert!(catalog.state.read().await.topics.len() <= 1);
            }
        }

        Ok(())
    }

    #[test(tokio::test)]
    async fn test_partition_active_limit() -> Result<()> {
        let (_root, catalog) = catalog_config(Config {
            max_partition_bytes: ByteSize::b(100),
            ..Default::default()
        })
        .await;

        let large = "x".repeat(11);

        let time = Utc::now()
            .checked_sub_signed(TimeDelta::try_seconds(10).unwrap())
            .unwrap()
            .with_nanosecond(0)
            .unwrap();

        let records: Vec<_> = std::iter::repeat_n(large, 6)
            .map(|message| Record {
                time,
                message: message.bytes().collect(),
            })
            .collect();

        for (ix, record) in records.iter().enumerate() {
            let name = format!("topic-{}", ix % 3);
            {
                let topic = catalog.get_topic(&name).await;

                let insert = topic
                    .extend_records("default", slice::from_ref(record))
                    .await?;
                topic
                    .ensure_index("default", RecordIndex(insert.end.0 - 1))
                    .await?;
            }
            catalog.checkpoint().await;
            catalog.retain().await;
        }

        // the third topic got closed due to the active byte limits
        assert_eq!(catalog.active_partitions().await, 2);

        for (ix, record) in records.iter().enumerate() {
            let name = format!("topic-{}", ix % 3);
            trace!("{name} {}", ix / 3);
            {
                let topic = catalog.get_topic(&name).await;
                assert_eq!(
                    topic
                        .get_record_by_index("default", RecordIndex(ix / 3))
                        .await,
                    Some(record.clone()),
                    "{name}/{}",
                    ix / 3
                );
            }
            catalog.prune_topics().await;

            assert!(catalog.active_topics().await >= 2);

            // The active segment for the third topic is persisted so the topic
            // comes back but does not invoke the byte limit
            assert_eq!(catalog.active_partitions().await, 2);
        }

        Ok(())
    }
}
