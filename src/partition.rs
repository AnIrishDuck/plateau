//! A `Partition` is responsible for reading the commit stream from a `Slog` and
//! ensuring the associated finalized `SegmentData` is persisted in the
//! `Manifest`.
//!
//! There can be many `Partition`s in a `Topic`. This unlocks the write parallelism
//! advantages inherent in having many concurrent `Slog` writer threads.
//!
//! The `Partition` is also responsible for assigning sequential unique
//! identifiers to each incoming record. A `(topic, partition, record)` tuple is
//! a globally unique identifier for each durably stored record.
//!
//! To ensure durability, `.commit()` must be called after a given `.append()`.
//! Otherwise, the record will only be durably stored on the next `.roll()`.
//!
//! A `Partition` also has `Rolling` and `Retention` policies that are used to
//! determine when to roll segments and expire old data. `Rolling` policies are
//! evaluated on every insert, and `Retention` policies are enforced on every
//! `roll`.
use crate::manifest::Manifest;
pub use crate::manifest::PartitionId;
pub use crate::segment::Record;
pub use crate::slog::{Index, RecordIndex};
use crate::slog::{SegmentIndex, SegmentRecordIndex, Slog};
use futures::future::OptionFuture;
use futures::FutureExt;
use log::info;
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};
use tokio::sync::{watch, RwLock};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Rolling {
    pub max_segment_size: usize,
    pub max_segment_index: usize,
    pub max_segment_duration: Option<Duration>,
}

impl Default for Rolling {
    fn default() -> Self {
        Rolling {
            max_segment_size: 100 * 1024 * 1024,
            max_segment_index: 100000,
            max_segment_duration: None,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Retention {
    pub max_segment_count: Option<usize>,
    pub max_bytes: usize,
}

impl Default for Retention {
    fn default() -> Self {
        Retention {
            max_segment_count: Some(10000),
            max_bytes: 1 * 1024 * 1024 * 1024,
        }
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Config {
    pub retain: Retention,
    pub roll: Rolling,
}

pub struct Partition {
    id: PartitionId,
    state: RwLock<State>,
    config: Config,
    manifest: Manifest,
}

pub struct State {
    open_index: RecordIndex,
    last_roll: Instant,
    messages: Slog,
    commits: watch::Receiver<SegmentIndex>,
}

impl Partition {
    pub(crate) async fn attach(
        root: PathBuf,
        manifest: Manifest,
        id: PartitionId,
        config: Config,
    ) -> Partition {
        if !Path::exists(&root) {
            fs::create_dir(&root).unwrap();
        }
        let segment = manifest
            .get_max_segment(&id)
            .await
            .map(|s| s.next())
            .unwrap_or(SegmentIndex(0));
        let (messages, mut writes) = Slog::attach(root.clone(), Partition::slog_name(&id), segment);

        let (commit_writer, commits) = watch::channel(segment);
        let commit_manifest = manifest.clone();
        let commit_id = id.clone();
        tokio::spawn(async move {
            while let Some(r) = writes.recv().await {
                commit_manifest
                    .update(commit_id.segment_id(r.segment), &r.data)
                    .await;
                // ok if no receivers, that means nothing is awaiting a commit
                commit_writer.send(r.segment).ok();
            }
        });

        let state = State {
            open_index: manifest.open_index(&id).await,
            last_roll: Instant::now(),
            messages,
            commits,
        };

        Partition {
            manifest,
            state: RwLock::new(state),
            id,
            config,
        }
    }

    fn slog_name(id: &PartitionId) -> String {
        format!("{}-{}", id.topic(), id.partition())
    }

    pub(crate) async fn append(&self, rs: &[Record]) -> Option<RecordIndex> {
        let mut state = self.state.write().await;
        let start = state.open_index;
        for r in rs {
            state.messages.append(r).await;
            state.roll_when_needed(&self).await;
        }

        if rs.len() > 0 {
            Some(RecordIndex(start.0 + rs.len()))
        } else {
            None
        }
    }

    pub(crate) async fn commit(&self) {
        self.state.write().await.commit(&self).await;
    }

    pub(crate) async fn get_record_by_index(&self, index: RecordIndex) -> Option<Record> {
        let state = self.state.read().await;
        let open = state.open_index;
        let manifest = &self.manifest;
        let slog_index = if index >= open {
            Some(Index {
                record: SegmentRecordIndex(index.0 - open.0),
                segment: state.messages.current_segment_ix().await,
            })
        } else {
            manifest
                .get_segment_for_ix(&self.id, index)
                .then(|s| {
                    OptionFuture::from(s.map(|segment| {
                        manifest
                            .get_segment_data(self.id.segment_id(segment))
                            .map(move |data| Index {
                                record: SegmentRecordIndex(index.0 - data.unwrap().index.start.0),
                                segment,
                            })
                    }))
                })
                .await
        };

        OptionFuture::from(slog_index.map(|ix| state.messages.get_record(ix)))
            .await
            .flatten()
    }

    async fn over_retention_limit(&self) -> bool {
        let retain = &self.config.retain;

        let size = self.manifest.get_size(&self.id).await.unwrap_or(0);
        if size > retain.max_bytes {
            info!("over limit {}: current size is {}", self.id, size);
            return true;
        }

        // TODO validate
        if let Some(count) = retain.max_segment_count {
            let max = self
                .manifest
                .get_max_segment(&self.id)
                .await
                .unwrap_or(SegmentIndex(0));
            let min = self
                .manifest
                .get_min_segment(&self.id)
                .await
                .unwrap_or(SegmentIndex(0));
            info!("over limit {}: {:?}..={:?}", self.id, min, max);
            return max.0 - min.0 > count;
        }

        false
    }
}

impl State {
    async fn roll(&mut self, partition: &Partition) {
        let record_count = self.messages.current_len().await;
        self.messages.roll(self.open_index).await;
        self.last_roll = Instant::now();
        self.open_index = RecordIndex(self.open_index.0 + record_count);
        self.retain(partition).await;
    }

    async fn roll_when_needed(&mut self, partition: &Partition) {
        if let Some(time) = self.messages.current_time_range().await {
            let roll = &partition.config.roll;
            if let Some(d) = roll.max_segment_duration {
                let dt = Instant::now() - self.last_roll;
                if dt > d {
                    info!(
                        "rolling {}: last roll was {}s ago",
                        partition.id,
                        dt.as_secs()
                    );
                    return self.roll(partition).await;
                }
            }

            let current_len = self.messages.current_len().await;
            if current_len > roll.max_segment_index {
                info!("rolling {}: length is {}", partition.id, current_len);
                return self.roll(partition).await;
            }

            let current_size = self.messages.current_size().await;
            if current_size > roll.max_segment_size {
                info!("rolling {}: current size is {}", partition.id, current_size);
                return self.roll(partition).await;
            }
        }
    }

    async fn retain(&mut self, partition: &Partition) {
        while partition.over_retention_limit().await {
            OptionFuture::from(
                partition
                    .manifest
                    .get_min_segment(&partition.id)
                    .await
                    .map(|ix| {
                        // TODO ensure we handle failure if this call
                        self.messages.destroy(ix);
                        info!("retain {}: destroyed {:?}", partition.id, ix);
                        // succeeds but this does not complete e.g. due to node failure
                        partition
                            .manifest
                            .remove_segment(partition.id.segment_id(ix))
                    }),
            )
            .await;
        }
    }

    pub(crate) async fn commit(&mut self, partition: &Partition) {
        let current = self.messages.current_segment_ix().await;
        let target = if let Some(time) = self.messages.current_time_range().await {
            // flush remaining messages
            let start = self.open_index;
            self.roll(partition).await;
            Some(current)
        } else {
            current.prev()
        };

        if let Some(target) = target {
            self.commits.changed().await.expect("commit watcher ended");
            while *self.commits.borrow() < target {
                self.commits.changed().await.expect("commit watcher ended");
            }
        }
    }
}

mod test {
    use super::*;
    use parquet::data_type::ByteArray;
    use std::thread;
    use std::time::SystemTime;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_append_get() {
        let id = PartitionId::new("topic", "testing");
        let dir = tempdir().unwrap();
        let root = PathBuf::from(dir.path());
        let manifest = Manifest::attach(root.join("manifest.sqlite")).await;
        let t = Partition::attach(root, manifest, id, Config::default()).await;

        let records: Vec<_> = vec!["abc", "def", "ghi", "jkl", "mno", "p"]
            .into_iter()
            .map(|message| Record {
                time: SystemTime::UNIX_EPOCH,
                message: ByteArray::from(message),
            })
            .collect();

        for record in records.iter() {
            t.append(&vec![record.clone()]).await;
        }

        for (ix, record) in records.iter().enumerate() {
            assert_eq!(
                t.get_record_by_index(RecordIndex(ix)).await,
                Some(record.clone())
            );
        }
        assert_eq!(
            t.get_record_by_index(RecordIndex(records.len())).await,
            None
        );
    }

    #[tokio::test]
    async fn test_rolling_get() {
        let id = PartitionId::new("topic", "testing-roll");
        let dir = tempdir().unwrap();
        let root = PathBuf::from(dir.path());
        let manifest = Manifest::attach(root.join("manifest.sqlite")).await;
        let t = Partition::attach(
            root,
            manifest,
            id,
            Config {
                roll: Rolling {
                    max_segment_index: 2,
                    ..Rolling::default()
                },
                ..Config::default()
            },
        )
        .await;

        let records: Vec<_> = vec!["abc", "def", "ghi", "jkl", "mno", "p"]
            .into_iter()
            .map(|message| Record {
                time: SystemTime::UNIX_EPOCH,
                message: ByteArray::from(message),
            })
            .collect();

        for record in records.iter() {
            t.append(&vec![record.clone()]).await;
        }
        t.commit().await;

        for (ix, record) in records.iter().enumerate() {
            assert_eq!(
                t.get_record_by_index(RecordIndex(ix)).await,
                Some(record.clone())
            );
        }
        assert_eq!(
            t.get_record_by_index(RecordIndex(records.len())).await,
            None
        );
    }

    #[tokio::test]
    async fn test_durability() {
        let id = PartitionId::new("topic", "testing-roll");
        let dir = tempdir().unwrap();
        let root = PathBuf::from(dir.path());
        let records: Vec<_> = vec!["abc", "def", "ghi", "jkl", "mno", "p"]
            .into_iter()
            .map(|message| Record {
                time: SystemTime::UNIX_EPOCH,
                message: ByteArray::from(message),
            })
            .collect();
        {
            let manifest = Manifest::attach(root.join("manifest.sqlite")).await;
            let t = Partition::attach(
                root.clone(),
                manifest,
                id.clone(),
                Config {
                    roll: Rolling {
                        max_segment_index: 2,
                        ..Rolling::default()
                    },
                    ..Config::default()
                },
            )
            .await;

            for record in records.iter() {
                t.append(&vec![record.clone()]).await;
            }
            t.commit().await;
        }

        {
            let manifest = Manifest::attach(root.join("manifest.sqlite")).await;
            let t = Partition::attach(
                root,
                manifest,
                id,
                Config {
                    roll: Rolling {
                        max_segment_index: 2,
                        ..Rolling::default()
                    },
                    ..Config::default()
                },
            )
            .await;

            for (ix, record) in records.iter().enumerate() {
                assert_eq!(
                    t.get_record_by_index(RecordIndex(ix)).await,
                    Some(record.clone())
                );
            }
            assert_eq!(
                t.get_record_by_index(RecordIndex(records.len())).await,
                None
            );
        }
    }
}
