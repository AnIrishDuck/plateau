//! A topic is a collection of partitions. It is an abstraction used for queries
//! of a given topic over _all_ partitions.

use std::collections::{HashMap, HashSet};
use std::fs;
use std::ops::{Range, RangeInclusive};
use std::path::{Path, PathBuf};

use crate::data::{
    chunk::Schema,
    limit::{BatchStatus, LimitedBatch, RowLimit},
    records::{LegacyRecords, Record},
};
use crate::manifest::{Manifest, PartitionId, Scope, SegmentData};

use crate::partition::Config as PartitionConfig;
use crate::partition::Partition;
use crate::slog::SegmentIndex;

use crate::data::index::{Ordering, RecordIndex};
use crate::transport::{PartitionFilter, PartitionSelector, SchemaChunk, TopicIterator};
use anyhow::Result;
use chrono::{DateTime, Utc};
use futures::future::{join_all, FutureExt};
use futures::stream;
use futures::stream::StreamExt;
use tokio::sync::{RwLock, RwLockReadGuard};
use tracing::debug;

type PartitionMap = HashMap<String, Partition>;

#[derive(Debug)]
#[must_use = "close() explicitly to flush writes"]
pub struct Topic {
    root: PathBuf,
    manifest: Manifest,
    name: String,
    partitions: RwLock<PartitionMap>,
    config: PartitionConfig,
}

#[derive(Debug)]
pub struct TopicRecordResponse {
    pub iter: TopicIterator,
    pub batch: LimitedBatch,
}

impl Topic {
    pub async fn attach(
        root: PathBuf,
        manifest: Manifest,
        name: String,
        config: PartitionConfig,
    ) -> Self {
        if !Path::exists(&root) {
            fs::create_dir(&root).unwrap();
        }

        let partitions = HashMap::new();

        Self {
            root,
            manifest,
            name,
            partitions: RwLock::new(partitions),
            config,
        }
    }

    pub(crate) fn partition_root(root: &Path, name: &str) -> PathBuf {
        root.join(name)
    }

    pub async fn readable_ids(
        &self,
        partition_filter: PartitionFilter,
    ) -> HashMap<String, Range<RecordIndex>> {
        debug!("partition map: {:?}", self.partitions);
        self.map_partitions(
            |partition| async move { partition.readable_ids().await },
            partition_filter,
        )
        .await
    }

    pub async fn byte_size(&self) -> usize {
        self.manifest.get_size(Scope::Topic(&self.name)).await
    }

    async fn partition_names(&self) -> Vec<String> {
        let active: HashSet<String> = { self.partitions.read().await.keys().cloned().collect() };
        let copy = active.clone();

        // Find all the "inactive" partitions that are not in self.partitions
        let stored = self
            .manifest
            .get_partitions(&self.name)
            .await
            .into_iter()
            .filter(|n| !copy.contains(n));

        active.into_iter().chain(stored).collect()
    }

    pub async fn map_partitions<'a, Fut, F, T>(
        &'a self,
        mut f: F,
        partition_filter: PartitionFilter,
    ) -> HashMap<String, T>
    where
        F: FnMut(RwLockReadGuard<'a, Partition>) -> Fut,
        Fut: futures::Future<Output = Option<T>>,
    {
        let partitions = self.partition_names().await;
        let expanded = self.partition_filter_expanded(&partitions, partition_filter);

        stream::iter(expanded.unwrap_or(partitions))
            .flat_map(|name| {
                async move {
                    let partition = self.get_partition(&name).await;
                    (name, partition)
                }
                .into_stream()
            })
            .flat_map(|(name, p)| {
                f(p).into_stream()
                    .flat_map(|v| stream::iter(v.into_iter()))
                    .map(move |v| (name.clone(), v))
            })
            .collect()
            .await
    }

    pub(crate) async fn active_data(&self) -> HashMap<String, SegmentData> {
        stream::iter(self.partitions.read().await.iter())
            .flat_map(|(name, active)| {
                active
                    .active_data()
                    .into_stream()
                    .flat_map(|data| stream::iter(data.map(|d| (name.clone(), d))))
            })
            .collect()
            .await
    }

    /// Read a partition's `sealed_ix` watermark *without* loading it into
    /// memory. Returns `None` when the partition is not currently resident
    /// (so reconcile can treat its on-disk segments as immutable rather than
    /// forcing a load, which would reset the in-memory watermark to `None`),
    /// or `Some(watermark)` when resident.
    pub(crate) async fn resident_sealed_ix(
        &self,
        partition_name: &str,
    ) -> Option<Option<SegmentIndex>> {
        self.partitions
            .read()
            .await
            .get(partition_name)
            .map(|partition| partition.sealed_ix())
    }

    pub async fn get_partition(&self, partition_name: &str) -> RwLockReadGuard<'_, Partition> {
        let partitions = self.partitions.read().await;
        let current_partition = RwLockReadGuard::try_map(partitions, |map| map.get(partition_name));
        if let Ok(part) = current_partition {
            part
        } else {
            drop(current_partition);
            let mut partitions = self.partitions.write().await;
            let id = PartitionId::new(&self.name, partition_name);
            let part = Partition::attach(
                Self::partition_root(&self.root, &self.name),
                self.manifest.clone(),
                id,
                self.config.clone(),
            )
            .await;
            partitions.insert(partition_name.to_string(), part);
            let partitions = partitions.downgrade();
            RwLockReadGuard::map(partitions, |map| {
                // note: this unwrap() is guaranteed to succeed, as we just inserted
                // the key and still hold the lock. ideally we'd use the
                // `OccupiedEntry` api here, but it is still unstable
                map.get(partition_name).unwrap()
            })
        }
    }

    pub(crate) async fn close_partition(&self, partition_name: &str) -> Option<SegmentData> {
        let mut partitions = self.partitions.write().await;
        let partition = partitions.remove(partition_name)?;

        let data = partition.active_data().await;
        partition.close().await;
        data
    }

    pub fn partition_filter_expanded(
        &self,
        partitions: &[String],
        partition_filter: PartitionFilter,
    ) -> Option<Vec<String>> {
        if let Some(partition_filters) = partition_filter {
            let expanded = partitions
                .iter()
                .filter(|name| partition_filters.iter().any(|filter| filter.matches(name)))
                .cloned()
                .collect();

            Some(expanded)
        } else {
            None
        }
    }

    pub async fn checkpoint(&self) {
        self.map_partitions::<_, _, ()>(
            |partition| async move {
                partition.checkpoint().await;
                None
            },
            None,
        )
        .await;
    }

    pub async fn extend(
        &self,
        partition_name: &str,
        data: SchemaChunk<Schema>,
    ) -> Result<Range<RecordIndex>> {
        let partition = self.get_partition(partition_name).await;
        partition.extend(data).await
    }

    pub async fn extend_records(
        &self,
        partition_name: &str,
        rs: &[Record],
    ) -> Result<Range<RecordIndex>> {
        self.extend(
            partition_name,
            SchemaChunk::try_from(LegacyRecords(rs.to_vec())).unwrap(),
        )
        .await
    }

    #[cfg(test)]
    pub async fn get_record_by_index(
        &self,
        partition_name: &str,
        index: RecordIndex,
    ) -> Option<Record> {
        let partition = self.get_partition(partition_name).await;
        partition.get_record_by_index(index).await
    }

    pub async fn get_records(
        &self,
        starts: TopicIterator,
        limit: RowLimit,
        order: Ordering,
        partition_filter: PartitionFilter,
    ) -> TopicRecordResponse {
        self.get_records_from_all(
            starts,
            limit,
            order,
            |partition, start, partition_limit, order| async move {
                partition.get_records(start, partition_limit, order).await
            },
            partition_filter,
        )
        .await
    }

    pub async fn get_records_by_time(
        &self,
        starts: TopicIterator,
        times: RangeInclusive<DateTime<Utc>>,
        limit: RowLimit,
        partition_filter: PartitionFilter,
    ) -> TopicRecordResponse {
        let times = &times;
        self.get_records_from_all(
            starts,
            limit,
            Ordering::Forward,
            |partition, start, partition_limit, _order| async move {
                partition
                    .get_records_by_time(start, times.clone(), partition_limit)
                    .await
            },
            partition_filter,
        )
        .await
    }

    /// Reads rows from one or more partitions in a topic. When starting a new
    /// forward read (i.e., all partitions starting at position 0), partitions
    /// are scanned in alphabetical order. Each partition is scanned in turn
    /// until `limit` records have been retrieved. Positions of all scanned
    /// partitions are updated in `iterator`. Forward iteration will include
    /// partitions as they are created, reverse operation will only process
    /// those partitions present at the first iteration.
    pub async fn iter_topic<'a, F, Fut>(
        &'a self,
        mut iterator: TopicIterator,
        limit: RowLimit,
        order: Ordering,
        fetch: F,
        partition_filter: Option<Vec<PartitionSelector>>,
    ) -> TopicRecordResponse
    where
        F: Fn(RwLockReadGuard<'a, Partition>, RecordIndex, RowLimit, Ordering) -> Fut,
        Fut: futures::Future<Output = LimitedBatch>,
    {
        let mut read_starts: Vec<_> = if order.is_reverse() && !iterator.is_empty() {
            // if reversing and the iterator already has a set of partitions established,
            // just get offsets/indexes for those partitions
            iterator
                .keys()
                .cloned()
                .collect::<Vec<String>>()
                .into_iter()
                .map(|k| (RecordIndex(*iterator.entry(k.clone()).or_insert(0)), k))
                .collect()
        } else {
            // otherwise scan all partitions, including new partitions created since the
            // iterator originally initialized
            self.readable_ids(partition_filter)
                .await
                .into_iter()
                .map(|(k, v)| {
                    (
                        RecordIndex(*iterator.entry(k.clone()).or_insert(if order.is_reverse() {
                            v.end.0
                        } else {
                            v.start.0
                        })),
                        k,
                    )
                })
                .collect()
        };

        read_starts.sort();
        if order.is_reverse() {
            read_starts.reverse();
        }

        let mut batch = LimitedBatch::open(limit);
        let mut any_schema_change = false;
        for (start, name) in read_starts.into_iter() {
            let partition = self.get_partition(&name).await;
            if let BatchStatus::Open { remaining } = batch.status {
                let batch_response = fetch(partition, start, remaining, order).await;

                if matches!(batch_response.status, BatchStatus::SchemaChanged) {
                    any_schema_change = true;
                }

                if batch.compatible_with(&batch_response) {
                    if let Some(final_record) = batch_response
                        .chunks
                        .last()
                        .and_then(|indexed| indexed.end())
                    {
                        match order {
                            Ordering::Forward => iterator.insert(name, (final_record + 1).0),
                            Ordering::Reverse => iterator.insert(name, final_record.0),
                        };
                    }

                    batch.extend(batch_response);
                } else {
                    any_schema_change = true;
                }
            } else {
                break;
            }
        }

        // we don't want to short-circuit when one partition has a schema
        // change; we want to fetch all records with the current schema
        // from all partitions.
        if any_schema_change {
            batch.status = BatchStatus::SchemaChanged;
        }

        TopicRecordResponse {
            iter: iterator,
            batch,
        }
    }

    pub async fn get_records_from_all<'a, F, Fut>(
        &'a self,
        iterator: TopicIterator,
        limit: RowLimit,
        order: Ordering,
        fetch: F,
        partition_filter: PartitionFilter,
    ) -> TopicRecordResponse
    where
        F: Fn(RwLockReadGuard<'a, Partition>, RecordIndex, RowLimit, Ordering) -> Fut,
        Fut: futures::Future<Output = LimitedBatch>,
    {
        self.iter_topic(iterator, limit, order, fetch, partition_filter)
            .await
    }

    #[cfg(test)]
    pub async fn commit(&self) -> Result<()> {
        for (_, part) in self.partitions.read().await.iter() {
            part.commit().await?
        }
        Ok(())
    }

    pub async fn close(self) {
        let mut partitions = self.partitions.write().await;
        join_all(partitions.drain().map(|(_, partition)| partition.close())).await;
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::data::records::TryIntoRecords;
    use crate::partition::test::{assert_limit_unreached, deindex};
    use crate::test::{inferences_schema_a, inferences_schema_b};
    use chrono::TimeZone;
    use std::slice;
    use tempfile::{tempdir, TempDir};

    impl Topic {
        // this could be used outside of tests once the issue below is fixed
        pub(crate) async fn ensure_index(
            &self,
            partition_name: &str,
            index: RecordIndex,
        ) -> Result<()> {
            let partition = self.get_partition(partition_name).await;
            // XXX - holds read lock for topic, so no new partitions may be added
            // until the future completes.
            partition.ensure_index(index).await
        }
    }

    fn dummy_records<I, S>(s: I) -> Vec<Record>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        timed_records(s.into_iter().enumerate().map(|(ix, is)| (ix as i64, is)))
    }

    fn timed_records<I, S>(s: I) -> Vec<Record>
    where
        I: IntoIterator<Item = (i64, S)>,
        S: Into<String>,
    {
        s.into_iter()
            .map(|(ts, is)| Record {
                time: Utc.timestamp_opt(ts, 0).unwrap(),
                message: is.into().into_bytes(),
            })
            .collect()
    }

    async fn scratch() -> (TempDir, Topic) {
        let dir = tempdir().unwrap();
        let root = PathBuf::from(dir.path());
        let manifest = Manifest::attach(root.join("manifest.sqlite")).await;
        (
            dir,
            Topic::attach(
                root,
                manifest,
                String::from("testing"),
                PartitionConfig::default(),
            )
            .await,
        )
    }

    async fn reload(old: Topic) -> Topic {
        let root = old.root.clone();
        let manifest = old.manifest.clone();
        let name = old.name.clone();
        drop(old);
        Topic::attach(root, manifest, name, PartitionConfig::default()).await
    }

    /// quick and dirty transform to string vector
    fn batch_to_vec(batch: LimitedBatch) -> Vec<String> {
        batch
            .try_into_records()
            .unwrap()
            .iter()
            .map(|r| String::from_utf8(r.message.clone()).unwrap())
            .collect::<Vec<_>>()
    }

    #[tokio::test]
    async fn test_independence() -> Result<()> {
        let (_tmpdir, topic) = scratch().await;
        let records = dummy_records(vec!["abc", "def", "ghi", "jkl", "mno", "p"]);

        for (ix, record) in records.iter().enumerate() {
            let name = format!("partition-{}", ix % 3);
            topic
                .extend(
                    &name,
                    SchemaChunk::try_from(LegacyRecords(vec![record.clone()]))?,
                )
                .await?;
        }

        topic.commit().await?;

        for (ix, record) in records.iter().enumerate() {
            let name = format!("partition-{}", ix % 3);
            assert_eq!(
                topic.get_record_by_index(&name, RecordIndex(ix / 3)).await,
                Some(record.clone())
            );
        }

        let topic = reload(topic).await;

        assert_eq!(
            topic.readable_ids(None).await,
            HashMap::<_, _>::from_iter([
                ("partition-0".to_string(), RecordIndex(0)..RecordIndex(2)),
                ("partition-1".to_string(), RecordIndex(0)..RecordIndex(2)),
                ("partition-2".to_string(), RecordIndex(0)..RecordIndex(2)),
            ])
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_time_iteration() -> Result<()> {
        let (_tmpdir, topic) = scratch().await;
        let records = timed_records(vec![
            (0, "abc"),
            (10, "def"),
            (12, "ghi"),
            (13, "jkl"),
            (14, "mno"),
            (15, "p"),
        ]);

        let mut part_records = [vec![], vec![], vec![]];
        let names: Vec<_> = (0..=3).map(|ix| format!("partition-{}", ix % 3)).collect();
        for (ix, record) in records.iter().enumerate() {
            part_records[ix % 3].push(record.clone());
            topic
                .extend(
                    &names[ix % 3],
                    SchemaChunk::try_from(LegacyRecords(vec![record.clone()]))?,
                )
                .await?;
        }

        topic.commit().await?;

        let span = Utc.timestamp_opt(5, 0).unwrap()..=Utc.timestamp_opt(18, 0).unwrap();

        let expected_it: TopicIterator = names.clone().into_iter().zip([2, 1, 0]).collect();
        // fetching two records will spill from partition-0, which has only one
        // record in the given time range, to partition-1
        let result = topic
            .get_records_by_time(HashMap::new(), span.clone(), RowLimit::records(2), None)
            .await;
        assert_eq!(result.batch.status, BatchStatus::RecordsExceeded);
        assert_eq!(result.iter, expected_it);
        assert_eq!(
            result
                .batch
                .try_into_records()?
                .into_iter()
                .collect::<HashSet<_>>(),
            vec![part_records[0][1].clone(), part_records[1][0].clone()]
                .into_iter()
                .collect::<HashSet<_>>()
        );

        // next fetch will use partition with lowest index in iterator, partition-2
        let prior_it = result.iter;
        let result = topic
            .get_records_by_time(prior_it, span.clone(), RowLimit::records(1), None)
            .await;
        let expected_it: TopicIterator = names.clone().into_iter().zip([2, 1, 1]).collect();
        assert_eq!(result.batch.status, BatchStatus::RecordsExceeded);
        assert_eq!(result.iter, expected_it);
        assert_eq!(
            result
                .batch
                .try_into_records()?
                .into_iter()
                .collect::<HashSet<_>>(),
            vec![part_records[2][0].clone()]
                .into_iter()
                .collect::<HashSet<_>>()
        );

        // final fetch will fetch both remaining records from partition-1 and partition-2
        let prior_it = result.iter;
        let result = topic
            .get_records_by_time(prior_it, span.clone(), RowLimit::records(5), None)
            .await;
        let expected_it: TopicIterator = names.clone().into_iter().zip([2, 2, 2]).collect();
        assert_limit_unreached(&result.batch.status);
        assert_eq!(result.iter, expected_it);
        assert_eq!(
            result
                .batch
                .try_into_records()?
                .into_iter()
                .collect::<HashSet<_>>(),
            vec![part_records[1][1].clone(), part_records[2][1].clone()]
                .into_iter()
                .collect::<HashSet<_>>()
        );

        // no more records left
        let prior_it = result.iter;
        let result = topic
            .get_records_by_time(prior_it, span, RowLimit::records(1), None)
            .await;
        assert_limit_unreached(&result.batch.status);
        assert_eq!(result.iter, expected_it);
        assert_eq!(result.batch.try_into_records()?, vec![]);

        Ok(())
    }

    #[tokio::test]
    async fn test_schema_in_iteration() -> Result<()> {
        let (_tmpdir, topic) = scratch().await;
        // test chunks include 5 records each
        let chunk_a = inferences_schema_a();
        let chunk_b = inferences_schema_b();

        let chunk_allocation = vec![
            vec![&chunk_a, &chunk_a, &chunk_a, &chunk_b, &chunk_b],
            vec![&chunk_a, &chunk_b, &chunk_a, &chunk_b, &chunk_b],
            vec![&chunk_a, &chunk_a, &chunk_a, &chunk_a, &chunk_b],
        ];
        let names: Vec<_> = (1..4).map(|ix| format!("partition-{ix}")).collect();

        fn to_iter(names: &[String], v: [usize; 3]) -> HashMap<String, usize> {
            names.iter().cloned().zip(v).collect()
        }

        for (name, chunks) in names.iter().zip(chunk_allocation.into_iter()) {
            for chunk in chunks {
                topic.extend(name, chunk.clone()).await?;
                topic.get_partition(name).await.commit().await?;
            }
        }

        let many_rows = RowLimit::records(100);

        // first, we zip through all schema-a chunks at the start
        let result = topic
            .get_records(HashMap::new(), many_rows, Ordering::Forward, None)
            .await;
        assert_eq!(result.iter, to_iter(&names, [15, 5, 20]));
        assert_eq!(result.batch.status, BatchStatus::SchemaChanged);

        // now, the min partition (partition-1) has a schema change to schema-b.
        // we set a row limit here so we can test final iteration over all
        // partitions with schema-b later.
        let result = topic
            .get_records(result.iter, RowLimit::records(5), Ordering::Forward, None)
            .await;
        assert_eq!(result.iter, to_iter(&names, [15, 10, 20]));
        assert_eq!(result.batch.status, BatchStatus::RecordsExceeded);

        // that same partition is still the min partition, but has
        // briefly changed back to schema-a
        let result = topic
            .get_records(result.iter, many_rows, Ordering::Forward, None)
            .await;
        assert_eq!(result.iter, to_iter(&names, [15, 15, 20]));
        assert_eq!(result.batch.status, BatchStatus::SchemaChanged);

        // now it has changed back to schema-b, and we can resume iterating
        // through all partitions.
        let result = topic
            .get_records(result.iter, many_rows, Ordering::Forward, None)
            .await;
        assert_eq!(result.iter, to_iter(&names, [25, 25, 25]));
        assert!(result.batch.status.is_open());

        Ok(())
    }

    #[tokio::test]
    async fn test_schema_in_reverse_iteration() -> Result<()> {
        let (_tmpdir, topic) = scratch().await;
        // test chunks include 5 records each
        let chunk_a = inferences_schema_a();
        let chunk_b = inferences_schema_b();

        let chunk_allocation = vec![
            vec![&chunk_a, &chunk_a, &chunk_a, &chunk_b, &chunk_b],
            vec![&chunk_a, &chunk_b, &chunk_a, &chunk_b, &chunk_b],
            vec![&chunk_a, &chunk_a, &chunk_a, &chunk_a, &chunk_b],
        ];
        let names: Vec<_> = (1..4).map(|ix| format!("p{ix}")).collect();

        for (name, chunks) in names.iter().zip(chunk_allocation.into_iter()) {
            for chunk in chunks {
                topic.extend(name, chunk.clone()).await?;
                topic.get_partition(name).await.commit().await?;
            }
        }

        let many_rows = RowLimit::records(100);

        // first, we zip through all schema-b chunks at the start
        let result = topic
            .get_records(HashMap::new(), many_rows, Ordering::Reverse, None)
            .await;
        assert_eq!(result.iter["p1"], 15);
        assert_eq!(result.iter["p2"], 15);
        assert_eq!(result.iter["p3"], 20);
        assert_eq!(result.batch.schema.unwrap(), chunk_b.schema);
        assert_eq!(result.batch.status, BatchStatus::SchemaChanged);

        // now we'll get the bulk of the schema-a chunks
        let result = topic
            .get_records(result.iter, RowLimit::default(), Ordering::Reverse, None)
            .await;
        assert_eq!(result.iter["p1"], 0);
        assert_eq!(result.iter["p2"], 10);
        assert_eq!(result.iter["p3"], 0);
        assert_eq!(result.batch.schema.unwrap(), chunk_a.schema);
        assert_eq!(result.batch.status, BatchStatus::SchemaChanged);

        // the final schema-b chunk
        let result = topic
            .get_records(result.iter, RowLimit::default(), Ordering::Reverse, None)
            .await;
        assert_eq!(result.iter["p1"], 0);
        assert_eq!(result.iter["p2"], 5);
        assert_eq!(result.iter["p3"], 0);
        assert_eq!(result.batch.schema.unwrap(), chunk_b.schema);
        assert_eq!(result.batch.status, BatchStatus::SchemaChanged);

        // and the final schema-a chunk
        let result = topic
            .get_records(result.iter, RowLimit::default(), Ordering::Reverse, None)
            .await;
        assert_eq!(result.iter["p1"], 0);
        assert_eq!(result.iter["p2"], 0);
        assert_eq!(result.iter["p3"], 0);
        assert_eq!(result.batch.schema.unwrap(), chunk_a.schema);
        assert!(result.batch.status.is_open());

        Ok(())
    }

    #[tokio::test]
    async fn test_filter_some_iteration() -> Result<()> {
        let (_tmpdir, topic) = scratch().await;
        let records_per_part = 103;
        let records = timed_records((0..records_per_part).map(|ix| (ix, format!("record-{ix}"))));

        // Create 3 partitions
        for record in &records {
            topic
                .extend_records("partition-0", slice::from_ref(record))
                .await?;
            topic
                .extend_records("partition-1", slice::from_ref(record))
                .await?;
            topic
                .extend_records("partition-2", slice::from_ref(record))
                .await?;
        }
        topic.commit().await?;

        let mut it: TopicIterator = HashMap::new();
        let mut fetched = vec![];
        let fetch_count = 11;

        // The above creates partitions (0..parts)
        let partitions_to_include = [0, 1];
        let filter: Vec<PartitionSelector> = partitions_to_include
            .into_iter()
            .map(|s| PartitionSelector::from(format!("partition-{s}")))
            .collect();

        for _ in 0..records.len() {
            let result = topic
                .get_records(
                    it,
                    RowLimit::records(fetch_count),
                    Ordering::Forward,
                    Some(filter.clone()),
                )
                .await;

            let status = result.batch.status;
            fetched.extend(result.batch.try_into_records().unwrap());

            if fetched.len() >= records.len() * partitions_to_include.len() {
                assert_limit_unreached(&status);
                break;
            } else {
                assert_eq!(status, BatchStatus::RecordsExceeded);
            }
            it = result.iter;
        }

        assert_eq!(fetched.len(), records.len() * partitions_to_include.len());

        topic.close().await;

        Ok(())
    }

    #[tokio::test]
    async fn select_non_existent_partition() -> Result<()> {
        let (_tmpdir, topic) = scratch().await;
        let records_per_part = 103;
        let records = timed_records((0..records_per_part).map(|ix| (ix, format!("record-{ix}"))));

        // Create 3 partitions
        for record in &records {
            topic
                .extend_records("partition-0", slice::from_ref(record))
                .await?;
            topic
                .extend_records("partition-1", slice::from_ref(record))
                .await?;
            topic
                .extend_records("partition-2", slice::from_ref(record))
                .await?;
        }
        topic.commit().await?;

        let mut it: TopicIterator = HashMap::new();
        let mut fetched = vec![];
        let fetch_count = 1000;

        // The above creates partitions (0..parts), but we want to expicitely ask for non-existent partition
        let partitions_to_include = [0, 1, 3];
        let filter: Vec<PartitionSelector> = partitions_to_include
            .into_iter()
            .map(|s| PartitionSelector::from(format!("partition-{s}")))
            .collect();

        let expected_data_len = records.len() * 2; // Only two valid partitions

        for _ in 0..records.len() {
            let result = topic
                .get_records(
                    it,
                    RowLimit::records(fetch_count),
                    Ordering::Forward,
                    Some(filter.clone()),
                )
                .await;

            let status = result.batch.status;
            fetched.extend(result.batch.try_into_records().unwrap());

            if fetched.len() >= expected_data_len {
                assert_limit_unreached(&status);
                break;
            } else {
                assert_eq!(status, BatchStatus::RecordsExceeded);
            }
            it = result.iter;
        }

        assert_eq!(fetched.len(), expected_data_len);

        topic.close().await;

        Ok(())
    }

    #[tokio::test]
    async fn test_filter_all_iteration() -> Result<()> {
        let (_tmpdir, topic) = scratch().await;
        let records = timed_records((0..103).map(|ix| (ix, format!("record-{ix}"))));

        let parts = 7;
        assert!(records.len() % parts != 0);
        let names: Vec<_> = (0..=parts).map(|ix| format!("partition-{ix}")).collect();
        for (ix, record) in records.iter().enumerate() {
            topic
                .extend_records(&names[ix % parts], slice::from_ref(record))
                .await?;
        }
        topic.commit().await?;

        let mut it: TopicIterator = HashMap::new();
        let mut fetched = vec![];
        let fetch_count = 11;
        assert!(fetch_count % parts != 0);
        assert!(records.len() % fetch_count != 0);

        // The above creates partitions (0..parts)
        let partitions_to_include = [0, 1, 2, 3, 4, 5, 6, 7];
        let filter: Vec<PartitionSelector> = partitions_to_include
            .into_iter()
            .map(|s| PartitionSelector::from(&names[s]))
            .collect();

        for _ in 0..records.len() {
            let result = topic
                .get_records(
                    it,
                    RowLimit::records(fetch_count),
                    Ordering::Forward,
                    Some(filter.clone()),
                )
                .await;

            if fetched.len() + fetch_count >= records.len() {
                assert_limit_unreached(&result.batch.status);
            } else {
                assert_eq!(result.batch.status, BatchStatus::RecordsExceeded);
            }
            fetched.extend(result.batch.try_into_records().unwrap());
            it = result.iter;
        }
        assert_eq!(
            records.into_iter().collect::<HashSet<_>>(),
            fetched.into_iter().collect::<HashSet<_>>()
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_filter_regex_iteration() -> Result<()> {
        let (_tmpdir, topic) = scratch().await;
        let records = timed_records((0..103).map(|ix| (ix, format!("record-{ix}"))));

        let parts = 7;
        assert!(records.len() % parts != 0);
        let names: Vec<_> = (0..=parts).map(|ix| format!("partition-{ix}")).collect();
        for (ix, record) in records.iter().enumerate() {
            topic
                .extend_records(&names[ix % parts], slice::from_ref(record))
                .await?;
        }
        // Create an extra partition that won't be captured by the regex.
        let uncap_records =
            timed_records((0..103).map(|ix| (ix, format!("uncaptured-record-{ix}"))));
        topic
            .extend_records("uncaptured-partition", &uncap_records)
            .await?;

        topic.commit().await?;

        let mut it: TopicIterator = HashMap::new();
        let mut fetched = vec![];
        let fetch_count = 11;
        assert!(fetch_count % parts != 0);
        assert!(records.len() % fetch_count != 0);

        for _ in 0..records.len() {
            let result = topic
                .get_records(
                    it,
                    RowLimit::records(fetch_count),
                    Ordering::Forward,
                    Some(vec![PartitionSelector::from("regex:partition-.*")]),
                )
                .await;

            let status = result.batch.status;
            fetched.extend(result.batch.try_into_records().unwrap());

            if fetched.len() >= records.len() {
                assert_limit_unreached(&status);
            } else {
                assert_eq!(status, BatchStatus::RecordsExceeded);
            }
            it = result.iter;
        }
        assert_eq!(
            records.iter().collect::<HashSet<_>>(),
            fetched.iter().collect::<HashSet<_>>()
        );

        assert_ne!(
            uncap_records.into_iter().collect::<HashSet<_>>(),
            fetched.into_iter().collect::<HashSet<_>>()
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_full_iteration() -> Result<()> {
        let (_tmpdir, topic) = scratch().await;
        let records = timed_records((0..103).map(|ix| (ix, format!("record-{ix}"))));

        let parts = 7;
        assert!(records.len() % parts != 0);
        let names: Vec<_> = (0..=parts).map(|ix| format!("partition-{ix}")).collect();
        for (ix, record) in records.iter().enumerate() {
            topic
                .extend_records(&names[ix % parts], slice::from_ref(record))
                .await?;
        }

        topic.commit().await?;

        let mut it: TopicIterator = HashMap::new();
        let mut fetched = vec![];
        let fetch_count = 11;
        assert!(fetch_count % parts != 0);
        assert!(records.len() % fetch_count != 0);
        for _ in 0..records.len() {
            let result = topic
                .get_records(it, RowLimit::records(fetch_count), Ordering::Forward, None)
                .await;

            if fetched.len() + fetch_count >= records.len() {
                assert_limit_unreached(&result.batch.status);
            } else {
                assert_eq!(result.batch.status, BatchStatus::RecordsExceeded);
            }
            fetched.extend(result.batch.try_into_records().unwrap());
            it = result.iter;
        }
        assert_eq!(
            records.into_iter().collect::<HashSet<_>>(),
            fetched.into_iter().collect::<HashSet<_>>()
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_full_reverse_iteration() -> Result<()> {
        let (_tmpdir, topic) = scratch().await;
        let records = timed_records((0..103).map(|ix| (ix, format!("record-{ix}"))));

        let parts = 7;
        assert!(records.len() % parts != 0);
        let names: Vec<_> = (0..=parts).map(|ix| format!("partition-{ix}")).collect();
        for (ix, record) in records.iter().enumerate() {
            topic
                .extend_records(&names[ix % parts], slice::from_ref(record))
                .await?;
        }

        topic.commit().await?;

        let mut it: TopicIterator = HashMap::new();
        let mut fetched = vec![];
        let fetch_count = 11;
        assert!(fetch_count % parts != 0);
        assert!(records.len() % fetch_count != 0);
        for _ in 0..records.len() {
            let result = topic
                .get_records(it, RowLimit::records(fetch_count), Ordering::Reverse, None)
                .await;

            if fetched.len() + fetch_count >= records.len() {
                assert_limit_unreached(&result.batch.status);
            } else {
                assert_eq!(result.batch.status, BatchStatus::RecordsExceeded);
            }
            fetched.extend(result.batch.try_into_records().unwrap());
            it = result.iter;
        }
        assert_eq!(
            records.into_iter().collect::<HashSet<_>>(),
            fetched.into_iter().collect::<HashSet<_>>()
        );
        Ok(())
    }

    const ITER_PARTITIONS: usize = 4;
    const ITER_RECORDS: usize = 20;

    async fn iteration_test_set(n_records: usize, n_partitions: usize) -> Result<(TempDir, Topic)> {
        let (tmpdir, topic) = scratch().await;

        // create small dataset with interleaved partition id's like
        // p0: r0, r4, r8,  r12, r16
        // p1: r1, r5, r9,  r13, r17
        // p2: r2, r6, r10, r14, r18
        // p3: r3, r7, r11, r15, r19
        let n = n_records / n_partitions;
        let data: Vec<_> = (0..ITER_RECORDS)
            .map(|i| (format!("r{i}"), format!("p{}", i / n % n_partitions)))
            .collect();

        for (record, part) in data {
            topic
                .extend_records(
                    &part,
                    &[Record {
                        time: Utc::now(),
                        message: record.into_bytes(),
                    }],
                )
                .await?;
        }

        topic.commit().await?;

        Ok((tmpdir, topic))
    }

    #[tokio::test]
    async fn test_reverse_iteration_ordering_t1() -> Result<()> {
        let (_tmpdir, topic) = iteration_test_set(20, 4).await.unwrap();

        // verify forward read
        let result = topic
            .get_records(
                HashMap::new(),
                RowLimit::records(5),
                Ordering::Forward,
                None,
            )
            .await;
        assert_eq!(
            vec!["r0", "r1", "r2", "r3", "r4"],
            batch_to_vec(result.batch)
        );

        // verify first tranche
        let result = topic
            .get_records(
                HashMap::new(),
                RowLimit::records(5),
                Ordering::Reverse,
                None,
            )
            .await;
        assert_eq!(
            vec!["r19", "r18", "r17", "r16", "r15"],
            batch_to_vec(result.batch)
        );

        //check iterator state
        assert_eq!(ITER_PARTITIONS, result.iter.len());
        assert_eq!(0, result.iter["p3"]);
        assert_eq!(5, result.iter["p2"]);
        assert_eq!(5, result.iter["p1"]);
        assert_eq!(5, result.iter["p0"]);

        Ok(())
    }

    #[tokio::test]
    async fn test_reverse_iteration_ordering_t2() -> Result<()> {
        // set initial state
        let (_tmpdir, topic) = iteration_test_set(ITER_RECORDS, ITER_PARTITIONS)
            .await
            .unwrap();
        let iter = HashMap::from([
            ("p0".to_string(), 5),
            ("p1".to_string(), 5),
            ("p2".to_string(), 5),
            ("p3".to_string(), 0),
        ]);

        // verify second tranche
        let result = topic
            .get_records(iter, RowLimit::records(5), Ordering::Reverse, None)
            .await;
        assert_eq!(
            vec!["r14", "r13", "r12", "r11", "r10"],
            batch_to_vec(result.batch)
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_reverse_iteration_ordering_t3() -> Result<()> {
        // set initial state
        let (_tmpdir, topic) = iteration_test_set(20, 4).await.unwrap();
        let iter = HashMap::from([
            ("p0".to_string(), 5),
            ("p1".to_string(), 5),
            ("p2".to_string(), 0),
            ("p3".to_string(), 0),
        ]);

        // insert new data
        topic
            .extend_records(
                "p4",
                &[Record {
                    time: Utc::now(),
                    message: "r21".as_bytes().to_vec(),
                }],
            )
            .await?;
        topic
            .extend_records(
                "p3",
                &[Record {
                    time: Utc::now(),
                    message: "r20".as_bytes().to_vec(),
                }],
            )
            .await?;
        topic.commit().await?;

        // verify tranche spanning partitions
        let result = topic
            .get_records(iter, RowLimit::records(7), Ordering::Reverse, None)
            .await;
        assert_eq!(
            vec!["r9", "r8", "r7", "r6", "r5", "r4", "r3"],
            batch_to_vec(result.batch)
        );

        // check iterator state
        assert_eq!(ITER_PARTITIONS, result.iter.len());
        assert_eq!(0, result.iter["p3"]);
        assert_eq!(0, result.iter["p2"]);
        assert_eq!(0, result.iter["p1"]);
        assert_eq!(3, result.iter["p0"]);

        // verify new data is returned in new iterator
        let result = topic
            .get_records(
                HashMap::new(),
                RowLimit::records(1000),
                Ordering::Reverse,
                None,
            )
            .await;
        assert_eq!(ITER_PARTITIONS + 1, result.iter.len());
        assert_eq!(
            ITER_RECORDS + 2,
            result.batch.try_into_records().unwrap().len()
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_reverse_iteration_ordering_t4() -> Result<()> {
        // set initial state
        let (_tmpdir, topic) = iteration_test_set(20, 4).await.unwrap();
        let iter = HashMap::from([
            ("p0".to_string(), 3),
            ("p1".to_string(), 0),
            ("p2".to_string(), 0),
            ("p3".to_string(), 0),
        ]);

        // verify final tranche
        let result = topic
            .get_records(iter, RowLimit::records(10), Ordering::Reverse, None)
            .await;
        assert_eq!(vec!["r2", "r1", "r0"], batch_to_vec(result.batch));

        Ok(())
    }

    #[tokio::test]
    async fn test_active_segments() -> Result<()> {
        let dir = tempdir().unwrap();
        let root = PathBuf::from(dir.path());
        let manifest = Manifest::attach(root.join("manifest.sqlite")).await;
        let topic = Topic::attach(
            root.clone(),
            manifest.clone(),
            String::from("testing"),
            PartitionConfig::default(),
        )
        .await;

        let records: Vec<_> = vec!["abc", "def", "ghi", "jkl", "mno", "p"]
            .into_iter()
            .map(|message| Record {
                time: Utc.timestamp_opt(0, 0).unwrap(),
                message: message.bytes().collect(),
            })
            .collect();

        for (ix, record) in records.iter().enumerate() {
            let name = format!("partition-{}", ix % 3);
            topic.extend_records(&name, slice::from_ref(record)).await?;
        }

        for (ix, record) in records.iter().enumerate() {
            let name = format!("partition-{}", ix % 3);
            assert_eq!(
                topic.get_record_by_index(&name, RecordIndex(ix / 3)).await,
                Some(record.clone())
            );
        }
        assert_eq!(
            deindex(
                topic
                    .get_partition("partition-0")
                    .await
                    .get_records(RecordIndex(0), RowLimit::records(1000), Ordering::Forward)
                    .await
                    .chunks
            ),
            vec![records[0].clone(), records[3].clone()]
        );

        assert_eq!(
            topic.readable_ids(None).await,
            HashMap::<_, _>::from_iter([
                ("partition-0".to_string(), RecordIndex(0)..RecordIndex(2)),
                ("partition-1".to_string(), RecordIndex(0)..RecordIndex(2)),
                ("partition-2".to_string(), RecordIndex(0)..RecordIndex(2)),
            ])
        );

        Ok(())
    }
}
