//! The manifest is used to track which segments have been stored, their time
//! and logical indices, and the overall stored size of partitions and topics.
//!
//! This is necessary because bulk segment storage of data in slogs is stateless
//! by design. This abstraction allows a variety of different slog types. One
//! example is object storage (e.g. S3), which has no performant methods for
//! querying any of the data recorded in the manifest.
//!
//! The only supported manifest type currently is SQLite. sqlx should
//! theoretically allow us to support somewhat arbitrary SQL databases, but
//! minor query modifications will be necessary.

use std::borrow::Borrow;
use std::fmt;
use std::ops::{Range, RangeInclusive};
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::str::FromStr;

use chrono::{DateTime, Utc};
use futures::stream;
use futures::stream::StreamExt;
use sqlx::migrate::Migrator;
use sqlx::query::Query;
use sqlx::sqlite::{Sqlite, SqliteArguments};
use sqlx::sqlite::{SqliteConnectOptions, SqlitePool, SqlitePoolOptions, SqliteRow};
use sqlx::{ColumnIndex, Row};
use tracing::{debug, error, info, trace};

use crate::data::{Ordering, RecordIndex};
use crate::slog::SegmentIndex;
pub use crate::transport::PartitionId;

pub const SEGMENT_FORMAT_VERSION: u16 = 1;

trait ToSqlOrder {
    fn to_sql_order(&self) -> &'static str;
}

impl ToSqlOrder for Ordering {
    fn to_sql_order(&self) -> &'static str {
        match self {
            Self::Forward => "ASC",
            Self::Reverse => "DESC",
        }
    }
}

#[derive(Debug)]
pub enum Scope<'a> {
    Global,
    Topic(&'a str),
    Partition(&'a PartitionId),
}

fn partition_id_from_row(row: &SqliteRow) -> PartitionId {
    PartitionId::new(
        row.get::<String, _>("topic"),
        row.get::<String, _>("partition"),
    )
}

#[derive(Clone, Debug)]
pub struct SegmentId<P: Borrow<PartitionId>> {
    pub(crate) partition_id: P,
    pub(crate) segment: SegmentIndex,
}

impl SegmentId<PartitionId> {
    fn from_row(row: &SqliteRow) -> Self {
        let partition_id = partition_id_from_row(row);
        Self {
            partition_id,
            segment: SegmentIndex::from_row(row, "segment_index"),
        }
    }
}

impl<P: Borrow<PartitionId>> fmt::Display for SegmentId<P> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let id = self.partition_id.borrow();
        format_args!("{}/{}", id.topic, id.partition).fmt(f)
    }
}

impl<P: Borrow<PartitionId>> SegmentId<P> {
    pub fn topic(&self) -> &str {
        &self.partition_id.borrow().topic
    }

    pub fn partition(&self) -> &str {
        &self.partition_id.borrow().partition
    }
}

#[derive(Clone, Debug)]
pub struct Manifest {
    pool: SqlitePool,
    path: PathBuf,
}

#[derive(Clone, Debug)]
pub struct SegmentData {
    pub index: SegmentIndex,
    pub time: RangeInclusive<DateTime<Utc>>,
    pub records: Range<RecordIndex>,
    pub size: usize,
    pub version: u16,
}

fn row_to_segment_data(row: SqliteRow) -> SegmentData {
    let index = SegmentIndex::from_row(&row, "segment_index");
    let t_start: DateTime<Utc> = row.get("time_start");
    let t_end: DateTime<Utc> = row.get("time_end");
    SegmentData {
        index,
        time: t_start..=t_end,
        records: RecordIndex::from_row(&row, "record_start")
            ..RecordIndex::from_row(&row, "record_end"),
        size: usize::try_from(row.get::<i64, _>("size")).unwrap(),
        version: row.try_get("version").unwrap_or_default(),
    }
}

fn row_to_option_segment(row: SqliteRow) -> Option<SegmentIndex> {
    row.get::<Option<i64>, _>(0)
        .map(|v| SegmentIndex(usize::try_from(v).unwrap()))
}

fn row_get_option_record(row: &SqliteRow, index: usize) -> Option<RecordIndex> {
    row.get::<Option<i64>, _>(index)
        .map(|v| RecordIndex(usize::try_from(v).unwrap()))
}

impl SegmentIndex {
    fn to_row(self) -> i64 {
        i64::try_from(self.0).unwrap()
    }

    fn from_row<I>(row: &SqliteRow, index: I) -> Self
    where
        I: ColumnIndex<SqliteRow>,
    {
        Self(usize::try_from(row.get::<i64, _>(index)).unwrap())
    }
}

trait SqliteMappable<T> {
    fn to_row(self) -> i64;
    fn from_row<I>(row: &SqliteRow, index: I) -> T
    where
        I: ColumnIndex<SqliteRow>;
}

impl SqliteMappable<Self> for RecordIndex {
    fn to_row(self) -> i64 {
        i64::try_from(self.0).unwrap()
    }

    fn from_row<I>(row: &SqliteRow, index: I) -> Self
    where
        I: ColumnIndex<SqliteRow>,
    {
        Self(usize::try_from(row.get::<i64, _>(index)).unwrap())
    }
}

fn add_suffix(path: &Path, suffix: &str) -> anyhow::Result<PathBuf> {
    let mut path = path.to_path_buf();
    let mut name = path
        .file_name()
        .ok_or_else(|| anyhow::anyhow!("no file name"))?
        .to_os_string();
    name.push(suffix);
    path.set_file_name(name);
    Ok(path)
}

fn copy_existing(src: &Path, dst: &Path) -> anyhow::Result<()> {
    if src.exists() {
        std::fs::copy(src, dst)?;
    }

    Ok(())
}

fn shm_path(db: &Path) -> anyhow::Result<PathBuf> {
    add_suffix(db, "-shm")
}

fn wal_path(db: &Path) -> anyhow::Result<PathBuf> {
    add_suffix(db, "-wal")
}

static MIGRATOR: Migrator = sqlx::migrate!();

impl Manifest {
    pub async fn current_prior_attach(current: PathBuf, prior: PathBuf) -> anyhow::Result<Self> {
        if prior.exists() && !current.exists() {
            info!("migrating {prior:?} to {current:?}");
            copy_existing(&shm_path(&prior)?, &shm_path(&current)?)?;
            copy_existing(&wal_path(&prior)?, &wal_path(&current)?)?;
            std::fs::copy(&prior, &current)?;
        }

        let manifest = Self::attach(current).await;

        Ok(manifest)
    }

    pub async fn attach(path: PathBuf) -> Self {
        // check for data directory and error if not exist
        if let Some(parent) = path.parent() {
            if !parent.is_dir() {
                panic!("Data directory '{}' not found.", parent.display())
            }
        }

        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .min_connections(1)
            .connect_with(
                SqliteConnectOptions::from_str(&format!("sqlite://{}", path.to_str().unwrap()))
                    .unwrap()
                    .create_if_missing(true),
            )
            .await
            .unwrap_or_else(|e| {
                panic!(
                    "Could not open manifest: {e} {:?}",
                    // attempt to write a new file to get a more detailed error,
                    // e.g. out of file handles, permissions, etc
                    std::fs::File::create(path.join(".test")).err()
                );
            });

        MIGRATOR
            .run(&pool)
            .await
            .expect("database migration failed!");
        // TODO clear pending segments (size=NULL)
        Self { pool, path }
    }

    /// Upserts data for a segment with the given identifier.
    pub(crate) async fn update(&self, id: &PartitionId, data: &SegmentData) {
        trace!("update {:?}: {:?}", id, data);
        sqlx::query(
            "
            INSERT INTO segments(
                topic,
                partition,
                segment_index,
                time_start,
                time_end,
                record_start,
                record_end,
                size,
                version
            ) VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
            ON CONFLICT(topic, partition, segment_index) DO UPDATE SET
                time_start=excluded.time_start,
                time_end=excluded.time_end,
                record_start=excluded.record_start,
                record_end=excluded.record_end,
                size=excluded.size,
                version=excluded.version;
        ",
        )
        .bind(id.topic())
        .bind(id.partition())
        .bind(data.index.to_row())
        .bind(data.time.start())
        .bind(data.time.end())
        .bind(data.records.start.to_row())
        .bind(data.records.end.to_row())
        .bind(i64::try_from(data.size).unwrap())
        .bind(SEGMENT_FORMAT_VERSION)
        .execute(&self.pool)
        .await
        .unwrap();
    }

    /// Inverse of upsert, gets any previously stored data for a segment.
    pub async fn get_segment_data(&self, id: SegmentId<&PartitionId>) -> Option<SegmentData> {
        sqlx::query(
            "
            SELECT segment_index, time_start, time_end, record_start, record_end, size, version FROM segments
            WHERE topic = ?1 AND partition = ?2 AND segment_index = ?3
        ",
        )
        .bind(id.topic())
        .bind(id.partition())
        .bind(id.segment.to_row())
        .map(row_to_segment_data)
        .fetch_optional(&self.pool)
        .await
        .unwrap()
    }

    async fn get_segment<'a>(
        &self,
        query: Query<'a, Sqlite, SqliteArguments<'a>>,
    ) -> Option<SegmentIndex> {
        query
            .map(row_to_option_segment)
            .fetch_optional(&self.pool)
            .await
            .unwrap()
            .flatten()
    }

    async fn get_ordered_segment(
        &self,
        id: &PartitionId,
        order: &Ordering,
    ) -> Option<SegmentIndex> {
        self.get_segment(
            sqlx::query(&format!(
                "
                SELECT segment_index FROM segments
                WHERE topic = ?1 AND partition = ?2
                ORDER BY segment_index {} LIMIT 1
            ",
                order.to_sql_order()
            ))
            .bind(&id.topic)
            .bind(&id.partition),
        )
        .await
    }

    /// Find the oldest segment, scoped to a topic when specified.
    pub async fn get_oldest_segment(&self, topic: Option<&str>) -> Option<SegmentId<PartitionId>> {
        let query = match topic.as_ref() {
            Some(t) => sqlx::query(
                "
                SELECT topic, partition, segment_index FROM segments
                WHERE topic = ?1
                ORDER BY time_start ASC LIMIT 1
            ",
            )
            .bind(t),
            None => sqlx::query(
                "
                SELECT topic, partition, segment_index FROM segments
                ORDER BY time_start ASC LIMIT 1
            ",
            ),
        };

        query
            .map(|row| SegmentId::from_row(&row))
            .fetch_optional(&self.pool)
            .await
            .unwrap()
    }

    /// Find the segment with the highest index for a given partition.
    pub async fn get_max_segment(&self, id: &PartitionId) -> Option<SegmentIndex> {
        self.get_ordered_segment(id, &Ordering::Reverse).await
    }

    /// Find the segment with the lowest index for a given partition.
    pub async fn get_min_segment(&self, id: &PartitionId) -> Option<SegmentIndex> {
        self.get_ordered_segment(id, &Ordering::Forward).await
    }

    pub fn stream_segments<'a>(
        &'a self,
        id: &'a PartitionId,
        start: RecordIndex,
        order: Ordering,
    ) -> impl futures::Stream<Item = SegmentData> + 'a + Send {
        let query = match order {
            Ordering::Forward => {
                "
                SELECT segment_index, time_start, time_end, record_start, record_end, size, version FROM segments
                WHERE topic = ?1 AND partition = ?2 AND record_end > ?3
                ORDER BY segment_index ASC
            "
            }
            Ordering::Reverse => {
                "
                SELECT segment_index, time_start, time_end, record_start, record_end, size, version FROM segments
                WHERE topic = ?1 AND partition = ?2 AND record_start < ?3
                ORDER BY segment_index DESC
            "
            }
        };

        sqlx::query(query)
            .bind(&id.topic)
            .bind(&id.partition)
            .bind(start.to_row())
            .map(row_to_segment_data)
            .fetch_many(&self.pool)
            .flat_map(|r| stream::iter(r.unwrap().right()))
    }

    pub fn stream_time_segments<'a>(
        &'a self,
        id: &'a PartitionId,
        start: RecordIndex,
        times: &'a RangeInclusive<DateTime<Utc>>,
    ) -> impl futures::Stream<Item = SegmentData> + 'a {
        // see discussion on finding the intersection of intervals here:
        // https://scicomp.stackexchange.com/questions/26258/the-easiest-way-to-find-intersection-of-two-intervals
        // our problem is a subset of that: we only want to return segments
        // where an intersection exists; we don't care what the intersection is
        sqlx::query(
            "
                SELECT segment_index, time_start, time_end, record_start, record_end, size, version FROM segments
                WHERE (
                    topic = ?1 AND partition = ?2
                    AND ?3 <= time_end AND time_start <= ?4
                    AND record_end > ?5
                )
                ORDER BY segment_index ASC
            ",
        )
        .bind(&id.topic)
        .bind(&id.partition)
        .bind(times.start())
        .bind(times.end())
        .bind(start.to_row())
        .map(row_to_segment_data)
        .fetch_many(&self.pool)
        .flat_map(|r| stream::iter(r.unwrap().right()))
    }

    pub async fn get_partition_range(&self, id: &PartitionId) -> Option<Range<RecordIndex>> {
        sqlx::query(
            "
            SELECT MIN(record_start), MAX(record_end) FROM segments
            WHERE topic = ?1 AND partition = ?2
            GROUP BY partition
        ",
        )
        .bind(id.topic())
        .bind(id.partition())
        .map(|row| {
            match (
                row_get_option_record(&row, 0),
                row_get_option_record(&row, 1),
            ) {
                (Some(start), Some(end)) => Some(start..end),
                _ => None,
            }
        })
        .fetch_optional(&self.pool)
        .await
        .unwrap()
        .flatten()
    }

    /// Remove the identified segment from the manifest.
    pub async fn remove_segment(&self, id: SegmentId<&PartitionId>) {
        trace!("remove segment {:?}", id);
        sqlx::query(
            "
            DELETE FROM segments
            WHERE topic = ?1 AND partition = ?2 AND segment_index = ?3
        ",
        )
        .bind(id.topic())
        .bind(id.partition())
        .bind(id.segment.to_row())
        .execute(&self.pool)
        .await
        .unwrap();
    }

    /// Get the total stored byte size of a given catalog, topic, or partition.
    pub async fn get_size(&self, scope: Scope<'_>) -> usize {
        let query = match scope {
            Scope::Global => sqlx::query("SELECT SUM(size) AS size FROM segments"),
            Scope::Partition(id) => sqlx::query(
                "
            SELECT SUM(size) AS size FROM segments
            WHERE topic = ?1 AND partition = ?2
            GROUP BY partition
        ",
            )
            .bind(&id.topic)
            .bind(&id.partition),
            Scope::Topic(topic) => sqlx::query(
                "
            SELECT SUM(size) AS size FROM segments
            WHERE topic = ?1
            GROUP BY topic
        ",
            )
            .bind(topic),
        };

        let bytes = query
            .map(|row: SqliteRow| Some(usize::try_from(row.get::<Option<i64>, _>(0)?).unwrap()))
            .fetch_optional(&self.pool)
            .await
            .unwrap()
            .flatten()
            .unwrap_or(0);

        if matches!(scope, Scope::Global) {
            let db_bytes = self.db_bytes();
            debug!(db_bytes);
            bytes + db_bytes
        } else {
            bytes
        }
    }

    pub fn db_bytes(&self) -> usize {
        fn try_size(path: &Path, transform: Option<fn(&Path) -> anyhow::Result<PathBuf>>) -> usize {
            let transformed = transform.and_then(|f| f(path).ok());

            let required = transformed.as_deref().is_none_or(|p| p.exists());
            if !required {
                return 0;
            };

            let path = transformed.as_deref().unwrap_or(path);
            path.metadata()
                .map(|meta| meta.size())
                .inspect_err(|e| error!("error getting size of {path:?}: {e:?}"))
                .unwrap_or(0) as usize
        }

        try_size(&self.path, None)
            + try_size(&self.path, Some(wal_path))
            + try_size(&self.path, Some(shm_path))
    }

    pub async fn get_partitions(&self, topic: &str) -> Vec<String> {
        sqlx::query(
            "
            SELECT DISTINCT partition FROM segments
            WHERE topic = ?1
        ",
        )
        .bind(topic)
        .map(|row: SqliteRow| row.get::<String, _>(0))
        .fetch_all(&self.pool)
        .await
        .unwrap()
    }

    pub async fn get_topics(&self) -> Vec<String> {
        sqlx::query(
            "
            SELECT DISTINCT topic FROM segments
        ",
        )
        .map(|row: SqliteRow| row.get::<String, _>(0))
        .fetch_all(&self.pool)
        .await
        .unwrap()
    }

    /// Topics ordered from newest (most recent `time_end`) to oldest. Used by
    /// stochastic reconcile to bias sampling toward recently-active topics.
    pub async fn get_topics_by_recency(&self) -> Vec<String> {
        sqlx::query(
            "
            SELECT topic FROM segments
            GROUP BY topic
            ORDER BY MAX(time_end) DESC
        ",
        )
        .map(|row: SqliteRow| row.get::<String, _>(0))
        .fetch_all(&self.pool)
        .await
        .unwrap()
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use chrono::TimeZone;
    use tempfile::tempdir;

    impl Manifest {
        async fn get_segment_for_ix(
            &self,
            id: &PartitionId,
            ix: RecordIndex,
        ) -> Option<SegmentIndex> {
            self.get_segment(
                sqlx::query(
                    "
                    SELECT segment_index FROM segments
                    WHERE topic = ?1 AND partition = ?2 AND record_start <= ?3
                    ORDER BY segment_index DESC LIMIT 1
                ",
                )
                .bind(&id.topic)
                .bind(&id.partition)
                .bind(ix.to_row()),
            )
            .await
        }
    }

    #[tokio::test]
    async fn test_update() {
        let id = PartitionId::new("topic", "");
        let root = tempdir().unwrap();
        let path = root.path().join("testing.sqlite");
        let state = Manifest::attach(path).await;
        assert_eq!(state.get_max_segment(&id).await, None);

        let time = Utc.timestamp_opt(0, 0).unwrap()..=Utc.timestamp_opt(0, 0).unwrap();
        for ix in 0..10 {
            let time = time.clone();
            state
                .update(
                    &id,
                    &SegmentData {
                        index: SegmentIndex(0),
                        time,
                        records: RecordIndex(0)..RecordIndex(ix),
                        size: ix + 1,
                        version: SEGMENT_FORMAT_VERSION,
                    },
                )
                .await
        }

        state
            .update(
                &id,
                &SegmentData {
                    index: SegmentIndex(1),
                    time,
                    records: RecordIndex(10)..RecordIndex(20),
                    size: 15,
                    version: SEGMENT_FORMAT_VERSION,
                },
            )
            .await;
        assert_eq!(
            state.get_segment_for_ix(&id, RecordIndex(0)).await,
            Some(SegmentIndex(0))
        );
        assert_eq!(
            state.get_segment_for_ix(&id, RecordIndex(5)).await,
            Some(SegmentIndex(0))
        );
        assert_eq!(
            state.get_segment_for_ix(&id, RecordIndex(9)).await,
            Some(SegmentIndex(0))
        );
        assert_eq!(
            state.get_segment_for_ix(&id, RecordIndex(15)).await,
            Some(SegmentIndex(1))
        );
        assert_eq!(
            state.get_segment_for_ix(&id, RecordIndex(200)).await,
            Some(SegmentIndex(1))
        );
        assert_eq!(state.get_max_segment(&id).await, Some(SegmentIndex(1)));
        assert_eq!(state.get_size(Scope::Topic(id.topic())).await, 25);
    }

    #[tokio::test]
    async fn test_partitions() {
        let topic = "topic";
        let root = tempdir().unwrap();
        let path = root.path().join("testing.sqlite");
        let state = Manifest::attach(path).await;
        let a = PartitionId::new(topic, "a");
        let b = PartitionId::new(topic, "b");

        let time = Utc.timestamp_opt(0, 0).unwrap()..=Utc.timestamp_opt(0, 0).unwrap();
        for ix in 0..10 {
            let time = time.clone();
            state
                .update(
                    &a,
                    &SegmentData {
                        index: SegmentIndex(0),
                        time,
                        records: RecordIndex(0)..RecordIndex(ix),
                        size: 10,
                        version: SEGMENT_FORMAT_VERSION,
                    },
                )
                .await
        }

        state
            .update(
                &a,
                &SegmentData {
                    index: SegmentIndex(1),
                    time: time.clone(),
                    records: RecordIndex(10)..RecordIndex(20),
                    size: 25,
                    version: SEGMENT_FORMAT_VERSION,
                },
            )
            .await;

        state
            .update(
                &b,
                &SegmentData {
                    index: SegmentIndex(0),
                    time,
                    records: RecordIndex(0)..RecordIndex(15),
                    size: 12,
                    version: SEGMENT_FORMAT_VERSION,
                },
            )
            .await;

        assert_eq!(state.get_size(Scope::Partition(&a)).await, 35);
        assert_eq!(state.get_size(Scope::Partition(&b)).await, 12);
        assert_eq!(
            state.get_partition_range(&a).await.unwrap(),
            RecordIndex(0)..RecordIndex(20)
        );
        assert_eq!(
            state.get_partition_range(&b).await.unwrap(),
            RecordIndex(0)..RecordIndex(15)
        );
        assert_eq!(
            state.get_partitions(a.topic()).await,
            vec!["a".to_string(), "b".to_string()]
        );
        assert_eq!(state.get_topics().await, vec![a.topic()]);
    }

    #[tokio::test]
    async fn test_query_oldest() {
        let topic = "topic";
        let other = "other_topic";
        let root = tempdir().unwrap();
        let path = root.path().join("testing.sqlite");
        let state = Manifest::attach(path).await;
        let a = PartitionId::new(topic, "a");
        let b = PartitionId::new(other, "a");
        let c = PartitionId::new(topic, "b");

        let data = SegmentData {
            index: SegmentIndex(0),
            time: Utc.timestamp_opt(00, 0).unwrap()..=Utc.timestamp_opt(20, 0).unwrap(),
            records: RecordIndex(0)..RecordIndex(15),
            size: 10,
            version: SEGMENT_FORMAT_VERSION,
        };

        assert!(state.get_oldest_segment(None).await.is_none());

        for (p, time) in [
            (
                &a,
                Utc.timestamp_opt(10, 0).unwrap()..=Utc.timestamp_opt(20, 0).unwrap(),
            ),
            (
                &b,
                Utc.timestamp_opt(5, 0).unwrap()..=Utc.timestamp_opt(20, 0).unwrap(),
            ),
            (
                &c,
                Utc.timestamp_opt(7, 0).unwrap()..=Utc.timestamp_opt(20, 0).unwrap(),
            ),
        ] {
            state
                .update(
                    p,
                    &SegmentData {
                        time,
                        ..data.clone()
                    },
                )
                .await;
            assert_eq!(state.get_size(Scope::Partition(p)).await, 10);
        }
        assert_eq!(state.get_size(Scope::Topic(a.topic())).await, 20);
        assert_eq!(state.get_size(Scope::Topic(c.topic())).await, 20);
        assert_eq!(state.get_size(Scope::Global).await, 30 + state.db_bytes());

        let oldest = state.get_oldest_segment(None).await.unwrap();
        assert_eq!(oldest.partition_id, b.clone());
        let oldest = state.get_oldest_segment(Some(topic)).await.unwrap();
        assert_eq!(oldest.partition_id, c.clone());
    }

    #[tokio::test]
    async fn test_remove() {
        let topic = "topic";
        let partition = "removal";
        let id = PartitionId::new(topic, partition);
        let root = tempdir().unwrap();
        let path = root.path().join("testing.sqlite");
        let state = Manifest::attach(path).await;
        assert_eq!(state.get_max_segment(&id).await, None);

        let time = Utc.timestamp_opt(0, 0).unwrap()..=Utc.timestamp_opt(0, 0).unwrap();
        for ix in 0..10 {
            let time = time.clone();
            state
                .update(
                    &id,
                    &SegmentData {
                        index: SegmentIndex(0),
                        time,
                        records: RecordIndex(0)..RecordIndex(ix),
                        size: 12,
                        version: SEGMENT_FORMAT_VERSION,
                    },
                )
                .await
        }
        assert_eq!(state.get_size(Scope::Partition(&id)).await, 12);

        state
            .update(
                &id,
                &SegmentData {
                    index: SegmentIndex(1),
                    time,
                    records: RecordIndex(10)..RecordIndex(20),
                    size: 13,
                    version: SEGMENT_FORMAT_VERSION,
                },
            )
            .await;
        assert_eq!(state.get_size(Scope::Partition(&id)).await, 25);

        assert_eq!(
            state.get_segment_for_ix(&id, RecordIndex(0)).await,
            Some(SegmentIndex(0))
        );
        assert_eq!(
            state.get_segment_for_ix(&id, RecordIndex(15)).await,
            Some(SegmentIndex(1))
        );
        assert_eq!(state.get_min_segment(&id).await, Some(SegmentIndex(0)));
        assert_eq!(state.get_max_segment(&id).await, Some(SegmentIndex(1)));

        state.remove_segment(SegmentIndex(0).to_id(&id)).await;
        assert_eq!(state.get_segment_for_ix(&id, RecordIndex(0)).await, None);
        assert_eq!(
            state.get_segment_for_ix(&id, RecordIndex(15)).await,
            Some(SegmentIndex(1))
        );
        assert_eq!(
            state.get_partition_range(&id).await,
            Some(RecordIndex(10)..RecordIndex(20))
        );
        assert_eq!(state.get_min_segment(&id).await, Some(SegmentIndex(1)));
        assert_eq!(state.get_max_segment(&id).await, Some(SegmentIndex(1)));
        assert_eq!(state.get_size(Scope::Partition(&id)).await, 13);

        state.remove_segment(SegmentIndex(1).to_id(&id)).await;
        assert_eq!(state.get_segment_for_ix(&id, RecordIndex(0)).await, None);
        assert_eq!(state.get_segment_for_ix(&id, RecordIndex(15)).await, None);
        assert_eq!(state.get_min_segment(&id).await, None);
        assert_eq!(state.get_max_segment(&id).await, None);
        assert_eq!(state.get_size(Scope::Partition(&id)).await, 0);
        assert_eq!(state.get_partition_range(&id).await, None);
    }

    #[tokio::test]
    async fn test_time_query() {
        let topic = "topic";
        let partition = "removal";
        let id = PartitionId::new(topic, partition);
        let root = tempdir().unwrap();
        let path = root.path().join("testing.sqlite");
        let state = Manifest::attach(path).await;
        assert_eq!(state.get_max_segment(&id).await, None);

        async fn add_record(
            state: &Manifest,
            segment: SegmentId<&PartitionId>,
            records: Range<usize>,
            times: RangeInclusive<i64>,
        ) {
            let time = Utc.timestamp_opt(*times.start(), 0).unwrap()
                ..=Utc.timestamp_opt(*times.end(), 0).unwrap();
            let records = RecordIndex(records.start)..RecordIndex(records.end);
            state
                .update(
                    segment.partition_id,
                    &SegmentData {
                        index: segment.segment,
                        time,
                        records,
                        size: 12,
                        version: SEGMENT_FORMAT_VERSION,
                    },
                )
                .await
        }

        async fn verify_stream(
            state: &Manifest,
            id: &PartitionId,
            start: usize,
            times: RangeInclusive<i64>,
            segments: Vec<usize>,
        ) {
            let times = Utc.timestamp_opt(*times.start(), 0).unwrap()
                ..=Utc.timestamp_opt(*times.end(), 0).unwrap();
            assert_eq!(
                state
                    .stream_time_segments(id, RecordIndex(start), &times)
                    .map(|data| data.index)
                    .collect::<Vec<_>>()
                    .await,
                segments.into_iter().map(SegmentIndex).collect::<Vec<_>>(),
                "query {times:?}"
            );
        }

        add_record(&state, SegmentIndex(0).to_id(&id), 0..10, 500..=600).await;
        // overlap
        add_record(&state, SegmentIndex(1).to_id(&id), 10..20, 580..=800).await;
        // gap
        add_record(&state, SegmentIndex(2).to_id(&id), 20..30, 820..=900).await;
        // out-of order
        add_record(&state, SegmentIndex(3).to_id(&id), 30..40, 590..=800).await;

        // single points-in-time
        verify_stream(&state, &id, 0, 400..=400, vec![]).await;
        verify_stream(&state, &id, 0, 500..=500, vec![0]).await;
        verify_stream(&state, &id, 0, 520..=520, vec![0]).await;
        verify_stream(&state, &id, 0, 585..=585, vec![0, 1]).await;
        verify_stream(&state, &id, 0, 595..=595, vec![0, 1, 3]).await;
        verify_stream(&state, &id, 0, 700..=700, vec![1, 3]).await;
        verify_stream(&state, &id, 0, 850..=850, vec![2]).await;
        verify_stream(&state, &id, 0, 910..=910, vec![]).await;

        // ranges: query before all
        verify_stream(&state, &id, 0, 400..=499, vec![]).await;

        // ranges: query after all
        verify_stream(&state, &id, 0, 910..=1000, vec![]).await;

        // ranges: start before record range, end within record range
        verify_stream(&state, &id, 0, 400..=500, vec![0]).await;
        verify_stream(&state, &id, 0, 400..=520, vec![0]).await;
        verify_stream(&state, &id, 0, 400..=585, vec![0, 1]).await;
        verify_stream(&state, &id, 0, 400..=700, vec![0, 1, 3]).await;

        // ranges: start and end within record range
        verify_stream(&state, &id, 0, 501..=520, vec![0]).await;
        verify_stream(&state, &id, 0, 501..=585, vec![0, 1]).await;
        verify_stream(&state, &id, 0, 501..=700, vec![0, 1, 3]).await;
        verify_stream(&state, &id, 22, 501..=700, vec![3]).await;
        verify_stream(&state, &id, 0, 820..=850, vec![2]).await;
        verify_stream(&state, &id, 0, 601..=700, vec![1, 3]).await;

        // ranges: start within record range, end after
        verify_stream(&state, &id, 0, 500..=1000, vec![0, 1, 2, 3]).await;
        verify_stream(&state, &id, 12, 500..=1000, vec![1, 2, 3]).await;
        verify_stream(&state, &id, 0, 520..=1000, vec![0, 1, 2, 3]).await;
        verify_stream(&state, &id, 20, 520..=1000, vec![2, 3]).await;
        verify_stream(&state, &id, 0, 705..=1000, vec![1, 2, 3]).await;
        verify_stream(&state, &id, 0, 850..=1000, vec![2]).await;

        // finally, verify when all segments are contained within the range
        verify_stream(&state, &id, 0, 250..=1000, vec![0, 1, 2, 3]).await;
    }
}
