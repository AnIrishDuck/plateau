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
use chrono::{DateTime, Utc};
use sqlx::query::Query;
use sqlx::sqlite::{Sqlite, SqliteArguments};
use sqlx::sqlite::{SqliteConnectOptions, SqlitePool, SqlitePoolOptions, SqliteRow};
use sqlx::{Executor, Row};
use std::convert::TryFrom;
use std::ops::{Deref, Range, RangeInclusive};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::time::SystemTime;

use crate::slog::{RecordIndex, SegmentIndex};

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct PartitionId {
    topic: String,
    partition: String,
}

impl PartitionId {
    pub fn new(topic: &str, partition: &str) -> Self {
        PartitionId {
            topic: String::from(topic),
            partition: String::from(partition),
        }
    }

    pub fn topic(&self) -> &str {
        &self.topic
    }

    pub fn partition(&self) -> &str {
        &self.partition
    }

    pub fn segment_id(&self, segment: SegmentIndex) -> SegmentId<&PartitionId> {
        SegmentId {
            partition_id: &self,
            segment,
        }
    }
}

impl std::fmt::Display for PartitionId {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}/{}", self.topic, self.partition)
    }
}

#[derive(Clone, Debug)]
pub struct SegmentId<P: Deref<Target = PartitionId>> {
    partition_id: P,
    segment: SegmentIndex,
}

impl<P: Deref<Target = PartitionId>> std::fmt::Display for SegmentId<P> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "{}/{}",
            self.partition_id.topic, self.partition_id.partition
        )
    }
}

impl<P: Deref<Target = PartitionId>> SegmentId<P> {
    pub fn topic(&self) -> &str {
        &self.partition_id.topic
    }

    pub fn partition(&self) -> &str {
        &self.partition_id.partition
    }

    pub fn segment(&self) -> SegmentIndex {
        self.segment
    }
}

#[derive(Clone)]
pub struct Manifest {
    path: PathBuf,
    pool: SqlitePool,
}

#[derive(Clone, Debug)]
pub struct SegmentData {
    pub time: RangeInclusive<SystemTime>,
    pub index: Range<RecordIndex>,
    pub size: usize,
}

fn row_to_option_segment(row: SqliteRow) -> Option<SegmentIndex> {
    row.get::<Option<i64>, _>(0)
        .map(|v| SegmentIndex(usize::try_from(v).unwrap()))
}

impl SegmentIndex {
    fn to_row(&self) -> i64 {
        i64::try_from(self.0).unwrap()
    }

    fn from_row(row: SqliteRow, index: usize) -> Self {
        SegmentIndex(usize::try_from(row.get::<i64, _>(index)).unwrap())
    }
}

impl RecordIndex {
    fn to_row(&self) -> i64 {
        i64::try_from(self.0).unwrap()
    }

    fn from_row(row: &SqliteRow, index: usize) -> Self {
        RecordIndex(usize::try_from(row.get::<i64, _>(index)).unwrap())
    }
}

impl Manifest {
    pub async fn attach(path: PathBuf) -> Self {
        let prior = Path::new(&path).exists();
        let pool = SqlitePoolOptions::new()
            .connect_with(
                SqliteConnectOptions::from_str(&format!("sqlite://{}", path.to_str().unwrap()))
                    .unwrap()
                    .create_if_missing(true),
            )
            .await
            .unwrap();
        if !prior {
            pool.execute(
                "CREATE TABLE segments (
                        id              INTEGER PRIMARY KEY,
                        topic           STRING NOT NULL,
                        partition       STRING NOT NULL,
                        segment_id      INTEGER NOT NULL,
                        time_start      DATETIME NOT NULL,
                        time_end        DATETIME NOT NULL,
                        index_start     INTEGER NOT NULL,
                        index_end       INTEGER NOT NULL,
                        size            INTEGER NOT NULL
                        )
                        ",
            )
            .await
            .unwrap();
            pool.execute(
                "CREATE UNIQUE INDEX segments_segment_id ON segments(topic, partition, segment_id)",
            )
            .await
            .unwrap();
            pool.execute("CREATE INDEX segments_start ON segments(topic, partition, index_start)")
                .await
                .unwrap();
        }
        // TODO clear pending segments (size=NULL)
        Manifest { path, pool }
    }

    /// Upserts data for a segment with the given identifier.
    pub(crate) async fn update(&self, id: SegmentId<&PartitionId>, data: &SegmentData) -> () {
        sqlx::query(
            "
            INSERT INTO segments(
                topic,
                partition,
                segment_id,
                time_start,
                time_end,
                index_start,
                index_end,
                size
            ) VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
            ON CONFLICT(topic, partition, segment_id) DO UPDATE SET
                time_start=excluded.time_start,
                time_end=excluded.time_end,
                index_start=excluded.index_start,
                index_end=excluded.index_end;
        ",
        )
        .bind(id.topic())
        .bind(id.partition())
        .bind(id.segment.to_row())
        .bind(DateTime::<Utc>::from(*data.time.start()))
        .bind(DateTime::<Utc>::from(*data.time.end()))
        .bind(data.index.start.to_row())
        .bind(data.index.end.to_row())
        .bind(i64::try_from(data.size).unwrap())
        .execute(&self.pool)
        .await
        .unwrap();
    }

    /// Inverse of upsert, gets any previously stored data for a segment.
    pub async fn get_segment_data(&self, id: SegmentId<&PartitionId>) -> Option<SegmentData> {
        sqlx::query(
            "
            SELECT time_start, time_end, index_start, index_end, size FROM segments
            WHERE topic = ?1 AND partition = ?2 AND segment_id = ?3
        ",
        )
        .bind(id.topic())
        .bind(id.partition())
        .bind(id.segment.to_row())
        .map(|row: SqliteRow| {
            let t_start: DateTime<Utc> = row.get(0);
            let t_end: DateTime<Utc> = row.get(1);
            Some(SegmentData {
                time: (SystemTime::from(t_start)..=SystemTime::from(t_end)),
                index: RecordIndex::from_row(&row, 2)..RecordIndex::from_row(&row, 3),
                size: usize::try_from(row.get::<i64, _>(4)).unwrap(),
            })
        })
        .fetch_optional(&self.pool)
        .await
        .unwrap()
        .flatten()
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

    /// Find the segment a record is stored within via sequential partition index.
    pub async fn get_segment_for_ix(
        &self,
        id: &PartitionId,
        ix: RecordIndex,
    ) -> Option<SegmentIndex> {
        self.get_segment(
            sqlx::query(
                "
                SELECT segment_id FROM segments
                WHERE topic = ?1 AND partition = ?2 AND index_start <= ?3 ORDER BY segment_id DESC LIMIT 1
            ",
            )
            .bind(&id.topic)
            .bind(&id.partition)
            .bind(ix.to_row()),
        )
        .await
    }

    async fn get_ordered_segment(&self, id: &PartitionId, order: &str) -> Option<SegmentIndex> {
        self.get_segment(
            sqlx::query(&format!(
                "
                SELECT segment_id FROM segments
                WHERE topic = ?1 AND partition = ?2
                ORDER BY segment_id {} LIMIT 1
            ",
                order
            ))
            .bind(&id.topic)
            .bind(&id.partition),
        )
        .await
    }

    /// Find the segment with the highest index for a given partition.
    pub async fn get_max_segment(&self, id: &PartitionId) -> Option<SegmentIndex> {
        self.get_ordered_segment(id, "DESC").await
    }

    /// Find the segment with the lowest index for a given partition.
    pub async fn get_min_segment(&self, id: &PartitionId) -> Option<SegmentIndex> {
        self.get_ordered_segment(id, "ASC").await
    }

    /// Remove the identified segment from the manifest.
    pub async fn remove_segment(&self, id: SegmentId<&PartitionId>) {
        sqlx::query(
            "
            DELETE FROM segments
            WHERE topic = ?1 AND partition = ?2 AND segment_id = ?3
        ",
        )
        .bind(id.topic())
        .bind(id.partition())
        .bind(id.segment.to_row())
        .execute(&self.pool)
        .await
        .unwrap();
    }

    /// Get the total stored byte size of a given partition.
    pub async fn get_size(&self, id: &PartitionId) -> Option<usize> {
        sqlx::query(
            "
            SELECT SUM(size) AS size FROM segments
            WHERE topic = ?1 AND partition = ?2
            GROUP BY partition
        ",
        )
        .bind(&id.topic)
        .bind(&id.partition)
        .map(|row: SqliteRow| Some(usize::try_from(row.get::<Option<i64>, _>(0)?).unwrap()))
        .fetch_optional(&self.pool)
        .await
        .unwrap()
        .flatten()
    }

    /// Find the "open index" of a given partition.
    /// The open index is the lowest index for a record that is not durably
    /// stored on disk.
    pub async fn open_index(&self, id: &PartitionId) -> RecordIndex {
        match self.get_max_segment(id).await {
            Some(segment_ix) => self
                .get_segment_data(id.segment_id(segment_ix))
                .await
                .map(|span| span.index.end),
            None => None,
        }
        .unwrap_or(RecordIndex(0))
    }
}

mod test {
    use super::*;
    use std::time::SystemTime;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_update() {
        let id = PartitionId::new("topic", "");
        let root = tempdir().unwrap();
        let path = root.path().join("testing.sqlite");
        let state = Manifest::attach(PathBuf::from(path)).await;
        assert_eq!(state.get_max_segment(&id).await, None);

        let time = SystemTime::UNIX_EPOCH..=SystemTime::UNIX_EPOCH;
        for ix in 0..10 {
            let time = time.clone();
            state
                .update(
                    id.segment_id(SegmentIndex(0)),
                    &SegmentData {
                        time,
                        index: RecordIndex(0)..RecordIndex(ix),
                        size: 10,
                    },
                )
                .await
        }

        state
            .update(
                id.segment_id(SegmentIndex(1)),
                &SegmentData {
                    time,
                    index: RecordIndex(10)..RecordIndex(20),
                    size: 15,
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
    }

    #[tokio::test]
    async fn test_partitions() {
        let topic = "topic";
        let root = tempdir().unwrap();
        let path = root.path().join("testing.sqlite");
        let state = Manifest::attach(PathBuf::from(path)).await;
        let a = PartitionId::new(&topic, "a");
        let b = PartitionId::new(&topic, "b");

        let time = SystemTime::UNIX_EPOCH..=SystemTime::UNIX_EPOCH;
        for ix in 0..10 {
            let time = time.clone();
            state
                .update(
                    a.segment_id(SegmentIndex(0)),
                    &SegmentData {
                        time,
                        index: RecordIndex(0)..RecordIndex(ix),
                        size: 10,
                    },
                )
                .await
        }

        state
            .update(
                a.segment_id(SegmentIndex(1)),
                &SegmentData {
                    time: time.clone(),
                    index: RecordIndex(10)..RecordIndex(20),
                    size: 25,
                },
            )
            .await;

        state
            .update(
                b.segment_id(SegmentIndex(0)),
                &SegmentData {
                    time,
                    index: RecordIndex(0)..RecordIndex(20),
                    size: 12,
                },
            )
            .await;
    }

    #[tokio::test]
    async fn test_remove() {
        let topic = "topic";
        let partition = "removal";
        let id = PartitionId::new(topic, partition);
        let root = tempdir().unwrap();
        let path = root.path().join("testing.sqlite");
        let state = Manifest::attach(PathBuf::from(path)).await;
        assert_eq!(state.get_max_segment(&id).await, None);

        let time = SystemTime::UNIX_EPOCH..=SystemTime::UNIX_EPOCH;
        for ix in 0..10 {
            let time = time.clone();
            state
                .update(
                    id.segment_id(SegmentIndex(0)),
                    &SegmentData {
                        time,
                        index: RecordIndex(0)..RecordIndex(ix),
                        size: 12,
                    },
                )
                .await
        }
        assert_eq!(state.get_size(&id).await, Some(12));

        state
            .update(
                id.segment_id(SegmentIndex(1)),
                &SegmentData {
                    time,
                    index: RecordIndex(10)..RecordIndex(20),
                    size: 13,
                },
            )
            .await;
        assert_eq!(state.get_size(&id).await, Some(25));

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

        state.remove_segment(id.segment_id(SegmentIndex(0))).await;
        assert_eq!(state.get_segment_for_ix(&id, RecordIndex(0)).await, None);
        assert_eq!(
            state.get_segment_for_ix(&id, RecordIndex(15)).await,
            Some(SegmentIndex(1))
        );
        assert_eq!(state.get_min_segment(&id).await, Some(SegmentIndex(1)));
        assert_eq!(state.get_max_segment(&id).await, Some(SegmentIndex(1)));
        assert_eq!(state.get_size(&id).await, Some(13));

        state.remove_segment(id.segment_id(SegmentIndex(1))).await;
        assert_eq!(state.get_segment_for_ix(&id, RecordIndex(0)).await, None);
        assert_eq!(state.get_segment_for_ix(&id, RecordIndex(15)).await, None);
        assert_eq!(state.get_min_segment(&id).await, None);
        assert_eq!(state.get_max_segment(&id).await, None);
        assert_eq!(state.get_size(&id).await, None);
    }
}
