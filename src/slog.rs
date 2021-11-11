//! The slog (segment log) is a sequence of segments with a well-known storage
//! location.
//!
//! Currently, local files named with a simple logical index is the only
//! supported slog type.
//!
//! Every slog has a background writer thread. This achieves two goals:
//!
//! - I/O write concurrency
//! - Load shedding
//!
//! Write concurrency allows us to ingest more records while waiting for the OS
//! to flush data to disk.
//!
//! Load is shed by failing any roll operation while an existing background
//! write is pending. This signals the topic partition to discard writes and
//! stall rolls until the write completes.
use crate::manifest::SegmentData;
use crate::segment::{Record, Segment};
use serde::{Deserialize, Serialize};
use std::convert::TryFrom;
use std::cmp::{max, min};
use std::fs;
use std::ops::RangeInclusive;
use std::path::{Path, PathBuf};
use std::time::SystemTime;
use tokio::sync::{mpsc, RwLock};

pub(crate) type SlogWrites = mpsc::Receiver<WriteResult>;

/// A slog (segment log) is a named and ordered series of segments.
pub(crate) struct Slog {
    root: PathBuf,
    name: String,
    state: RwLock<State>,
}

pub struct State {
    active: Vec<Record>,
    active_ix: usize,
    active_size: usize,
    writer: mpsc::Sender<WriteRequest>,
    pending: Option<Vec<Record>>,
    time_range: Option<RangeInclusive<SystemTime>>,
}

struct WriteRequest {
    segment: usize,
    start: u128,
    time: RangeInclusive<SystemTime>,
    records: Vec<Record>,
}

#[derive(Debug)]
pub(crate) struct WriteResult {
    pub(crate) segment: usize,
    pub(crate) data: SegmentData
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct Index {
    pub segment: usize,
    pub record: usize,
}

impl Slog {
    //! Because a slog is stateless, whatever attaches it is responsible for processing
    //! commit events. The channel is bounded to a size of one; if it is not consumed,
    //! the writer thread will immediately stall.
    pub fn attach(root: PathBuf, name: String, active_ix: usize) -> (Self, SlogWrites) {
        let (writer, rx) = spawn_slog_thread(root.clone(), name.clone());
        let state = State {
            active_ix,
            active: vec![],
            active_size: 0,
            pending: None,
            time_range: None,
            writer,
        };

        let slog = Slog {
            root,
            name,
            state: RwLock::new(state),
        };

        (slog, rx)
    }

    fn segment_path(root: &PathBuf, name: &str, segment_ix: usize) -> PathBuf {
        let file = PathBuf::from(format!("{}-{}", name, segment_ix));
        root.join(file)
    }

    pub(crate) fn segment_from_name(root: &PathBuf, name: &str, segment_ix: usize) -> Segment {
        Segment::at(Slog::segment_path(root, name, segment_ix))
    }

    pub(crate) fn get_segment(&self, segment_ix: usize) -> Segment {
        Slog::segment_from_name(&self.root, &self.name, segment_ix)
    }

    pub(crate) async fn get_record(&self, ix: Index) -> Option<Record> {
        let state = self.state.read().await;
        assert!(ix.segment <= state.active_ix);
        state.get_record(ix.clone()).await.or_else(|| {
            let segment = self.get_segment(ix.segment);
            if Path::new(segment.path()).exists() {
                segment.read().read_all().get(ix.record).cloned()
            } else {
                None
            }
        })
    }

    pub(crate) async fn append(&self, r: &Record) -> Index {
        self.state.write().await.append(r).await
    }

    pub(crate) fn destroy(&self, segment_ix: usize) {
        fs::remove_file(Slog::segment_path(&self.root, &self.name, segment_ix)).unwrap()
    }

    pub(crate) async fn current_segment_ix(&self) -> usize {
        self.state.read().await.active_ix
    }

    pub(crate) async fn current_len(&self) -> usize {
        self.state.read().await.active.len()
    }

    pub(crate) async fn current_size(&self) -> usize {
        self.state.read().await.active_size
    }

    pub(crate) async fn current_time_range(&self) -> Option<RangeInclusive<SystemTime>> {
        self.state.read().await.time_range.clone()
    }

    pub(crate) async fn roll(&self, start: u128) -> bool {
        self.state.write().await.roll(start).await
    }
}

impl State {
    pub(crate) async fn append(&mut self, r: &Record) -> Index {
        let record = self.active.len();
        self.active_size += r.message.len();
        self.active.push(r.clone());
        self.time_range = self
            .time_range
            .clone()
            .map(|range| (min(*range.start(), r.time)..=max(*range.end(), r.time)))
            .or(Some(r.time..=r.time));
        Index {
            segment: self.active_ix,
            record,
        }
    }

    pub(crate) async fn get_record(&self, ix: Index) -> Option<Record> {
        if ix.segment == self.active_ix {
            self.active.get(ix.record).cloned()
        } else {
            match &self.pending {
                Some(pending) if ix.segment + 1 == self.active_ix => {
                    pending.get(ix.record).cloned()
                }
                _ => None,
            }
        }
    }

    pub(crate) async fn roll(&mut self, start: u128) -> bool {
        if let Some(time_range) = &self.time_range {
            dbg!(self.active_ix);
            let ready = self.writer.try_send(WriteRequest {
                segment: self.active_ix,
                start,
                time: time_range.clone(),
                records: self.active.clone()
            });

            if ready.is_ok() {
                self.pending = Some(std::mem::replace(&mut self.active, vec![]));
                self.active_size = 0;
                self.time_range = None;
                self.active_ix += 1;
                true
            } else {
                panic!("log overrun")
            }
        } else {
            panic!("cannot roll empty log");
        }
    }
}

fn spawn_slog_thread(
    root: PathBuf,
    name: String,
) -> (mpsc::Sender<WriteRequest>, SlogWrites) {
    let (tx, mut rx_records) = mpsc::channel(1);
    let (tx_done, rx) = mpsc::channel(1);

    std::thread::spawn(move || {
        let mut active = true;
        while active {
            match rx_records.blocking_recv() {
                Some(WriteRequest { segment, start, time, records }) => {
                    let mut writer = Slog::segment_from_name(&root, &name, segment).create();
                    let index = start..start + u128::try_from(records.len()).unwrap();
                    writer.log(records);
                    let size = usize::try_from(writer.close()).unwrap();
                    let response = WriteResult {
                        segment,
                        data: SegmentData {
                            index,
                            time,
                            size
                        }
                    };
                    tx_done
                        .blocking_send(response)
                        .expect("channel closed");
                }
                None => active = false,
            }
        }
    });

    (tx, rx)
}

mod test {
    use super::*;
    use parquet::data_type::ByteArray;
    use std::path::PathBuf;
    use std::time::SystemTime;
    use tempfile::tempdir;

    #[tokio::test]
    async fn basic_sequencing() {
        let root = tempdir().unwrap();
        let (slog, mut commits) =
            Slog::attach(PathBuf::from(root.path()), String::from("testing"), 0);
        let records: Vec<_> = vec!["abc", "def", "ghi"]
            .into_iter()
            .map(|message| Record {
                time: SystemTime::UNIX_EPOCH,
                message: ByteArray::from(message),
            })
            .collect();

        let abc = slog.append(&records[0]).await;
        assert_eq!(slog.get_record(abc.clone()).await, Some(records[0].clone()));
        slog.roll(0).await;
        assert_eq!(
            commits.recv().await.map(|r| (r.data.index, r.data.size > 0)),
            Some((0..1, true))
        );
        let def = slog.append(&records[1]).await;
        let ghi = slog.append(&records[2]).await;
        slog.roll(1).await;
        assert_eq!(
            commits.recv().await.map(|r| (r.data.index, r.data.size > 0)),
            Some((1..3, true))
        );

        assert_eq!(slog.get_record(abc).await, Some(records[0].clone()));
        assert_eq!(slog.get_record(def).await, Some(records[1].clone()));
        assert_eq!(slog.get_record(ghi).await, Some(records[2].clone()));
    }
}
