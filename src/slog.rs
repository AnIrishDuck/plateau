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
use crate::segment::{Record, Segment, SegmentWriter};
use serde::{Deserialize, Serialize};
use std::cmp::{max, min};
use std::fs;
use std::ops::RangeInclusive;
use std::path::PathBuf;
use std::sync::{mpsc, Mutex};
use std::thread::{spawn, JoinHandle};
use std::time::{Duration, SystemTime};

/// A slog (segment log) is a named and ordered series of segments.
pub(crate) struct Slog {
    root: PathBuf,
    name: String,
    current: usize,
    pending: Vec<Record>,
    pending_size: usize,
    time_range: Option<RangeInclusive<SystemTime>>,
    writer: SlogThreadControl,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct Index {
    pub segment: usize,
    pub record: usize,
}

impl Slog {
    pub fn attach(root: PathBuf, name: String, current: usize) -> Self {
        let writer = SlogThread::spawn(root.clone(), name.clone(), current);
        Slog {
            root,
            name,
            current,
            writer,
            pending: vec![],
            pending_size: 0,
            time_range: None,
        }
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

    pub(crate) fn get_record(&self, ix: Index) -> Option<Record> {
        assert!(ix.segment <= self.current);
        if ix.segment == self.current {
            self.pending.get(ix.record).cloned()
        } else {
            self.get_segment(ix.segment)
                .read()
                .read_all()
                .get(ix.record)
                .cloned()
        }
    }

    pub(crate) fn append(&mut self, r: &Record) -> Index {
        let record = self.pending.len();
        self.pending_size += r.message.len();
        self.pending.push(r.clone());
        self.time_range = self
            .time_range
            .clone()
            .map(|range| (min(*range.start(), r.time)..=max(*range.end(), r.time)))
            .or(Some(r.time..=r.time));
        Index {
            segment: self.current,
            record,
        }
    }

    pub(crate) fn destroy(&self, segment_ix: usize) {
        fs::remove_file(Slog::segment_path(&self.root, &self.name, segment_ix)).unwrap()
    }

    pub(crate) fn current_segment_ix(&self) -> usize {
        self.current
    }

    pub(crate) fn current_len(&self) -> usize {
        self.pending.len()
    }

    pub(crate) fn current_size(&self) -> usize {
        self.pending_size
    }

    pub(crate) fn current_time_range(&self) -> Option<RangeInclusive<SystemTime>> {
        self.time_range.clone()
    }

    pub(crate) fn roll(&mut self) -> Option<(usize, u64)> {
        let (ready, data) = self
            .writer
            .try_send(std::mem::replace(&mut self.pending, vec![]));
        if ready {
            self.pending_size = 0;
            self.time_range = None;
            self.current += 1;
            data
        } else {
            panic!("log overrun")
        }
    }

    pub(crate) fn commit(&mut self) -> Option<(usize, u64)> {
        self.writer.commit()
    }
}

struct SlogThread {
    writer: SegmentWriter,
}

enum SlogThreadMessage {
    Write(Vec<Record>),
    Close,
}

struct SlogThreadControl {
    write_handle: JoinHandle<()>,
    ready: bool,
    tx: Mutex<mpsc::Sender<SlogThreadMessage>>,
    rx: Mutex<mpsc::Receiver<(usize, u64)>>,
}

impl SlogThreadControl {
    fn try_send(&mut self, rs: Vec<Record>) -> (bool, Option<(usize, u64)>) {
        let mut size_data = None;
        if !self.ready {
            match self
                .rx
                .lock()
                .expect("rx lock")
                .recv_timeout(Duration::from_millis(1000))
            {
                Ok(data) => {
                    self.ready = true;
                    size_data = Some(data)
                }
                Err(_) => return (false, None),
            }
        }
        self.tx
            .lock()
            .expect("tx lock")
            .send(SlogThreadMessage::Write(rs))
            .expect("sending record batch");
        self.ready = false;
        (true, size_data)
    }

    fn commit(&mut self) -> Option<(usize, u64)> {
        if !self.ready {
            let data = self.rx.lock().expect("rx lock").recv().unwrap();
            self.ready = true;
            Some(data)
        } else {
            None
        }
    }
}

impl Drop for SlogThreadControl {
    fn drop(&mut self) {
        self.tx
            .lock()
            .expect("tx lock")
            .send(SlogThreadMessage::Close)
            .expect("send writer thread shutdown");
    }
}

impl SlogThread {
    fn spawn(root: PathBuf, name: String, mut current: usize) -> SlogThreadControl {
        let (tx, rx_records) = mpsc::channel();
        let (tx_done, rx) = mpsc::channel();

        let write_handle = spawn(move || {
            let mut active = true;
            while active {
                let mut segment = Slog::segment_from_name(&root, &name, current).create();
                match rx_records.recv().unwrap() {
                    SlogThreadMessage::Write(rs) => {
                        segment.log(rs);
                        let size = segment.close();
                        tx_done.send((current, size)).unwrap();
                        current += 1;
                    }
                    SlogThreadMessage::Close => active = false,
                }
            }
        });

        SlogThreadControl {
            write_handle,
            ready: true,
            tx: Mutex::new(tx),
            rx: Mutex::new(rx),
        }
    }
}

mod test {
    use super::*;
    use parquet::data_type::ByteArray;
    use std::path::PathBuf;
    use std::time::SystemTime;
    use tempfile::tempdir;

    #[test]
    fn basic_sequencing() {
        let root = tempdir().unwrap();
        let mut slog = Slog::attach(PathBuf::from(root.path()), String::from("testing"), 0);
        let records: Vec<_> = vec!["abc", "def", "ghi"]
            .into_iter()
            .map(|message| Record {
                time: SystemTime::UNIX_EPOCH,
                message: ByteArray::from(message),
            })
            .collect();

        let abc = slog.append(&records[0]);
        assert_eq!(slog.get_record(abc.clone()), Some(records[0].clone()));
        slog.roll();
        assert_eq!(
            slog.commit().map(|(ix, size)| (ix, size > 0)),
            Some((0, true))
        );
        let def = slog.append(&records[1]);
        let ghi = slog.append(&records[2]);
        slog.roll();
        assert_eq!(
            slog.commit().map(|(ix, size)| (ix, size > 0)),
            Some((1, true))
        );
        assert_eq!(slog.commit(), None);

        assert_eq!(slog.get_record(abc), Some(records[0].clone()));
        assert_eq!(slog.get_record(def), Some(records[1].clone()));
        assert_eq!(slog.get_record(ghi), Some(records[2].clone()));
    }
}
