//! A segment contains a bundle of time and logically indexed records.
//!
//! Currently, the only supported segment format is local Parquet files.
use std::convert::{TryFrom, TryInto};
use std::{fs, path::PathBuf, sync::Arc, time::Duration, time::SystemTime};

use parquet::{
    column::reader::ColumnReader,
    column::writer::ColumnWriter,
    data_type::ByteArray,
    file::{
        properties::WriterProperties,
        reader::{FileReader, SerializedFileReader},
        writer::{FileWriter, SerializedFileWriter},
    },
    schema::parser::parse_message_type,
};

#[derive(Clone, Debug, PartialEq)]
pub struct Record {
    pub time: SystemTime,
    pub message: ByteArray,
}

pub struct Segment {
    path: PathBuf,
}

impl Segment {
    pub(crate) fn at(path: PathBuf) -> Segment {
        Segment { path }
    }

    pub(crate) fn path(&self) -> &PathBuf {
        &self.path
    }

    pub(crate) fn create(&self) -> SegmentWriter {
        // TODO: better schema for timestamps.
        // something like (TIMESTAMP(isAdjustedToUTC=true, unit=MILLIS));
        let message_type = "
        message schema {
            REQUIRED INT64 time;
            REQUIRED BYTE_ARRAY message (UTF8);
        }
        ";
        // TODO: fix all these unwraps()
        let schema = Arc::new(parse_message_type(message_type).unwrap());
        let props = Arc::new(WriterProperties::builder().build());

        let file = fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&self.path)
            .unwrap();
        let writer = SerializedFileWriter::new(file.try_clone().unwrap(), schema, props).unwrap();

        SegmentWriter {
            path: self.path.clone(),
            file,
            writer,
        }
    }

    pub(crate) fn read(&self) -> SegmentReader {
        let file = fs::File::open(&self.path).unwrap();
        let reader = SerializedFileReader::new(file).unwrap();
        SegmentReader { reader }
    }
}

pub(crate) struct SegmentWriter {
    path: PathBuf,
    file: fs::File,
    writer: SerializedFileWriter<std::fs::File>,
}

impl SegmentWriter {
    pub(crate) fn log(&mut self, mut record: Vec<Record>) {
        let mut row_group_writer = self.writer.next_row_group().unwrap();

        let mut times = vec![];
        let mut messages = vec![];
        for r in record.drain(..) {
            let dt = r.time.duration_since(SystemTime::UNIX_EPOCH).unwrap();
            times.push(dt.as_millis().try_into().unwrap());
            messages.push(r.message);
        }

        if let Some(mut col_writer) = row_group_writer.next_column().unwrap() {
            match col_writer {
                ColumnWriter::Int64ColumnWriter(ref mut typed_writer) => {
                    typed_writer.write_batch(&times, None, None).unwrap();
                }
                _ => panic!("invalid column type"),
            };
            row_group_writer.close_column(col_writer).unwrap();
        }

        if let Some(mut col_writer) = row_group_writer.next_column().unwrap() {
            match col_writer {
                ColumnWriter::ByteArrayColumnWriter(ref mut typed_writer) => {
                    typed_writer.write_batch(&messages, None, None).unwrap();
                }
                _ => panic!("invalid column type"),
            };
            row_group_writer.close_column(col_writer).unwrap();
        }
        self.writer.close_row_group(row_group_writer).unwrap();
    }

    pub(crate) fn close(mut self) -> u64 {
        self.writer.close().unwrap();
        self.file.sync_data().unwrap();
        fs::metadata(self.path).unwrap().len()
    }
}

pub(crate) struct SegmentReader {
    reader: SerializedFileReader<fs::File>,
}

impl SegmentReader {
    pub(crate) fn read_all(&self) -> Vec<Record> {
        let metadata = self.reader.metadata();

        let mut results = vec![];
        let mut def_levels = vec![0; 8];
        let mut rep_levels = vec![0; 8];

        for i in 0..metadata.num_row_groups() {
            let row_group_reader = self.reader.get_row_group(i).unwrap();
            let row_group_metadata = metadata.row_group(i);
            let rows = usize::try_from(row_group_metadata.num_rows()).unwrap();
            let mut tvs = vec![0; rows];
            let mut arrs: Vec<ByteArray> = vec![ByteArray::from(vec![0; 1024])]
                .into_iter()
                .cycle()
                .take(rows)
                .collect();

            let mut column_reader = row_group_reader.get_column_reader(0).unwrap();
            match column_reader {
                ColumnReader::Int64ColumnReader(ref mut typed_reader) => {
                    typed_reader
                        .read_batch(
                            rows, // batch size
                            Some(&mut def_levels),
                            Some(&mut rep_levels),
                            &mut tvs,
                        )
                        .expect("batch read");
                }
                _ => panic!("invalid column type"),
            };

            let mut column_reader = row_group_reader.get_column_reader(1).unwrap();
            match column_reader {
                ColumnReader::ByteArrayColumnReader(ref mut typed_reader) => {
                    // TODO: arbitrary message sizes
                    typed_reader
                        .read_batch(
                            rows,
                            Some(&mut def_levels),
                            Some(&mut rep_levels),
                            &mut arrs,
                        )
                        .expect("batch read");
                }
                _ => panic!("invalid column type"),
            };

            results.extend(tvs.into_iter().zip(arrs.into_iter()).map(|(tv, message)| {
                let time = SystemTime::UNIX_EPOCH + Duration::from_millis(tv.try_into().unwrap());
                Record { time, message }
            }));
        }

        results
    }
}

mod test {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn round_trip() {
        let root = tempdir().unwrap();
        let path = root.path().join("testing.parquet");
        let s = Segment::at(PathBuf::from(path));
        let records: Vec<_> = vec!["abc", "def", "ghi"]
            .into_iter()
            .map(|message| Record {
                time: SystemTime::UNIX_EPOCH,
                message: ByteArray::from(message),
            })
            .collect();

        let mut w = s.create();
        for r in records.iter() {
            w.log(vec![r.clone()]);
        }
        w.close();

        let r = s.read();
        assert_eq!(r.read_all(), records);
    }
}
