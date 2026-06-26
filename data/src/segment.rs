//! A segment contains a bundle of time and logically indexed rows.
//!
//! Additionally, a segment keeps an "active chunk" cache to avoid chunk
//! fragmentation in low write frequency workloads. This cache persists the
//! "current" non-full chunk in the segment. New rows may be appended to this
//! cache via [SegmentWriter2::update_cache] until a full row group is written
//! via [SegmentWriter2::log_arrow].
//!
//! At that point, the cache is discarded and a new empty active chunk cache is
//! opened for the next chunk in the file.
//!
//! `{segment}.arrows` is the file that records the active chunk cache. See
//! [cache] for more information about the contents of this file.
//!
//! For caching and crash recovery, each segment file may have a variety of
//! other associated files. See [arrow] and [parquet] for details on these
//! additional files.

use std::io::Read;
use std::{fs, path::Path, path::PathBuf};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use tracing::{error, trace, warn};

// Use arrow-rs Schema instead of arrow2 Schema
use arrow_schema::Schema;
use plateau_transport::SegmentChunk;
#[allow(dead_code)]
mod arrow;
mod cache;
// Commented out parquet module as part of arrow-rs migration
// mod parquet;

const PLATEAU_HEADER: &str = "plateau1";

/// Remove a file, tolerating the case where it has already been removed.
///
/// Retention and (eventually) reconciliation can both delete the same segment
/// file, so the loser of that race finds it already gone. A missing file is
/// logged at `warn` and treated as success; every other error (permissions,
/// I/O, ...) propagates.
fn remove_file_if_present(path: &Path) -> Result<()> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            warn!("attempted to remove missing file {:?}", path);
            Ok(())
        }
        other => other.context(format!("removing {path:?}")),
    }
}

fn validate_header(mut reader: impl Read) -> Result<()> {
    let mut buffer = [0u8; 8];
    reader.read_exact(&mut buffer)?;
    if std::str::from_utf8(&buffer)? != PLATEAU_HEADER {
        anyhow::bail!("invalid checkpoint header");
    }

    Ok(())
}

/// This is currently a placeholder for future segment storage settings (e.g.
/// compression)
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Config {
    durable_checkpoints: bool,
    arrow: bool,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            durable_checkpoints: true,
            arrow: true,
        }
    }
}

pub trait SegmentIterator: DoubleEndedIterator<Item = Result<SegmentChunk>> {
    fn schema(&self) -> &Schema;
}

#[derive(Clone, Debug)]
pub struct Segment {
    path: PathBuf,
}

impl Segment {
    pub fn at(path: PathBuf) -> Self {
        Self { path }
    }

    pub fn path(&self) -> &PathBuf {
        &self.path
    }

    fn file(&self) -> Result<fs::File> {
        if self.path.exists() {
            warn!("truncating extant segment file at {}", self.path.display());
        }

        Ok(fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&self.path)?)
    }

    pub fn create(&self, schema: Schema, config: Config) -> Result<Writer> {
        let file = self.file()?;
        let writer = if config.arrow {
            WriteFormat::Arrow(arrow::Writer::create(file, &schema)?)
        } else {
            anyhow::bail!("parquet format is no longer supported");
        };

        let cache = cache::ActiveChunk::new(self.cache_path());
        Ok(Writer {
            segment: self.clone(),
            writer,
            schema,
            chunk_ix: 0,
            cache,
        })
    }

    pub fn parts(&self) -> impl Iterator<Item = PathBuf> {
        let arrow_parts = arrow::Segment::new(self.path.clone())
            .map(|s| s.parts())
            .inspect_err(|e| error!("error enumerating parquet parts for {:?}, {e:?}", self.path))
            .ok();

        arrow_parts.into_iter().flatten()
    }

    pub fn destroy(&self) -> Result<()> {
        // The main segment file is expected to exist; a missing one is worth a
        // warning (e.g. a concurrent remover beat us to it) but not an error.
        remove_file_if_present(&self.path)?;

        // Parts and the active-chunk cache are auxiliary and routinely absent,
        // so only attempt the ones present. Routing through the tolerant helper
        // still lets a remove that loses a race no-op while propagating any
        // genuine I/O error (previously these were swallowed).
        for part in self.parts().filter(|p| p.exists()) {
            remove_file_if_present(&part)?;
        }

        if self.cache_path().exists() {
            remove_file_if_present(&self.cache_path())?;
        }

        Ok(())
    }

    pub fn validate(&self) -> bool {
        match self.iter() {
            Ok(_) => true,
            Err(err) => {
                warn!("error validating segment: {err:?}");
                false
            }
        }
    }

    pub fn iter(&self) -> Result<impl SegmentIterator> {
        let cache = cache::read(self.cache_path()).unwrap_or_else(|err| {
            error!("error reading cache at {:?}: {err:?}", self.cache_path());
            None
        });

        if self.path.exists() {
            trace!(
                "found segment file {:?}, cache: {}",
                self.path,
                cache.is_some()
            );
            let mut file = fs::File::open(&self.path)?;

            // Check for a header
            let parquet: Result<bool> = Ok(false); // parquet::check_file(&mut file);
            let arrow = arrow::check_file(&mut file);
            if let (Ok(parquet), Ok(arrow)) = (parquet, arrow) {
                return if parquet {
                    trace!("{:?} in parquet format", self.path);
                    anyhow::bail!("parquet format is no longer supported");
                } else if arrow {
                    trace!("{:?} in arrow format", self.path);
                    let segment = arrow::Segment::new(self.path.clone())?;
                    Ok(ReadFormat::Arrow(segment.read(cache)?))
                } else {
                    anyhow::bail!("unable to detect file format for segment {:?}", self.path)
                };
            }

            trace!("empty segment file {:?}", self.path);
        }

        if let Some(data) = cache {
            trace!("only cache file present");
            anyhow::ensure!(
                data.chunk_ix == 0,
                "cache file requires segment {:?} that is not present",
                self.path
            );
            Ok(ReadFormat::OnlyCache(
                data.rows.schema,
                std::iter::once(Ok(data.rows.chunk)),
            ))
        } else {
            anyhow::bail!("no segment file or cache data for {:?}", self.path)
        }
    }

    pub fn cache_path(&self) -> PathBuf {
        let mut path: PathBuf = self.path.clone();
        assert!(path.set_extension("arrows"));
        path
    }

    /// Return an estimate of the on-disk size of the corresponding file(s),
    /// including the active chunk cache if present.
    pub fn size_estimate(&self) -> Result<usize> {
        let main_size = fs::metadata(&self.path).map(|p| p.len()).unwrap_or(0);
        let part_size: u64 = self
            .parts()
            .map(|part| fs::metadata(part).map(|p| p.len()).unwrap_or(0))
            .sum();
        let cache_size = fs::metadata(self.cache_path()).map(|p| p.len()).unwrap_or(0);
        Ok(usize::try_from(main_size + part_size + cache_size)?)
    }
}

enum ReadFormat {
    Arrow(arrow::Reader),
    OnlyCache(Schema, std::iter::Once<Result<SegmentChunk>>),
}

impl Iterator for ReadFormat {
    type Item = Result<SegmentChunk>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Arrow(a) => a.next(),
            Self::OnlyCache(_, c) => c.next(),
        }
    }
}

impl DoubleEndedIterator for ReadFormat {
    fn next_back(&mut self) -> Option<Self::Item> {
        match self {
            Self::Arrow(a) => a.next_back(),
            Self::OnlyCache(_, c) => c.next_back(),
        }
    }
}

impl SegmentIterator for ReadFormat {
    fn schema(&self) -> &Schema {
        match self {
            Self::Arrow(a) => a.schema(),
            Self::OnlyCache(schema, _) => schema,
        }
    }
}

#[allow(clippy::large_enum_variant)]
enum WriteFormat {
    Arrow(arrow::Writer),
}

#[allow(missing_debug_implementations)]
pub struct Writer {
    segment: Segment,
    writer: WriteFormat,
    schema: Schema,
    chunk_ix: u32,

    cache: cache::ActiveChunk,
}

impl Writer {
    pub fn check_schema(&self, schema: &Schema) -> bool {
        &self.schema == schema
    }

    fn write_chunk(&mut self, chunk: SegmentChunk) -> Result<()> {
        match &mut self.writer {
            WriteFormat::Arrow(a) => a.write_chunk(chunk),
        }
    }

    /// Log a combination of full and active chunks to this segment.
    ///
    /// This operation is append-only. All full chunks are appended as-is onto
    /// the underlying segment file.
    ///
    /// The "active" chunk is also considered append-only. If no full chunks
    /// are present, all rows in the active chunk after the end of cache are
    /// appended onto the cache.
    ///
    /// All rows currently in the cache are assumed to be equivalent to their
    /// same-index counterparts in the "new" active chunk.
    ///
    /// When full chunks are present, the cache is reset, as the active chunk
    /// is always considered to be the last chunk in the file.
    pub fn log_arrows(
        &mut self,
        schema: &Schema,
        full: Vec<SegmentChunk>,
        active: Option<SegmentChunk>,
    ) -> Result<()> {
        anyhow::ensure!(
            self.check_schema(schema),
            "cannot use different schemas within the same segment"
        );

        let chunk_count = full.len();
        for chunk in full {
            self.write_chunk(chunk)?;
        }

        if chunk_count > 0 {
            self.chunk_ix += chunk_count as u32;
            self.cache.clear();
        }

        if let Some(active) = active {
            self.cache.update(self.chunk_ix, schema, active)?;
        }

        self.write_checkpoint()?;

        Ok(())
    }

    fn write_checkpoint(&self) -> Result<()> {
        // First, sync the segment file itself. The cache will not be valid if the
        // chunks that precede it are missing.
        match &self.writer {
            WriteFormat::Arrow(a) => a.checkpoint()?,
        }

        // Then, we can sync the active chunk cache
        self.cache.sync()?;

        Ok(())
    }

    fn get_path(&self) -> &Path {
        &self.segment.path
    }

    pub fn end(mut self) -> Result<usize> {
        if let Some(rows) = self.cache.take() {
            self.write_chunk(rows.chunk)?;
        }

        // NOTE: it is critical that the writer syncs the file as part of the
        // end operation, otherwise the data in cache may be lost in recovery
        // scenarios.
        let segment = self.segment;
        match self.writer {
            WriteFormat::Arrow(a) => a.end()?,
        }

        self.cache.destroy()?;

        segment.size_estimate()
    }

    /// Return an estimate of the on-disk size of the corresponding file(s).
    pub fn size_estimate(&self) -> Result<usize> {
        self.segment.size_estimate()
    }

    pub fn close(self) -> Result<usize> {
        let mut parent = self.get_path().to_path_buf();
        let size = self.end()?;

        // NOTE: the file data is now synchronized, but the file itself may not appear in the
        // parent directory on crash unless we fsync that too.
        parent.pop();
        let directory = fs::File::open(&parent)?;
        directory.sync_all()?;

        Ok(size)
    }
}

#[cfg(test)]
pub mod test {
    use std::borrow::Borrow;

    use super::*;
    use crate::test::inferences_schema_a;
    // Use arrow-rs transport
    use plateau_transport as transport;
    use sample_arrow_rs::{
        array::ArbitraryArray,
        chunk::ArbitraryChunk,
        datatypes::{sample_flat, ArbitraryDataType},
    };
    use sample_std::{Chance, Regex};
    use tempfile::tempdir;
    use test::arrow::test::partial_write;
    use transport::SchemaChunk;

    impl Config {
        pub fn nocommit() -> Self {
            Self {
                durable_checkpoints: false,
                arrow: false,
            }
        }

        pub fn parquet() -> Self {
            Self {
                arrow: false,
                ..Self::default()
            }
        }

        pub fn arrow() -> Self {
            Self {
                arrow: true,
                ..Self::default()
            }
        }
    }

    impl Writer {
        pub fn log_arrow<S: Borrow<Schema> + Clone + PartialEq>(
            &mut self,
            data: SchemaChunk<S>,
            active: Option<SegmentChunk>,
        ) -> Result<()> {
            self.log_arrows(data.schema.borrow(), vec![data.chunk], active)
        }

        pub fn update_cache(&mut self, active: SegmentChunk) -> Result<()> {
            self.cache.update(self.chunk_ix, &self.schema, active)
        }
    }

    // nulls=true breaks arrow2's parquet support, but is fine for feather
    pub fn deep_chunk(depth: usize, len: usize, nulls: bool) -> ArbitraryChunk<Regex, Chance> {
        let names = Regex::new("[a-z]{4,8}");
        let data_type = ArbitraryDataType {
            struct_branch: 1..3,
            names: names.clone(),
            nullable: if nulls { Chance(0.5) } else { Chance(0.0) },
            flat: sample_flat,
        }
        .sample_depth(depth);

        let array = ArbitraryArray {
            names,
            branch: 0..10,
            len: len..(len + 1),
            null: Chance(0.1),
            // this appears to break arrow2's parquet support
            // is_nullable: true,
            is_nullable: false,
        };

        ArbitraryChunk {
            chunk_len: 10..1000,
            array_count: 1..2,
            data_type,
            array,
        }
    }

    #[test]
    fn test_interrupted_cache_write() -> Result<()> {
        let root = tempdir()?;
        let path = root.path().join("partial-write.parquet");
        let s = Segment::at(path.clone());

        let a = inferences_schema_a();
        let mut w = s.create(a.schema.clone(), Config::default())?;
        w.log_arrow(a.clone(), Some(a.chunk.clone()))?;
        drop(w);

        let f = fs::File::options().append(true).open(s.cache_path())?;
        f.set_len(f.metadata()?.len() - 15)?;

        let mut r = s.iter()?;
        assert_eq!(r.next().map(|v| v.unwrap()), Some(a.chunk));
        assert_eq!(r.next().map(|v| v.ok()), None);

        Ok(())
    }

    #[test]
    fn test_partial_cache_write() -> Result<()> {
        let root = tempdir()?;
        let path = root.path().join("partial-write.parquet");
        let s = Segment::at(path.clone());

        let a = inferences_schema_a();
        let mut w = s.create(a.schema.clone(), Config::default())?;
        w.log_arrow(a.clone(), Some(a.chunk.clone()))?;

        let more = crate::chunk::concatenate(&[a.chunk.clone(), a.chunk.clone()])?;
        w.log_arrows(&a.schema, vec![], Some(more))?;
        drop(w);

        let f = fs::File::options().append(true).open(s.cache_path())?;
        f.set_len(f.metadata()?.len() - 15)?;

        let mut r = s.iter()?;
        assert_eq!(r.next().map(|v| v.unwrap()), Some(a.chunk.clone()));
        assert_eq!(r.next().map(|v| v.unwrap()), Some(a.chunk));
        assert_eq!(r.next().map(|v| v.ok()), None);

        Ok(())
    }

    #[test]
    fn test_arrow_with_truncated_cache() -> Result<()> {
        let root = tempdir()?;
        let path = root.path().join("partial-write.arrow");
        let s = Segment::at(path.clone());

        let a = inferences_schema_a();
        let mut w = s.create(a.schema.clone(), Config::arrow())?;
        w.log_arrow(a.clone(), Some(a.chunk.clone()))?;
        w.log_arrow(a.clone(), Some(a.chunk.clone()))?;

        let more = crate::chunk::concatenate(&[a.chunk.clone(), a.chunk.clone()])?;
        w.log_arrows(&a.schema, vec![], Some(more))?;
        drop(w);

        let f = fs::File::options().append(true).open(s.cache_path())?;
        f.set_len(f.metadata()?.len() - 15)?;

        let mut r = s.iter()?;
        // two chunks from file, one from cache (the other will have its frame
        // interrupted by above corruption)
        assert_eq!(r.next().map(|v| v.unwrap()), Some(a.chunk.clone()));
        assert_eq!(r.next().map(|v| v.unwrap()), Some(a.chunk.clone()));
        assert_eq!(r.next().map(|v| v.unwrap()), Some(a.chunk.clone()));
        assert_eq!(r.next().map(|v| v.ok()), None);

        Ok(())
    }

    #[test]
    fn test_arrow_corruption_with_cache_write() -> Result<()> {
        let root = tempdir()?;
        let path = root.path().join("partial-write.arrow");
        let s = Segment::at(path.clone());

        let a = inferences_schema_a();
        let mut w = s.create(a.schema.clone(), Config::arrow())?;
        w.log_arrow(a.clone(), Some(a.chunk.clone()))?;
        w.log_arrow(a.clone(), Some(a.chunk.clone()))?;

        let more = crate::chunk::concatenate(&[a.chunk.clone(), a.chunk.clone()])?;
        w.log_arrows(&a.schema, vec![], Some(more))?;
        drop(w);

        let f = fs::File::options().append(true).open(s.path())?;
        f.set_len(f.metadata()?.len() - 15)?;

        let mut r = s.iter()?;
        assert_eq!(r.next().map(|v| v.unwrap()), Some(a.chunk.clone()));
        // we need to discard the whole cache because of the gap created above
        assert_eq!(r.next().map(|v| v.ok()), None);

        Ok(())
    }

    #[test]
    fn test_dual_format() -> Result<()> {
        let root = tempdir()?;
        let parquet = Segment::at(root.path().join("test.parquet"));
        let arrow = Segment::at(root.path().join("test.arrow"));

        let a = inferences_schema_a();

        let mut w = parquet.create(a.schema.clone(), Config::default())?;
        w.log_arrow(a.clone(), Some(a.chunk.clone()))?;
        w.end()?;

        let mut w = arrow.create(a.schema.clone(), Config::arrow())?;
        w.log_arrow(a.clone(), Some(a.chunk.clone()))?;
        w.end()?;

        // verify we don't need to provide the format here, it's autodetected
        let mut r = parquet.iter()?;
        assert_eq!(r.next().map(|v| v.unwrap()), Some(a.chunk.clone()));
        assert_eq!(r.next().map(|v| v.unwrap()), Some(a.chunk.clone()));
        assert_eq!(r.next().map(|v| v.ok()), None);

        let mut r = arrow.iter()?;
        assert_eq!(r.next().map(|v| v.unwrap()), Some(a.chunk.clone()));
        assert_eq!(r.next().map(|v| v.unwrap()), Some(a.chunk));
        assert_eq!(r.next().map(|v| v.ok()), None);

        Ok(())
    }

    #[test]
    fn test_arrow_cache_updates() -> Result<()> {
        let root = tempdir()?;

        let a = inferences_schema_a();

        let all_counts = [1, 3, 4, 2, 1];
        for ix in 1..all_counts.len() {
            trace!("iter: {ix} counts: 1 + {:?}", &all_counts[0..ix]);
            let mut chunk = a.chunk.clone();

            let path = root.path().join(format!("{ix:?}.arrow"));
            let s = Segment::at(path.clone());
            let mut w = s.create(a.schema.clone(), Config::arrow())?;

            for count in &all_counts[0..ix] {
                let new_parts: Vec<_> = std::iter::once(chunk.clone())
                    .chain(std::iter::repeat_n(a.chunk.clone(), *count))
                    .collect();
                chunk = crate::chunk::concatenate(&new_parts)?;
                w.update_cache(chunk.clone())?;
            }

            drop(w);

            let mut r = s.iter()?;
            assert_eq!(r.next().map(|v| v.unwrap()), Some(chunk));
        }

        Ok(())
    }

    #[test]
    fn test_partial_write_size_destroy() -> Result<()> {
        let root = tempdir()?;
        let a = inferences_schema_a();
        let arrow_segment = partial_write(root.path(), a.clone())?;

        let paths: Vec<_> = arrow_segment.clone().parts().into_iter().collect();
        let segment = Segment::at(arrow_segment.into_path());

        segment.iter()?.count();

        assert!(segment.size_estimate()? > fs::metadata(&segment.path)?.len() as usize);
        segment.destroy()?;

        for path in paths {
            assert!(!path.exists());
        }

        Ok(())
    }
}
