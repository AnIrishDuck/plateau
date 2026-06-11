//! Cache file format for the "active" chunk.
//!
//! On disk, the file is prefixed with a fixed version identifier and the active
//! chunk index. After this index, arrow chunks are appended in *streaming* IPC
//! format, not the typical v2 feather format. The additional footer required by
//! the feather format would add overhead and create headaches during crash
//! recovery.
use std::{
    fs,
    io::{Read, Write},
    mem,
    path::{Path, PathBuf},
    sync::Arc,
    time::Instant,
};

use anyhow::Result;
use arrow_ipc::{
    reader::StreamReader,
    writer::{IpcWriteOptions, StreamWriter},
};
use arrow_schema::Schema;
use plateau_transport::{SchemaChunk, SegmentChunk};
use tracing::{debug, error, trace, warn};

use super::{validate_header, PLATEAU_HEADER};
use crate::chunk::RecordBatchExt;

pub struct ActiveChunk {
    path: PathBuf,
    writer: Option<Writer>,
}

impl ActiveChunk {
    pub fn new(path: PathBuf) -> Self {
        Self { path, writer: None }
    }

    pub fn clear(&mut self) {
        self.writer = None;
    }

    pub fn take(&mut self) -> Option<SchemaChunk<Schema>> {
        mem::take(&mut self.writer).map(|w| w.into_inner())
    }

    pub fn update(&mut self, chunk_ix: u32, schema: &Schema, chunk: SegmentChunk) -> Result<()> {
        match &mut self.writer {
            Some(cache) => cache.update(schema, chunk)?,
            None => {
                self.writer = Some(Writer::new(
                    self.path.clone(),
                    SchemaChunk {
                        schema: schema.clone(),
                        chunk,
                    },
                    chunk_ix,
                )?);
            }
        }

        Ok(())
    }

    pub fn sync(&self) -> Result<()> {
        if let Some(writer) = &self.writer {
            writer.sync()?;
        }

        Ok(())
    }

    pub fn destroy(&mut self) -> Result<()> {
        // The active-chunk cache is routinely absent (it is discarded as chunks
        // fill), so only attempt to remove it when present. The tolerant helper
        // still no-ops on a lost race and propagates any genuine I/O error.
        if Path::exists(&self.path) {
            super::remove_file_if_present(&self.path)?;
        }
        self.writer = None;

        Ok(())
    }

    pub fn size(&self) -> u64 {
        self.writer.as_ref().map_or(0, |w| w.size())
    }
}

pub struct Writer {
    path: PathBuf,
    file: fs::File,

    writer: StreamWriter<fs::File>,
    partial: SchemaChunk<Schema>,
}

impl Writer {
    pub fn new(path: PathBuf, initial: SchemaChunk<Schema>, chunk_ix: u32) -> Result<Self> {
        trace!("start {:?} with {}", path, initial.chunk.len());
        let mut file = fs::File::create(&path)?;
        file.write_all(PLATEAU_HEADER.as_bytes())?;

        let buffer = chunk_ix.to_le_bytes();
        file.write_all(&buffer)?;

        let _options = IpcWriteOptions::default();
        let schema_ref = Arc::new(initial.schema.clone());
        let mut writer = StreamWriter::try_new(file.try_clone()?, &schema_ref)?;
        writer.write(&initial.chunk.to_record_batch(&initial.schema)?)?;

        Ok(Self {
            path,
            file,
            writer,
            partial: initial,
        })
    }

    fn len(&self) -> usize {
        self.partial.chunk.len()
    }

    pub(super) fn update(&mut self, schema: &Schema, chunk: SegmentChunk) -> Result<()> {
        if self.len() == chunk.len() {
            return Ok(());
        }

        anyhow::ensure!(schema.fields() == self.partial.schema.fields());

        let new_len = chunk.len();
        let additional = crate::chunk::slice(chunk.clone(), self.len(), new_len - self.len());
        trace!("{} => {}", self.len(), chunk.len());
        self.writer
            .write(&additional.to_record_batch(&self.partial.schema)?)?;
        self.partial.chunk = chunk;

        Ok(())
    }

    pub(super) fn sync(&self) -> Result<()> {
        let now = Instant::now();
        self.file.sync_data()?;
        debug!("cache sync elapsed: {:?}", now.elapsed());

        Ok(())
    }

    pub(super) fn into_inner(self) -> SchemaChunk<Schema> {
        self.partial
    }

    pub(super) fn size(&self) -> u64 {
        fs::metadata(&self.path).map(|p| p.len()).unwrap_or(0)
    }
}

pub struct Data {
    pub chunk_ix: u32,
    pub rows: SchemaChunk<Schema>,
}

pub fn read(path: PathBuf) -> Result<Option<Data>> {
    if path.exists() {
        let mut reader = fs::File::open(&path)?;
        validate_header(&mut reader)?;

        let mut buffer = [0; 4];
        reader.read_exact(&mut buffer)?;
        let chunk_ix = u32::from_le_bytes(buffer);

        // Create stream reader with error handling
        let stream_reader = match StreamReader::try_new(&mut reader, None) {
            Ok(reader) => reader,
            Err(e) => {
                error!(
                    "Error creating stream reader for cache file {:?}: {}",
                    path, e
                );
                return Ok(None);
            }
        };
        let schema = stream_reader.schema().clone();

        let mut chunks = Vec::new();
        for batch_result in stream_reader {
            match batch_result {
                Ok(batch) => {
                    chunks.push(SegmentChunk::from_record_batch(batch));
                }
                Err(e) => {
                    error!("error reading cache segment: {:?}", e);
                }
            }
        }

        let concat = if chunks.is_empty() {
            // This can really only happen if plateau / the system crashes while
            // writing the first chunk to the stream. That seems unlikely enough
            // to warn about.
            warn!("could not read any chunks from cache at {:?}", path);
            return Ok(None);
        } else {
            let chunk = crate::chunk::concatenate(&chunks)?;
            trace!(
                "read {} rows ({} chunks) from cache",
                chunk.len(),
                chunks.len()
            );
            chunk
        };

        Ok(Some(Data {
            chunk_ix,
            rows: SchemaChunk {
                schema: schema.as_ref().clone(),
                chunk: concat,
            },
        }))
    } else {
        Ok(None)
    }
}
