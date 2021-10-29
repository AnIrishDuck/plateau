plateau[^1] is a low-profile event and log aggregator

## Features

- Fast. Single-node throughput of up to 1M 128-byte records/s on common cloud
  hardware.
- Record grouping via "topic", write parallelism via "partition".
- Fixed-size per-topic retention policies with additional optional age-based
  expiration.
- Graceful failure under load via predictable load shedding.

## Architecture

plateau is divided into two high-level components: the manifest and segment storage.

### The Manifest

The manifest tracks high-level metadata about segments such as size, alongside
time and logical index spans.

The manifest tracks per-partition state of each topic. It optimizes record
queries by indexing segment spans so that it queries only need to read relevant
segments. It is also used for tracking and hitting retention targets.

Currently, the only supported manifest is a local SQLite database.

### Segment Storage

Segment storage is responsible for bulk batch storage of records. Records are
grouped into "segments" of variable size. Writes are optimized by writing
segment data in bulk. This aggregates writes and reduces manifest commits,
which is critical for high throughput.

The "segment log" (slog) is a sequence of segments. It provides an interface
to finalize segments at specified boundaries, cache and commit current segment
data, and roll out new segments.

A slog does not track any of its own state (e.g.  current segment index). The
partition data stored in the manifest is used for this purpose. Each partition
is backed by a single slog.

slog write parallelism is achieved with a background per-partition writer
thread. This also enables load shedding. If a background write is in progress
and the current segment is full, incoming records are rejected until the write
clears.

Currently, the only supported segment storage type is bulk indexed Parquet
files.

## Future Work

plateau will eventually have pluggable backends for the manifest and bulk
segment store. These could be selected transparently while using the same ingest
and query API.

plateau will also eventually support replication to bulk object storage (e.g.
S3). Other types of replication may be added as needed.

The current design uses per-partition threads for simplicity. A future switch
to an async I/O system could enable much higher partition counts.

plateau is currently only designed to run as a single node for speed and
simplicity. At some point, it may gain redundant bulk storage and HA
manifest plugins that will allow it to operate with high availability.

[^1]: plateau is named after the Plateau sawmill, which is by [some
estimations](https://www.sawmilldatabase.com/productiontoplist.php?country_id=10)
the largest log processor in Canada.