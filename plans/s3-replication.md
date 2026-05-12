# S3 Replication of Segments — Implementation Plan

## Goal

Asynchronously replicate finalized segment files to S3 (and S3-compatible
object stores) so that local storage can be treated as a cache while bulk
storage lives in durable object storage. This is a one-way mirror of
finalized segments only — active/pending segments stay local.

## Constraints & shape of the problem

- **Source of truth is the manifest.** All segment lifecycle decisions
  (finalize, retain, delete) flow through `catalog/src/manifest.rs` and
  `catalog/src/partition.rs`. Replication state must live in the manifest
  so it survives restarts and is queryable alongside `segments`.
- **Only finalized segments are replicable.** A segment is "finalized"
  once the slog writer thread has called `close()` and the manifest row
  has its `size` populated (`catalog/src/slog.rs:719-725`,
  `catalog/src/manifest.rs:246-282`). The active `.arrows` cache must
  never be uploaded.
- **Retention can delete local files.** `Catalog::retain()` →
  `Partition::remove_oldest()` → `manifest.remove_segment()` →
  `Slog::destroy()`. Replication must either complete before deletion or
  block deletion of unreplicated segments (configurable).
- **There is no existing object store dependency.** Inter-host
  replication (`server/src/replication.rs`, `arrow-rs/client/src/
  replicate.rs`) is HTTP record-shipping, not segment-file copying, and
  isn't a usable base.
- **Segment files have side-cars.** Arrow segments may have a
  `.feather` part; Parquet segments have `.parquet` side-cars. Whatever
  we upload must round-trip back to a working segment on disk if we ever
  want to restore. v1 can ignore restore and only target durability.

## Out of scope (for this plan)

- Reading segments back from S3 to serve queries (cache miss path).
- Restoring a node from S3 alone.
- Replicating the manifest itself.
- Cross-region or multi-bucket fan-out.
- GCS/Azure (the abstraction should permit it, but only S3 ships).

---

## Work chunks

Each chunk is sized to be a single reviewable PR (~200–600 lines). They
are ordered so that each one is independently shippable and the system
keeps working with the feature disabled at every step.

### Chunk 1 — Object store abstraction crate

**Scope:** Add a thin trait + S3 implementation, no wiring.

- New crate `object-store` (or feel free to use the `object_store` crate
  from arrow-rs directly — evaluate in this PR; if we depend on it,
  this chunk becomes just a thin wrapper + config).
- Trait surface (minimum):
  - `put(key, reader, size, content_type) -> Result<()>`
  - `head(key) -> Result<Option<ObjectMeta>>` (size, etag)
  - `delete(key) -> Result<()>`
  - `list(prefix) -> Stream<ObjectMeta>` (only used by ops tooling in
    later chunks; can be stubbed if we use `object_store`)
- S3 backend with config: `endpoint`, `region`, `bucket`, `prefix`,
  credentials (env / IMDS / static), `path_style`, request timeout,
  multipart threshold.
- Unit tests against a mock (e.g. `aws-sdk-s3` test client) or
  `minio`-in-docker behind a `#[ignore]` integration test.

**Definition of done:** `cargo test -p object-store` green;
no other crate depends on it yet.

### Chunk 2 — Manifest schema for replication state

**Scope:** Schema only, no behavior.

- New migration in `catalog/migrations/` (next sequence number after
  `20240216215440_version`). Add a table:
  ```sql
  CREATE TABLE IF NOT EXISTS segment_replication (
      segment_id      INTEGER PRIMARY KEY REFERENCES segments(id) ON DELETE CASCADE,
      backend         STRING NOT NULL,        -- e.g. "s3"
      key             STRING NOT NULL,        -- object key
      etag            STRING,
      uploaded_at     DATETIME NOT NULL,
      bytes           INTEGER NOT NULL
  );
  CREATE INDEX segment_replication_backend ON segment_replication(backend);
  ```
- Decision to record in this PR: do we track *pending/failed* uploads
  here, or keep that purely in-memory? Recommend in-memory queue with a
  rebuild-on-startup query (`SELECT id FROM segments LEFT JOIN
  segment_replication ... WHERE segment_replication.segment_id IS
  NULL`). Less schema churn.
- Add typed accessors on `Manifest`: `record_replicated(...)`,
  `unreplicated_segments(backend, limit) -> Vec<SegmentRef>`,
  `is_replicated(segment_id, backend) -> bool`.
- Down migration must drop the table cleanly.

**Definition of done:** migrations apply and roll back; new accessors
covered by `catalog` tests; no integration with the rest of the system
yet.

### Chunk 3 — Replication config + plumbing (feature-flagged off)

**Scope:** Config struct only; nothing runs.

- Add `s3_replication: Option<S3ReplicationConfig>` to
  `server/src/config.rs::PlateauConfig`. Fields:
  - object-store config (from chunk 1)
  - `key_template` (e.g. `{topic}/{partition}/{segment_index}`)
  - `concurrency: usize`
  - `block_retention_on_unreplicated: bool` (default `false` for v1)
  - `topic_filter: Option<Vec<String>>` (allow/deny by topic)
  - `backoff` (reuse the existing `Backoff` shape in
    `server/src/replication.rs`)
- Wire config loading into `binary_config()` and add an
  `/etc/s3-replication.{yaml,toml}` source path mirroring the existing
  replication config.
- Add an example `s3-replication.yaml` to `examples/`.

**Definition of done:** server boots with and without the block
present; config struct round-trips through serde tests.

### Chunk 4 — Replication worker (push side)

**Scope:** The core async loop. This is the biggest chunk; consider
splitting into 4a (worker skeleton + tests with a fake store) and 4b
(real S3 path) if it grows past ~600 lines.

- New module `server/src/s3_replication.rs` (mirror of
  `server/src/replication.rs`).
- Spawn task: scan manifest for unreplicated finalized segments, upload
  in bounded-concurrency parallelism, write `segment_replication` row on
  success, exponential backoff on failure.
- Important: **read the segment file, not the active cache.** Use the
  `Segment` struct (`data/src/segment.rs:68`) to resolve the on-disk
  path; do not include `.arrows` side-cars.
- Hook into the catalog's existing "segment finalized" path so newly
  finalized segments get queued promptly. Options:
  1. Add a `tokio::sync::Notify` that the slog writer pokes after
     `Manifest::update()` completes; the worker waits on it before each
     scan.
  2. Pure polling on `config.period`. Simpler, but adds latency.

  Recommend (1) with a polling fallback every N seconds for safety
  against missed notifies (restart races, etc.).
- Metrics: `s3_replication_uploaded_total`, `_failed_total`,
  `_pending_segments`, `_bytes_uploaded_total`, `_lag_seconds` (now -
  oldest unreplicated time_start).
- Idempotency: a `HEAD` before `PUT` to skip already-present keys.
- Wire into `server/src/lib.rs` next to the existing replication task
  spawn (`lib.rs:106-107`).

**Definition of done:** integration test that boots a server with a
fake object store, writes records, rolls a segment, and asserts the
object appears and the manifest row is written. Restart mid-upload
recovers.

### Chunk 5 — Retention interaction

**Scope:** Make retention aware of replication state.

- In `catalog/src/catalog.rs::Catalog::retain()` (around line 222),
  consult `block_retention_on_unreplicated`. When set, refuse to delete
  segments without a `segment_replication` row for every configured
  backend.
- This must not let the disk fill: if blocked deletions would exceed
  `headroom`, log a loud error and a metric
  (`retention_blocked_by_replication`) but still don't delete. Operator
  decision territory; document in the example config.
- When `block_retention_on_unreplicated = false` (default), retention
  proceeds as today and the manifest row + S3 object are both removed.
  See chunk 6 for the S3 side.

**Definition of done:** unit tests in `catalog` for both blocking and
non-blocking modes; integration test asserting a segment stays around
until replicated.

### Chunk 6 — S3-side cleanup on retention

**Scope:** Delete S3 objects when their segment is reaped locally.

- When `manifest.remove_segment()` removes a row, also enqueue a
  delete-from-S3 task for any rows that existed in
  `segment_replication` (the `ON DELETE CASCADE` from chunk 2 nukes the
  row; capture the key *before* deletion via a transaction or a
  "tombstone" pattern).
- The delete worker should be separate from the upload worker so a slow
  delete can't starve uploads.
- Failed deletes are retried but never block local deletion — orphan
  objects are an operator concern, not a correctness concern. Provide
  a CLI listing (chunk 7) to find orphans.

**Definition of done:** integration test that exercises full
upload-then-retain cycle and asserts S3 is empty afterward; orphan
counter increments on simulated S3 failure.

### Chunk 7 — Operator CLI

**Scope:** Make the system debuggable in production.

- Extend `cli/` with subcommands:
  - `plateau-cli s3 status` — print pending/uploaded/failed counts
    per topic.
  - `plateau-cli s3 reconcile` — list segment_replication rows that
    don't exist in S3, and S3 objects that don't have manifest rows
    (orphans).
  - `plateau-cli s3 backfill --topic X` — force-enqueue replication for
    a topic (e.g. after enabling replication on an existing node).
- These read the live manifest and talk to S3 directly; no server
  changes needed.

**Definition of done:** each subcommand has a smoke test against the
fake object store.

### Chunk 8 — Docs + README

**Scope:** Documentation only.

- Update README "Future Work" section to reflect that S3 replication
  has shipped, with link to a new `docs/s3-replication.md`.
- The doc covers: enabling, key layout, retention interaction,
  metrics, recovery from orphan objects, what's *not* supported
  (read-through, restore).

**Definition of done:** docs land; nothing in code changes.

---

## Sequencing & dependencies

```
1 (object store) ──┐
                   ├──► 4 (worker) ──► 5 (retain block) ──► 6 (delete) ──► 7 (CLI) ──► 8 (docs)
2 (schema)     ────┤
3 (config)     ────┘
```

Chunks 1–3 can be reviewed in parallel. Chunk 4 is the gate.

## Open questions to resolve before chunk 4

1. **Reuse `object_store` crate or hand-roll?** The arrow-rs ecosystem
   already pulls it in transitively; using it would cut chunk 1 in half
   but ties us to its trait shape.
2. **Key layout.** Proposed
   `{prefix}/{topic}/{partition}/{segment_index:020}` so listings are
   sorted by index. Confirm operators are OK with this — it's a public
   contract.
3. **Multi-file segments.** Decide whether to tar segment + side-cars
   into one object or upload them as separate keys. Single object is
   simpler for delete/idempotency; multi-key is simpler if we ever want
   to do partial reads from S3.
4. **Notify hook vs. polling** for newly finalized segments (chunk 4).
5. **Default for `block_retention_on_unreplicated`.** I've proposed
   `false` — fewer surprises — but a durability-focused operator might
   want `true` by default.

## Risk hotspots

- Race between manifest delete and S3 delete (chunk 6). Must capture
  the S3 key under the same transaction that removes the manifest row,
  or accept orphans.
- The slog writer thread is on the hot write path; the notify in
  chunk 4 must be non-blocking (`Notify::notify_one`).
- S3 `PUT` of large segments can take minutes — uploads must be
  cancel-safe so server shutdown doesn't leave half-uploaded multipart
  objects (`AbortMultipartUpload` on drop).
