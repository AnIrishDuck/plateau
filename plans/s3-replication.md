# S3 Replication of Segments — Implementation Plan

## Goal

A `plateau-cli s3-sync` subcommand that copies local segments to S3 (and
S3-compatible object stores). It's a one-shot, incremental sync:

- Run it once → it mirrors current state.
- Run it again later → it only uploads what's new or changed.
- Interrupted → re-running picks up where it left off.

The operator (or a cron / systemd timer) drives it. No background thread
in the server, no new server endpoints, no manifest schema changes.

## How this fits with existing replication

The existing `client/src/replicate.rs` is a host-to-host record-shipping
worker driven by a `pump()`-once method and wrapped in
`run_forever()` for the server-side thread (`server/src/replication.rs`).

We reuse its **shape** — config struct, worker, job, `pump()` —
but the new job ships *finalized segments* to *S3* instead of *records*
to a *peer host*. The server thread is untouched. The CLI calls
`pump()` exactly once and exits.

## Why CLI, not server thread

- No long-running thread to monitor or restart.
- No fight over the SQLite DB — manifest is in WAL mode, so an external
  read-only opener is safe.
- Trivial to run on-demand, against a snapshot, or from a different host
  that has the data directory mounted.
- "Incremental" falls out for free: **S3 itself is the replication
  cursor**. List+HEAD what's already there, upload what isn't.

## What gets uploaded

Two categories, both driven by the local manifest:

1. **Finalized segments** (`segments.size IS NOT NULL`). Immutable once
   uploaded — skip if the key exists in S3 with matching size/etag.
2. **The active segment** (the one currently being written, `size` not
   yet finalized in the manifest). Re-upload on every run because its
   bytes are still growing. The "overwrite active segments" semantics
   the user asked about: each sync overwrites the S3 copy of the active
   segment with the current local bytes. Once it finalizes, it joins
   category 1 and stops being re-uploaded.

Reading a still-growing segment file gives a consistent byte prefix
because the slog writer only appends and fsyncs whole chunks. The S3
object will be a valid-up-to-some-suffix copy until the segment seals.

---

## Work chunks

Three PRs, ordered. Each is independently reviewable.

### Chunk 1 — S3 client wrapper + config

**Scope:** Pure I/O layer, no plateau integration.

- Pick the backend: prefer the `object_store` crate (already in the
  arrow-rs dependency graph) unless it pulls something heavy we don't
  want — fall back to `aws-sdk-s3` if so. Decide in this PR.
- Thin wrapper exposing only what we need:
  - `head(key) -> Result<Option<ObjectMeta>>` (size, etag)
  - `put(key, AsyncRead, size) -> Result<()>` (multipart automatically
    above N MB)
  - `list(prefix) -> Stream<ObjectMeta>` (for reconciliation)
- Config struct, serde-ready:
  - `endpoint`, `region`, `bucket`, `prefix` (key namespace)
  - `credentials`: env / IMDS / static
  - `path_style: bool`, `multipart_threshold: ByteSize`
- Add to a new module in `client/src/s3.rs` (or a new `s3` crate if it
  bloats `client`).

**DoD:** unit tests with a mock backend; one `#[ignore]` integration
test against MinIO in docker.

### Chunk 2 — Segment-sync job + worker integration

**Scope:** The actual sync logic, no CLI wiring yet.

- New module `client/src/replicate_s3.rs` (sibling of `replicate.rs` —
  do *not* graft S3 into the existing `ReplicationWorker`; the data
  flow is fundamentally different (filesystem+sqlite vs HTTP) and
  mashing them together obscures both).
- Reuse the patterns from `replicate.rs`:
  - `S3Replicate` config: data path, manifest path, S3 config,
    `topics: Vec<TopicFilter>`, `parallel: usize`, `include_active:
    bool` (default `true`).
  - `S3ReplicationWorker` with a `pump()` that returns when nothing
    more needs uploading this pass.
  - `S3PartitionJob` analogous to `ReplicatePartitionJob` —
    one per (topic, partition).
- Each `S3PartitionJob::page()`:
  1. Open manifest read-only (separate `SqlitePool`, `read_only=true`).
     Query segments for this partition ordered by `segment_index`.
  2. For each finalized segment beyond the job's cursor: `HEAD` the
     target key. If present with matching size, advance the cursor and
     continue. Otherwise `PUT` the segment file, then advance.
  3. Active segment (if `include_active`): always `PUT` (overwrite).
     Don't advance the cursor — next pass will re-upload.
  4. Return `done = true` when all finalized segments past cursor are
     uploaded and the active segment (if any) has been pushed once
     this pass.
- Key layout (this is a public contract — pin it now):
  `{prefix}/{topic}/{partition}/{segment_index:020}.{ext}`
  where `ext` is the on-disk file extension (`feather` or `parquet`).
  Side-car files (e.g. `.arrows` cache) are **not** uploaded. If a
  segment has multiple on-disk parts, upload each with a suffix:
  `.../{segment_index:020}.part-{n}.{ext}`. Document this clearly.
- Concurrency: bounded parallelism via `FuturesUnordered`, copy the
  pattern from `ReplicationWorker::page_all`.
- Skip detection: an `S3Object` is considered "up to date" if its size
  equals the manifest's `segments.size` for that index. For the active
  segment we can't compare against the manifest (size is NULL there),
  so we always re-upload. Optionally compare against the on-disk file
  size to skip when no growth has happened — nice-to-have for v1.1.

**DoD:** library-level tests using a fake object store from chunk 1,
covering: fresh sync, idempotent re-run (no extra uploads), partial
prior run (interruption recovery), active segment re-uploaded.

### Chunk 3 — CLI subcommand

**Scope:** Make it runnable.

- Add to `cli/src/main.rs`:
  ```
  plateau-cli s3-sync --config s3-replication.yaml
  plateau-cli s3-sync --data-path PATH --bucket B [--prefix P] \
                     [--endpoint URL] [--topic T]... [--once]
  ```
- The `--config` form deserializes an `S3Replicate` and calls
  `worker.pump()` once.
- The flag form is for ad-hoc / one-off runs without a config file.
- Exit codes:
  - 0: success, everything in sync.
  - 1: error (S3 unreachable, permission denied, etc.).
  - 2: partial — some uploads failed but the run made forward
    progress. Suitable for cron retry.
- Logging: per-segment INFO, per-partition summary at end, totals
  (bytes uploaded, segments uploaded, segments skipped).
- Document running as a cron / systemd timer in the example config.

**DoD:** smoke test invokes the CLI against a fake object store and a
prepared data dir, asserts segments land at the expected keys.

---

## Sequencing

```
1 (s3 client) → 2 (sync job) → 3 (CLI)
```

Strictly serial — each chunk's tests need the prior chunk.

## Open questions to resolve before chunk 2

1. **`object_store` crate vs `aws-sdk-s3`.** Investigate during chunk 1.
2. **Multi-file segments.** Confirm whether real segments have side-cars
   that need to land in S3 to be usable, or whether the base file alone
   suffices for a byte-mirror v1. The README hints there's just one
   primary file per segment; need to verify in `data/src/segment.rs`.
3. **Concurrent runs.** Two `s3-sync` processes against the same data
   dir + bucket would race on the active segment. v1: document
   "don't do that". v1.1: add a `flock`-based lock file in the data
   dir.
4. **Manifest opener lock.** WAL mode allows concurrent readers, but
   the SQLite file must be on a local filesystem (not NFS) for this to
   be safe. Verify and document.

## Risk hotspots

- **Active segment is mid-write.** Reading a growing file yields a
  consistent prefix, but if the slog writer is mid-chunk-flush the
  file may end inside an unfinished frame. That's fine for a byte
  mirror (S3 just has stale bytes until next sync), but if we ever
  want to read these back from S3, the consumer must tolerate
  truncated tails.
- **Large segment uploads.** Use multipart with `AbortMultipartUpload`
  on drop so a Ctrl-C doesn't leave half-uploaded objects accumulating
  S3 storage charges.
- **Retention can delete a segment between manifest query and upload.**
  Handle `ENOENT` on file open by skipping that segment and logging —
  not an error.
