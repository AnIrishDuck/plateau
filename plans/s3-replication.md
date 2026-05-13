# S3 Replication of Segments — Implementation Plan

## Goal

Add a `ReplicateCatalogJob` to the existing `client/src/replicate.rs`
worker. It ships segments from a source plateau host (over HTTP) to an
S3 bucket. A `plateau-cli replicate` subcommand builds a
`ReplicationWorker` and calls `pump()` once, so the same code path that
the server thread uses for record replication also serves CLI-driven
S3 sync.

The CLI runs **remotely** — its only inputs are a source host URL, an
S3 endpoint/bucket, and credentials. No filesystem or manifest access.

## Architecture (what fits where)

This is the constraint that drives everything:

- **Existing `ReplicationWorker`** in `client/src/replicate.rs` is the
  orchestrator. It already holds `topics` and `partitions` maps of
  jobs and pumps them in parallel via `page_all()`.
- **Existing `Client`** wraps the plateau HTTP API. The new job uses it
  for *source*-side reads.
- **New `ReplicateCatalogJob`** sits alongside `ReplicateTopicJob` and
  `ReplicatePartitionJob` in the same module. Its `page()` follows the
  same shape: do one unit of work, return `Ok(true)` when caught up.
- **New `S3Target`** is a small wrapper used only by
  `ReplicateCatalogJob` for sink writes. It is **not** a peer of
  `Client` in the worker's `hosts` map — the existing host plumbing is
  HTTP-only.
- **New CLI subcommand** is a ~50-line wrapper: parse config →
  `ReplicationWorker::from_replicate(...)` → `worker.pump().await` →
  exit. It exercises the same constructor as the server thread.

What we are **not** doing:
- Not building a parallel worker.
- Not reading the local manifest or local segment files.
- Not changing the manifest schema.
- Not changing `run_forever()` or the server-side replication thread.

## What gets uploaded

Source defines the truth; the CLI is a thin pump.

- **Finalized segments** (manifest says `size` is set, `time_end`
  is in the past): uploaded once. Idempotent — `HEAD` first, skip if
  size matches.
- **The active segment** (still being appended): uploaded on every
  pump and overwritten in S3. The user's "overwrite any active
  segments" requirement maps here. Once it finalizes, it joins the
  finalized set and stops being re-uploaded.

Incrementality across CLI invocations falls out of `HEAD`-first
behavior — the S3 listing *is* the cursor. No state file, no manifest
changes.

---

## Work chunks

Four PRs. Chunk 4 is genuinely small because it's just a CLI mount of
the existing constructor.

### Chunk 1 — HTTP endpoints for segment access (server)

**Scope:** Read-side endpoints the new job will call. Server-only PR.

Add to `server/src/http.rs`:

- `GET /topic/:topic/partition/:partition/segments` →
  `Vec<SegmentInfo>` (`segment_index`, `time_start`, `time_end`,
  `record_start`, `record_end`, `size`, `version`, `is_active: bool`).
  Backed by a new `Manifest` query that returns finalized + the active
  segment in one shot.
- `GET /topic/:topic/partition/:partition/segment/:index` → streams
  the raw segment file bytes (`Content-Type: application/octet-stream`,
  `Content-Length` set for finalized segments, chunked for active).
- New transport types in `transport/src/`: `SegmentInfo`, list query
  params (`start_index`, `limit`).
- These mirror the read-only side of how `Catalog`/`Manifest` already
  expose data internally; no new lifecycle code.

**DoD:** OpenAPI doc updated; integration tests in `server/tests/`
covering finalized + active segment fetches and the listing endpoint.

### Chunk 2 — S3 target wrapper

**Scope:** Pure I/O, no replication wiring.

- New module `client/src/s3.rs` (gated behind a `s3` cargo feature so
  the existing `replicate` feature doesn't pull in AWS deps when not
  needed).
- Decide between the `object_store` crate (already transitively in
  arrow-rs deps) and `aws-sdk-s3`. Recommend `object_store` for the
  smaller surface and built-in multipart.
- `S3Target` type exposing only what `ReplicateCatalogJob` needs:
  - `head(key) -> Result<Option<ObjectMeta>>`
  - `put_streaming(key, AsyncRead, content_length: Option<u64>)`
  - `list(prefix) -> Stream<ObjectMeta>` (used for catch-up listing at
    job-begin)
- Serde config (`S3Config`): `endpoint`, `region`, `bucket`, `prefix`,
  credentials (env / IMDS / static), `path_style`, `multipart_threshold`.

**DoD:** unit tests against a mock; one `#[ignore]` test against MinIO.

### Chunk 3 — `ReplicateCatalogJob` in `replicate.rs`

**Scope:** This is the actual extension the user asked for.

Add to `client/src/replicate.rs`, alongside the existing job types:

```rust
#[derive(Clone, Debug)]
pub struct ReplicateCatalogJob {
    source: ClientPartition,
    target: S3Target,
    key_prefix: String,
    include_active: bool,
    next_index: SegmentIndex,   // cursor for finalized segments
}
```

- `begin()`: list the S3 prefix once to derive `next_index` (highest
  contiguous finalized segment already present + 1). This makes
  resume-after-interruption automatic.
- `page()`:
  1. Call `client.list_segments(topic, partition, start=next_index)`
     against the source.
  2. For each finalized segment past the cursor: `HEAD` S3, skip if
     size matches, otherwise stream `get_segment_bytes` → `put_streaming`,
     then advance `next_index`.
  3. If `include_active` and the listing includes an active segment:
     stream + overwrite at its key. Do not advance the cursor.
  4. Return `Ok(true)` when nothing was uploaded this call.
- Config extension to `Replicate`:
  ```rust
  pub struct Replicate {
      pub config: Config,
      pub hosts: Vec<ReplicateHost>,
      pub topics: Vec<ReplicateTopic>,
      pub partitions: Vec<ReplicatePartition>,
      #[serde(default)]
      pub s3_targets: Vec<S3TargetConfig>,   // named, like hosts
      #[serde(default)]
      pub catalogs: Vec<ReplicateCatalog>,   // new job entries
  }

  pub struct ReplicateCatalog {
      pub source: HostPartition,        // reuse existing type
      pub target: String,               // s3_targets key
      pub key_prefix: Option<String>,   // default: "{topic}/{partition}"
      pub include_active: bool,         // default: true
  }
  ```
- `ReplicationWorker` extension:
  - Add `s3_targets: HashMap<String, S3Target>` and
    `catalogs: HashMap<CatalogKey, ReplicateCatalogJob>` fields.
  - `from_replicate()` constructs them.
  - `page_all()` adds a third loop that pushes catalog-job futures
    into the same `FuturesUnordered`, respecting `config.parallel`.
  - `all_jobs()` iterator returns catalog jobs too for start/end
    logging.
- Key layout: `{key_prefix}/{segment_index:020}.{ext}` where `ext` is
  the segment's on-disk file extension (`feather` or `parquet`).
  Multi-part segments → `…/{index:020}.part-{n}.{ext}`. Pin this now
  as a public contract.

**DoD:** integration test in `client/tests/` (or extend the existing
replicate tests) that spins up a plateau server, writes records to
roll a few segments, runs the worker, and asserts the expected S3
keys land — then rolls more, re-runs, asserts only the new ones are
uploaded.

### Chunk 4 — `plateau-cli replicate` subcommand

**Scope:** Trivial wrapper.

Add to `cli/src/main.rs`:

```
plateau-cli replicate --config replicate.yaml
plateau-cli replicate --config replicate.yaml --once   # default
plateau-cli replicate --config replicate.yaml --watch  # opt-in run_forever
```

- Deserializes a `Replicate` (the same struct the server-side config
  uses).
- Calls `ReplicationWorker::from_replicate(...).pump().await` for the
  one-shot case, or `run_forever()` for `--watch`.
- Exit codes:
  - 0 — all jobs caught up.
  - 1 — config / connectivity error.
  - 2 — partial: some jobs errored but others made progress (suitable
    for cron retry).
- Logs per-job summary at end: segments uploaded, segments skipped,
  bytes.
- A worked example config in `examples/replicate-s3.yaml` showing both
  record-shipping and catalog-to-S3 jobs in one file.

**DoD:** smoke test invokes the CLI binary against a fake S3 backend
and a plateau test server; asserts uploads and clean exit.

---

## Sequencing

```
1 (endpoints) ──┐
                ├──► 3 (catalog job) ──► 4 (CLI)
2 (s3 target) ──┘
```

Chunks 1 and 2 are independent and can be reviewed in parallel.

## Open questions to resolve before chunk 3

1. **`object_store` vs `aws-sdk-s3`** — decide during chunk 2.
2. **Multi-file segments.** Look at `data/src/segment.rs` to confirm
   whether a single segment has multiple on-disk files we must upload
   for the S3 copy to be useful. If yes, the segment-bytes endpoint in
   chunk 1 must expose all parts (likely as a list of named files, or
   via `:part` path component); the key layout reflects that.
3. **Active-segment listing.** Confirm in chunk 1 whether the existing
   manifest query helpers already differentiate active vs finalized,
   or whether we need a small SQL addition. No schema change either
   way.
4. **Auth on the new endpoints.** They expose raw segment bytes — same
   trust model as the existing record endpoints, which means whatever
   front-door auth the operator runs in front of plateau. Document.

## Risk hotspots

- **Active segment is mid-write** when the HTTP stream reads it. The
  server streams a snapshot of the file as of the open; a growing tail
  shows up on the next sync. That's fine for a byte mirror. If we ever
  want to *read* these S3 copies, consumers must tolerate truncated
  tails.
- **Multipart upload abort.** Use the `object_store` crate's
  cancel-on-drop behavior (or `AbortMultipartUpload` explicitly) so
  Ctrl-C doesn't leave half-uploaded parts incurring storage charges.
- **Retention races.** A segment listed by the source can be deleted
  before the bytes endpoint is hit. Handle 404 by dropping that index
  from this pass and logging — not an error. Next pass picks up the
  next index naturally.
- **Concurrent CLIs against the same bucket prefix.** The active
  segment's PUT becomes a last-writer-wins race. Document; defer a
  lock to a follow-up.
