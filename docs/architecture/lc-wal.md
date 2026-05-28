# WAL — Detailed Technical Documentation

---

## Summary

**WAL** is a compact, file‑based write‑ahead log helper that appends length‑prefixed, CRC32‑protected JSON batch records to disk and supports safe replay and compaction. It is designed to be simple, robust against partial writes and truncation, and to integrate with a worker that persists change batches (for example, a `JobStateRegistry` worker). The WAL uses file rotation by size and provides atomic append semantics via file descriptors.

> **From the implementation:** “WAL helper (length‑prefixed + CRC32)”  
> **From the implementation:** “Record format: [4 bytes BE length][4 bytes BE crc32][payload bytes (UTF‑8 JSON)]”

These two lines summarize the WAL’s core design: each record is a length + CRC header followed by a UTF‑8 JSON payload, and the CRC is verified during replay to detect corruption or truncation.

---

## Table of contents

1. Goals and design principles  
2. On‑disk record format (precise bytes)  
3. Public API (methods, signatures, behavior)  
4. Append semantics and rotation policy  
5. Replay semantics and truncation handling  
6. Compaction algorithm and guarantees  
7. File naming, ordering, and atomicity considerations  
8. Error handling and robustness to partial writes  
9. Integration patterns and recommended usage with a registry/worker  
10. Performance characteristics and complexity analysis  
11. Operational guidance (monitoring, backups, recovery)  
12. Tests and validation scenarios  
13. Extension points and safe modifications  
14. Quick reference (examples and return shapes)

---

## 1. Goals and design principles

- **Durability with simplicity:** provide an append‑only log that can persist batches of events (JSON objects) with minimal dependencies and a straightforward on‑disk layout.  
- **Detect and stop on corruption:** use CRC32 checks to detect payload corruption and stop replay at the first sign of truncation or CRC mismatch.  
- **Atomic append:** write records using the file descriptor API to avoid partial writes being visible to readers.  
- **Rotation and compaction:** rotate files by size to bound single file sizes and support compaction to remove fully acknowledged records.  
- **Interoperability:** payloads are JSON objects; the WAL does not impose a schema but expects `toSeq` (or `batch.toSeq`) to be present for compaction decisions.  
- **Recoverability:** replay stops at the first truncated or corrupted record so the system can safely recover up to the last good record.

---

## 2. On‑disk record format (precise bytes)

Each record written to a WAL file has the following binary layout:

1. **4 bytes** — big‑endian unsigned 32‑bit integer: **payload length** in bytes (`len`).  
2. **4 bytes** — big‑endian unsigned 32‑bit integer: **CRC32** of the payload bytes.  
3. **`len` bytes** — **payload**: UTF‑8 encoded JSON string.

Concretely:  
```
[ 4 bytes BE length ][ 4 bytes BE crc32 ][ payload bytes (UTF-8 JSON) ]
```

- The CRC is computed over the payload bytes only (not over the length or CRC fields).  
- The length field allows the reader to know exactly how many bytes to read for the payload; the CRC verifies integrity.  
- This layout enables the reader to detect truncated headers (less than 8 bytes available), truncated payloads (header length exceeds remaining bytes), and CRC mismatches.

---

## 3. Public API — methods, signatures, and behavior

All methods are asynchronous (return Promises) except `crc32` helpers which are internal.

### `new WAL({ walDir, workerId, walRotateBytes })`
- **Parameters**:
  - `walDir` (string) — directory to store WAL files (default `./wal`).
  - `workerId` (string) — identifier used in file names (default `'worker'`).
  - `walRotateBytes` (number) — rotate when current file reaches this many bytes (default 64 MiB).
- **Behavior**: constructs the WAL instance; does not open files until needed.

### `appendBatch(batch)`
- **Parameters**: `batch` — a JSON‑serializable object. The implementation expects `batch.toSeq` (number) or `batch.batch.toSeq` to be present for compaction, but will append even if missing (with a console warning).
- **Behavior**:
  - Serializes `batch` to UTF‑8 JSON.
  - Computes payload length and CRC32.
  - Writes `[len][crc][payload]` atomically to the current WAL file via the open file descriptor.
  - Updates `currentSize` and rotates file if `currentSize >= walRotateBytes`.
- **Errors**: throws if `batch` is not an object; otherwise errors from the filesystem propagate.

### `replay()`
- **Returns**: an array of parsed batch objects in lexicographic file order.
- **Behavior**:
  - Lists WAL files for `workerId`, sorts them lexicographically (file naming ensures chronological order).
  - For each file, reads sequentially:
    - Reads 8‑byte header; if header cannot be read (truncated), stops replay and returns collected records.
    - Reads `len` bytes payload; if payload truncated, stops and returns collected records.
    - Verifies CRC32; if mismatch, stops and returns collected records.
    - Parses JSON; if parse fails, stops and returns collected records.
  - Continues to next file until all files processed or a truncation/CRC/parse error occurs.
- **Guarantee**: replay returns only fully validated records; it stops at the first sign of corruption/truncation.

### `compactUpTo(seq)`
- **Parameters**: `seq` (number) — sequence up to which records are considered acknowledged.
- **Behavior**:
  - Iterates WAL files in order.
  - For each file:
    - Reads records and determines `recToSeq` for each record (from `obj.batch.toSeq` or `obj.toSeq`).
    - If all records in the file have `recToSeq <= seq`, the file is deleted.
    - If some records must be kept (`recToSeq > seq`), the file is rewritten to contain only those kept records (written to a `.tmp` file and atomically renamed).
    - If the file contains records that cannot be interpreted (missing `toSeq`), compaction conservatively keeps the file (or treats as mixed and may keep).
  - After compaction, updates `currentSize` for the open file if necessary.
- **Guarantee**: records with `toSeq <= seq` are removed; records with `toSeq > seq` are preserved in order.

### `stats()`
- **Returns**: `{ walFiles, walBytes, currentFile, currentSize }`.
- **Behavior**: reports number of WAL files and total bytes on disk for the `workerId`.

---

## 4. Append semantics and rotation policy

- **Atomic append**: `appendBatch` writes the full record buffer via the file descriptor `write` call. Using the file descriptor avoids race conditions with other processes that might open the same file for append (but WAL is designed for single‑writer per `workerId`).
- **Rotation**:
  - When `currentSize >= walRotateBytes`, `_rotate()` closes the current file and opens a new file named with a timestamp and random sequence to ensure lexicographic ordering.
  - File names are deterministic and sortable: `wal-<workerId>-<timestamp>-<seq>.log` with zero‑padded fields.
  - Rotation bounds the size of any single WAL file and simplifies compaction and replay.

---

## 5. Replay semantics and truncation handling

- **Stop on truncation**: replay reads headers and payloads sequentially; if a header or payload is truncated (file ended mid‑record), replay stops and returns the records parsed so far. This behavior is safe for recovery: it avoids returning partially written or corrupted records.
- **Stop on CRC mismatch**: if CRC32 of a payload does not match the stored CRC, replay stops at that point. This protects against silent corruption.
- **Stop on JSON parse error**: malformed JSON halts replay; the system can then inspect the file and decide whether to repair or truncate.
- **Ordering**: files are processed in lexicographic order; within a file, records are returned in append order.

---

## 6. Compaction algorithm and guarantees

- **Purpose**: remove records that are fully acknowledged (their `toSeq` ≤ provided `seq`) to reclaim disk space.
- **Per‑file decision**:
  - If every record in a file has `recToSeq ≤ seq`, the file is deleted.
  - If some records must be kept, the file is rewritten with only the kept records.
  - If a file contains records without a determinable `toSeq`, compaction conservatively keeps the file (or treats it as mixed and may keep).
- **Atomic rewrite**:
  - Kept records are written to a temporary file (`.tmp`) and then atomically renamed to replace the original file. If rename fails, the code attempts unlink + rename fallback.
- **Post‑compaction consistency**:
  - After compaction, the WAL ensures `currentSize` reflects the actual size of the current file (stat is used).
- **Guarantee**: compaction never discards records with `recToSeq > seq`. It may conservatively retain files when `toSeq` cannot be determined.

---

## 7. File naming, ordering, and atomicity considerations

- **File name format**: `wal-<workerId>-<timestampPadded>-<seqPadded>.log`. Padding ensures lexicographic order equals chronological order.
- **Atomic creation**: new files are created with `fs.writeFile(p, '')` then opened for append. Rotation closes the previous file descriptor before opening a new one.
- **Atomic replacement during compaction**: rewrite to `tmp` and `fs.rename(tmp, p)` ensures atomic swap on POSIX filesystems. Fallback logic handles rename failures.
- **Single‑writer assumption**: WAL assumes a single writer per `workerId`. If multiple processes write to the same `workerId` directory concurrently, behavior is undefined unless external coordination is added.

---

## 8. Error handling and robustness to partial writes

- **Append errors**: filesystem errors during append propagate to the caller; the caller should retry or escalate.
- **Replay safety**: replay stops at the first sign of truncation or CRC mismatch; this prevents returning corrupted data.
- **Compaction resilience**: compaction reads files and, on encountering unreadable or malformed records, skips compaction for that file (conservative behavior).
- **Rotation safety**: rotation closes the file descriptor before opening a new file; if rotation fails, WAL attempts to continue using the existing file or open a new one on the next append.
- **Warnings**: `appendBatch` logs a console warning if `toSeq` is missing; this helps operators detect batches that cannot be compacted.

---

## 9. Integration patterns and recommended usage

### Typical integration with a registry + worker
1. **Registry** collects change events and appends them to an in‑memory `changeLog`.  
2. **WorkerBatcher** periodically calls `getChangeBatch(fromSeq, options)` on the registry to obtain a coalesced batch.  
3. Worker calls `wal.appendBatch(envelope)` where `envelope` contains the batch and `toSeq`.  
4. After successful WAL append and any downstream persistence, worker calls `wal.compactUpTo(toSeq)` (or the registry calls it) to remove acknowledged records.  
5. On restart, the worker calls `wal.replay()` to obtain any batches that were persisted but not yet processed.

### Boot and recovery
- On startup:
  - Call `wal.replay()` to obtain persisted batches and process them (idempotently).
  - Optionally call `importState()` on the registry with the latest snapshot, then replay WAL to catch up.
- On graceful shutdown:
  - Ensure worker flushes pending batches to WAL and downstream storage before exit.

### Multi‑worker considerations
- Use distinct `workerId` per writer to avoid concurrent writers to the same files.
- If multiple writers must share a WAL directory, implement an external coordination mechanism (leader election or centralized sequencer) or use a shared durable queue (e.g., Kafka, Redis Streams) instead.

---

## 10. Performance characteristics and complexity

- **Append**: O(1) per record (serialize + write). Cost dominated by JSON serialization and disk I/O. Using buffered writes and a fast filesystem improves throughput.
- **Replay**: O(totalRecords) across WAL files until truncation. Replay cost is linear in the number of records read.
- **Compaction**: O(totalRecordsInFile) per file; rewriting a file is proportional to the number of kept records. Compaction is I/O heavy; schedule during low load or throttle it.
- **Memory**: WAL does not hold payloads in memory except during append and during compaction when kept payloads are buffered for rewrite. `replay()` returns an array of parsed objects — for very large WALs, consider streaming replay rather than collecting all objects in memory.

---

## 11. Operational guidance

### Monitoring
- Track `walFiles` and `walBytes` via `stats()` to detect runaway growth.
- Monitor `currentSize` to ensure rotation is occurring as expected.
- Alert on frequent CRC mismatches or truncated replays — these indicate disk corruption or abrupt truncation.

### Backups and retention
- Periodically snapshot the registry state (separate from WAL) and store snapshots off‑site.
- Use WAL compaction to remove acknowledged records and keep disk usage bounded.
- Keep a small retention window of WAL files to allow replay in case of downstream failures.

### Recovery strategy
- If `replay()` stops early due to corruption, inspect the last WAL file manually:
  - If the last record is truncated, truncate the file to the last valid offset and continue.
  - If CRC mismatch occurs, consider restoring from the last snapshot and replaying subsequent WAL files.
- Always prefer restoring from a recent snapshot + replaying WAL rather than attempting to repair corrupted payloads.

---

## 12. Tests and validation scenarios

- **Append + replay roundtrip**: append N batches, call `replay()`, assert returned batches equal appended batches in order.  
- **Truncation handling**: simulate truncated file (truncate last file mid‑record) and assert `replay()` returns only fully written records.  
- **CRC detection**: corrupt a payload byte and assert `replay()` stops before the corrupted record.  
- **Rotation**: set `walRotateBytes` small, append many batches, assert multiple files created and `replay()` returns all records in order.  
- **Compaction correctness**: create files with mixed `toSeq` values, call `compactUpTo(seq)`, assert files deleted or rewritten and that all records with `toSeq <= seq` are removed.  
- **Atomic rewrite**: during compaction, simulate process crash and ensure either original file or new file remains consistent (no partial tmp file left).  
- **Concurrency safety**: test single‑writer assumption; if multiple writers are used, assert behavior is undefined and document requirement for external coordination.

---

## 13. Extension points and safe modifications

- **Streaming replay**: change `replay()` to yield records via an async iterator to avoid holding all records in memory.  
- **Compression**: support optional payload compression (e.g., gzip) with a header flag; ensure CRC is computed on compressed bytes and include a version field in payload envelope.  
- **Signed envelopes**: add HMAC or signature to envelope for tamper detection in multi‑host scenarios.  
- **Time‑window coalescing**: integrate `coalesceWindowMs` semantics into `getChangeBatch` so worker can coalesce only events within a time window.  
- **Multi‑writer coordination**: add a global sequencer or use a centralized queue if multiple processes must append to the same logical WAL.

---

## 14. Quick reference — examples

### Append a batch
```js
await wal.appendBatch({ batch: myBatch, toSeq: myBatch.toSeq });
```

### Replay all persisted batches
```js
const batches = await wal.replay();
// process batches in order
```

### Compact up to sequence
```js
await wal.compactUpTo(12345);
```

### Get WAL stats
```js
const s = await wal.stats();
// { walFiles, walBytes, currentFile, currentSize }
```

---

## 15. Final notes and recommendations

- **Single‑writer per `workerId`** is the simplest and safest deployment model. If you need multiple writers, design an external coordination layer.  
- **Snapshots + WAL**: combine periodic full snapshots of in‑memory state with WAL for incremental durability and fast recovery.  
- **Compaction cadence**: run compaction after confirming downstream consumers have processed and acknowledged up to a safe `toSeq`. Avoid aggressive compaction during peak load.  
- **Testing**: include truncation and CRC scenarios in CI to ensure replay semantics remain correct across code changes.

---

If you’d like, I can now:
- produce a **compact README** (one page) with the most important commands and examples, or  
- generate a **Node.js test harness** (small script) that exercises append/replay/compaction and demonstrates recovery from a truncated file.

Which would you prefer next?