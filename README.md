# write-ahead-log-recovery

**Component 1 of 3 in Storage & Persistence** of the [`strata-database-engine`](https://github.com/juanmmm21/strata-database-engine) ecosystem.
Repo: [`github.com/juanmmm21/write-ahead-log-recovery`](https://github.com/juanmmm21/write-ahead-log-recovery)

A binary write-ahead log (WAL), written from scratch in Python, with periodic checkpointing and redo/undo recovery after a process crash. It is the durability foundation the ecosystem's two storage engines depend on ([`bplus-tree-storage-engine`](https://github.com/juanmmm21/bplus-tree-storage-engine) and [`lsm-tree-engine`](https://github.com/juanmmm21/lsm-tree-engine)), and the replication unit that [`raft-replication-log`](https://github.com/juanmmm21/raft-replication-log) will use.

---

## What it is and what problem it solves

No database engine can guarantee that a committed write survives a process crash unless it first writes a description of that change to a sequential log and forces it to disk (`fsync`) before answering "committed" to the client. That is the write-ahead logging rule: **no page or row is considered durable until its corresponding record is in the WAL and has gone through `fsync`.**

This project implements that piece in isolation and in a verifiable way: a binary record format with a checksum, a writer that assigns monotonic LSNs (*log sequence numbers*) and syncs every write to disk, and a recovery procedure that rebuilds the correct state of an external storage engine after a crash — whether it happened mid-write (truncation) or as corruption of bytes already written.

## Role in `strata-database-engine`

```text
                         ┌────────────────────────────┐
                         │  write-ahead-log-recovery   │   (this repo)
                         │  LSN · fsync · redo/undo     │
                         └──────────────┬───────────────┘
                                        │ implements StorageApplier
                         ┌──────────────┼───────────────┐
                         ▼                              ▼
          ┌──────────────────────────┐    ┌──────────────────────────┐
          │ bplus-tree-storage-engine │    │     lsm-tree-engine      │
          └──────────────────────────┘    └──────────────────────────┘
                         │                              │
                         └──────────────┬───────────────┘
                                        ▼
                              mvcc-transaction-manager /
                              lock-manager-deadlock-detector
                                        │
                                        ▼
                                     nanosql
```

This repo does not import or depend on any other subproject in the ecosystem: it exposes its binary format and the `StorageApplier` `Protocol` as its integration contract. The actual integration (a storage engine replaying WAL records to rebuild its state) happens inside `nanosql`.

## Goal / skills demonstrated

- Designing a versioned binary record format with an integrity checksum and variable-length fields.
- The WAL-before-data durability invariant and explicit `fsync`.
- **ARIES-style redo/undo recovery** (analysis, full history redo, undo of losing transactions) without physical pages.
- Checkpointing with computation of the safe truncation LSN and atomic file compaction (`os.replace`).
- Explicit handling of I/O failures (disk full, permissions, mid-write truncation) with dedicated typed exceptions, never generic ones.
- Property-based tests with fixed-seed random sequences, plus dedicated tests simulating a crash (truncation) and corruption (invalid checksum).

## How it works

### Record format

All integers are encoded big-endian:

```text
+----------+---------+------+-----------------+----------------+------------+----------+
| MAGIC(4) | LSN(8)  | type | transaction_id  | payload_len(4) | payload(N) | crc32(4) |
|  "WLR1"  |         | (1)  |       (8)        |                |            |          |
+----------+---------+------+-----------------+----------------+------------+----------+
```

The `payload` encodes, with an explicit length per field: the table (`str`), the key (`bytes`), the "after" value (`new_value`, optional), the "before" value (`old_value`, optional), and — only in checkpoint records — the list of active transaction ids. The CRC32 checksum covers the full header + payload, so any corrupted bit in any field is detected.

Record types (`RecordType`): `INSERT`, `UPDATE`, `DELETE`, `COMMIT`, `ABORT`, `CHECKPOINT_BEGIN`, `CHECKPOINT_END`.

### Durable writes

Every call to `append_insert` / `append_update` / `append_delete` / `commit` / `abort` atomically assigns the next LSN, serializes the record, writes the bytes, and calls `flush()` and `os.fsync()` **before returning the LSN to the caller** — the record is not considered committed until that point.

### Recovery (ARIES-style redo/undo)

1. **Analysis:** the full log is scanned, noting which transactions ended up with a `COMMIT` record.
2. **Redo:** **all** operations in the history are reapplied in LSN order — not just those from committed transactions. This rebuilds the exact state storage had right before the crash (which already reflected writes from transactions that were not yet committed), a necessary condition for each operation's `old_value` to be valid in the next step. Limiting redo to committed transactions breaks reconstruction whenever an uncommitted transaction and a later committed one touch the same key.
3. **Undo:** transactions without a `COMMIT` (explicitly aborted, or interrupted by the crash) are rolled back in reverse LSN order using each of their operations' `old_value`, on top of the state already reconstructed in the previous step.

A truncated tail (`TruncatedRecordError`, a half-written record) or real corruption (`ChecksumMismatchError` / `InvalidRecordError`) stop reading at that exact point; everything before it — already confirmed with `fsync` — is still recovered. The process never crashes because of this: the issue is reflected in the `RecoveryReport`'s `stopped_due_to_truncation` / `stopped_due_to_corruption` flags.

### Checkpointing and safe truncation

`checkpoint(active_transaction_ids)` writes a `CHECKPOINT_BEGIN`/`CHECKPOINT_END` pair and computes `safe_truncation_lsn`: the minimum between the checkpoint itself and the LSN of the first record of each transaction still active. `truncate_before(safe_lsn)` rewrites the file keeping only records with `lsn >= safe_lsn`, writing to a temporary file and swapping it in atomically (`os.replace`) — a crash mid-compaction leaves the original WAL untouched.

## Architecture

```text
src/write_ahead_log_recovery/
├── __init__.py     # re-exported public API
├── models.py       # RecordType, LogRecord, CheckpointInfo, RecoveryReport, exceptions
├── protocols.py     # StorageApplier: the contract storage engines implement
├── pipeline.py       # binary (de)serialization, WriteAheadLog, recover()
└── __main__.py       # demonstration CLI
```

- **`models.py`** — immutable data types (`dataclass(frozen=True, slots=True)`) and the exception hierarchy (`WalError` → `WalIOError` / `LogCorruptionError` → `TruncatedRecordError` / `ChecksumMismatchError` / `InvalidRecordError`).
- **`protocols.py`** — `StorageApplier`, the single coupling point with a real storage engine.
- **`pipeline.py`** — all the logic: binary encoding/decoding, `WriteAheadLog` (writer with `fsync`, checkpoint and truncation) and `recover()` (a pure recovery function).
- **`__main__.py`** — a demonstration CLI with subcommands, plus an in-memory `StorageApplier` (`InMemoryApplier`) to illustrate `recover` without depending on any real engine.

**Concurrency:** `WriteAheadLog` guards the next LSN, the next transaction id, and the file's write position with a single `threading.Lock` — the design assumes a single logical writer per WAL file (several threads may call `append_*` concurrently, but serialization to disk is always sequential).

## Requirements and installation

- Python `>=3.11`

```bash
git clone https://github.com/juanmmm21/write-ahead-log-recovery.git
cd write-ahead-log-recovery
python3 -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"  # or: pip install -e . pytest mypy ruff
```

## Usage

### CLI

```bash
# Start a transaction, insert, commit
txn=$(python -m write_ahead_log_recovery begin --path /tmp/demo.wal)
python -m write_ahead_log_recovery insert --path /tmp/demo.wal --txn "$txn" \
    --table users --key alice --value v1
python -m write_ahead_log_recovery commit --path /tmp/demo.wal --txn "$txn"

# Inspect the log
python -m write_ahead_log_recovery dump --path /tmp/demo.wal

# Recover (redo/undo) into a demo in-memory StorageApplier
python -m write_ahead_log_recovery recover --path /tmp/demo.wal

# Checkpoint + truncate
python -m write_ahead_log_recovery checkpoint --path /tmp/demo.wal --active "" --truncate
```

### Programmatic usage

```python
from pathlib import Path
from write_ahead_log_recovery import WriteAheadLog, recover

class DictApplier:
    def __init__(self) -> None:
        self.data: dict[bytes, bytes] = {}
    def apply_insert(self, table: str, key: bytes, value: bytes) -> None:
        self.data[key] = value
    def apply_update(self, table: str, key: bytes, value: bytes) -> None:
        self.data[key] = value
    def apply_delete(self, table: str, key: bytes) -> None:
        self.data.pop(key, None)

wal_path = Path("demo.wal")
with WriteAheadLog(wal_path) as wal:
    txn = wal.begin_transaction()
    wal.append_insert(txn, "users", b"alice", b"v1")
    wal.commit(txn)

applier = DictApplier()
report = recover(wal_path, applier)
print(applier.data, report)
```

## Data format / interface exposed to `nanosql`

Any storage engine that wants to replay this WAL's effects implements the `StorageApplier` `Protocol` (`apply_insert`, `apply_update`, `apply_delete`) and passes it to `recover(path, applier)`. The file's binary format (`MAGIC = b"WLR1"`) is versioned in every record's header so it can evolve without breaking existing logs.

## Development

```bash
pytest
ruff check .
ruff format --check .
mypy --strict src/
```

The test suite covers: binary serialization round-trips for all 7 record types, truncation detection at different cut points, checksum corruption detection without interrupting the process, basic redo/undo, checkpoint + safe truncation, and property-based tests with fixed-seed random workloads that compare the recovered state against a reference oracle.

## Benchmarks

Not applicable at this stage: this subproject's goal is the correctness of the durability invariant and of recovery, not performance. `bplus-tree-storage-engine` and `lsm-tree-engine`, which do have real performance pressure, will include their own benchmarks.

## Troubleshooting

- **`ChecksumMismatchError` when reading an existing WAL:** there is real corruption of bytes already written (not a truncation). `recover()` does not raise this exception — it is reflected in `RecoveryReport.stopped_due_to_corruption`, and reading stops at that point while keeping everything read before it. If the exact detail is needed, `iter_records` can be called directly, since it does propagate it.
- **`WalIOError` on `append_*` or `truncate_before`:** a real I/O failure (disk full, permissions). The message includes the path and the original `OSError`.
- **The WAL doesn't grow after several `checkpoint` calls:** `checkpoint()` alone doesn't truncate anything — it only computes `safe_truncation_lsn`. `truncate_before(info.safe_truncation_lsn)` must be called explicitly (or use `--truncate` on the CLI).

## Roadmap

- [ ] Publish a real `StorageApplier` adapter in `nanosql` over `bplus-tree-storage-engine` and `lsm-tree-engine`.
- [ ] Automatic checkpointing based on a log-size threshold, instead of only on demand.
- [ ] Log size / truncation rate metrics exposed by the CLI.

## License

MIT — see [`LICENSE`](./LICENSE).
