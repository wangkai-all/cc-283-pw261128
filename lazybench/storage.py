"""Core storage layer for the lazy-ingestion workbench.

Layout of a dataset directory:

    data/<dataset>/
      structures/sv_<n>/structure.json   # schema + row count + source hash per structure version
      structures/sv_<n>/rowindex.bin     # u64 file offset of every data row (row positioning)
      structures/sv_<n>/rowhash.bin      # 8-byte blake2 hash of every raw row (row identity)
      structures/sv_<n>/source.csv       # immutable source snapshot (only if keep_snapshot)
      structure.current                  # pointer: current structure version number
      columns/<col>/<build_id>.seg       # published columnar segments (immutable)
      staging/<build_id>.segpart         # in-progress column builds (checkpoints)
      quarantine/<build_id>.seg          # stale builds that lost the version race
      manifests/gen_<n>.json             # published manifests, one per generation
      manifest.current                   # pointer: current generation
      builds/<build_id>.json             # build task state (stop/resume checkpoints)
      fullimport/                        # eager full import used as the comparison baseline
      filters.json                       # row filter rules (query-time only)
      events.jsonl                       # audit log

Row identity: a row is identified by its ordinal in the *original* import
(row_id) plus the blake2 hash of its raw bytes recorded at import time.
Backfills always read through the stored offsets and re-verify the hash, so
they can never silently join against a mutated file's shifted line numbers.
"""

import hashlib
import json
import os
import shutil
import struct
import threading
import time

SEG_MAGIC = b"LSEG1"
SEG_FOOT_MAGIC = b"LENDLSEG"
NULL_LEN = 0xFFFFFFFF
RIDX_MAGIC = b"RIDX1"
RHSH_MAGIC = b"RHSH1"
ROW_HASH_LEN = 8
HEADER_LEN = 9  # magic(5) + u32 count


def now_ms():
    return int(time.time() * 1000)


def row_hash(raw_line_bytes):
    return hashlib.blake2b(raw_line_bytes, digest_size=ROW_HASH_LEN).digest()


def file_hash(path, chunk=1 << 20):
    h = hashlib.blake2b(digest_size=16)
    with open(path, "rb") as f:
        while True:
            b = f.read(chunk)
            if not b:
                break
            h.update(b)
    return h.hexdigest()


# ---------------------------------------------------------------- row index

def write_rowindex(path, offsets):
    with open(path, "wb") as f:
        f.write(RIDX_MAGIC)
        f.write(struct.pack("<I", len(offsets)))
        for off in offsets:
            f.write(struct.pack("<Q", off))


def rowindex_count(path):
    with open(path, "rb") as f:
        if f.read(5) != RIDX_MAGIC:
            raise ValueError("bad rowindex magic")
        return struct.unpack("<I", f.read(4))[0]


def read_offsets(path, i, j):
    """Return (offset_of_row_i, offset_of_row_j_or_None)."""
    with open(path, "rb") as f:
        f.seek(HEADER_LEN + 8 * i)
        off_i = struct.unpack("<Q", f.read(8))[0]
        n = (os.fstat(f.fileno()).st_size - HEADER_LEN) // 8
        if j < n:
            f.seek(HEADER_LEN + 8 * j)
            off_j = struct.unpack("<Q", f.read(8))[0]
        else:
            off_j = None
    return off_i, off_j


def write_rowhashes(path, hashes):
    with open(path, "wb") as f:
        f.write(RHSH_MAGIC)
        f.write(struct.pack("<I", len(hashes)))
        for h in hashes:
            f.write(h)


def read_rowhash(path, i):
    with open(path, "rb") as f:
        f.seek(HEADER_LEN + ROW_HASH_LEN * i)
        return f.read(ROW_HASH_LEN)


def rowhash_checksum(path):
    """Checksum over the hash payloads only (skips the 9-byte header)."""
    h = hashlib.blake2b(digest_size=16)
    with open(path, "rb") as f:
        f.seek(HEADER_LEN)
        while True:
            b = f.read(1 << 20)
            if not b:
                break
            h.update(b)
    return h.hexdigest()


# ------------------------------------------------------------------ segment

class SegWriter:
    """Append-only writer for a column segment under construction."""

    def __init__(self, path, col_name):
        self.path = path
        self.f = open(path, "wb")
        name = col_name.encode("utf-8")
        self.f.write(SEG_MAGIC)
        self.f.write(struct.pack("<H", len(name)))
        self.f.write(name)
        self.f.write(struct.pack("<I", 0))      # row_count, patched at finalize
        self.f.write(b"\x00" * 16)              # checksum, patched at finalize

    def append(self, value):
        if value is None:
            self.f.write(struct.pack("<I", NULL_LEN))
        else:
            b = value.encode("utf-8")
            self.f.write(struct.pack("<I", len(b)))
            self.f.write(b)

    def close(self):
        self.f.close()


def seg_scan(path):
    """Scan a (possibly partial) segment's value region.

    Returns (row_count, values_end_offset). Used to recover the exact resume
    position after a stop/crash: the staging file itself is the checkpoint.
    """
    n = 0
    with open(path, "rb") as f:
        if f.read(5) != SEG_MAGIC:
            raise ValueError("bad segment magic")
        name_len = struct.unpack("<H", f.read(2))[0]
        f.seek(name_len, os.SEEK_CUR)
        f.seek(4 + 16, os.SEEK_CUR)  # row_count + checksum placeholders
        while True:
            b = f.read(4)
            if not b:
                break
            if len(b) < 4:
                f.seek(-len(b), os.SEEK_CUR)  # truncated length prefix
                break
            (ln,) = struct.unpack("<I", b)
            if ln != NULL_LEN:
                f.seek(ln, os.SEEK_CUR)
                # truncated tail (crash mid-write): roll back to length prefix
                if f.tell() > os.fstat(f.fileno()).st_size:
                    f.seek(-(ln + 4), os.SEEK_CUR)
                    break
            n += 1
        return n, f.tell()


def seg_finalize(path, row_count):
    """Append the row-offset table + footer, patch the header. Returns checksum."""
    n, values_end = seg_scan(path)
    if n != row_count:
        raise ValueError(f"segment has {n} rows, expected {row_count}")
    offsets = []
    with open(path, "rb") as f:
        f.seek(5)
        name_len = struct.unpack("<H", f.read(2))[0]
        values_start = 5 + 2 + name_len + 4 + 16
        f.seek(values_start)
        for _ in range(row_count):
            offsets.append(f.tell())
            (ln,) = struct.unpack("<I", f.read(4))
            if ln != NULL_LEN:
                f.seek(ln, os.SEEK_CUR)
    h = hashlib.blake2b(digest_size=16)
    with open(path, "rb") as f:
        f.seek(values_start)
        remaining = values_end - values_start
        while remaining > 0:
            b = f.read(min(1 << 20, remaining))
            if not b:
                break
            h.update(b)
            remaining -= len(b)
    checksum = h.digest()
    with open(path, "r+b") as f:
        f.seek(0, os.SEEK_END)
        for off in offsets:
            f.write(struct.pack("<Q", off))
        f.write(struct.pack("<Q", values_end))  # offsets_start
        f.write(SEG_FOOT_MAGIC)
        # patch header: row_count + checksum
        f.seek(5 + 2 + name_len)
        f.write(struct.pack("<I", row_count))
        f.write(checksum)
    return checksum.hex()


class SegReader:
    """Random-access reader over a finalized, immutable segment."""

    def __init__(self, path):
        self.path = path
        self.f = open(path, "rb")
        if self.f.read(5) != SEG_MAGIC:
            raise ValueError(f"bad segment magic: {path}")
        (name_len,) = struct.unpack("<H", self.f.read(2))
        self.name = self.f.read(name_len).decode("utf-8")
        (self.rows,) = struct.unpack("<I", self.f.read(4))
        self.checksum = self.f.read(16).hex()
        self.values_start = 5 + 2 + name_len + 4 + 16
        self.f.seek(-16, os.SEEK_END)
        (self.offsets_start,) = struct.unpack("<Q", self.f.read(8))
        if self.f.read(8) != SEG_FOOT_MAGIC:
            raise ValueError(f"bad segment footer: {path}")

    def row(self, i):
        if i < 0 or i >= self.rows:
            raise IndexError(i)
        self.f.seek(self.offsets_start + 8 * i)
        (off,) = struct.unpack("<Q", self.f.read(8))
        self.f.seek(off)
        (ln,) = struct.unpack("<I", self.f.read(4))
        if ln == NULL_LEN:
            return None
        return self.f.read(ln).decode("utf-8")

    def scan(self, start=0, stop=None):
        """Sequential generator of (row_id, value)."""
        stop = self.rows if stop is None else min(stop, self.rows)
        if start >= stop:
            return
        self.f.seek(self.offsets_start + 8 * start)
        (off,) = struct.unpack("<Q", self.f.read(8))
        self.f.seek(off)
        for i in range(start, stop):
            (ln,) = struct.unpack("<I", self.f.read(4))
            if ln == NULL_LEN:
                yield i, None
            else:
                yield i, self.f.read(ln).decode("utf-8")

    def close(self):
        self.f.close()


# ------------------------------------------------------------------ dataset

class Dataset:
    def __init__(self, root, name):
        self.root = root
        self.name = name
        self.dir = os.path.join(root, name)
        self._manifest_lock = threading.Lock()
        self._readers_lock = threading.Lock()
        self._readers = {}  # generation -> active reader count

    # -- paths
    def p(self, *parts):
        return os.path.join(self.dir, *parts)

    def sv_dir(self, sv):
        return self.p("structures", f"sv_{sv}")

    def structure_path(self, sv):
        return os.path.join(self.sv_dir(sv), "structure.json")

    def rowindex_path(self, sv):
        return os.path.join(self.sv_dir(sv), "rowindex.bin")

    def rowhash_path(self, sv):
        return os.path.join(self.sv_dir(sv), "rowhash.bin")

    def snapshot_path(self, sv):
        return os.path.join(self.sv_dir(sv), "source.csv")

    # -- structure versions
    def current_sv(self):
        with open(self.p("structure.current")) as f:
            return int(f.read().strip())

    def set_current_sv(self, sv):
        tmp = self.p("structure.current.tmp")
        with open(tmp, "w") as f:
            f.write(str(sv))
        os.replace(tmp, self.p("structure.current"))

    def structure(self, sv=None):
        sv = self.current_sv() if sv is None else sv
        with open(self.structure_path(sv)) as f:
            return json.load(f)

    def list_structures(self):
        d = self.p("structures")
        out = []
        for name in sorted(os.listdir(d)):
            if name.startswith("sv_"):
                with open(os.path.join(d, name, "structure.json")) as f:
                    out.append(json.load(f))
        return out

    # -- manifests
    def current_gen(self):
        with open(self.p("manifest.current")) as f:
            return int(f.read().strip())

    def manifest(self, gen=None):
        gen = self.current_gen() if gen is None else gen
        with open(self.p("manifests", f"gen_{gen}.json")) as f:
            return json.load(f)

    def update_manifest(self, fn):
        """Apply fn(manifest) and publish as a new generation (atomic swap)."""
        with self._manifest_lock:
            cur = self.manifest()
            new = json.loads(json.dumps(cur))
            fn(new)
            new["generation"] = cur["generation"] + 1
            new["published_at"] = now_ms()
            gen = new["generation"]
            tmp = self.p("manifests", f"gen_{gen}.json.tmp")
            with open(tmp, "w") as f:
                json.dump(new, f, indent=1)
            os.replace(tmp, self.p("manifests", f"gen_{gen}.json"))
            tmp2 = self.p("manifest.current.tmp")
            with open(tmp2, "w") as f:
                f.write(str(gen))
            os.replace(tmp2, self.p("manifest.current"))
            return gen

    def acquire(self):
        """Pin the current generation for a consistent read."""
        with self._readers_lock:
            gen = self.current_gen()
            self._readers[gen] = self._readers.get(gen, 0) + 1
            return gen

    def release(self, gen):
        with self._readers_lock:
            self._readers[gen] -= 1
            if self._readers[gen] <= 0:
                del self._readers[gen]

    def pinned_gens(self):
        with self._readers_lock:
            return set(self._readers.keys())

    # -- events
    def event(self, kind, **detail):
        rec = {"ts": now_ms(), "type": kind, **detail}
        with open(self.p("events.jsonl"), "a") as f:
            f.write(json.dumps(rec) + "\n")

    def events(self, limit=200):
        path = self.p("events.jsonl")
        if not os.path.exists(path):
            return []
        with open(path) as f:
            lines = f.readlines()[-limit:]
        return [json.loads(x) for x in reversed(lines)]

    # -- storage metrics
    def storage_breakdown(self):
        cats = {"snapshot": 0, "row_index": 0, "segments": 0, "staging": 0,
                "fullimport": 0, "quarantine": 0, "metadata": 0}
        for dirpath, _dirnames, filenames in os.walk(self.dir):
            for fn in filenames:
                size = os.path.getsize(os.path.join(dirpath, fn))
                rel = os.path.relpath(dirpath, self.dir)
                if fn == "source.csv":
                    cats["snapshot"] += size
                elif fn in ("rowindex.bin", "rowhash.bin"):
                    cats["row_index"] += size
                elif rel.startswith("columns"):
                    cats["segments"] += size
                elif rel.startswith("staging"):
                    cats["staging"] += size
                elif rel.startswith("fullimport"):
                    cats["fullimport"] += size
                elif rel.startswith("quarantine"):
                    cats["quarantine"] += size
                else:
                    cats["metadata"] += size
        cats["total"] = sum(cats.values())
        return cats

    # -- garbage collection of superseded segments
    def gc(self):
        """Delete segment files not referenced by the current manifest, any
        pinned generation, or the full-import baseline. Returns removed files."""
        referenced = set()
        for gen in self.pinned_gens() | {self.current_gen()}:
            try:
                m = self.manifest(gen)
            except FileNotFoundError:
                continue
            for info in m["columns"].values():
                referenced.add(info["file"])
        removed = []
        cdir = self.p("columns")
        if os.path.isdir(cdir):
            for dirpath, _d, filenames in os.walk(cdir):
                for fn in filenames:
                    rel = os.path.relpath(os.path.join(dirpath, fn), self.dir)
                    if rel not in referenced:
                        os.remove(os.path.join(dirpath, fn))
                        removed.append(rel)
        if removed:
            self.event("gc", removed=removed)
        return removed


def new_dataset_dirs(ds):
    for sub in ("structures", "columns", "staging", "quarantine",
                "manifests", "builds", "fullimport"):
        os.makedirs(ds.p(sub), exist_ok=True)
