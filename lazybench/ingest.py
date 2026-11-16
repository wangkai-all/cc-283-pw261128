"""Ingestion: structure versions, row manifests, source snapshots, full import.

A *structure version* captures the shape and identity of the source at import
time: schema, row count, per-row file offsets (row positioning) and per-row
content hashes (row identity). All later column backfills are anchored to a
structure version, never to the live file's current line numbers.
"""

import csv
import json
import os
import shutil

from . import storage as st


def _norm_line(line):
    if line.endswith(b"\n"):
        line = line[:-1]
    if line.endswith(b"\r"):
        line = line[:-1]
    return line


def _count_csv_records(path):
    """Count logical CSV records (handles quoted newlines) for validation."""
    with open(path, newline="", encoding="utf-8") as f:
        return sum(1 for _ in csv.reader(f))


def build_structure_version(ds, sv, source_path, keep_snapshot, note):
    """Index + hash every row of source_path into structures/sv_<n>/."""
    sv_dir = ds.sv_dir(sv)
    os.makedirs(sv_dir, exist_ok=True)

    src_hash = st.file_hash(source_path)
    size = os.path.getsize(source_path)

    offsets = []
    hashes = []
    with open(source_path, "rb") as f:
        header = _norm_line(f.readline())
        schema = next(csv.reader([header.decode("utf-8")]))
        while True:
            off = f.tell()
            line = f.readline()
            if not line:
                break
            line = _norm_line(line)
            if line == b"":
                continue  # tolerate trailing blank line
            offsets.append(off)
            hashes.append(st.row_hash(line))

    # Reject files whose records span multiple physical lines: the row index
    # is line-based, so quoted embedded newlines would break positioning.
    if _count_csv_records(source_path) != len(offsets) + 1:
        raise ValueError(
            "CSV contains quoted embedded newlines; row indexing requires "
            "one record per physical line")

    st.write_rowindex(ds.rowindex_path(sv), offsets)
    st.write_rowhashes(ds.rowhash_path(sv), hashes)

    snapshot = None
    if keep_snapshot:
        snapshot = ds.snapshot_path(sv)
        shutil.copyfile(source_path, snapshot)

    structure = {
        "version": sv,
        "schema": schema,
        "row_count": len(offsets),
        "source_path": os.path.abspath(source_path),
        "source_hash": src_hash,
        "source_size": size,
        "keep_snapshot": keep_snapshot,
        "has_snapshot": keep_snapshot,
        "row_hashes_checksum": st.rowhash_checksum(ds.rowhash_path(sv)),
        "created_at": st.now_ms(),
        "note": note,
    }
    with open(ds.structure_path(sv), "w") as f:
        json.dump(structure, f, indent=1)
    return structure


def create_dataset(root, name, source_path, keep_snapshot=True, prefetch=()):
    """Import a source file: only row index + hashes (+ optional snapshot).

    No data column is materialized unless listed in `prefetch` (the
    "read a few extra columns up front" strategy knob).
    """
    if not os.path.isfile(source_path):
        raise FileNotFoundError(source_path)
    ds = st.Dataset(root, name)
    if os.path.exists(ds.dir):
        raise FileExistsError(f"dataset {name!r} already exists")
    st.new_dataset_dirs(ds)

    t0 = st.now_ms()
    structure = build_structure_version(ds, 1, source_path, keep_snapshot,
                                        note="initial import")
    ds.set_current_sv(1)

    # generation 0: no materialized columns yet
    manifest = {
        "generation": 0,
        "dataset": name,
        "structure_version": 1,
        "published_at": st.now_ms(),
        "columns": {},
    }
    with open(ds.p("manifests", "gen_0.json"), "w") as f:
        json.dump(manifest, f, indent=1)
    with open(ds.p("manifest.current"), "w") as f:
        f.write("0")

    with open(ds.p("filters.json"), "w") as f:
        json.dump({"rules": [], "active_id": None}, f)

    ds.event("import", structure_version=1, rows=structure["row_count"],
             columns=len(structure["schema"]), keep_snapshot=keep_snapshot,
             source_hash=structure["source_hash"],
             elapsed_ms=st.now_ms() - t0)
    return ds, structure


def replace_source(ds, new_path, keep_snapshot=None):
    """Install a replacement source as a new structure version.

    Old columns stay published under their old structure version and remain
    identifiable; in-flight builds against the old version will be marked
    stale at publish time instead of publishing misaligned data.
    """
    if not os.path.isfile(new_path):
        raise FileNotFoundError(new_path)
    cur = ds.structure()
    new_hash = st.file_hash(new_path)
    if new_hash == cur["source_hash"]:
        return {"changed": False, "structure_version": cur["version"]}

    sv = cur["version"] + 1
    ks = cur["keep_snapshot"] if keep_snapshot is None else keep_snapshot
    t0 = st.now_ms()
    structure = build_structure_version(ds, sv, new_path, ks,
                                        note=f"source replaced (was sv{cur['version']})")
    ds.set_current_sv(sv)

    # new generation with an empty column set for the new structure version
    def reset(m):
        m["structure_version"] = sv
        m["columns"] = {}
    gen = ds.update_manifest(reset)

    ds.event("source_replaced", old_version=cur["version"], new_version=sv,
             old_hash=cur["source_hash"], new_hash=new_hash,
             old_rows=cur["row_count"], new_rows=structure["row_count"],
             generation=gen, elapsed_ms=st.now_ms() - t0)
    return {"changed": True, "structure_version": sv, "generation": gen,
            "old_rows": cur["row_count"], "new_rows": structure["row_count"]}


def open_row_source(ds, sv):
    """Return the path to read rows from for a structure version.

    Prefers the immutable snapshot; falls back to the external source (whose
    rows will be hash-verified against the row manifest on every read).
    """
    snap = ds.snapshot_path(sv)
    if os.path.exists(snap):
        return snap, True
    return ds.structure(sv)["source_path"], False


def read_rows(ds, sv, source_file, start, stop):
    """Read raw row bytes [start, stop) via stored offsets and verify hashes.

    Raises MutationError if any row's content hash differs from the import
    manifest — i.e. the source moved under us and line numbers no longer
    identify the same rows.
    """
    idx = ds.rowindex_path(sv)
    n = st.rowindex_count(idx)
    if start >= n:
        return []
    stop = min(stop, n)
    off_start, off_end = st.read_offsets(idx, start, stop)
    if off_end is None:
        # last chunk ends at the import-time EOF, not the file's current
        # size — an appended-to source must not leak extra rows into the read
        off_end = ds.structure(sv)["source_size"]
    with open(source_file, "rb") as f:
        f.seek(off_start)
        buf = f.read(off_end - off_start)
    lines = buf.split(b"\n")
    if lines and lines[-1] == b"":
        lines.pop()
    if len(lines) != stop - start:
        raise MutationError(
            f"expected {stop - start} rows at [{start},{stop}), "
            f"got {len(lines)} — source file changed since import")
    out = []
    rh = ds.rowhash_path(sv)
    for k, line in enumerate(lines):
        line = _norm_line(line)
        if st.row_hash(line) != st.read_rowhash(rh, start + k):
            raise MutationError(
                f"row {start + k} hash mismatch — source file changed since "
                f"import; refusing to backfill from shifted line numbers")
        out.append(line)
    return out


class MutationError(Exception):
    """The source no longer matches the row manifest of this structure version."""
