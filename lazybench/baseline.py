"""Eager full-import baseline and lazy-vs-full comparison.

The full import materializes every schema column in one pass into an
isolated `fullimport/` area. It is the ground truth the lazy backfills are
compared against: per-column row counts and content checksums must match
exactly, and mismatches are reported with the exact row ids involved.
"""

import csv
import json
import os
import threading

from . import ingest
from . import storage as st


def _status_path(ds):
    return ds.p("fullimport", "status.json")


def full_import_status(ds):
    if not os.path.exists(_status_path(ds)):
        return None
    with open(_status_path(ds)) as f:
        return json.load(f)


def _save_status(ds, s):
    tmp = _status_path(ds) + ".tmp"
    with open(tmp, "w") as f:
        json.dump(s, f, indent=1)
    os.replace(tmp, _status_path(ds))


def start_full_import(ds):
    cur = full_import_status(ds)
    if cur and cur.get("status") == "running":
        raise ValueError("full import already running")
    structure = ds.structure()
    sv = structure["version"]
    status = {
        "status": "running",
        "structure_version": sv,
        "source_hash": structure["source_hash"],
        "row_count": structure["row_count"],
        "columns_total": len(structure["schema"]),
        "columns_done": 0,
        "current_column": None,
        "started_at": st.now_ms(),
        "finished_at": None,
        "total_ms": None,
        "columns": {},
    }
    _save_status(ds, status)
    t = threading.Thread(target=_run_full_import, args=(ds, sv),
                         daemon=True, name=f"fullimport-{ds.name}")
    t.start()
    return status


def _run_full_import(ds, sv):
    structure = ds.structure(sv)
    source_path, _ = ingest.open_row_source(ds, sv)
    row_count = structure["row_count"]
    cdir = ds.p("fullimport", "columns")
    os.makedirs(cdir, exist_ok=True)
    t0 = st.now_ms()
    try:
        for col_idx, col in enumerate(structure["schema"]):
            status = full_import_status(ds)
            status["current_column"] = col
            _save_status(ds, status)
            ct = st.now_ms()
            part = os.path.join(cdir, f"{col}.segpart")
            w = st.SegWriter(part, col)
            done = 0
            while done < row_count:
                stop = min(done + 5000, row_count)
                lines = ingest.read_rows(ds, sv, source_path, done, stop)
                for line in lines:
                    fields = next(csv.reader([line.decode("utf-8")]))
                    w.append(fields[col_idx] if col_idx < len(fields) else None)
                done = stop
            w.close()
            checksum = st.seg_finalize(part, row_count)
            final = os.path.join(cdir, f"{col}.seg")
            os.replace(part, final)
            status = full_import_status(ds)
            status["columns"][col] = {
                "file": os.path.relpath(final, ds.dir),
                "rows": row_count,
                "bytes": os.path.getsize(final),
                "checksum": checksum,
                "duration_ms": st.now_ms() - ct,
            }
            status["columns_done"] += 1
            _save_status(ds, status)
        status = full_import_status(ds)
        status["status"] = "done"
        status["finished_at"] = st.now_ms()
        status["total_ms"] = status["finished_at"] - t0
        _save_status(ds, status)
        ds.event("full_import", structure_version=sv,
                 columns=status["columns_done"],
                 total_ms=status["total_ms"])
    except Exception as e:
        status = full_import_status(ds)
        status["status"] = "aborted"
        status["error"] = f"{type(e).__name__}: {e}"
        status["finished_at"] = st.now_ms()
        _save_status(ds, status)
        ds.event("full_import_aborted", error=status["error"])


def compare(ds, sample_mismatches=5):
    """Compare lazily built columns against the full-import baseline."""
    manifest = ds.manifest()
    full = full_import_status(ds)
    if not full or full.get("status") != "done":
        raise ValueError("no completed full import to compare against")
    if full["structure_version"] != manifest["structure_version"]:
        raise ValueError(
            f"full import was built against sv{full['structure_version']}, "
            f"dataset is now on sv{manifest['structure_version']}; "
            f"re-run the full import")

    lazy_cols = manifest["columns"]
    full_cols = full["columns"]
    report = {"structure_version": manifest["structure_version"],
              "columns": [], "all_match": True, "mismatch_count": 0,
              "pending_count": 0}
    for col in sorted(set(lazy_cols) | set(full_cols)):
        entry = {"column": col,
                 "lazy": col in lazy_cols, "full": col in full_cols}
        if col in lazy_cols and col in full_cols:
            l, f = lazy_cols[col], full_cols[col]
            entry["lazy_checksum"] = l["checksum"]
            entry["full_checksum"] = f["checksum"]
            entry["lazy_rows"] = l["rows"]
            entry["full_rows"] = f["rows"]
            if l["checksum"] == f["checksum"] and l["rows"] == f["rows"]:
                entry["status"] = "match"
            else:
                entry["status"] = "mismatch"
                entry["mismatches"] = _sample_mismatches(
                    ds, l, f, sample_mismatches)
                report["all_match"] = False
                report["mismatch_count"] += 1
        else:
            # not yet backfilled (or only in baseline): pending, not an error
            entry["status"] = "pending"
            report["pending_count"] += 1
        report["columns"].append(entry)
    return report


def _sample_mismatches(ds, lazy_info, full_info, limit):
    out = []
    lr = st.SegReader(ds.p(lazy_info["file"]))
    fr = st.SegReader(ds.p(full_info["file"]))
    try:
        for i in range(min(lr.rows, fr.rows)):
            lv, fv = lr.row(i), fr.row(i)
            if lv != fv:
                out.append({"row_id": i, "lazy": lv, "full": fv})
                if len(out) >= limit:
                    break
    finally:
        lr.close()
        fr.close()
    return out
