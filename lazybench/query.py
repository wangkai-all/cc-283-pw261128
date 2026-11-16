"""Query engine: snapshot-isolated point reads over materialized columns.

A query pins the manifest generation active when it starts, opens only the
column segments it needs, and applies row filter rules at read time. Filter
changes never rewrite stored data; concurrent publishes never affect a query
already in flight, because segments are immutable and generations are
reference-counted for GC.

Execution model: the filter column is scanned sequentially (fast, one pass);
values for the returned page are fetched by row id through each segment's
offset table. `matched` is always the true total match count.
"""

import json
import os

from . import storage as st

OPS = {
    "eq": lambda a, b: a == b,
    "ne": lambda a, b: a != b,
    "lt": lambda a, b: a < b,
    "le": lambda a, b: a <= b,
    "gt": lambda a, b: a > b,
    "ge": lambda a, b: a >= b,
    "contains": lambda a, b: b in a,
}


def _coerce(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return value


def match(value, op, target):
    if value is None:
        return False
    if op == "contains":
        return target in value
    a, b = _coerce(value), _coerce(target)
    if type(a) is not type(b):
        a, b = str(value), str(target)
    return OPS[op](a, b)


def load_filters(ds):
    path = ds.p("filters.json")
    if not os.path.exists(path):
        return {"rules": [], "active_id": None}
    with open(path) as f:
        return json.load(f)


def save_filters(ds, filters):
    tmp = ds.p("filters.json.tmp")
    with open(tmp, "w") as f:
        json.dump(filters, f, indent=1)
    os.replace(tmp, ds.p("filters.json"))


def query(ds, columns, limit=100, offset=0, rule=None, count_only=False):
    """Run a query against a pinned manifest generation.

    Returns rows with stable row_ids, the generation actually read, the true
    total match count, and any requested columns that are not materialized
    yet (listed, never faked).
    """
    t0 = st.now_ms()
    gen = ds.acquire()
    try:
        manifest = ds.manifest(gen)
        structure = ds.structure(manifest["structure_version"])
        available = manifest["columns"]
        missing = [c for c in columns if c not in available]
        wanted = [c for c in columns if c in available]

        readers = {}
        try:
            for c in wanted:
                readers[c] = st.SegReader(ds.p(available[c]["file"]))
            if rule is not None:
                fcol = rule["column"]
                if fcol not in available:
                    raise ValueError(
                        f"filter column {fcol!r} is not materialized")
                if fcol not in readers:
                    readers[fcol] = st.SegReader(ds.p(available[fcol]["file"]))

            row_count = structure["row_count"]
            page_ids = []
            matched = 0
            if rule is not None:
                for i, v in readers[rule["column"]].scan():
                    if match(v, rule["op"], rule["value"]):
                        if matched >= offset and len(page_ids) < limit:
                            page_ids.append(i)
                        matched += 1
            else:
                matched = row_count
                if not count_only:
                    page_ids = list(range(offset, min(offset + limit,
                                                      row_count)))
            rows = [] if count_only else [
                {"row_id": i,
                 "values": {c: readers[c].row(i) for c in wanted}}
                for i in page_ids
            ]
        finally:
            for r in readers.values():
                r.close()

        return {
            "generation": gen,
            "structure_version": manifest["structure_version"],
            "row_count": row_count,
            "matched": matched,
            "returned": len(rows),
            "rows": rows,
            "missing_columns": missing,
            "filter": rule,
            "elapsed_ms": st.now_ms() - t0,
        }
    finally:
        ds.release(gen)
