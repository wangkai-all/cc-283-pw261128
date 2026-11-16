"""HTTP API + web UI for the lazy-ingestion workbench.

Run:  python -m lazybench.server [--port 8000] [--data ./data]
"""

import argparse
import hashlib
import json
import os
import re
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from . import baseline
from . import builder as builder_mod
from . import demo_data
from . import ingest
from . import query as query_mod
from . import storage as st

STATIC_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "static")


class Registry:
    """Dataset handles + their build managers, created lazily."""

    def __init__(self, root):
        self.root = root
        os.makedirs(root, exist_ok=True)
        self._lock = threading.Lock()
        self._managers = {}
        self._hash_cache = {}

    def list_datasets(self):
        out = []
        for name in sorted(os.listdir(self.root)):
            d = os.path.join(self.root, name)
            if os.path.isfile(os.path.join(d, "manifest.current")):
                ds = st.Dataset(self.root, name)
                s = ds.structure()
                out.append({"name": name, "row_count": s["row_count"],
                            "structure_version": s["version"],
                            "columns": len(s["schema"])})
        return out

    def manager(self, name):
        with self._lock:
            if name not in self._managers:
                ds = st.Dataset(self.root, name)
                if not os.path.isdir(ds.dir):
                    raise FileNotFoundError(f"dataset {name!r}")
                self._managers[name] = builder_mod.BuildManager(ds)
            return self._managers[name]

    def dataset(self, name):
        return self.manager(name).ds

    def external_changed(self, structure):
        """Has the external source file moved since this structure version?"""
        path = structure["source_path"]
        if not os.path.exists(path):
            return {"changed": None, "detail": "source file missing"}
        st_ = os.stat(path)
        key = (path, st_.st_size, int(st_.st_mtime))
        cached = self._hash_cache.get(key)
        if cached is None:
            h = hashlib.blake2b(digest_size=16)
            with open(path, "rb") as f:
                while True:
                    b = f.read(1 << 20)
                    if not b:
                        break
                    h.update(b)
            cached = h.hexdigest()
            self._hash_cache.clear()
            self._hash_cache[key] = cached
        changed = cached != structure["source_hash"]
        return {"changed": changed,
                "detail": ("external source differs from import-time hash"
                           if changed else "external source unchanged")}


REG = None  # set in main()


# ------------------------------------------------------------ overview

def overview(reg, name):
    mgr = reg.manager(name)
    ds = mgr.ds
    manifest = ds.manifest()
    sv = ds.structure()
    builds = mgr.list_builds()
    by_col = {}
    for b in builds:
        if b["status"] in (builder_mod.RUNNING, builder_mod.PAUSED):
            by_col[b["column"]] = b

    columns = []
    for col in sv["schema"]:
        if col in manifest["columns"]:
            info = manifest["columns"][col]
            columns.append({"name": col, "status": "materialized", **info})
        elif col in by_col:
            b = by_col[col]
            columns.append({"name": col, "status": b["status"],
                            "build_id": b["id"], "rows_done": b["rows_done"],
                            "row_count": b["row_count"]})
        else:
            last = next((b for b in reversed(builds) if b["column"] == col),
                        None)
            columns.append({"name": col, "status": "missing",
                            "last_build": ({"status": last["status"],
                                            "error": last["error"]}
                                           if last else None)})

    # columns published under older structure versions (old backfill results,
    # still identifiable, never silently reused)
    previous = []
    mdir = ds.p("manifests")
    best_per_sv = {}
    for fn in os.listdir(mdir):
        m = re.fullmatch(r"gen_(\d+)\.json", fn)
        if not m:
            continue
        with open(os.path.join(mdir, fn)) as f:
            data = json.load(f)
        svno = data["structure_version"]
        if svno not in best_per_sv or data["generation"] > best_per_sv[svno][0]:
            best_per_sv[svno] = (data["generation"], data)
    for svno, (_gen, data) in sorted(best_per_sv.items()):
        if svno == manifest["structure_version"]:
            continue
        for col, info in data["columns"].items():
            previous.append({"name": col, "structure_version": svno,
                             **info})

    return {
        "name": name,
        "current": {
            "structure_version": sv["version"],
            "generation": manifest["generation"],
            "schema": sv["schema"],
            "row_count": sv["row_count"],
            "source_path": sv["source_path"],
            "source_hash": sv["source_hash"],
            "source_size": sv["source_size"],
            "keep_snapshot": sv["keep_snapshot"],
            "has_snapshot": sv["has_snapshot"],
            "created_at": sv["created_at"],
            "note": sv["note"],
        },
        "structures": ds.list_structures(),
        "columns": columns,
        "previous_columns": previous,
        "builds": builds,
        "quarantine": mgr.quarantined(),
        "storage": ds.storage_breakdown(),
        "events": ds.events(80),
        "mutation": reg.external_changed(sv),
        "full_import": baseline.full_import_status(ds),
        "filters": query_mod.load_filters(ds),
    }


# ------------------------------------------------------------ http layer

class Handler(BaseHTTPRequestHandler):
    server_version = "lazybench/1.0"
    protocol_version = "HTTP/1.1"

    def log_message(self, *args):
        pass

    # -- helpers
    def _send(self, code, obj, ctype="application/json"):
        body = (json.dumps(obj) if isinstance(obj, (dict, list))
                else obj).encode() if isinstance(obj, (dict, list)) else obj
        if isinstance(body, str):
            body = body.encode()
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _ok(self, obj, code=200):
        self._send(code, obj)

    def _err(self, code, e):
        msg = f"{type(e).__name__}: {e}"
        self._send(code, {"error": msg})

    def _body(self):
        n = int(self.headers.get("Content-Length") or 0)
        if not n:
            return {}
        return json.loads(self.rfile.read(n))

    # -- routing
    ROUTES = []

    def do_GET(self):
        self._route("GET")

    def do_POST(self):
        self._route("POST")

    def _route(self, method):
        try:
            for m, pattern, fn in self.ROUTES:
                if m != method:
                    continue
                match = re.fullmatch(pattern, self.path.split("?")[0])
                if match:
                    return fn(self, **match.groupdict())
            self._send(404, {"error": f"no route {method} {self.path}"})
        except (ValueError, FileNotFoundError, FileExistsError) as e:
            self._err(400, e)
        except Exception as e:
            self._err(500, e)


def route(method, pattern):
    def deco(fn):
        Handler.ROUTES.append((method, pattern, fn))
        return fn
    return deco


# ------------------------------------------------------------ endpoints

@route("GET", r"/")
def index(h):
    with open(os.path.join(STATIC_DIR, "index.html"), "rb") as f:
        h._send(200, f.read(), "text/html; charset=utf-8")


@route("GET", r"/api/datasets")
def list_datasets(h):
    h._ok({"datasets": REG.list_datasets()})


@route("POST", r"/api/datasets")
def create_dataset(h):
    body = h._body()
    name = body["name"]
    if not re.fullmatch(r"[A-Za-z0-9_.-]+", name):
        raise ValueError("dataset name must be [A-Za-z0-9_.-]+")
    ds, structure = ingest.create_dataset(
        REG.root, name, body["path"],
        keep_snapshot=bool(body.get("keep_snapshot", True)),
    )
    # optional up-front extra columns ("read a few more columns at once")
    prefetch = [c for c in body.get("prefetch", []) if c in structure["schema"]]
    if prefetch:
        mgr = REG.manager(name)
        ids = [mgr.start(c, origin="prefetch")["id"] for c in prefetch]
        for bid in ids:
            mgr.wait(bid)
        ds.event("prefetch_done", columns=prefetch)
    h._ok(overview(REG, name))


@route("GET", r"/api/datasets/(?P<name>[^/]+)")
def get_overview(h, name):
    h._ok(overview(REG, name))


@route("POST", r"/api/datasets/(?P<name>[^/]+)/columns/(?P<col>[^/]+)/build")
def build_column(h, name, col):
    b = REG.manager(name).start(col, origin="lazy")
    h._ok(b)


@route("POST", r"/api/datasets/(?P<name>[^/]+)/builds/(?P<bid>[^/]+)/stop")
def stop_build(h, name, bid):
    h._ok(REG.manager(name).stop(bid))


@route("POST", r"/api/datasets/(?P<name>[^/]+)/builds/(?P<bid>[^/]+)/resume")
def resume_build(h, name, bid):
    h._ok(REG.manager(name).resume(bid))


@route("POST", r"/api/datasets/(?P<name>[^/]+)/source")
def replace_source(h, name):
    body = h._body()
    ds = REG.dataset(name)
    result = ingest.replace_source(ds, body["path"],
                                   body.get("keep_snapshot"))
    if result.get("changed"):
        # the active filter may reference a column that does not exist (yet)
        # in the new structure version; deactivate rather than break queries
        filters = query_mod.load_filters(ds)
        if filters.get("active_id") is not None:
            filters["active_id"] = None
            query_mod.save_filters(ds, filters)
            ds.event("filter_changed", active=None,
                     note="deactivated on source replacement")
    h._ok(result)


@route("POST", r"/api/datasets/(?P<name>[^/]+)/query")
def run_query(h, name):
    body = h._body()
    ds = REG.dataset(name)
    rule = body.get("rule")
    if rule is None:
        filters = query_mod.load_filters(ds)
        if filters.get("active_id") is not None:
            rule = next((r for r in filters["rules"]
                         if r["id"] == filters["active_id"]), None)
    h._ok(query_mod.query(
        ds, body.get("columns", []),
        limit=int(body.get("limit", 100)),
        offset=int(body.get("offset", 0)),
        rule=rule, count_only=bool(body.get("count_only", False))))


@route("POST", r"/api/datasets/(?P<name>[^/]+)/filters")
def add_filter(h, name):
    body = h._body()
    ds = REG.dataset(name)
    filters = query_mod.load_filters(ds)
    rid = "f" + hashlib.blake2b(
        json.dumps(body, sort_keys=True).encode(), digest_size=4).hexdigest()
    rule = {"id": rid, "column": body["column"], "op": body["op"],
            "value": str(body["value"])}
    filters["rules"] = [r for r in filters["rules"] if r["id"] != rid]
    filters["rules"].append(rule)
    if body.get("activate", True):
        filters["active_id"] = rid
    query_mod.save_filters(ds, filters)
    ds.event("filter_changed", rule=rule, active=filters["active_id"])
    h._ok(filters)


@route("POST", r"/api/datasets/(?P<name>[^/]+)/filters/activate")
def activate_filter(h, name):
    body = h._body()
    ds = REG.dataset(name)
    filters = query_mod.load_filters(ds)
    filters["active_id"] = body.get("id")
    query_mod.save_filters(ds, filters)
    ds.event("filter_changed", active=filters["active_id"])
    h._ok(filters)


@route("POST", r"/api/datasets/(?P<name>[^/]+)/full-import")
def full_import(h, name):
    h._ok(baseline.start_full_import(REG.dataset(name)))


@route("GET", r"/api/datasets/(?P<name>[^/]+)/compare")
def compare(h, name):
    h._ok(baseline.compare(REG.dataset(name)))


@route("POST", r"/api/datasets/(?P<name>[^/]+)/gc")
def gc(h, name):
    h._ok({"removed": REG.dataset(name).gc()})


@route("POST", r"/api/demo/source")
def demo_source(h):
    body = h._body()
    path = body.get("path") or os.path.join(REG.root, "_sources", "demo.csv")
    h._ok(demo_data.generate(path, rows=int(body.get("rows", 100_000))))


@route("POST", r"/api/demo/mutate")
def demo_mutate(h):
    body = h._body()
    h._ok(demo_data.mutate(body["path"], body["mode"]))


def main():
    global REG
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=8000)
    ap.add_argument("--data", default="./data")
    args = ap.parse_args()
    REG = Registry(os.path.abspath(args.data))
    srv = ThreadingHTTPServer(("127.0.0.1", args.port), Handler)
    print(f"lazybench on http://127.0.0.1:{args.port}  (data: {REG.root})")
    srv.serve_forever()


if __name__ == "__main__":
    main()
