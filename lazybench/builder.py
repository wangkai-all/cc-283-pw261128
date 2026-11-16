"""Column build tasks: chunked, stoppable, resumable, atomically published.

Safety rules enforced here:

* A build reads rows only through the structure-version row index and
  verifies every row hash, so it can never publish values joined against a
  mutated file's shifted line numbers.
* A build publishes only if the dataset is still on the same structure
  version (and source hash) it started against. Otherwise the finished
  segment is moved to quarantine and the build is marked `stale` — old
  backfill results stay identifiable and are never silently adopted.
* Publishing is an atomic manifest swap; concurrent queries pin the
  generation they started on and never see a half-written column.
"""

import csv
import json
import os
import threading
import uuid

from . import ingest
from . import storage as st

CHUNK_ROWS = 5000

# build statuses
RUNNING = "running"
PAUSED = "paused"
DONE = "done"
STALE = "stale"      # finished against a superseded structure version
ABORTED = "aborted"  # failed (e.g. source mutation detected)


class BuildManager:
    def __init__(self, ds):
        self.ds = ds
        self._lock = threading.Lock()
        self._threads = {}      # build_id -> Thread
        self._stop_flags = {}   # build_id -> threading.Event
        # crash recovery: builds that were running when we last stopped are
        # resumable from their staging-file checkpoint
        for b in self.list_builds():
            if b["status"] == RUNNING:
                b["status"] = PAUSED
                b["note"] = "interrupted by restart; resumable"
                self._save(b)

    # ------------------------------------------------------------- state
    def _path(self, build_id):
        return self.ds.p("builds", f"{build_id}.json")

    def _save(self, b):
        b["updated_at"] = st.now_ms()
        tmp = self._path(b["id"]) + ".tmp"
        with open(tmp, "w") as f:
            json.dump(b, f, indent=1)
        os.replace(tmp, self._path(b["id"]))

    def get(self, build_id):
        with open(self._path(build_id)) as f:
            return json.load(f)

    def list_builds(self):
        out = []
        d = self.ds.p("builds")
        if os.path.isdir(d):
            for fn in sorted(os.listdir(d)):
                if fn.endswith(".json"):
                    with open(os.path.join(d, fn)) as f:
                        out.append(json.load(f))
        out.sort(key=lambda b: b["created_at"])
        return out

    # ------------------------------------------------------------- control
    def start(self, column, origin="lazy"):
        sv = self.ds.structure()
        if column not in sv["schema"]:
            raise ValueError(f"unknown column {column!r}")
        manifest = self.ds.manifest()
        if column in manifest["columns"]:
            raise ValueError(f"column {column!r} already materialized")
        for b in self.list_builds():
            if b["column"] == column and b["status"] in (RUNNING, PAUSED):
                raise ValueError(f"column {column!r} already has an active build")
        build_id = "b" + uuid.uuid4().hex[:12]
        b = {
            "id": build_id,
            "dataset": self.ds.name,
            "column": column,
            "origin": origin,  # lazy | prefetch | full
            "structure_version": sv["version"],
            "source_hash": sv["source_hash"],
            "status": RUNNING,
            "rows_done": 0,
            "row_count": sv["row_count"],
            "created_at": st.now_ms(),
            "updated_at": st.now_ms(),
            "started_at": st.now_ms(),
            "finished_at": None,
            "duration_ms": None,
            "error": None,
        }
        self._save(b)
        self.ds.event("build_start", build_id=build_id, column=column,
                      origin=origin, structure_version=sv["version"])
        self._spawn(build_id)
        return b

    def stop(self, build_id):
        b = self.get(build_id)
        if b["status"] != RUNNING:
            return b
        flag = self._stop_flags.get(build_id)
        if flag:
            flag.set()
        return b

    def wait(self, build_id, timeout=None):
        t = self._threads.get(build_id)
        if t:
            t.join(timeout)
        return self.get(build_id)

    def resume(self, build_id):
        b = self.get(build_id)
        if b["status"] != PAUSED:
            raise ValueError(f"build {build_id} is {b['status']}, not paused")
        # refuse to resume work anchored to a superseded structure version
        if b["structure_version"] != self.ds.current_sv():
            b["status"] = STALE
            b["note"] = "structure version moved on; cannot resume"
            self._save(b)
            self._quarantine(b)
            raise ValueError("build belongs to a superseded structure version")
        b["status"] = RUNNING
        self._save(b)
        self.ds.event("build_resumed", build_id=build_id, column=b["column"],
                      rows_done=b["rows_done"])
        self._spawn(build_id)
        return b

    def _spawn(self, build_id):
        with self._lock:
            old = self._threads.get(build_id)
            if old and old.is_alive():
                return
            flag = threading.Event()
            self._stop_flags[build_id] = flag
            t = threading.Thread(target=self._run, args=(build_id, flag),
                                 daemon=True, name=f"build-{build_id}")
            self._threads[build_id] = t
            t.start()

    # ------------------------------------------------------------- worker
    def _run(self, build_id, stop_flag):
        ds = self.ds
        b = self.get(build_id)
        sv = b["structure_version"]
        structure = ds.structure(sv)
        col_idx = structure["schema"].index(b["column"])
        row_count = structure["row_count"]
        source_path, from_snapshot = ingest.open_row_source(ds, sv)
        staging = ds.p("staging", f"{build_id}.segpart")

        # resume position comes from the staging file itself
        if os.path.exists(staging):
            rows_done, values_end = st.seg_scan(staging)
            with open(staging, "r+b") as f:
                f.truncate(values_end)
            f = open(staging, "ab")
        else:
            rows_done = 0
            w = st.SegWriter(staging, b["column"])
            w.close()
            f = open(staging, "ab")

        def append(value):
            if value is None:
                f.write(b"\xff\xff\xff\xff")
            else:
                raw = value.encode("utf-8")
                f.write(len(raw).to_bytes(4, "little"))
                f.write(raw)

        try:
            while rows_done < row_count:
                if stop_flag.is_set():
                    f.flush()
                    f.close()
                    b = self.get(build_id)
                    b["status"] = PAUSED
                    b["rows_done"] = rows_done
                    self._save(b)
                    ds.event("build_paused", build_id=build_id,
                             column=b["column"], rows_done=rows_done)
                    return
                stop = min(rows_done + CHUNK_ROWS, row_count)
                lines = ingest.read_rows(ds, sv, source_path, rows_done, stop)
                for line in lines:
                    fields = next(csv.reader([line.decode("utf-8")]))
                    append(fields[col_idx] if col_idx < len(fields) else None)
                rows_done = stop
                f.flush()
                b = self.get(build_id)
                b["rows_done"] = rows_done
                self._save(b)
            f.close()

            checksum = st.seg_finalize(staging, row_count)
            self._publish(build_id, sv, structure, staging, checksum,
                          from_snapshot)
        except ingest.MutationError as e:
            f.close()
            b = self.get(build_id)
            b["status"] = ABORTED
            b["error"] = str(e)
            b["finished_at"] = st.now_ms()
            self._save(b)
            ds.event("mutation_detected", build_id=build_id,
                     column=b["column"], error=str(e))
        except Exception as e:  # keep the task state inspectable on any failure
            f.close()
            b = self.get(build_id)
            b["status"] = ABORTED
            b["error"] = f"{type(e).__name__}: {e}"
            b["finished_at"] = st.now_ms()
            self._save(b)
            ds.event("build_aborted", build_id=build_id, column=b["column"],
                     error=b["error"])

    def _publish(self, build_id, sv, structure, staging, checksum,
                 from_snapshot):
        ds = self.ds
        b = self.get(build_id)
        cur_sv = ds.current_sv()
        cur_hash = ds.structure()["source_hash"]
        if sv != cur_sv or b["source_hash"] != cur_hash:
            # the world moved while we were building: quarantine, never publish
            b["status"] = STALE
            b["finished_at"] = st.now_ms()
            b["note"] = (f"built against sv{sv} but dataset is now sv{cur_sv}")
            self._save(b)
            self._quarantine(b)
            return

        final_rel = os.path.join("columns", b["column"], f"{build_id}.seg")
        final_abs = ds.p(final_rel)
        os.makedirs(os.path.dirname(final_abs), exist_ok=True)
        os.replace(staging, final_abs)
        size = os.path.getsize(final_abs)
        duration = st.now_ms() - b["started_at"]

        def add(m):
            m["columns"][b["column"]] = {
                "file": final_rel,
                "rows": b["row_count"],
                "bytes": size,
                "checksum": checksum,
                "build_id": build_id,
                "origin": b["origin"],
                "structure_version": sv,
                "source_hash": b["source_hash"],
                "from_snapshot": from_snapshot,
                "built_at": st.now_ms(),
                "duration_ms": duration,
            }
        gen = ds.update_manifest(add)

        b["status"] = DONE
        b["finished_at"] = st.now_ms()
        b["duration_ms"] = duration
        b["published_generation"] = gen
        self._save(b)
        ds.event("build_published", build_id=build_id, column=b["column"],
                 origin=b["origin"], generation=gen, structure_version=sv,
                 duration_ms=duration, bytes=size)

    def _quarantine(self, b):
        ds = self.ds
        # audit first: the event is the durable signal that this build lost
        # the structure-version race
        ds.event("build_stale", build_id=b["id"], column=b["column"],
                 structure_version=b["structure_version"],
                 reason=b.get("note", ""))
        staging = ds.p("staging", f"{b['id']}.segpart")
        if os.path.exists(staging):
            dst = ds.p("quarantine", f"{b['id']}.seg")
            os.replace(staging, dst)
            b["quarantined_to"] = os.path.relpath(dst, ds.dir)
            self._save(b)

    def quarantined(self):
        out = []
        d = self.ds.p("quarantine")
        if os.path.isdir(d):
            for fn in sorted(os.listdir(d)):
                out.append({"file": os.path.join("quarantine", fn),
                            "bytes": os.path.getsize(os.path.join(d, fn))})
        return out
