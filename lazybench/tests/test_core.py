"""End-to-end tests for the lazy-ingestion workbench.

Run from the repo root:  python -m unittest lazybench.tests.test_core -v
"""

import os
import shutil
import tempfile
import threading
import time
import unittest

from .. import baseline
from .. import builder as builder_mod
from .. import demo_data
from .. import ingest
from .. import query as query_mod
from .. import storage as st

ROWS = 20_000


def wait_for(fn, timeout=30, interval=0.02):
    deadline = time.time() + timeout
    while time.time() < deadline:
        v = fn()
        if v:
            return v
        time.sleep(interval)
    raise TimeoutError("condition not met within timeout")


class Base(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.mkdtemp(prefix="lazybench-test-")
        cls.src = os.path.join(cls.tmp, "src.csv")
        demo_data.generate(cls.src, rows=ROWS)

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.tmp, ignore_errors=True)

    def make_ds(self, name, keep_snapshot=True, prefetch=()):
        ds, structure = ingest.create_dataset(
            self.tmp, name, self.src, keep_snapshot=keep_snapshot)
        mgr = builder_mod.BuildManager(ds)
        for col in prefetch:
            b = mgr.start(col, origin="prefetch")
            mgr.wait(b["id"])
        return ds, mgr


class TestImportAndQuery(Base):
    def test_import_only_indexes_rows(self):
        ds, mgr = self.make_ds("t1")
        s = ds.structure()
        self.assertEqual(s["row_count"], ROWS)
        self.assertEqual(len(s["schema"]), len(demo_data.COLUMNS))
        self.assertEqual(ds.manifest()["columns"], {})
        # row index + hashes are tiny next to the snapshot
        storage = ds.storage_breakdown()
        self.assertGreater(storage["snapshot"], 0)
        self.assertEqual(storage["row_index"], ROWS * 16 + 18)
        self.assertEqual(storage["segments"], 0)

    def test_prefetch_and_alignment(self):
        ds, mgr = self.make_ds("t2", prefetch=["id"])
        res = query_mod.query(ds, ["id"], limit=500)
        self.assertEqual(res["returned"], 500)
        for row in res["rows"]:
            # demo data canary: the id column must equal the manifest row id
            self.assertEqual(int(row["values"]["id"]), row["row_id"])
        prov = ds.manifest()["columns"]["id"]
        self.assertEqual(prov["origin"], "prefetch")
        self.assertEqual(prov["structure_version"], 1)
        self.assertEqual(prov["rows"], ROWS)

    def test_missing_column_reported_not_faked(self):
        ds, mgr = self.make_ds("t3", prefetch=["id"])
        res = query_mod.query(ds, ["id", "salary"], limit=5)
        self.assertEqual(res["missing_columns"], ["salary"])
        self.assertEqual(list(res["rows"][0]["values"].keys()), ["id"])


class TestLazyBuild(Base):
    def test_build_publishes_with_provenance(self):
        ds, mgr = self.make_ds("t4")
        b = mgr.start("email")
        mgr.wait(b["id"])
        b = mgr.get(b["id"])
        self.assertEqual(b["status"], "done")
        info = ds.manifest()["columns"]["email"]
        self.assertEqual(info["build_id"], b["id"])
        self.assertEqual(info["source_hash"], ds.structure()["source_hash"])
        self.assertTrue(info["from_snapshot"])
        self.assertGreater(info["duration_ms"], -1)
        res = query_mod.query(ds, ["email"], limit=100)
        self.assertEqual(res["returned"], 100)

    def test_stop_and_resume_is_exact(self):
        ds, mgr = self.make_ds("t5")
        old_chunk = builder_mod.CHUNK_ROWS
        builder_mod.CHUNK_ROWS = 500
        try:
            b = mgr.start("city")
            wait_for(lambda: mgr.get(b["id"])["rows_done"] >= 1000)
            mgr.stop(b["id"])
            wait_for(lambda: mgr.get(b["id"])["status"] == "paused")
            paused = mgr.get(b["id"])
            self.assertLess(paused["rows_done"], ROWS)
            mgr.resume(b["id"])
            wait_for(lambda: mgr.get(b["id"])["status"] == "done")
        finally:
            builder_mod.CHUNK_ROWS = old_chunk
        res = query_mod.query(ds, ["id", "city"], limit=50)
        # resume must not duplicate or skip a single row
        self.assertEqual([r["row_id"] for r in res["rows"]], list(range(50)))
        self.assertEqual(ds.manifest()["columns"]["city"]["rows"], ROWS)

    def test_crash_recovery_marks_build_paused(self):
        ds, mgr = self.make_ds("t6")
        old_chunk = builder_mod.CHUNK_ROWS
        builder_mod.CHUNK_ROWS = 500
        try:
            b = mgr.start("dept")
            wait_for(lambda: mgr.get(b["id"])["rows_done"] >= 500)
            mgr.stop(b["id"])
            wait_for(lambda: mgr.get(b["id"])["status"] == "paused")
            # simulate a restart while "running": flip state back, re-init
            st_ = mgr.get(b["id"]); st_["status"] = "running"
            mgr._save(st_)
            mgr2 = builder_mod.BuildManager(ds)
            self.assertEqual(mgr2.get(b["id"])["status"], "paused")
            mgr2.resume(b["id"])
            wait_for(lambda: mgr2.get(b["id"])["status"] == "done")
        finally:
            builder_mod.CHUNK_ROWS = old_chunk


class TestMutation(Base):
    def test_snapshot_strategy_survives_source_mutation(self):
        ds, mgr = self.make_ds("t7", keep_snapshot=True)
        demo_data.mutate(self.src, "delete")
        b = mgr.start("country")
        mgr.wait(b["id"])
        self.assertEqual(mgr.get(b["id"])["status"], "done")
        res = query_mod.query(ds, ["country"], limit=10)
        self.assertEqual(res["returned"], 10)

    def test_reference_strategy_aborts_on_mutation(self):
        ds, mgr = self.make_ds("t8", keep_snapshot=False)
        demo_data.mutate(self.src, "delete")
        b = mgr.start("country")
        mgr.wait(b["id"])
        b = mgr.get(b["id"])
        self.assertEqual(b["status"], "aborted")
        self.assertIn("changed since import", b["error"])
        self.assertNotIn("country", ds.manifest()["columns"])
        events = [e["type"] for e in ds.events()]
        self.assertIn("mutation_detected", events)

    def test_append_mutation_still_reads_original_rows(self):
        src2 = os.path.join(self.tmp, "src2.csv")
        demo_data.generate(src2, rows=5000)
        ds, structure = ingest.create_dataset(self.tmp, "t9", src2,
                                              keep_snapshot=False)
        mgr = builder_mod.BuildManager(ds)
        demo_data.mutate(src2, "append")  # original rows untouched
        b = mgr.start("id")
        mgr.wait(b["id"])
        self.assertEqual(mgr.get(b["id"])["status"], "done")
        res = query_mod.query(ds, ["id"], limit=5000)
        self.assertTrue(all(int(r["values"]["id"]) == r["row_id"]
                            for r in res["rows"]))


class TestSourceReplacement(Base):
    def test_replace_detects_identical_content(self):
        ds, mgr = self.make_ds("t10")
        same = os.path.join(self.tmp, "same.csv")
        shutil.copyfile(self.src, same)
        r = ingest.replace_source(ds, same)
        self.assertFalse(r["changed"])

    def test_inflight_build_goes_stale_not_published(self):
        ds, mgr = self.make_ds("t11")
        old_chunk = builder_mod.CHUNK_ROWS
        builder_mod.CHUNK_ROWS = 200
        try:
            b = mgr.start("score")
            wait_for(lambda: mgr.get(b["id"])["rows_done"] >= 400)
            new_src = os.path.join(self.tmp, "newsrc.csv")
            demo_data.generate(new_src, rows=ROWS + 3000, seed=99)
            r = ingest.replace_source(ds, new_src)
            self.assertTrue(r["changed"])
            self.assertEqual(r["structure_version"], 2)
            wait_for(lambda: mgr.get(b["id"])["status"] == "stale")
            wait_for(lambda: len(mgr.quarantined()) == 1)
            # the stale segment is quarantined and never published
            self.assertNotIn("score", ds.manifest()["columns"])
            self.assertEqual(ds.manifest()["structure_version"], 2)
            events = [e["type"] for e in ds.events()]
            self.assertIn("build_stale", events)
            self.assertIn("source_replaced", events)
        finally:
            builder_mod.CHUNK_ROWS = old_chunk
        # old columns remain identifiable under sv1
        ds2, mgr2 = self.make_ds("t11b", prefetch=["id"])
        new_src2 = os.path.join(self.tmp, "newsrc2.csv")
        demo_data.generate(new_src2, rows=1000, seed=5)
        ingest.replace_source(ds2, new_src2)
        self.assertEqual(ds2.manifest()["columns"], {})
        # new builds anchor to the new structure version
        b = mgr2.start("id")
        mgr2.wait(b["id"])
        self.assertEqual(ds2.manifest()["columns"]["id"]["structure_version"], 2)
        res = query_mod.query(ds2, ["id"], limit=1000)
        self.assertEqual(res["row_count"], 1000)


class TestFilters(Base):
    def test_filter_applies_at_query_time(self):
        ds, mgr = self.make_ds("t12", prefetch=["id", "salary"])
        all_res = query_mod.query(ds, ["id"], limit=10, count_only=True)
        self.assertEqual(all_res["matched"], ROWS)
        rule = {"column": "salary", "op": "gt", "value": "100000"}
        filtered = query_mod.query(ds, ["id", "salary"], limit=ROWS,
                                   rule=rule, count_only=True)
        self.assertLess(filtered["matched"], ROWS)
        page = query_mod.query(ds, ["id", "salary"], limit=50, rule=rule)
        for r in page["rows"]:
            self.assertGreater(float(r["values"]["salary"]), 100000)
        # changing the rule needs no rebuild and touches no stored data
        before = ds.manifest()["generation"]
        rule2 = {"column": "salary", "op": "le", "value": "100000"}
        other = query_mod.query(ds, ["id"], limit=10, rule=rule2,
                                count_only=True)
        self.assertEqual(filtered["matched"] + other["matched"], ROWS)
        self.assertEqual(ds.manifest()["generation"], before)


class TestFullImportCompare(Base):
    def test_lazy_matches_full_baseline(self):
        ds, mgr = self.make_ds("t13", prefetch=["id"])
        for col in ("email", "salary", "status"):
            mgr.wait(mgr.start(col)["id"])
        baseline.start_full_import(ds)
        wait_for(lambda: (baseline.full_import_status(ds) or {})
                 .get("status") == "done", timeout=120)
        report = baseline.compare(ds)
        # materialized columns must match the baseline exactly; columns not
        # yet backfilled are reported as pending, not as mismatches
        self.assertTrue(report["all_match"],
                        [c for c in report["columns"]
                         if c["status"] == "mismatch"])
        by_status = {}
        for c in report["columns"]:
            by_status[c["status"]] = by_status.get(c["status"], 0) + 1
        self.assertEqual(by_status.get("match"), 4)    # id + 3 backfilled
        self.assertEqual(by_status.get("pending"), len(demo_data.COLUMNS) - 4)
        self.assertEqual(report["pending_count"], len(demo_data.COLUMNS) - 4)
        fi = baseline.full_import_status(ds)
        self.assertGreater(fi["total_ms"], 0)


class TestConcurrency(Base):
    def test_queries_never_see_misaligned_data(self):
        ds, mgr = self.make_ds("t14", prefetch=["id"])
        errors = []
        stop = threading.Event()

        def reader():
            try:
                while not stop.is_set():
                    res = query_mod.query(ds, ["id"], limit=200)
                    for r in res["rows"]:
                        assert int(r["values"]["id"]) == r["row_id"], \
                            f"misaligned row {r}"
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=reader) for _ in range(6)]
        for t in threads:
            t.start()
        # publish columns while queries are in flight
        for col in ("name", "email", "city", "dept"):
            mgr.wait(mgr.start(col)["id"])
        # give readers a few more rounds against later generations
        time.sleep(0.3)
        stop.set()
        for t in threads:
            t.join()
        self.assertEqual(errors, [])
        self.assertGreater(ds.current_gen(), 1)


class TestGC(Base):
    def test_superseded_segments_are_collectable(self):
        ds, mgr = self.make_ds("t15", prefetch=["id"])
        first_file = ds.manifest()["columns"]["id"]["file"]
        self.assertTrue(os.path.exists(ds.p(first_file)))
        # rebuild the same column: delete from manifest then rebuild
        def drop(m):
            del m["columns"]["id"]
        ds.update_manifest(drop)
        b = mgr.start("id")
        mgr.wait(b["id"])
        second_file = ds.manifest()["columns"]["id"]["file"]
        self.assertNotEqual(first_file, second_file)
        # old segment still on disk (identifiable) until GC
        self.assertTrue(os.path.exists(ds.p(first_file)))
        removed = ds.gc()
        self.assertIn(first_file, removed)
        self.assertFalse(os.path.exists(ds.p(first_file)))
        self.assertTrue(os.path.exists(ds.p(second_file)))


if __name__ == "__main__":
    unittest.main()
