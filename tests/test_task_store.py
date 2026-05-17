import json
import os
import time
from pathlib import Path

import pytest

from social_post_extractor_mcp import task_store as ts


@pytest.fixture
def store(tmp_path, monkeypatch):
    monkeypatch.setenv("AGENT_REACH_HOME", str(tmp_path))
    # task_store reads the env at call time, not import time
    return tmp_path


def test_new_task_writes_queued_file(store):
    task_id = ts.new_task("https://v.douyin.com/abc/", "douyin")
    assert len(task_id) == 32
    data = json.loads(ts.task_path(task_id).read_text())
    assert data["status"] == "queued"
    assert data["platform"] == "douyin"
    assert data["url"] == "https://v.douyin.com/abc/"
    assert data["pid"] is None
    assert data["progress"] == 0.0


def test_write_task_merges_and_is_atomic(store):
    task_id = ts.new_task("u", "youtube")
    ts.write_task(task_id, status="running", pid=4242, progress=0.1)
    data = ts.read_task(task_id)
    assert data["status"] == "running"
    assert data["pid"] == 4242
    assert data["url"] == "u"  # preserved across merge
    assert not list(ts.task_path(task_id).parent.glob("*.tmp"))  # no temp left


def test_read_missing_returns_none(store):
    assert ts.read_task("deadbeef" * 4) is None


def test_gc_stale_deletes_old_and_keeps_fresh(store):
    old = ts.new_task("o", "douyin")
    new = ts.new_task("n", "douyin")
    ts.log_path(old).write_text("log")
    stale = time.time() - 8 * 86400
    os.utime(ts.task_path(old), (stale, stale))
    os.utime(ts.log_path(old), (stale, stale))
    deleted = ts.gc_stale(max_age_sec=7 * 86400)
    assert deleted == 1
    assert not ts.task_path(old).exists()
    assert not ts.log_path(old).exists()
    assert ts.task_path(new).exists()


def test_resolve_unknown_task(store):
    r = ts.resolve_status("nope" * 8)
    assert r["status"] == "error"
    assert "unknown task_id" in r["error"]


def test_resolve_dead_worker(store):
    task_id = ts.new_task("u", "douyin")
    ts.write_task(task_id, status="running", pid=999999, stage="extracting", progress=0.1)
    data = ts.read_task(task_id)
    data["updated_at"] = time.time() - 200
    ts._atomic_write(ts.task_path(task_id), data)
    r = ts.resolve_status(task_id)
    assert r["status"] == "failed"
    assert "worker exited" in r["error"]


def test_resolve_running_fresh_not_marked_dead(store):
    task_id = ts.new_task("u", "douyin")
    ts.write_task(task_id, status="running", pid=999999, stage="extracting", progress=0.1)
    r = ts.resolve_status(task_id)
    assert r["status"] == "running"


def test_resolve_succeeded_passthrough(store):
    task_id = ts.new_task("u", "youtube")
    ts.write_task(task_id, status="succeeded", progress=1.0, transcript="hello",
                  metadata={"title": "t"})
    r = ts.resolve_status(task_id)
    assert r["status"] == "succeeded"
    assert r["transcript"] == "hello"
    assert r["metadata"] == {"title": "t"}
