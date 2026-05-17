import json
import os
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
