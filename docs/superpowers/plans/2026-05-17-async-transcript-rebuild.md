# Async Transcript Rebuild Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the five long-running MCP transcript tools with an async submit/poll pair (plus one blocking convenience tool) backed by a detached worker + on-disk task store, so transcription works through `mcporter call` despite its 60s stdio limit.

**Architecture:** `submit_transcript` does <1s of work (URL→platform regex, write task JSON, spawn a fully detached `python -m social_post_extractor_mcp.worker <task_id>` via `Popen(start_new_session=True)`, return `task_id`). The worker process runs the **existing, unmodified** extraction logic and writes progress/result atomically into `~/.agent-reach/tasks/<task_id>.json`. `get_transcript` just reads that file and applies a dead-worker rule.

**Tech Stack:** Python 3.10+, `mcp` FastMCP, stdlib `subprocess`/`os`/`json`/`pathlib`, `pytest`/`unittest`. No new dependencies.

**Spec:** `docs/superpowers/specs/2026-05-17-async-transcript-rebuild-design.md`

---

## File Structure

- **Create** `social_post_extractor_mcp/task_store.py` — task file CRUD (atomic write, read, GC, dead-worker resolution). Pure stdlib, no network, no extraction imports. ~120 lines.
- **Create** `social_post_extractor_mcp/worker.py` — detached worker entrypoint. Loads env, routes by stored platform, calls existing extraction, writes result. ~90 lines.
- **Create** `tests/test_task_store.py`, `tests/test_worker.py`, `tests/test_async_tools.py`.
- **Modify** `social_post_extractor_mcp/server.py` — remove 5 tools, add 3, update guide prompt.
- **Modify** `tests/test_social_extractor.py:156` — fix pre-existing wrong assertion (blocks the pytest gate).

**Routing note (refines spec):** `submit_transcript` detects platform by **regex on the URL string only** — it must NOT call `parse_social_post` (that does a network fetch and would blow the <1s budget). The worker calls the real pipeline.

**Progress granularity (refines spec):** the existing extraction functions are monolithic with no progress hooks and the spec freezes them. The worker therefore writes **coarse** progress only: `queued`(0.0) → `running`/stage `"extracting"`(0.1) → `succeeded`(1.0) | `failed`. The granular stage list in the spec is aspirational and explicitly out of scope here.

**Worker arg (refines spec):** entrypoint takes `<task_id>` (not `<task_file>`); the path is derived inside the worker. Cleaner and avoids path-quoting in `Popen`.

---

## Task 1: Fix pre-existing broken test (unblock the pytest gate)

**Files:**
- Modify: `tests/test_social_extractor.py:156`

- [ ] **Step 1: Run the failing test to confirm the pre-existing bug**

Run: `cd ~/.agent-reach/tools/social-post-extractor-mcp && .venv/bin/python -m pytest tests/test_social_extractor.py -k DefaultModel -q` (if no match, run the whole file and note the failure at line 156)
Expected: FAIL — `AssertionError: 'qwen3-asr-flash-filetrans' != 'paraformer-v2'`

- [ ] **Step 2: Fix the assertion**

In `tests/test_social_extractor.py`, line 156, change:

```python
        self.assertEqual(DEFAULT_ASR_MODEL, "paraformer-v2")
```

to:

```python
        self.assertEqual(DEFAULT_ASR_MODEL, "qwen3-asr-flash-filetrans")
```

Do NOT touch lines 420/460 (`asr_model="paraformer-v2"` there are intentional call-arg fixtures, not the default assertion).

- [ ] **Step 3: Run it to verify it passes**

Run: `.venv/bin/python -m pytest tests/test_social_extractor.py -q`
Expected: the line-156 assertion no longer fails (other pre-existing failures, if any, are unrelated — note them, do not fix).

- [ ] **Step 4: Commit**

```bash
git add tests/test_social_extractor.py
git commit -m "test: fix DEFAULT_ASR_MODEL assertion to match qwen3-asr-flash-filetrans"
```

---

## Task 2: Task store — atomic write & read

**Files:**
- Create: `social_post_extractor_mcp/task_store.py`
- Test: `tests/test_task_store.py`

- [ ] **Step 1: Write the failing test**

Create `tests/test_task_store.py`:

```python
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv/bin/python -m pytest tests/test_task_store.py -q`
Expected: FAIL — `ModuleNotFoundError: ... task_store`

- [ ] **Step 3: Write minimal implementation**

Create `social_post_extractor_mcp/task_store.py`:

```python
"""On-disk task store for async transcript jobs. Stdlib only, no network."""
from __future__ import annotations

import json
import os
import time
import uuid
from pathlib import Path
from typing import Any, Optional


def _home() -> Path:
    return Path(os.environ.get("AGENT_REACH_HOME") or (Path.home() / ".agent-reach"))


def tasks_dir() -> Path:
    return _home() / "tasks"


def task_path(task_id: str) -> Path:
    return tasks_dir() / f"{task_id}.json"


def log_path(task_id: str) -> Path:
    return tasks_dir() / f"{task_id}.log"


def _atomic_write(path: Path, data: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
    os.replace(tmp, path)


def read_task(task_id: str) -> Optional[dict[str, Any]]:
    p = task_path(task_id)
    if not p.exists():
        return None
    return json.loads(p.read_text(encoding="utf-8"))


def new_task(url: str, platform: str) -> str:
    task_id = uuid.uuid4().hex
    _atomic_write(
        task_path(task_id),
        {
            "task_id": task_id,
            "url": url,
            "platform": platform,
            "status": "queued",
            "pid": None,
            "stage": "queued",
            "progress": 0.0,
            "updated_at": time.time(),
            "log_path": str(log_path(task_id)),
        },
    )
    return task_id


def write_task(task_id: str, **fields: Any) -> None:
    data = read_task(task_id) or {"task_id": task_id}
    data.update(fields)
    data["updated_at"] = time.time()
    _atomic_write(task_path(task_id), data)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `.venv/bin/python -m pytest tests/test_task_store.py -q`
Expected: PASS (3 passed)

- [ ] **Step 5: Commit**

```bash
git add social_post_extractor_mcp/task_store.py tests/test_task_store.py
git commit -m "feat: task_store atomic write/read/new"
```

---

## Task 3: Task store — stale GC & dead-worker resolution

**Files:**
- Modify: `social_post_extractor_mcp/task_store.py`
- Test: `tests/test_task_store.py`

- [ ] **Step 1: Write the failing test**

Append to `tests/test_task_store.py`:

```python
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
    # status running, pid that cannot exist, updated_at older than 120s
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
    r = ts.resolve_status(task_id)  # updated_at just now -> still running
    assert r["status"] == "running"


def test_resolve_succeeded_passthrough(store):
    task_id = ts.new_task("u", "youtube")
    ts.write_task(task_id, status="succeeded", progress=1.0, transcript="hello",
                  metadata={"title": "t"})
    r = ts.resolve_status(task_id)
    assert r["status"] == "succeeded"
    assert r["transcript"] == "hello"
    assert r["metadata"] == {"title": "t"}


import time  # noqa: E402  (used by tests above)
```

(Move the `import time` to the top of the test file with the other imports rather than at the bottom — shown here only to flag the dependency.)

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv/bin/python -m pytest tests/test_task_store.py -q`
Expected: FAIL — `AttributeError: module ... has no attribute 'gc_stale'`

- [ ] **Step 3: Write minimal implementation**

Append to `social_post_extractor_mcp/task_store.py`:

```python
_TERMINAL = {"succeeded", "failed"}
_DEAD_AFTER_SEC = 120


def gc_stale(max_age_sec: int = 7 * 86400) -> int:
    d = tasks_dir()
    if not d.exists():
        return 0
    cutoff = time.time() - max_age_sec
    deleted = 0
    for f in list(d.glob("*.json")):
        if f.stat().st_mtime < cutoff:
            f.unlink(missing_ok=True)
            f.with_name(f"{f.stem}.log").unlink(missing_ok=True)
            deleted += 1
    return deleted


def _pid_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def resolve_status(task_id: str) -> dict[str, Any]:
    """Public-facing status. Read-only: synthesizes 'failed' for a dead worker
    without mutating the file (idempotent, race-free)."""
    data = read_task(task_id)
    if data is None:
        return {"status": "error", "error": f"unknown task_id: {task_id}"}
    status = data.get("status")
    pid = data.get("pid")
    if (
        status not in _TERMINAL
        and pid
        and (time.time() - data.get("updated_at", 0)) > _DEAD_AFTER_SEC
        and not _pid_alive(int(pid))
    ):
        return {
            "status": "failed",
            "task_id": task_id,
            "error": "worker exited without completing",
            "log_path": data.get("log_path"),
        }
    out = {
        "status": status,
        "task_id": task_id,
        "stage": data.get("stage"),
        "progress": data.get("progress"),
    }
    for k in ("transcript", "metadata", "error", "log_path"):
        if k in data and data[k] is not None:
            out[k] = data[k]
    return out
```

- [ ] **Step 4: Run test to verify it passes**

Run: `.venv/bin/python -m pytest tests/test_task_store.py -q`
Expected: PASS (8 passed)

- [ ] **Step 5: Commit**

```bash
git add social_post_extractor_mcp/task_store.py tests/test_task_store.py
git commit -m "feat: task_store gc_stale + dead-worker resolve_status"
```

---

## Task 4: Worker entrypoint

**Files:**
- Create: `social_post_extractor_mcp/worker.py`
- Test: `tests/test_worker.py`

- [ ] **Step 1: Write the failing test**

Create `tests/test_worker.py`:

```python
import sys
from pathlib import Path
from unittest import mock

import pytest

from social_post_extractor_mcp import task_store as ts
from social_post_extractor_mcp import worker


@pytest.fixture
def store(tmp_path, monkeypatch):
    monkeypatch.setenv("AGENT_REACH_HOME", str(tmp_path))
    return tmp_path


def test_worker_youtube_success(store, monkeypatch):
    task_id = ts.new_task("https://youtu.be/x", "youtube")
    fake = {"video_id": "x", "title": "T", "transcript": "hello world"}
    monkeypatch.setattr(
        worker, "extract_youtube_transcript_value", lambda url, asr_model=None: fake
    )
    worker.run(task_id)
    r = ts.resolve_status(task_id)
    assert r["status"] == "succeeded"
    assert r["transcript"] == "hello world"
    assert r["metadata"]["title"] == "T"
    assert "transcript" not in r["metadata"]


def test_worker_social_success(store, monkeypatch):
    task_id = ts.new_task("https://v.douyin.com/abc/", "douyin")
    fake = {
        "platform": "douyin", "content_type": "video", "post_id": "9",
        "raw_transcript": "你好", "script_preview": "ignored",
        "script_path": "/s", "info_path": "/i", "info": {"k": 1},
    }
    svc = mock.Mock()
    svc.extract_social_post.return_value = fake
    monkeypatch.setattr(worker, "SocialExtractorService", lambda: svc)
    worker.run(task_id)
    r = ts.resolve_status(task_id)
    assert r["status"] == "succeeded"
    assert r["transcript"] == "你好"
    assert r["metadata"]["post_id"] == "9"
    svc.extract_social_post.assert_called_once()


def test_worker_failure_records_error(store, monkeypatch):
    task_id = ts.new_task("https://youtu.be/x", "youtube")
    def boom(url, asr_model=None):
        raise RuntimeError("geo blocked")
    monkeypatch.setattr(worker, "extract_youtube_transcript_value", boom)
    worker.run(task_id)
    r = ts.resolve_status(task_id)
    assert r["status"] == "failed"
    assert "geo blocked" in r["error"]
    assert r["log_path"]


def test_worker_writes_pid_running(store, monkeypatch):
    task_id = ts.new_task("https://youtu.be/x", "youtube")
    seen = {}
    def capture(url, asr_model=None):
        seen.update(ts.read_task(task_id))
        return {"transcript": "ok"}
    monkeypatch.setattr(worker, "extract_youtube_transcript_value", capture)
    worker.run(task_id)
    assert seen["status"] == "running"
    assert seen["pid"] > 0
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv/bin/python -m pytest tests/test_worker.py -q`
Expected: FAIL — `ModuleNotFoundError: ... worker`

- [ ] **Step 3: Write minimal implementation**

Create `social_post_extractor_mcp/worker.py`:

```python
"""Detached worker: runs the existing extraction pipeline for one task_id and
writes progress/result into the task store. Spawned by submit_transcript via
`python -m social_post_extractor_mcp.worker <task_id>`."""
from __future__ import annotations

import os
import sys
import traceback
from pathlib import Path

# MUST load env files before importing extraction code: social_extractor reads
# os.getenv directly; the env-file load normally happens at server.py import
# time, which this module does NOT import.
from .env_loader import load_default_env_files

load_default_env_files(Path(__file__).resolve().parents[1])

from . import task_store as ts
from .social_extractor import (  # noqa: E402
    SocialExtractorService,
    extract_youtube_transcript_value,
)

_SOCIAL = {"douyin", "xiaohongshu", "bilibili"}


def run(task_id: str) -> None:
    ts.write_task(task_id, status="running", pid=os.getpid(),
                  stage="extracting", progress=0.1)
    task = ts.read_task(task_id) or {}
    url = task.get("url", "")
    platform = task.get("platform", "")
    asr_model = task.get("asr_model")
    try:
        if platform in _SOCIAL:
            result = SocialExtractorService().extract_social_post(
                url, asr_model=asr_model
            )
            transcript = result.get("raw_transcript") or result.get("script_preview") or ""
            metadata = {
                "platform": result.get("platform"),
                "content_type": result.get("content_type"),
                "post_id": result.get("post_id"),
                "script_path": result.get("script_path"),
                "info_path": result.get("info_path"),
                "info": result.get("info"),
            }
        else:
            result = extract_youtube_transcript_value(url, asr_model=asr_model)
            transcript = result.get("transcript", "")
            metadata = {k: v for k, v in result.items() if k != "transcript"}
        ts.write_task(task_id, status="succeeded", stage="done",
                      progress=1.0, transcript=transcript, metadata=metadata)
    except Exception as exc:  # noqa: BLE001 - worker boundary, record & exit
        ts.log_path(task_id).parent.mkdir(parents=True, exist_ok=True)
        ts.log_path(task_id).write_text(traceback.format_exc(), encoding="utf-8")
        ts.write_task(task_id, status="failed", stage="done",
                      error=str(exc)[:2000], log_path=str(ts.log_path(task_id)))


def main(argv: list[str] | None = None) -> int:
    args = argv if argv is not None else sys.argv[1:]
    if len(args) != 1:
        print("usage: python -m social_post_extractor_mcp.worker <task_id>",
              file=sys.stderr)
        return 2
    run(args[0])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
```

- [ ] **Step 4: Run test to verify it passes**

Run: `.venv/bin/python -m pytest tests/test_worker.py -q`
Expected: PASS (4 passed)

- [ ] **Step 5: Commit**

```bash
git add social_post_extractor_mcp/worker.py tests/test_worker.py
git commit -m "feat: detached worker entrypoint with env-load + result mapping"
```

---

## Task 5: MCP tools — remove 5, add submit/get/blocking

**Files:**
- Modify: `social_post_extractor_mcp/server.py`
- Test: `tests/test_async_tools.py`

- [ ] **Step 1: Write the failing test**

Create `tests/test_async_tools.py`:

```python
import json
from unittest import mock

import pytest

import social_post_extractor_mcp.server as srv
from social_post_extractor_mcp import task_store as ts


@pytest.fixture
def store(tmp_path, monkeypatch):
    monkeypatch.setenv("AGENT_REACH_HOME", str(tmp_path))
    return tmp_path


def test_detect_platform():
    assert srv._detect_platform("https://v.douyin.com/abc/") == "douyin"
    assert srv._detect_platform("see https://www.xiaohongshu.com/x or xhslink.com/y") == "xiaohongshu"
    assert srv._detect_platform("https://b23.tv/abc") == "bilibili"
    assert srv._detect_platform("https://www.bilibili.com/video/BV1") == "bilibili"
    assert srv._detect_platform("https://youtu.be/x") == "youtube"
    assert srv._detect_platform("https://example.com/v.mp4") == "generic"


def test_submit_invalid_url_does_not_spawn(store):
    with mock.patch("social_post_extractor_mcp.server.subprocess.Popen") as popen:
        out = json.loads(srv.submit_transcript("not a url"))
    assert out["status"] == "error"
    popen.assert_not_called()


def test_submit_spawns_detached_and_returns_task_id(store):
    with mock.patch("social_post_extractor_mcp.server.subprocess.Popen") as popen:
        out = json.loads(srv.submit_transcript("https://youtu.be/x"))
    assert out["status"] == "running"
    assert len(out["task_id"]) == 32
    popen.assert_called_once()
    kwargs = popen.call_args.kwargs
    assert kwargs["start_new_session"] is True
    assert ts.read_task(out["task_id"])["platform"] == "youtube"


def test_get_transcript_delegates_to_resolve(store):
    tid = ts.new_task("u", "youtube")
    ts.write_task(tid, status="succeeded", transcript="hi", progress=1.0)
    out = json.loads(srv.get_transcript(tid))
    assert out["status"] == "succeeded"
    assert out["transcript"] == "hi"


def test_removed_tools_are_gone():
    for name in ("extract_douyin_text", "social_extract_transcript",
                 "youtube_extract_transcript", "social_capture_url",
                 "extract_social_post_script"):
        assert not hasattr(srv, name), f"{name} should have been removed"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv/bin/python -m pytest tests/test_async_tools.py -q`
Expected: FAIL — `AttributeError: module ... has no attribute '_detect_platform'`

- [ ] **Step 3: Write minimal implementation**

In `social_post_extractor_mcp/server.py`:

(a) Add imports near the top (after existing imports):

```python
import re
import subprocess
import sys
import time

from . import task_store as ts
```

(b) **Delete** these five `@mcp.tool()` functions entirely: `extract_social_post_script`, `social_capture_url`, `social_extract_transcript`, `youtube_extract_transcript`, `extract_douyin_text`. Also delete the now-unused helpers `extract_social_post_script_value` and `_detect_legacy_asr_provider`, and drop `extract_youtube_transcript_value` from the `.social_extractor` import (it moves to the worker). Keep `parse_social_post_info`, `parse_social_post_info_value`, `parse_douyin_video_info`, `get_douyin_download_link`, `social_analyze_owner_posts`.

(c) Add the platform detector and three tools:

```python
_PLATFORM_PATTERNS = [
    ("douyin", re.compile(r"douyin\.com|iesdouyin", re.I)),
    ("xiaohongshu", re.compile(r"xiaohongshu\.com|xhslink", re.I)),
    ("bilibili", re.compile(r"bilibili\.com|b23\.tv", re.I)),
    ("youtube", re.compile(r"youtube\.com|youtu\.be", re.I)),
]


def _detect_platform(text: str) -> str:
    for name, pat in _PLATFORM_PATTERNS:
        if pat.search(text):
            return name
    if re.search(r"https?://", text):
        return "generic"
    return ""


@mcp.tool()
def submit_transcript(url: str, asr_model: Optional[str] = None) -> str:
    """提交一个转写任务，立即返回 task_id（<1s，不会超时）。

    用 get_transcript(task_id) 轮询结果。支持抖音/小红书/B站/YouTube/通用视频 URL。
    这是 mcporter 等有 60s 限制的客户端应使用的入口。
    """
    try:
        platform = _detect_platform(url)
        if not platform:
            return json.dumps(
                {"status": "error", "error": f"无法识别为有效视频 URL: {url[:120]}"},
                ensure_ascii=False, indent=2,
            )
        ts.gc_stale()
        task_id = ts.new_task(url, platform)
        if asr_model:
            ts.write_task(task_id, asr_model=asr_model)
        subprocess.Popen(
            [sys.executable, "-m", "social_post_extractor_mcp.worker", task_id],
            stdin=subprocess.DEVNULL,
            stdout=open(ts.log_path(task_id), "a"),
            stderr=subprocess.STDOUT,
            start_new_session=True,
            close_fds=True,
        )
        return json.dumps(
            {"status": "running", "task_id": task_id, "platform": platform},
            ensure_ascii=False, indent=2,
        )
    except Exception as exc:
        return json.dumps({"status": "error", "error": str(exc)},
                          ensure_ascii=False, indent=2)


@mcp.tool()
def get_transcript(task_id: str) -> str:
    """读取 submit_transcript 任务的状态/结果。<1s。

    status: queued|running|succeeded|failed|error。
    succeeded 时含 transcript 与 metadata；failed 时含 error 与 log_path。
    """
    try:
        return json.dumps(ts.resolve_status(task_id), ensure_ascii=False, indent=2)
    except Exception as exc:
        return json.dumps({"status": "error", "error": str(exc)},
                          ensure_ascii=False, indent=2)


@mcp.tool()
def extract_transcript_blocking(
    url: str,
    timeout_sec: int = 900,
    asr_model: Optional[str] = None,
    ctx: Context = None,
) -> str:
    """⚠️ 仅供 Claude Code 等可长连接客户端。会在 mcporter 上 60s 超时——
    那里请改用 submit_transcript / get_transcript。

    内部先 submit（任务已落盘，连接断了仍可用 get_transcript(task_id) 找回），
    再轮询直到完成或 timeout_sec。
    """
    try:
        submitted = json.loads(submit_transcript(url, asr_model=asr_model))
        if submitted.get("status") != "running":
            return json.dumps(submitted, ensure_ascii=False, indent=2)
        task_id = submitted["task_id"]
        deadline = time.monotonic() + timeout_sec
        while time.monotonic() < deadline:
            time.sleep(5)
            res = ts.resolve_status(task_id)
            if ctx:
                ctx.info(f"[{res.get('status')}] {res.get('stage')} {res.get('progress')}")
            if res["status"] in ("succeeded", "failed", "error"):
                return json.dumps(res, ensure_ascii=False, indent=2)
        return json.dumps(
            {"status": "running", "task_id": task_id,
             "note": f"未在 {timeout_sec}s 内完成，请用 get_transcript({task_id}) 继续轮询"},
            ensure_ascii=False, indent=2,
        )
    except Exception as exc:
        return json.dumps({"status": "error", "error": str(exc)},
                          ensure_ascii=False, indent=2)
```

(d) Replace the body of the `social_post_extraction_guide` prompt's "推荐工具" / "兼容旧工具" sections so it lists `submit_transcript`, `get_transcript`, `extract_transcript_blocking`, `parse_social_post_info`, `parse_douyin_video_info`, `get_douyin_download_link`, `social_analyze_owner_posts` and no longer mentions any removed tool.

- [ ] **Step 4: Run test to verify it passes**

Run: `.venv/bin/python -m pytest tests/test_async_tools.py -q`
Expected: PASS (5 passed)

- [ ] **Step 5: Run the full suite (regression)**

Run: `.venv/bin/python -m pytest -q`
Expected: no NEW failures vs. the Task-1 baseline; the async/worker/task_store suites green. Note any pre-existing unrelated failures, do not fix them.

- [ ] **Step 6: Commit**

```bash
git add social_post_extractor_mcp/server.py tests/test_async_tools.py
git commit -m "feat: async submit/get/blocking tools; remove 5 long-running tools"
```

---

## Task 6: Manual E2E gate (the success criterion — run before merge)

**Files:**
- Create: `docs/superpowers/specs/e2e-manual-gate.md` (record of the run)

- [ ] **Step 1: Re-point/refresh the mcporter douyin alias to this branch venv** (already points at this dir; just confirm)

Run: `python3 -c "import json;print(json.load(open(__import__('os').path.expanduser('~/.mcporter/mcporter.json')))['mcpServers']['douyin']['command'])"`
Expected: path under `social-post-extractor-mcp/.venv/bin/python`

- [ ] **Step 2: Submit a real Douyin clip via mcporter**

Run: `mcporter call 'douyin.submit_transcript(url: "<a real douyin share link>")'`
Expected: returns `{status:"running", task_id:...}` in well under 60s.

- [ ] **Step 3: Poll to completion**

Run repeatedly (~every 30s): `mcporter call 'douyin.get_transcript(task_id: "<task_id>")'`
Expected: eventually `status:"succeeded"` with a non-empty Chinese `transcript`.

- [ ] **Step 4: Repeat for a YouTube URL with subtitles and one without**

Expected: `succeeded`, `metadata.transcript_source` = `subtitles` then `asr` respectively.

- [ ] **Step 5: Record results and commit**

Write the three task_ids, timings, and transcript lengths into `docs/superpowers/specs/e2e-manual-gate.md`.

```bash
git add docs/superpowers/specs/e2e-manual-gate.md
git commit -m "docs: record async transcript E2E gate results"
```

---

## Post-Merge Cleanup (separate task list — do NOT do inside the implementation tasks above)

Tracked in the orchestrator's task list, executed only after the E2E gate is green and the branch is ready:

1. Delete `~/.agent-reach/bin/douyin-transcript`.
2. Rewrite `~/.claude/skills/agent-reach/references/video.md` + `SKILL.md` quick commands to `submit_transcript`/`get_transcript`.
3. Rewrite `project_agent_reach_install.md` memory note.
4. Update in-repo `AGENTS.md` ("Expected tools" + examples), `README.md` examples, `AGENT_REACH_INTEGRATION.md` references.

---

## Self-Review

**Spec coverage:**
- Async submit/poll + detached worker → Tasks 2–5. ✓
- Task file schema + atomic writes + result mapping → Tasks 2, 4 (mapping table mirrored in worker code). ✓
- Dead-worker precise rule (pid + 120s + os.kill) → Task 3 `resolve_status`. ✓
- Worker `load_default_env_files` (CRITICAL from review) → Task 4 code, top of module. ✓
- Remove 5 tools / keep fast ones / update prompt → Task 5 (b)(d) + `test_removed_tools_are_gone`. ✓
- Pre-existing test bug → Task 1. ✓
- 7-day GC on submit → Task 3 `gc_stale`, called in `submit_transcript`. ✓
- `extract_transcript_blocking` submit-first + mcporter warning → Task 5 (c). ✓
- Success criterion E2E → Task 6. ✓
- Cleanup blast radius incl. AGENTS.md/README/INTEGRATION → Post-Merge list. ✓

**Placeholder scan:** no TBD/TODO; every code step shows full code; commands have expected output. ✓

**Type consistency:** `ts.new_task(url, platform)`, `ts.write_task(task_id, **fields)`, `ts.resolve_status(task_id)`, `worker.run(task_id)`, `srv._detect_platform(text)`, `srv.submit_transcript/get_transcript/extract_transcript_blocking` — names identical across Tasks 2→5 and their tests. ✓
