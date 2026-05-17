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
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return None


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
