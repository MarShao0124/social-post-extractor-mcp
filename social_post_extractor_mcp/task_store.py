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
