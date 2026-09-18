# src/gmv_max_live/utils/log.py
"""Run logging — terminal transcript, section-specific text logs, and
structured JSON-lines events.

- setup_run_logging   : stdout/stderr transcript (production runs only).
- write_section_log   : section text logs (production runs only).
- setup_event_logging : etl_events_<run_key>.jsonl, written for production AND
  dry-run runs; every emit() writes one NDJSON object.
All logs are kept indefinitely under logs/run_{run_key}/.
"""
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

PIPELINE_NAME = os.getenv("PIPELINE_NAME", "GMVMAX LIVE")

# Module-level JSON-lines event logger state.
_event_file = None
_event_ctx = None  # dict(run_key, environment)


class Tee:
    """Duplicate writes to both the original stream and a log file."""

    def __init__(self, original, log_file):
        self.original = original
        self.log_file = log_file

    def write(self, message):
        self.original.write(message)
        if message.strip():
            ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            self.log_file.write(f"[{ts}] {message}")
        else:
            self.log_file.write(message)

    def flush(self):
        self.original.flush()
        self.log_file.flush()


def setup_run_logging(run_key: str, log_dir: str = "logs"):
    """Start capturing stdout+stderr to logs/run_{run_key}/etl_full_{run_key}.log.

    Returns (log_file_path, restore_fn).
    Caller MUST call restore_fn() when done.
    """
    run_folder = os.path.join(log_dir, f"run_{run_key}")
    Path(run_folder).mkdir(parents=True, exist_ok=True)

    log_path = os.path.join(run_folder, f"etl_full_{run_key}.log")
    log_file = open(log_path, "w", encoding="utf-8")

    orig_stdout = sys.stdout
    orig_stderr = sys.stderr
    sys.stdout = Tee(orig_stdout, log_file)
    sys.stderr = Tee(orig_stderr, log_file)

    def restore():
        sys.stdout = orig_stdout
        sys.stderr = orig_stderr
        log_file.close()

    return log_path, restore


def get_log_folder(run_key: str, log_dir: str = "logs") -> str:
    """Return the log folder path for this run (creates if needed)."""
    run_folder = os.path.join(log_dir, f"run_{run_key}")
    Path(run_folder).mkdir(parents=True, exist_ok=True)
    return run_folder


def write_section_log(log_folder: str, filename: str, content: str):
    """Write a section log file (wm_monitor, quarantine, etc.)."""
    path = os.path.join(log_folder, filename)
    with open(path, "w", encoding="utf-8") as f:
        f.write(content)
    return path


def _event_timestamp() -> str:
    """UTC ISO-8601 with millisecond precision + 'Z' suffix."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def setup_event_logging(run_key: str, dry_run: bool = False, log_dir: str = "logs"):
    """Open the JSON-lines event log for this run.

    Written for BOTH production and dry-run runs (unlike setup_run_logging).
    Returns (event_log_path, stop_fn). Caller MUST call stop_fn() when done.
    """
    global _event_file, _event_ctx
    run_folder = get_log_folder(run_key, log_dir)
    path = os.path.join(run_folder, f"etl_events_{run_key}.jsonl")
    _event_file = open(path, "w", encoding="utf-8")
    _event_ctx = {
        "run_key": run_key,
        "environment": "dry_run" if dry_run else "production",
    }

    def stop():
        global _event_file, _event_ctx
        if _event_file:
            _event_file.flush()
            _event_file.close()
        _event_file = None
        _event_ctx = None

    return path, stop


def is_event_logging_enabled() -> bool:
    """True when a JSON-lines event log is currently open."""
    return _event_ctx is not None


def emit(
    phase: str,
    component: str,
    message: str = "",
    level: str = "INFO",
    metrics: dict | None = None,
    source: dict | None = None,
    target: dict | None = None,
):
    """Write one JSON events line (NDJSON) to the active event log.

    No-op when no event log is configured, so standalone runs stay safe.
    Fill defaults: timestamp, pipeline_name, pipeline_run_id,
    execution_environment. Omitted metrics/source/target become {}.
    """
    if _event_ctx is None:
        return
    event = {
        "timestamp": _event_timestamp(),
        "log_level": level,
        "pipeline_name": PIPELINE_NAME,
        "pipeline_run_id": f"run-{_event_ctx['run_key']}",
        "phase": phase,
        "component": component,
        "message": message,
        "metrics": metrics or {},
        "source": source or {},
        "target": target or {},
        "execution_environment": _event_ctx["environment"],
    }
    _event_file.write(json.dumps(event, default=str) + "\n")
    _event_file.flush()