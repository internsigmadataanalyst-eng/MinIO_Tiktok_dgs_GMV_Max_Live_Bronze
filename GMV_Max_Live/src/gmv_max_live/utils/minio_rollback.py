# src/gmv_max_live/utils/minio_rollback.py
"""MinIO point-in-time rollback for the pesanan_affiliasi ETL.

Why this exists
---------------
Pipeline order is: parquet -> MinIO -> BigQuery bronze append -> Watermark ->
BigQuery silver MERGE. If the bronze append FAILS the watermark has NOT yet
advanced, so a plain re-run safely re-selects those rows. This module provides
manual/emergency rollback when needed.

Prerequisite (ONE TIME)
-----------------------
The bucket must have object versioning enabled:
    mc version enable <alias>/<bucket>
or via this module:
    python -m src.pesanan_affiliasi.utils.minio_rollback --enable-versioning
Without versioning each object has a single version, so time-travel restore
cannot work; the module then does nothing and reports the missing setup.

Mechanism
---------
Listing with include_version=True exposes every object version. To restore
state to before_ts we delete each version whose last_modified >= before_ts,
snapping each object back to its previous version and removing objects that
only appeared after before_ts. This is the SDK equivalent of
`mc cp --recursive --rewind <ts>`.

Safety
------
- The manual CLI defaults to a --dry-run preview; --execute is required to
  actually delete versions.
- The 'full' scope additionally purges the failed run's parquet/quarantine/
  manifest artifacts, but only versions in the [before_ts .. now] window, so
  a same-day sibling run's earlier versions are never touched.
"""

import argparse
import json
import os
from datetime import datetime, timezone

from dotenv import load_dotenv
from minio import Minio
from minio.commonconfig import CopySource
from minio.error import S3Error

# Credentials live in .env (load here so the CLI works standalone; re-running
# inside the pipeline is a harmless no-op since dotenv does not override).
load_dotenv()

# Watermark object + prefixes this ETL owns.
WATERMARK_PATH = "watermarks/gmv_max.json"
WATERMARK_PREFIX = "watermarks/"
BACKUP_PREFIX = "rollback_backup/"

_PROD_BUCKET_ENV = "MINIO_BUCKET"


# ---------------------------------------------------------------------------
# Time helpers
# ---------------------------------------------------------------------------

def _local_tzinfo():
    """The machine's current local tzinfo (run_key is made from local time)."""
    return datetime.now().astimezone().tzinfo


def _to_utc_naive(dt: datetime) -> datetime:
    """Normalize to naive UTC for comparison against server LastModified
    (MinIO stores LastModified in UTC)."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=_local_tzinfo())
    return dt.astimezone(timezone.utc).replace(tzinfo=None)


def run_key_to_before_ts(run_key: str) -> datetime:
    """Parse a run_key (YYYYMMDDHHMM) into the run's start, as naive UTC.

    Version writes happen mid-run, so every version >= this instant belongs to
    the failed run (or later). Limit: run_key has minute granularity, so two
    runs in the same minute are indistinguishable.
    """
    dt = datetime.strptime(run_key, "%Y%m%d%H%M")
    return _to_utc_naive(dt)


def parse_iso(iso: str) -> datetime:
    """Parse a user-supplied local timestamp into naive UTC."""
    for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M", "%Y-%m-%d %H:%M"):
        try:
            return _to_utc_naive(datetime.strptime(iso, fmt))
        except ValueError:
            continue
    raise ValueError(f"Unrecognized timestamp: {iso!r} (use e.g. 2026-09-17T14:30:00)")


def _to_local_str(utc_naive: datetime) -> str:
    """Render a naive-UTC datetime in the machine's local time for display."""
    if utc_naive is None:
        return "?"
    aware_utc = utc_naive.replace(tzinfo=timezone.utc)
    return aware_utc.astimezone().strftime("%Y-%m-%d %H:%M:%S")


# ---------------------------------------------------------------------------
# Version listing
# ---------------------------------------------------------------------------

def _versions(client: Minio, bucket: str, prefixes: list[str]):
    """Yield (object_name, version_id, is_delete_marker, last_modified_naive_utc)
    for every version under the given prefixes."""
    for prefix in prefixes:
        try:
            objs = client.list_objects(
                bucket, prefix=prefix, recursive=True, include_version=True
            )
            for obj in objs:
                lm = getattr(obj, "last_modified", None)
                last_modified = None
                if lm is not None:
                    last_modified = lm.replace(tzinfo=None) if lm.tzinfo else lm
                yield (
                    obj.object_name,
                    getattr(obj, "version_id", None),
                    bool(getattr(obj, "is_delete_marker", False)),
                    last_modified,
                )
        except S3Error as e:
            if e.code in ("NoSuchKey", "NoSuchBucket", "AccessDenied"):
                continue
            raise


def list_versions_after(client: Minio, bucket: str, prefixes: list[str], after_ts: datetime) -> list[tuple]:
    """Versions with last_modified >= after_ts across the scoped prefixes."""
    hits = []
    for name, version_id, is_dm, last_modified in _versions(client, bucket, prefixes):
        if last_modified is not None and last_modified >= after_ts:
            hits.append((name, version_id, is_dm, last_modified))
    return hits


def _scope_prefixes(scope: str, run_key: str | None = None) -> list[str]:
    """Prefixes covered by the given scope. Watermark is always included."""
    prefixes = [WATERMARK_PREFIX]
    if scope == "full":
        if not run_key:
            raise ValueError("full scope requires a run_key to scope the day prefixes")
        day = run_key[:8]
        prefixes += [
            f"gmv/max/date={day}/",
            f"quarantine/date={day}/",
            "error_list_watermark/",
            f"fix_error_list_watermark/date={day}/",
        ]
    return prefixes


# ---------------------------------------------------------------------------
# Versioning
# ---------------------------------------------------------------------------

def bucket_versioning_enabled(client: Minio, bucket: str) -> bool:
    try:
        cfg = client.get_bucket_versioning(bucket)
        return getattr(cfg, "status", None) == "Enabled"
    except (S3Error, Exception):
        return False


def enable_versioning(client: Minio, bucket: str):
    from minio.versioningconfig import VersioningConfig

    client.set_bucket_versioning(bucket, VersioningConfig(status="Enabled"))
    return client.get_bucket_versioning(bucket)


# ---------------------------------------------------------------------------
# Restore primitives
# ---------------------------------------------------------------------------

def _object_exists(client: Minio, bucket: str, name: str) -> bool:
    try:
        client.stat_object(bucket, name)
        return True
    except S3Error as e:
        if e.code in ("NoSuchKey", "AccessDenied"):
            return False
        raise


def backup_watermark(client: Minio, bucket: str, tag: str) -> str | None:
    """Copy the current watermark object to the backup prefix (safety net).

    Returns the backup object name, or None if no watermark is present.
    """
    if not _object_exists(client, bucket, WATERMARK_PATH):
        return None
    backup = f"{BACKUP_PREFIX}watermark_before_{tag}.json"
    client.copy_object(bucket, backup, CopySource(bucket, WATERMARK_PATH))
    print(f"[rollback] Backed up current watermark to: {backup}")
    return backup


def _print_watermark_summary(client: Minio, bucket: str):
    """Print the restored watermark's group count + max date (verification)."""
    try:
        got = client.get_object(bucket, WATERMARK_PATH)
        data = got.read().decode("utf-8")
        got.close()
        got.release_conn()
    except Exception:
        print("[rollback] WARNING: could not read watermark after rollback.")
        return
    recs = json.loads(data).get("sheets", [])
    dates = [str(r.get("last_processed_date") or "") for r in recs if r.get("last_processed_date")]
    maxd = max(dates) if dates else ""
    print(f"[rollback] Restored watermark: {len(recs)} group(s), max date {maxd}")


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def auto_restore_watermark(client: Minio, bucket: str, run_key: str) -> str:
    """Manual/emergency restore of the watermark to just before a failed run.

    Note: This function is no longer called automatically by the pipeline.
    It is retained for manual/emergency use via the CLI.
      1. bucket versioning is Enabled,
      2. a prior watermark version exists (last_modified < before_ts).
    Scope: the watermark object ONLY (never touches parquet / quarantine).

    Returns a human-readable report string for the failure email / event log.
    """
    before_ts = run_key_to_before_ts(run_key)

    if not bucket_versioning_enabled(client, bucket):
        return (
            "AUTO ROLLBACK SKIPPED: bucket versioning is not Enabled. One-time setup: "
            "mc version enable <alias>/<bucket> (or --enable-versioning)."
        )

    if not _object_exists(client, bucket, WATERMARK_PATH):
        return "AUTO ROLLBACK SKIPPED: no watermark object present."

    prior = [
        v for v in _versions(client, bucket, [WATERMARK_PREFIX])
        if v[0] == WATERMARK_PATH and v[3] is not None and v[3] < before_ts
    ]
    if not prior:
        return (
            "AUTO ROLLBACK SKIPPED: no pre-run watermark version exists "
            "(the run created/overwrote the only version). Restore manually."
        )

    backup = backup_watermark(client, bucket, run_key)
    removed = 0
    for name, version_id, _is_dm, _lm in list_versions_after(
            client, bucket, [WATERMARK_PREFIX], before_ts):
        if version_id is None:
            continue  # never delete the null (pre-versioning) version
        client.remove_object(bucket, name, version_id=version_id)
        removed += 1

    _print_watermark_summary(client, bucket)
    return (
        f"AUTO ROLLBACK DONE: removed {removed} watermark version(s) written at/after "
        f"run {run_key}; watermark restored to pre-run state. Backup: {backup}"
    )


def preview_rollback(client: Minio, bucket: str, before_ts: datetime,
                     run_key: str | None = None, scope: str = "watermark") -> list[tuple]:
    """Print every version that a rollback would remove. No mutation."""
    prefixes = _scope_prefixes(scope, run_key)
    hits = list_versions_after(client, bucket, prefixes, before_ts)
    print("\n=== ROLLBACK PREVIEW (no changes made) ===")
    print(f"  bucket: {bucket}")
    print(f"  scope : {scope}")
    print(f"  target: restore to {_to_local_str(before_ts)} (local)")
    print(f"  prefixes: {', '.join(prefixes)}")
    if not hits:
        print("  No versions to remove - state is already at/before the target.")
    for name, version_id, is_dm, last_modified in sorted(hits, key=lambda x: (x[0], x[3] or datetime.min)):
        kind = "delete-marker" if is_dm else "version"
        print(f"  remove  {name}  [{kind} {version_id or 'null'}]  {_to_local_str(last_modified)}")
    print(f"  TOTAL: {len(hits)} version(s) would be removed.")
    return hits


def execute_rollback(client: Minio, bucket: str, before_ts: datetime,
                     run_key: str | None = None, scope: str = "watermark",
                     dry_run: bool = True, backup: bool = True) -> dict:
    """Delete every version written at/after before_ts across the scope.

    dry_run=True only previews (this is the default and the safe path).
    Returns {'removed': int, 'skipped_null': int, 'dry_run': bool}.
    """
    prefixes = _scope_prefixes(scope, run_key)
    if dry_run:
        preview_rollback(client, bucket, before_ts, run_key, scope)
        return {"removed": 0, "skipped_null": 0, "dry_run": True}

    hits = list_versions_after(client, bucket, prefixes, before_ts)
    print("\n=== ROLLBACK EXECUTE ===")
    print(f"  bucket: {bucket}")
    print(f"  scope : {scope}")
    print(f"  target: restore to {_to_local_str(before_ts)} (local)")

    if backup:
        backup_watermark(client, bucket, datetime.now().strftime("%Y%m%d%H%M%S"))

    removed = 0
    skipped_null = 0
    for name, version_id, _is_dm, _lm in sorted(hits, key=lambda x: (x[0], x[3] or datetime.min)):
        if version_id is None:
            skipped_null += 1
            continue
        client.remove_object(bucket, name, version_id=version_id)
        removed += 1
        print(f"  removed  {name}  [{version_id}]")

    if removed:
        _print_watermark_summary(client, bucket)
    print(f"  Removed {removed} version(s); skipped {skipped_null} null-version(s).")
    return {"removed": removed, "skipped_null": skipped_null, "dry_run": False}


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _cli_client(args) -> tuple[Minio, str]:
    if args.endpoint or args.access or args.secret or args.bucket:
        client = Minio(
            args.endpoint or os.getenv("MINIO_ENDPOINT"),
            access_key=args.access or os.getenv("MINIO_ACCESS_KEY"),
            secret_key=args.secret or os.getenv("MINIO_SECRET_KEY"),
            secure=bool(args.secure),
        )
        bucket = args.bucket or os.getenv(_PROD_BUCKET_ENV)
    else:
        from src.gmv_max_live.utils.minio_client import get_minio_client

        client, bucket = get_minio_client()
    return client, bucket


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        description="MinIO point-in-time rollback for pesanan_affiliasi "
                    "(restores the watermark/bucket state before a failed run)."
    )
    ap.add_argument("--endpoint", help="defaults to MINIO_ENDPOINT env")
    ap.add_argument("--access", help="defaults to MINIO_ACCESS_KEY env")
    ap.add_argument("--secret", help="defaults to MINIO_SECRET_KEY env")
    ap.add_argument("--secure", action="store_true", help="use HTTPS (default off)")
    ap.add_argument("--bucket", help=f"defaults to {_PROD_BUCKET_ENV} env")
    grp = ap.add_mutually_exclusive_group()
    grp.add_argument("--run-key", help="failed run key YYYYMMDDHHMM -> restore to just before it")
    grp.add_argument("--before", help="local ISO timestamp -> restore to just before it, e.g. 2026-09-17T14:30:00")
    ap.add_argument("--scope", choices=["watermark", "full"], default="watermark",
                    help="watermark = restore watermark only (default); full = also purge the "
                         "failed run's parquet/quarantine/manifest artifacts (needs --run-key)")

    ap.add_argument("--execute", action="store_true",
                    help="actually delete versions (default is a dry-run preview)")
    ap.add_argument("--no-backup", action="store_true",
                    help="skip the watermark backup before executing")
    ap.add_argument("--enable-versioning", action="store_true",
                    help="one-time setup: enable bucket versioning (the prerequisite)")
    args = ap.parse_args(argv)

    client, bucket = _cli_client(args)

    if args.enable_versioning:
        cfg = enable_versioning(client, bucket)
        print(f"[rollback] Versioning enabled on {bucket}: status={getattr(cfg, 'status', None)}")
        return 0

    if args.run_key:
        notbefore = run_key_to_before_ts(args.run_key)
    elif args.before:
        notbefore = parse_iso(args.before)
    else:
        ap.error("provide --run-key or --before")
        return 2

    if args.scope == "full" and not args.run_key:
        print("ERROR: --scope full requires --run-key (to scope the run's day prefixes).")
        return 2

    if not bucket_versioning_enabled(client, bucket):
        print("ERROR: bucket versioning is not Enabled. One-time setup:")
        print("  python -m src.pesanan_affiliasi.utils.minio_rollback --enable-versioning")
        return 2

    if args.execute:
        execute_rollback(client, bucket, notbefore,
                         run_key=args.run_key, scope=args.scope,
                         dry_run=False, backup=not args.no_backup)
    else:
        preview_rollback(client, bucket, notbefore, run_key=args.run_key, scope=args.scope)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())