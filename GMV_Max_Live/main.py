import sys
from datetime import datetime

print(">>> main.py started")

from src.gmv_max_live.pipelines.run_daily_etl import run_daily_etl
from src.gmv_max_live.pipelines.config import _failure_ctx, BQ_TARGETS
from src.gmv_max_live.utils.log import (
    setup_run_logging,
    setup_event_logging,
    emit,
)
from src.gmv_max_live.utils.notify import (
    send_alert_email,
    build_pipeline_failure_email,
    close_smtp,
)

if __name__ == "__main__":
    dry_run = "--dry-run" in sys.argv or "-n" in sys.argv
    if dry_run:
        print(">>> dry-run mode enabled")

    run_key = datetime.now().strftime("%Y%m%d%H%M")

    # Only capture run transcripts for production runs, not dry-run
    if not dry_run:
        log_path, restore_logging = setup_run_logging(run_key)
        print(f">>> Log file: {log_path}")
    else:
        restore_logging = None
        log_path = ""

    # Structured JSON events: written for BOTH production and dry-run
    _, stop_event_logging = setup_event_logging(run_key, dry_run)

    try:
        print(f">>> run_key={run_key}")
        emit("PIPELINE", "main", f"ETL start (run_key={run_key})")
        print(f">>> calling run_daily_etl()")
        run_daily_etl(dry_run=dry_run)
        emit("PIPELINE", "main", "ETL Pipeline finished")
        print(">>> main.py finished")
    except Exception as e:
        emit(
            "PIPELINE", "main",
            f"Unhandled exception: {type(e).__name__} {e}",
            level="ERROR",
        )
        ctx = _failure_ctx or {}
        subject, body_html = build_pipeline_failure_email(
            e,
            run_key=run_key,
            log_path=log_path,
            stage=ctx.get("stage", ""),
            bq_updates=BQ_TARGETS,
            minio_files=ctx.get("minio_files") or [],
            rollback_hint=ctx.get("rollback_hint", ""),
            enable_explanation=not dry_run,
        )
        send_alert_email(subject, body_html, dry_run=dry_run)
        raise
    finally:
        close_smtp()
        stop_event_logging()
        if restore_logging:
            restore_logging()
