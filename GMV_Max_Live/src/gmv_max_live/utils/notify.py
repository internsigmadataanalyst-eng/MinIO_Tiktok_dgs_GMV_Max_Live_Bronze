# src/pesanan_affiliasi/utils/notify.py
"""Email alerting for the ETL pipeline, with interchangeable backends selected
via NOTIFY_BACKEND:

  NOTIFY_BACKEND=smtp
    Plain smtplib + a Gmail App Password on a normal consumer Gmail account.
    Use this when the domain is NOT on Google Workspace (no admin console,
    only @gmail.com addresses). Setup:
      1. Enable 2-Step Verification on the sending Gmail account.
      2. Create an App Password: myaccount.google.com/apppasswords
      3. Set NOTIFY_SENDER_EMAIL and NOTIFY_SENDER_APP_PASSWORD.
    No domain, no admin access, no new external dependency required —
    smtplib is in the Python standard library.

  NOTIFY_BACKEND=gmail_api  (alternative)
    Gmail API + domain-wide delegation on the existing service account
    (the same key used by gsheet/bq clients). Requires a real Google
    Workspace domain. Setup:
      1. Enable the Gmail API on the GCP project (database-sigma).
      2. Workspace Admin Console > Security > API Controls > Domain-wide
         Delegation, add the service account's OAuth Client ID and scope:
         https://www.googleapis.com/auth/gmail.send
      3. Set NOTIFY_IMPERSONATE_EMAIL to a real Workspace mailbox.

Whichever backend is active, send_alert_email() never raises: any setup gap
or send failure is caught, logged via emit() (see utils/log.py) and returned
as False, so a broken mail channel can never mask or interrupt the pipeline.
"""
import base64
import html as _html
import os
import smtplib
from email.mime.text import MIMEText
from pathlib import Path
from typing import Iterable

from dotenv import load_dotenv

from src.gmv_max_live.utils.log import PIPELINE_NAME, emit

# .env lives at the package root (GMV_Max_Live/.env), three levels up.
load_dotenv(Path(__file__).resolve().parents[3] / ".env")

SCOPES = ["https://www.googleapis.com/auth/gmail.send"]

BACKEND = os.getenv("NOTIFY_BACKEND", "gmail_api")  # "smtp" | "gmail_api"

# Master switch. When disabled, send_alert_email() is a silent no-op.
ENABLED = os.getenv("NOTIFY_ENABLED", "true").strip().lower() in {
    "1", "true", "yes", "y",
}

# --- gmail_api backend config ---
# Same service-account JSON already used by get_gsheet_client()/get_bq_client().
SA_KEY_PATH = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "")
# Mailbox the service account impersonates to send mail. Must be a real
# Workspace user with a mailbox, delegated per the docstring above.
IMPERSONATE_EMAIL = os.getenv("NOTIFY_IMPERSONATE_EMAIL", "")

# --- smtp backend config ---
SMTP_HOST = os.getenv("NOTIFY_SMTP_HOST", "smtp.gmail.com")
SMTP_PORT = int(os.getenv("NOTIFY_SMTP_PORT", "587"))
SENDER_EMAIL = os.getenv("NOTIFY_SENDER_EMAIL", "")
SENDER_APP_PASSWORD = os.getenv("NOTIFY_SENDER_APP_PASSWORD", "")

# Default recipient list for pipeline alerts (shared by both backends).
DEFAULT_RECIPIENTS = [
    r.strip()
    for r in os.getenv("NOTIFY_RECIPIENTS", "").split(",")
    if r.strip()
]

# Number of sample bad rows included (source order) in the quarantine email.
QUARANTINE_SAMPLE_ROWS = int(os.getenv("NOTIFY_QUARANTINE_SAMPLE_ROWS", "3"))


def _send_via_gmail_api(subject: str, body_html: str, to_list: list[str]) -> None:
    from google.oauth2 import service_account
    from googleapiclient.discovery import build

    if not IMPERSONATE_EMAIL:
        raise RuntimeError(
            "NOTIFY_IMPERSONATE_EMAIL is not set — domain-wide delegation "
            "target mailbox is required before send_alert_email() can run."
        )
    if not SA_KEY_PATH or not Path(SA_KEY_PATH).exists():
        raise RuntimeError(f"Service account key not found at {SA_KEY_PATH!r}")

    creds = service_account.Credentials.from_service_account_file(
        SA_KEY_PATH, scopes=SCOPES
    ).with_subject(IMPERSONATE_EMAIL)

    service = build("gmail", "v1", credentials=creds, cache_discovery=False)

    msg = MIMEText(body_html, "html")
    msg["to"] = ", ".join(to_list)
    msg["from"] = IMPERSONATE_EMAIL
    msg["subject"] = subject

    raw = base64.urlsafe_b64encode(msg.as_bytes()).decode()
    service.users().messages().send(userId="me", body={"raw": raw}).execute()


def _send_via_smtp(subject: str, body_html: str, to_list: list[str]) -> None:
    if not SENDER_EMAIL or not SENDER_APP_PASSWORD:
        raise RuntimeError(
            "NOTIFY_SENDER_EMAIL / NOTIFY_SENDER_APP_PASSWORD not set — "
            "required for the smtp backend."
        )

    msg = MIMEText(body_html, "html")
    msg["Subject"] = subject
    msg["From"] = SENDER_EMAIL
    msg["To"] = ", ".join(to_list)

    with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
        server.ehlo()
        server.starttls()
        server.ehlo()
        server.login(SENDER_EMAIL, SENDER_APP_PASSWORD)
        server.sendmail(SENDER_EMAIL, to_list, msg.as_string())


def send_alert_email(
    subject: str,
    body_html: str,
    recipients: Iterable[str] | None = None,
    dry_run: bool = False,
) -> bool:
    """Send an HTML alert email via the configured backend (NOTIFY_BACKEND).

    Returns True on success, False on any failure (never raises). No-op
    (True) when notifications are disabled; in dry-run mode the send is
    skipped and only logged.
    """
    if not ENABLED:
        return True

    to_list = list(recipients) if recipients else DEFAULT_RECIPIENTS

    if dry_run:
        print(f"[notify] DRY RUN ({BACKEND}) — would send to {to_list}: {subject}")
        emit("NOTIFY", "gmail_notifier",
             f"DRY-RUN ({BACKEND}) would send alert to {to_list}: {subject}")
        return True

    if not to_list:
        print("[notify] No recipients configured, skipping send.")
        emit("NOTIFY", "gmail_notifier",
             "Alert skipped: no recipients configured", level="WARN")
        return False

    try:
        if BACKEND == "smtp":
            _send_via_smtp(subject, body_html, to_list)
        elif BACKEND == "gmail_api":
            _send_via_gmail_api(subject, body_html, to_list)
        else:
            raise RuntimeError(f"Unknown NOTIFY_BACKEND: {BACKEND!r}")

        print(f"[notify] Alert email sent via {BACKEND}: {subject}")
        emit("NOTIFY", "gmail_notifier", f"Alert email sent via {BACKEND}: {subject}",
             metrics={"recipients": to_list})
        return True

    except smtplib.SMTPException as e:
        print(f"[notify] SMTP error, alert NOT sent: {e}")
        emit("NOTIFY", "gmail_notifier",
             f"SMTP error, alert NOT sent: {e}", level="ERROR")
        return False
    except Exception as e:
        # Covers HttpError from googleapiclient (imported lazily above,
        # so caught generically here rather than named at module level)
        # plus any other setup/send failure.
        print(f"[notify] {type(e).__name__}, alert NOT sent: {e}")
        emit("NOTIFY", "gmail_notifier",
             f"{type(e).__name__}, alert NOT sent: {e}", level="ERROR")
        return False


def notify_self_check() -> dict:
    """Report what's configured vs. still missing for the active backend,
    without sending anything or requiring network access."""
    checks = {"backend": BACKEND}

    if BACKEND == "gmail_api":
        try:
            import googleapiclient  # noqa: F401
            checks["dependency installed"] = True
        except ImportError:
            checks["dependency installed"] = False
        checks["key file found"] = bool(SA_KEY_PATH) and Path(SA_KEY_PATH).exists()
        checks["impersonate email set"] = bool(IMPERSONATE_EMAIL)
    elif BACKEND == "smtp":
        checks["sender email set"] = bool(SENDER_EMAIL)
        checks["app password set"] = bool(SENDER_APP_PASSWORD)
    else:
        checks["valid backend"] = False

    checks["recipients set"] = bool(DEFAULT_RECIPIENTS)

    print(f"[notify] Self-check (backend={BACKEND}):")
    for label, val in checks.items():
        if label == "backend":
            continue
        mark = "[OK]" if val else "[MISS]"
        print(f"  {mark} {label}")

    expected_ok = all(v for k, v in checks.items() if k != "backend")
    if expected_ok:
        if BACKEND == "gmail_api":
            print("  All config present. Note: this does NOT confirm domain-wide")
            print("  delegation is actually granted — that only shows up as an")
            print("  error on a real (non-dry-run) send attempt.")
        else:
            print("  All config present. A real (non-dry-run) send is still the")
            print("  only way to confirm the App Password is valid.")

    if not ENABLED:
        print("  NOTE: NOTIFY_ENABLED=false — sending is currently a no-op.")

    return checks


# ---------------------------------------------------------------------------
# Convenience builders for the pipeline's specific alert cases
# ---------------------------------------------------------------------------

def _render_bq_targets(bq_updates: list[dict] | None) -> list[str]:
    """Render the BigQuery target bullet section shared by all alert emails.

    Each entry: {"table": <full project.dataset.table>, "action": str,
    "rows": int|None}. Bullets are <li>, ready to be joined into a <ul>.
    """
    bullets = []
    for entry in bq_updates or []:
        table = _html.escape(str(entry.get("table") or ""))
        action = _html.escape(str(entry.get("action") or ""))
        rows = entry.get("rows")
        suffix = f" ({rows} row(s))" if rows is not None else ""
        bullets.append(f"<li><b>{table}</b> &mdash; {action}{suffix}</li>")
    return bullets


def build_pipeline_success_email(
    pipeline_name: str = PIPELINE_NAME,
    run_key: str = "",
    log_path: str = "",
    status: str = "",
    watermark_updates: dict | None = None,
    bq_updates: list[dict] | None = None,
) -> tuple[str, str]:
    """Summary email for a successful ETL termination.

    `watermark_updates` maps (creds, sheet_name, toko) -> max_date for the
    grains advanced this run. `bq_updates` lists the BigQuery targets and
    what happened to them (see _render_bq_targets).
    """
    subject = f"[ETL NOTIFY — {pipeline_name}] Pipeline SUCCESS"
    parts = []
    if status:
        parts.append(f"<p><b>Status:</b> {_html.escape(status)}</p>")
    if run_key:
        parts.append(f"<p><b>Run key:</b> {run_key}</p>")
    if log_path:
        parts.append(f"<p><b>Log:</b> <code>{_html.escape(log_path)}</code></p>")

    bq_targets = _render_bq_targets(bq_updates)
    if bq_targets:
        parts.append("<p><b>BigQuery updates:</b><ul>" + "".join(bq_targets) + "</ul></p>")

    if watermark_updates:
        wm_rows = "".join(
            f"<li>{_html.escape(str(sheet))} / {_html.escape(str(toko))} "
            f"&rarr; {_html.escape(str(date))}</li>"
            for (creds, sheet, toko), date in sorted(watermark_updates.items())
        )
        parts.append(f"<p><b>Watermark updates:</b><ul>{wm_rows}</ul></p>")
    elif isinstance(watermark_updates, dict):
        parts.append("<p><b>Watermark updates:</b> none</p>")

    body = (
        "<html><body><h3>Pipeline run succeeded</h3>"
        + "".join(parts)
        + "</body></html>"
    )
    return subject, body


def build_pipeline_failure_email(
    error: Exception,
    pipeline_name: str = PIPELINE_NAME,
    run_key: str = "",
    log_path: str = "",
    stage: str = "",
    bq_updates: list[dict] | None = None,
    minio_files: list[str] | None = None,
    rollback_hint: str = "",
    rollback_command: str = "",
    auto_rollback_note: str = "",
) -> tuple[str, str]:
    """Summary email for an unhandled pipeline exception.

    `stage` names the pipeline stage that failed (e.g. BigQuery bronze load).
    `minio_files` lists MinIO artifacts written/maybe-written this run so an
    operator can decide on rollback; `rollback_hint` gives stage-accurate
    guidance, `rollback_command` a ready-to-paste manual command, and
    `auto_rollback_note` reports what (if anything) the pipeline already
    restored automatically (see minio_rollback.py).
    """
    subject = f"[ETL ALERT — {pipeline_name}] Pipeline FAILED"
    parts = [
        f"<p><b>Pipeline:</b> {pipeline_name}</p>",
        f"<p><b>Error type:</b> {type(error).__name__}</p>",
        f"<p><b>Message:</b> {_html.escape(str(error))}</p>",
    ]
    if run_key:
        parts.append(f"<p><b>Run key:</b> {run_key}</p>")
    if log_path:
        parts.append(f"<p><b>Log:</b> <code>{_html.escape(log_path)}</code></p>")
    if stage:
        parts.append(f"<p><b>Stage failed:</b> {_html.escape(stage)}</p>")

    bq_targets = _render_bq_targets(bq_updates)
    if bq_targets:
        parts.append("<p><b>BigQuery targets:</b><ul>" + "".join(bq_targets) + "</ul></p>")

    if minio_files:
        files = "".join(
            f"<li><code>{_html.escape(str(f))}</code></li>" for f in minio_files
        )
        parts.append(
            f"<p><b>MinIO artifacts written / may exist this run:</b><ul>{files}</ul></p>"
        )
    if rollback_hint:
        parts.append(f"<p><b>Rollback guidance:</b> {_html.escape(rollback_hint)}</p>")
    if auto_rollback_note:
        parts.append(f"<p><b>Auto rollback:</b> {_html.escape(auto_rollback_note)}</p>")
    if rollback_command:
        parts.append(
            f"<p><b>Rollback command:</b> <code>{_html.escape(rollback_command)}</code></p>"
        )

    body = (
        "<html><body><h3>Pipeline run failed</h3>"
        + "".join(parts)
        + "</body></html>"
    )
    return subject, body


def build_gate_abort_email(
    gate: str,
    message: str,
    mode: str = "",
    lists: dict[str, list] | None = None,
    note: str = "",
    bq_updates: list[dict] | None = None,
    drift_rows: list[dict] | None = None,
    pipeline_name: str = PIPELINE_NAME,
) -> tuple[str, str]:
    """Summary email for a pre-flight watermark-gate abort.

    One email per abort (not per toko). `lists` maps a human label to item
    lists (e.g. {"error_sheets": [...]}), rendered as one <p> per label.
    `drift_rows` is the per-sheet watermark-drift table (Sheet | Toko |
    Sheet max date | Current watermark | Status), rendered when provided.
    """
    subject = f"[ETL ALERT — {pipeline_name}] ETL aborted — pre-flight gate {gate} rejected"
    parts = [f"<p>{_html.escape(message)}</p>"]
    if mode:
        parts.append(f"<p><b>Mode:</b> {_html.escape(mode)}</p>")
    for label, items in (lists or {}).items():
        joined = ", ".join(str(i) for i in items) or "—"
        parts.append(f"<p><b>{_html.escape(str(label))}:</b> {_html.escape(joined)}</p>")

    if drift_rows:
        headers = ["Sheet", "Toko", "Sheet max date", "Current watermark", "Status"]
        head_html = "".join(f"<th>{_html.escape(h)}</th>" for h in headers)
        rows_html = ""
        cols = ["sheet_name", "toko", "gsheet_max", "watermark", "status"]
        for rec in drift_rows:
            tds = "".join(
                f"<td>{_html.escape(str(rec.get(c, '')))}</td>" for c in cols
            )
            rows_html += f"<tr>{tds}</tr>"
        parts.append(
            "<p><b>Per-sheet watermark drift:</b>"
            f"<table border='1' cellpadding='4' cellspacing='0'>"
            f"<tr>{head_html}</tr>{rows_html}</table></p>"
        )

    if note:
        parts.append(f"<p><b>Note:</b> {_html.escape(note)}</p>")

    bq_targets = _render_bq_targets(bq_updates)
    if bq_targets:
        parts.append(
            "<p><b>BigQuery targets (no writes — run aborted pre-flight):</b><ul>"
            + "".join(bq_targets)
            + "</ul></p>"
        )

    body = (
        "<html><body>"
        "<h3>ETL aborted by pre-flight watermark gate</h3>"
        + "".join(parts)
        + "<p>If this is expected (no genuinely new data), no action needed. "
          "Otherwise check the source sheets for date/format anomalies.</p>"
        "</body></html>"
    )
    return subject, body


QUARANTINE_SAMPLE_COLUMNS = ["sheet_name", "Tanggal", "Toko", "ID Campaign", "error_reason"]


def build_quarantine_email(
    n_quarantined: int,
    reason_counts: dict | None = None,
    affected_columns: list[str] | None = None,
    sample_rows: list[dict] | None = None,
    minio_path: str = "",
    log_path: str = "",
    sample_size: int = QUARANTINE_SAMPLE_ROWS,
    bq_updates: list[dict] | None = None,
) -> tuple[str, str]:
    """Summary email when bad rows are quarantined this run.

    `sample_rows` is a list of dicts (source order, at most `sample_size`)
    with keys from QUARANTINE_SAMPLE_COLUMNS, rendered as an HTML table.
    `minio_path` / `log_path` point to the full quarantine data records.
    """
    subject = f"[ETL ALERT] Quarantine — {n_quarantined} bad row(s) quarantined"
    parts = [f"<p><b>Quarantined this run:</b> {n_quarantined} row(s)</p>"]
    if affected_columns:
        cols = ", ".join(str(c) for c in affected_columns) or "—"
        parts.append(f"<p><b>Affected columns:</b> {_html.escape(cols)}</p>")
    reason_counts = dict(reason_counts) if reason_counts is not None else {}
    if reason_counts:
        rows = "".join(
            f"<li>{_html.escape(str(reason))}: {count} row(s)</li>"
            for reason, count in sorted(reason_counts.items())
        )
        parts.append(f"<p><b>Error reasons breakdown:</b><ul>{rows}</ul></p>")

    if sample_rows:
        sample = sample_rows[: max(0, sample_size)]
        headers = "".join(
            f"<th>{_html.escape(str(c))}</th>" for c in QUARANTINE_SAMPLE_COLUMNS
        )
        trs = ""
        for rec in sample:
            tds = "".join(
                f"<td>{_html.escape(str(rec.get(c, '')))}</td>"
                for c in QUARANTINE_SAMPLE_COLUMNS
            )
            trs += f"<tr>{tds}</tr>"
        parts.append(
            "<p><b>Sample bad rows (first "
            f"{len(sample)} of {n_quarantined}):</b>"
            f"<table border='1' cellpadding='4' cellspacing='0'>"
            f"<tr>{headers}</tr>{trs}</table></p>"
        )

    if minio_path:
        parts.append(
            f"<p><b>Full data (MinIO):</b> <code>{_html.escape(minio_path)}</code></p>"
        )
    if log_path:
        parts.append(
            f"<p><b>Full log:</b> <code>{_html.escape(log_path)}</code></p>"
        )

    bq_targets = _render_bq_targets(bq_updates)
    if bq_targets:
        parts.append(
            "<p><b>BigQuery targets (bronze/silver not written at this stage):</b><ul>"
            + "".join(bq_targets)
            + "</ul></p>"
        )

    body = (
        "<html><body><h3>Bad rows quarantined from ETL</h3>"
        + "".join(parts)
        + "<p>Review the quarantine log — this may indicate a broken "
          "date column or bulk paste error in the source sheet.</p>"
        "</body></html>"
    )
    return subject, body


def build_recovery_email(
    resolved: int = 0,
    recovered_rows: int = 0,
    absent: int = 0,
    count_mismatch_skipped: int = 0,
    dataset_name: str = "",
    bq_updates: list[dict] | None = None,
    pipeline_name: str = PIPELINE_NAME,
) -> tuple[str, str]:
    """Summary email when previously-broken rows are recovered and re-loaded."""
    subject = f"[ETL RECOVERY - {PIPELINE_NAME}] {recovered_rows} row(s) recovered"
    parts = []
    if dataset_name:
        parts.append(f"<p><b>Dataset:</b> {_html.escape(str(dataset_name))}</p>")
    parts += [

        f"<p><b>Resolved error entries:</b> {resolved}</p>",
        f"<p><b>Rows re-loaded (path A):</b> {recovered_rows}</p>",
        f"<p><b>Absent (rows removed from sheet):</b> {absent}</p>",
        f"<p><b>Count-mismatch skipped:</b> {count_mismatch_skipped}</p>",
    ]

    bq_targets = _render_bq_targets(bq_updates)
    if bq_targets:
        parts.append(
            "<p><b>BigQuery targets (recovered rows load to bronze then merge silver):</b><ul>"
            + "".join(bq_targets)
            + "</ul></p>"
        )

    body = (
        "<html><body><h3>Errors resolved since last run — rows recovered</h3>"
        + "".join(parts)
        + "</body></html>"
    )
    return subject, body