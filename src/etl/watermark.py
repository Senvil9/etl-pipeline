from __future__ import annotations

from datetime import datetime, timezone
from sqlalchemy import create_engine, text


def get_watermark(dsn: str, pipeline: str) -> datetime:
    """Return the last successful run timestamp for *pipeline* (UTC)."""
    engine = create_engine(dsn)
    with engine.connect() as cx:
        row = cx.execute(
            text("SELECT last_run_at FROM public.etl_watermarks WHERE pipeline = :p"),
            {"p": pipeline},
        ).fetchone()
    if row is None:
        return datetime(1970, 1, 1, tzinfo=timezone.utc)
    ts = row[0]
    # Ensure timezone-aware
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return ts


def set_watermark(dsn: str, pipeline: str, ts: datetime) -> None:
    """Update the watermark for *pipeline* to *ts*."""
    engine = create_engine(dsn)
    with engine.begin() as cx:
        cx.execute(
            text(
                """
                UPDATE public.etl_watermarks
                SET last_run_at = :ts
                WHERE pipeline = :p
                """
            ),
            {"ts": ts, "p": pipeline},
        )