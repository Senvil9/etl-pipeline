"""
main.py - ETL pipeline entrypoint.

Supported modes (source x load strategy):
  1.  API  → COPY
  2.  API  → UPSERT
  3.  CSV  → COPY
  4.  CSV  → UPSERT
  5.  DB   → COPY
  6.  DB   → UPSERT

Usage:
    python -m etl.main --source api --load copy
    python -m etl.main --source csv --load upsert
    python -m etl.main --source db  --load copy
"""

from __future__ import annotations
import argparse
import logging
from datetime import datetime, timezone
import pandas as pd

from etl import config
from etl.logging_setup import setup_logging
from etl.extract.api_client import ApiClient
from etl.extract.file_reader import read_file
from etl.extract.db_reader import read_in_chunks
from etl.transform.core import normalize_users
from etl.load.postgres_copy import copy_dataframe
from etl.load.postgres_upsert import upsert_rows
from etl.watermark import get_watermark, set_watermark

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _log_metrics(source: str, load_mode: str, extracted: int, loaded: int, rejected: int) -> None:
    log.info(
        json.dumps(
            {
                "event":     "pipeline_complete",
                "source":    source,
                "load_mode": load_mode,
                "extracted_count": extracted,
                "loaded_count":    loaded,
                "rejected_count":  rejected,
            }
        )
    )


def _log_error(source: str, context: dict, exc: Exception) -> None:
    log.error(
        json.dumps(
            {
                "event":   "pipeline_error",
                "source":  source,
                "context": context,
                "error":   type(exc).__name__,
                "detail":  str(exc),
            }
        )
    )


# ---------------------------------------------------------------------------
# Extract helpers — each returns a flat list[dict]
# ---------------------------------------------------------------------------

def _extract_api(cfg: dict) -> list[dict]:
    api_cfg = cfg["sources"]["api"]
    client = ApiClient(
        base_url=api_cfg["base_url"],
        token=config.env("API_TOKEN"),
        rate_limit_per_sec=api_cfg.get("rate_limit_per_sec", 5.0),
    )
    records = list(
        client.paginate(
            endpoint=api_cfg["endpoint"],
            page_param=api_cfg["page_param"],
            per_page_param=api_cfg["per_page_param"],
            per_page=api_cfg["per_page"],
            data_key=api_cfg["data_key"],
        )
    )
    log.info("API extract: %d raw records fetched", len(records))
    return records


def _extract_csv(cfg: dict) -> list[dict]:
    file_cfg = cfg["sources"]["file"]
    return read_file(file_cfg["path"], fmt=file_cfg.get("fmt", "csv"))


def _extract_db(cfg: dict, dsn: str, since: datetime) -> list[dict]:
    db_cfg = cfg["sources"]["db"]
    all_rows: list[dict] = []
    for chunk in read_in_chunks(
        dsn=dsn,
        sql=db_cfg["query"],
        since=since,
        chunk_size=db_cfg.get("chunk_size", 500),
    ):
        all_rows.extend(chunk)
    log.info("DB extract: %d raw records fetched (since %s)", len(all_rows), since.isoformat())
    return all_rows


# ---------------------------------------------------------------------------
# Load helper — dispatches to COPY or UPSERT
# ---------------------------------------------------------------------------

def _load(
    load_mode: str,
    dsn: str,
    table: str,
    rows: list[dict],
    key_cols: list[str],
    batch_size: int,
) -> int:
    if load_mode == "copy":
        df = pd.DataFrame(rows)
        return copy_dataframe(dsn, table, df)
    elif load_mode == "upsert":
        return upsert_rows(dsn, table, rows, key_cols, batch_size=batch_size)
    else:
        raise ValueError(f"Unknown load mode: {load_mode!r}")


# ---------------------------------------------------------------------------
# Pipeline orchestrator
# ---------------------------------------------------------------------------

def run(source: str, load_mode: str) -> None:
    setup_logging()
    cfg = config.load_settings()
    dsn = config.env("POSTGRES_DSN")
    key_cols: list[str] = cfg["load"]["key_columns"]
    batch_size: int = cfg["run"]["batch_size"]
    run_start = datetime.now(tz=timezone.utc)

    log.info(
        json.dumps({"event": "pipeline_start", "source": source, "load_mode": load_mode})
    )

    # -- 1. Extract -----------------------------------------------------------
    try:
        if source == "api":
            raw_records = _extract_api(cfg)
            target_table = cfg["load"]["api_target_table"]

        elif source == "csv":
            raw_records = _extract_csv(cfg)
            target_table = cfg["load"]["csv_target_table"]

        elif source == "db":
            since = get_watermark(dsn, "db")
            raw_records = _extract_db(cfg, dsn, since)
            target_table = cfg["load"]["db_target_table"]

        else:
            raise ValueError(f"Unknown source: {source!r}")

    except Exception as exc:
        _log_error(source, {"stage": "extract"}, exc)
        raise SystemExit(1) from exc

    extracted_count = len(raw_records)

    # -- 2. Transform ---------------------------------------------------------
    try:
        valid_rows, rejected_count = normalize_users(raw_records, source_label=source)
    except Exception as exc:
        _log_error(source, {"stage": "transform", "extracted": extracted_count}, exc)
        raise SystemExit(1) from exc

    if not valid_rows:
        log.warning("No valid rows after transformation — skipping load.")
        _log_metrics(source, load_mode, extracted_count, 0, rejected_count)
        return

    # -- 3. Load --------------------------------------------------------------
    try:
        loaded_count = _load(load_mode, dsn, target_table, valid_rows, key_cols, batch_size)
    except Exception as exc:
        _log_error(
            source,
            {"stage": "load", "load_mode": load_mode, "table": target_table},
            exc,
        )
        raise SystemExit(1) from exc

    # -- 4. Update watermark --------------------------------------------------
    set_watermark(dsn, source, run_start)

    # -- 5. Emit metrics ------------------------------------------------------
    _log_metrics(source, load_mode, extracted_count, loaded_count, rejected_count)


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Run the ETL pipeline")
    ap.add_argument(
        "--source",
        choices=["api", "csv", "db"],
        required=True,
        help="Data source: api | csv | db",
    )
    ap.add_argument(
        "--load",
        dest="load_mode",
        choices=["copy", "upsert"],
        required=True,
        help="Load strategy: copy (bulk) | upsert (on conflict do update)",
    )
    args = ap.parse_args()
    run(args.source, args.load_mode)