from __future__ import annotations

import logging
from typing import Iterator

from sqlalchemy import create_engine, text

log = logging.getLogger(__name__)

def _chunks(seq: list, size: int) -> Iterator[list]:
    """Yield successive fixed-size chunks from *seq*."""
    for i in range(0, len(seq), size):
        yield seq[i : i + size]


def upsert_rows(
    dsn: str,
    table: str,
    rows: list[dict],
    key_cols: list[str],
    batch_size: int = 500,
) -> int:
    """
    Upsert *rows* into *table* using INSERT … ON CONFLICT DO UPDATE.
    Returns the total number of rows affected.
    """
    if not rows:
        log.info("upsert_rows: no rows to upsert.")
        return 0

    cols = list(rows[0].keys())
    conflict_cols = ", ".join(key_cols)
    update_cols = [c for c in cols if c not in key_cols]

    if not update_cols:
        # All columns are key columns — just do INSERT … ON CONFLICT DO NOTHING
        do_update = "DO NOTHING"
    else:
        set_clause = ", ".join(f"{c} = EXCLUDED.{c}" for c in update_cols)
        do_update = f"DO UPDATE SET {set_clause}"

    insert_cols = ", ".join(cols)
    placeholders = ", ".join(f":{c}" for c in cols)

    sql = text(
        f"""
        INSERT INTO {table} ({insert_cols})
        VALUES ({placeholders})
        ON CONFLICT ({conflict_cols})
        {do_update}
        """
    )

    engine = create_engine(dsn)
    total = 0
    with engine.begin() as cx:
        for batch in _chunks(rows, batch_size):
            result = cx.execute(sql, batch)
            total += result.rowcount

    log.info("upsert_rows: %d rows upserted into %s", total, table)
    return total