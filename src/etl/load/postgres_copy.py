from __future__ import annotations
import io
import logging
import pandas as pd
from sqlalchemy import create_engine, text

log = logging.getLogger(__name__)

def copy_dataframe(dsn: str, table: str, df: pd.DataFrame) -> int:
    """
    Bulk-load *df* into *table* via Postgres COPY using a staging temp table
    Returns the number of rows inserted.
    """
    if df.empty:
        log.info("copy_dataframe: DateFrame is empty, nothing to load.")
        return
    
    engine = create_engine(dsn)
    with engine.begin() as cx:
        # Create a temp table mirroring the target schema (no indexes/constraints)
        cx.execute(
            text(f'CREATE TEMP TABLE _stg_copy (LIKE {table} INCLUDING DEFAULTS) ON
                 COMMIT DROP')
        )

        # Stream CSV bytes into Postgres via COPY
        buf = io.StringIO()
        df.to_csv(buf, index=False, header=False)
        buf.seek(0)
        
        raw = cx.connection
        with raw.cursor() as cur:
            cur.copy_expert(
                f"COPY _stg_copy ({','.join(df.columns)}) FROM STDIN WITH (FORMAT CSV, HEADER TRUE)",
                buf,
            )
        
        # Insert from staging into target ( skip duplicate PKs )
        result = cx.execute(
            text(
                f"""
                INSERT INTO {table} ({','.join(df.columns)})
                SELECT {','.join(df.columns)} FROM _stg_copy
                ON CONFLICT DO NOTHING
                """
            )
        )
        inserted = result.rowcount
        log.info("copy_dataframe: inserted %d rows into %s", inserted, table)
        return inserted