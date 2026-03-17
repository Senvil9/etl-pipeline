from __future__ import annotations
import io
import logging
import pandas as pd
from sqlalchemy import create_engine, text

log = logging.getLogger(__name__)

def copy_dataframe(dsn: str, table: str, df: pd.DataFrame) -> int:
    if df.empty:
        log.info("copy_dataframe: DataFrame is empty, nothing to load.")
        return 0

    engine = create_engine(dsn)
    cols = list(df.columns)
    col_list = ", ".join(cols)

    with engine.begin() as cx:
        # Create temp table with only the columns we have in the DataFrame
        col_defs = ", ".join(f"{c} TEXT" for c in cols)
        cx.execute(text(f"CREATE TEMP TABLE _stg_copy ({col_defs}) ON COMMIT DROP"))

        # Stream CSV into temp table
        buf = io.StringIO()
        df.to_csv(buf, index=False, header=True)
        buf.seek(0)

        raw_conn = cx.connection
        with raw_conn.cursor() as cur:
            cur.copy_expert(
                f"COPY _stg_copy ({col_list}) FROM STDIN WITH (FORMAT CSV, HEADER TRUE)",
                buf,
            )


        # Cast and insert into real target table
        select_cols = ", ".join(
            f"{c}::integer" if c == "user_id" else c
            for c in cols
        )
        result = cx.execute(
            text(
                f"""
                INSERT INTO {table} ({col_list})
                SELECT {select_cols} FROM _stg_copy
                ON CONFLICT DO NOTHING
                """
            )
        )
        inserted = result.rowcount
        log.info("copy_dataframe: inserted %d rows into %s", inserted, table)
        return inserted