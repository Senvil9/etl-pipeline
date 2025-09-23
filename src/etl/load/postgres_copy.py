from __future__ import annotations
import io
import pandas as pd
from sqlalchemy import create_engine, text

def copy_dataframe(dsn: str, table: str, df: pd.DataFrame) -> None:
    if df.empty:
        return
    
    engine = create_engine(dsn)
    with engine.begin() as cx:
        cx.execute(text(f'CREATE TEMP TABLE _stg_copy as TABLE {table} WITH NO DATA'))
        
        buf = io.StringIO()
        df.to_csv(buf, index=False, header=False)
        buf.seek(0)
        
        raw = cx.connection
        with raw.cursor() as cur:
            cur.copy_expert(
                f"COPY _stg_copy FROM STDIN WITH (FORMAT CSV)",
                buf
            )
            
        cx.execute(text(f'INSERT INTO {table} SELECT * FROM _stg_copy'))