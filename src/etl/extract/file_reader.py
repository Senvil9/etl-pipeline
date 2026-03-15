from __future__ import annotations
import logging
import pandas as pd
from pathlib import Path

log = logging.getLogger(__name__)

def read_file(path: str, fmt: str = "csv") -> list[dict]:
    """Read a CSV or Parquet file and return a list of row dicts."""
    p=Path(path)
    if not p.exists():
        raise FileNotFoundError(f"Source file not found: {p.resolve()}")
    
    if fmt == "csv":
        df = pd.read_csv(p)
    elif fmt == "parquet":
        df = pd.read_parquet(p)
    else:
        raise ValueError(f"Unsupported file format: {fmt!r}")
    
    log.info("Read %d rows from %s", len(df), p)
    return df.to_dict(orient="records")
