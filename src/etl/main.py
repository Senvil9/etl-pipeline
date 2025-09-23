from __future__ import annotations
import argparse
import logging
import pandas as pd

from etl import config
from etl.logging_setup import setup_logging
from etl.extract import file_reader, api_client, db_reader
from etl.transform.core import normalize_customers
from etl.load.postgres_copy import copy_dataframe
from etl.load.postgres_upsert import upsert_rows

log = logging.getLogger(__name__)

def run(source: str, load_mode:str) -> None:
    
    setup_logging()
    cfg = config.load_settings()
    
    #extract
    if source=="file":
        rows = file_reader.read_file(cfg["sources"]["file"])
    elif source=="api":
        rows = api_client.ApiClient.get(cfg["sources"]["api"])
    elif source=="db":
        rows = db_reader.read_in_chunks(cfg["sources"]["api"])
    else:
        raise SystemExit(f"Unknown source")
    
    log.info("Extraction completed")
    
    #transform
    normalized = [c.model_dump() for c in normalize_customers(rows)]
    
    #load
    dsn = config.env("POSTGRES_DSN")
    target_table = cfg['run']['target_table']
    
    if load_mode == 'upsert':
        upsert_rows(dsn, target_table,normalized, cfg["load"]["key_column"])
    elif load_mode == 'copy':
        df_out = pd.DataFrame(normalized)
        copy_dataframe(dsn, target_table, df_out)
    else:
        raise SystemExit(f"unknown load mode")
    
if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="RUN ETL PIPELINE")
    ap.add_argument("--source", choices=["file", "api", "db"], required=True, 
                    help="Data source to extract from")
    ap.add_argument("--load", dest="load_mode", choices=["copy", "upsert"], required=True, 
                    help="Load strategy: fast copy opr upsert on conflict")
    args = ap.parse_args()
    
    run(args.source, args.load_mode)
        
        
        
#from file, bulk copy into postgres
python -m etl.main --source file --load copy

#from API, upsert into postgres
python -m etl.main --source api -- load upsert

#from database, bulk copy into postgres
python -m etl.main --source db --load copy