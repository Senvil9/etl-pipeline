from __future__ import annotations
import logging
from datetime import datetime
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from typing import Iterator, Mapping

log = logging.getLogger(__name__)


def read_in_chunks(dsn: str, sql: str, since: datetime, chunk_size: int = 500) -> Iterator[list[dict]]:
    """
    Stream rows from postgres in chunks using LIMIT/OFFSET.
    *since* is passed as the :since watermark parameter in the SQL query
    """
    engine = create_engine(dsn, pool_pre_ping=True)
    offset=0

    while True:
        with engine.connect() as cx:
            rows = cx.execute(
                text(sql), 
                {"since": since, "limit": chunk_size, "offset": offset}
                ).mappings().all()
        if not rows
            log.info("DB extract complete -  no more rows at offset %d", offset)
            break
        log.info("DB chunk: offset=%d rows=%d", offset, len(rows))
        yield[dict(r) for r in rows]
        offset += chunk_size
    