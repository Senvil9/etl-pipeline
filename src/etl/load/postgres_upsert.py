from sqlalchemy import create_engine, text

def upsert_rows(dsn, table, rows, key_cols):
    eng = create_engine(dsn)
    cols = None
    with eng.begin() as cx:
        for batch in _chunks(rows, 1000):
            if cols is None:
                cols = list(batch[0].keys()) 
            #if cols =["customer_id", name, ...] result ":customer_id, :name, ...."
            placeholders = ",".join(f":{c}" for c in cols)
            updates = ",".join(f"{c}=EXCLUDED.{c}" for c in cols if c not in key_cols)
            sql = f"""((
            INSERT INTO {table} ({",".join(cols)})
            VALUES ({placeholders})
            ON CONFLICT ({",".join(key_cols)})
            DO UPDATE SET {updates}
            """
            cx.execute(text(sql), batch)
            
def _chunks(seq, size):
    buf = []
    for item in seq:
        buf.append(item)
        if len(buf) >= size:
            yield buf; buf=[]
        if buf: yield buf