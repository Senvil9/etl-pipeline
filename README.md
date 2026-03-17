# ETL Pipeline

A production-ready ETL pipeline built on top of [sskarkid3v/etl-skeleton](https://github.com/sskarkid3v/etl-skeleton).

Supports **6 execution modes** across three data sources (API, CSV, DB) and two load strategies (COPY, UPSERT), loading into PostgreSQL running in Docker.

---

## Project Structure

```
etl-pipeline/
├── config/
│   ├── settings.yaml       # pipeline config (tables, endpoints, chunk sizes)
│   └── logging.yaml        # JSON structured logging
├── data/                   # generated at runtime by generate_csv.py
├── scripts/
│   └── generate_csv.py     # fake-user CSV generator
├── sql/
│   └── init.sql            # schema + table definitions, watermark seed
├── src/etl/
│   ├── extract/
│   │   ├── api_client.py   # reqres.in paginated fetch with retries
│   │   ├── file_reader.py  # CSV / Parquet reader
│   │   └── db_reader.py    # chunked Postgres source reader
│   ├── transform/
│   │   └── core.py         # validation + normalisation → User model
│   ├── load/
│   │   ├── postgres_copy.py    # bulk COPY via staging table
│   │   └── postgres_upsert.py  # INSERT … ON CONFLICT DO UPDATE
│   ├── config.py           # settings.yaml + .env loader
│   ├── logging_setup.py    # dictConfig from logging.yaml
│   ├── main.py             # CLI entrypoint — all 6 modes
│   ├── models.py           # Pydantic User model with validators
│   └── watermark.py        # incremental load helpers
├── .env.example
├── docker-compose.yml
├── pyproject.toml
└── requirements.txt
```

---

## Prerequisites

- Python 3.10+
- Docker + Docker Compose
- A [reqres.in](https://reqres.in) API key (free account)

---

## Setup

### 1. Clone and enter the repo

```bash
git clone <your-repo-url>
cd etl-pipeline
```

### 2. Create and activate a virtual environment

```bash
python -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate
pip install -r requirements.txt
pip install -e .
```

### 3. Configure environment variables

```bash
cp .env.example .env
```

Open `.env` and fill in your values:

```dotenv
POSTGRES_USER=etl_user
POSTGRES_PASSWORD=etl_pass
POSTGRES_DB=etl_db
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DSN=postgresql+psycopg2://etl_user:etl_pass@localhost:5432/etl_db

API_TOKEN=your_reqres_api_key_here   # from https://reqres.in
```

### 4. Start Postgres

```bash
docker compose up -d
```

The container mounts `sql/init.sql` on first start, which creates:
- `raw_schema.users` — target for the CSV pipeline, source for DB→DB
- `staging_schema.users` — target for API and DB pipelines
- `public.etl_watermarks` — tracks last successful run per pipeline

Wait for the health check to pass:

```bash
docker compose ps   # STATUS should show "(healthy)"
```

### 5. Generate the CSV source file

```bash
python scripts/generate_csv.py             # 100 rows → data/users.csv
python scripts/generate_csv.py --rows 500  # custom row count
```

---

## Running the Pipeline

All six modes are driven by a single entrypoint with `--source` and `--load` flags.

```bash
python -m etl.main --source <source> --load <strategy>
```

| Mode | Command |
|------|---------|
| API → COPY   | `python -m etl.main --source api --load copy`   |
| API → UPSERT | `python -m etl.main --source api --load upsert` |
| CSV → COPY   | `python -m etl.main --source csv --load copy`   |
| CSV → UPSERT | `python -m etl.main --source csv --load upsert` |
| DB → COPY    | `python -m etl.main --source db  --load copy`   |
| DB → UPSERT  | `python -m etl.main --source db  --load upsert` |

> **Recommended order for a first run:**
> 1. `csv --load copy` — populates `raw_schema.users`
> 2. `api --load upsert` — populates `staging_schema.users` from the API
> 3. `db  --load upsert` — copies from `raw_schema` → `staging_schema`

---

## Configuration

All non-secret settings live in `config/settings.yaml`:

```yaml
run:
  batch_size: 500           # rows per upsert batch

sources:
  api:
    base_url: "https://reqres.in/api"
    endpoint: "/users"
    per_page: 6             # reqres.in max page size
    data_key: "data"
    rate_limit_per_sec: 5.0

  file:
    path: "data/users.csv"
    fmt: "csv"

  db:
    query: >                # watermark (:since), pagination (:limit, :offset)
      SELECT user_id, email, first_name, last_name, avatar, created_at
      FROM raw_schema.users
      WHERE created_at >= :since
      LIMIT :limit OFFSET :offset
    chunk_size: 500

load:
  api_target_table: "staging_schema.users"
  db_target_table:  "staging_schema.users"
  csv_target_table: "raw_schema.users"
  key_columns: ["user_id"]
```

---

## Transformations Applied

All sources pass through the same `normalize_users()` function before loading:

| Rule | Behaviour |
|------|-----------|
| Missing `id` / `user_id` | Row is **dropped** and counted as `rejected` |
| Empty `first_name` or `last_name` after trimming | Row is **dropped** |
| Whitespace in string fields | **Trimmed** via Pydantic validator |
| `email` casing | **Lowercased** |

---

## Incremental Loads (Watermark)

Each pipeline tracks its last successful run in `public.etl_watermarks`.  
On the next run, the DB extractor queries only rows newer than that timestamp (`WHERE created_at >= :since`).  
The API and CSV pipelines also update the watermark so you can audit when each last ran.

To reset a watermark (force a full reload):

```sql
UPDATE public.etl_watermarks SET last_run_at = '1970-01-01' WHERE pipeline = 'db';
```

---

## Logging

All output is structured JSON, emitted to stdout. Example lines:

```json
{"time":"2025-01-01T12:00:00Z","level":"INFO","logger":"etl.main","message":"{\"event\":\"pipeline_start\",\"source\":\"api\",\"load_mode\":\"copy\"}"}
{"time":"2025-01-01T12:00:01Z","level":"INFO","logger":"etl.main","message":"{\"event\":\"pipeline_complete\",\"source\":\"api\",\"load_mode\":\"copy\",\"extracted_count\":12,\"loaded_count\":12,\"rejected_count\":0}"}
```

On failure, an `pipeline_error` event is logged with `source`, `stage`, and `detail` before exit.

---

## Stopping the Database

```bash
docker compose down          # stop containers, keep volume
docker compose down -v       # stop containers AND delete all data
```