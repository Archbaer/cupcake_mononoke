# Cupcake Mononoke

A production‑oriented ETL (Extract–Transform–Load) pipeline for multi‑asset financial market data integrating Alpha Vantage and Yahoo Finance, with Airflow orchestration, modular processing, database loading, and test coverage.

## 1. Overview

Cupcake Mononoke automates:
- Extraction of raw JSON data (commodities, cryptocurrencies, stocks, forex, exchange rates, company financials).
- Transformation into normalized, de‑duplicated, hash‑keyed tabular datasets.
- Loading into PostgreSQL with table mappings persisted for downstream consumption.
- Scheduled execution via Apache Airflow (daily DAG).

It supports API key rotation to mitigate free‑tier rate limits and isolates raw vs processed zones under `artifacts/`.

## 2. Supported Data Domains

| Domain | Source | Examples |
|--------|--------|----------|
| Commodities | Alpha Vantage | COPPER, WTI, BRENT, ALUMINUM |
| Cryptocurrencies | Alpha Vantage | BTC/USD, ETH/USD |
| Stocks | Alpha Vantage + Yahoo Finance | AAPL |
| Forex (daily) | Alpha Vantage | BRL/USD, EUR/USD |
| Exchange Rates (realtime) | Alpha Vantage | GBP/JPY, USD/EUR |
| Company Financials / Info | Yahoo Finance | Income statements, officers |

## 3. Repository Structure

```
.
├── main.py
├── config/
│   └── config.yaml
├── src/mononoke/
│   ├── __init__.py
│   ├── utils/
│   │   └── common.py
│   └── pipeline/
│       ├── source.py
│       ├── extract.py
│       ├── transform.py
│       └── load.py
├── artifacts/
│   ├── raw/
│   └── processed/
├── dags/
│   └── dag.py
├── tests/
│   ├── test_source.py
│   ├── test_extract.py
│   ├── test_transform.py
│   └── test_load.py
├── logs/
│   └── running_logs.log
├── docker-compose_dev.yaml
├── Dockerfile
├── requirements.txt
└── .env
```

Core files:
- Extraction: [`src/mononoke/pipeline/extract.py`](src/mononoke/pipeline/extract.py)
- API clients: [`src/mononoke/pipeline/source.py`](src/mononoke/pipeline/source.py)
- Transformation: [`src/mononoke/pipeline/transform.py`](src/mononoke/pipeline/transform.py)
- Loading: [`src/mononoke/pipeline/load.py`](src/mononoke/pipeline/load.py)
- Utilities: [`src/mononoke/utils/common.py`](src/mononoke/utils/common.py)
- Airflow DAG: [`dags/dag.py`](dags/dag.py)
- Configuration: [`config/config.yaml`](config/config.yaml)
- Entry point: [`main.py`](main.py)

## 4. Data Flow

1. Extract:
   - Reads targets from [`config/config.yaml`](config/config.yaml).
   - Calls Alpha Vantage / Yahoo Finance via [`QueryAlphaVantage`](src/mononoke/pipeline/source.py) and [`QueryYahooFinance`](src/mononoke/pipeline/source.py).
   - Writes raw JSON under `artifacts/raw/<domain>/`.

2. Transform:
   - Iterates raw folders in [`Transform.transform`](src/mononoke/pipeline/transform.py).
   - Builds `instruments.csv` (metadata) + `timeseries.csv` per domain (except Yahoo which adds `information.csv`, `company_officers.csv`, `financials.csv`).
   - De‑duplicates via `_upsert_csv`.

3. Load:
   - Scans processed directories in [`Load._find_directory_files`](src/mononoke/pipeline/load.py).
   - Creates schema(s) from `database_schemas` in config.
   - Bulk loads CSVs with PostgreSQL `COPY`.
   - Saves table mapping to `artifacts/table_mappings.json`.

## 5. Configuration

Excerpt (active targets commented for dev control):

```yaml
# config/config.yaml
extract_targets:
  commodities:
    - COPPER
  stock_symbols:
    - AAPL
  outputsize: compact
  forex_pairs:
    - [BRL, USD]
data_directory:
  raw_data: ./artifacts/raw/
  processed_data: ./artifacts/processed/
database_schemas:
  - finance
```

Adjust targets to scale breadth. Keep free‑tier limits in mind.

## 6. Environment Variables (`.env`)

```env
ALPHA_VANTAGE=primary_key
ALPHA_VANTAGE2=secondary_key
DB_HOST=db
DB_PORT=5432
DB_USER=admin
DB_PASSWORD=admin
DB_NAME=etl_db
AIRFLOW_HOME=/opt/airflow
```

Keys are consumed in [`main.py`](main.py) and [`dags/dag.py`](dags/dag.py).

## 7. API Key Rotation

Implemented in [`QueryAlphaVantage._make_request`](src/mononoke/pipeline/source.py):
- On rate limit message (`Note`, `Information`), rotates `current_key_index`.
- Raises when all keys exhausted.
- Inserts delay before retry (`time.sleep(300)` free‑tier friendly).

## 8. Logging & Observability

Central logger configured in [`src/mononoke/__init__.py`](src/mononoke/__init__.py):
- Console + file sink: `logs/running_logs.log`
- Each stage emits success/error messages.
- Airflow tasks surface logs in the UI; underlying file still collects.

Typical error pattern:
```
[YYYY-MM-DD HH:MM:SS,mmm: ERROR: source]: Alpha Vantage error fetching Forex USD->JPY: ...
```

## 9. Running Locally

Install deps:
```bash
pip install -r requirements.txt
```

Run ETL:
```bash
python main.py
```

Artifacts appear under:
```
artifacts/
  raw/
  processed/
```

## 10. Running with Docker + Airflow

Start stack:
```bash
docker compose -f docker-compose_dev.yaml up --build
```

Airflow Web (default):
```
http://localhost:8080
```

Trigger DAG: `finance_etl` defined in [`dags/dag.py`](dags/dag.py).

## 11. Database Loading

Requires reachable PostgreSQL (container service `db`).
Tables created as `<schema>.<folder>_<filename_stem>`, e.g.:
```
finance.commodities_timeseries
finance.stocks_instruments
```
Mappings saved to: [`artifacts/table_mappings.json`](artifacts/table_mappings.json)

## 12. Data Schema (Processed)

Common columns:
- `instrument_id`: MD5 hash from [`Transform.generate_hash_id`](src/mononoke/pipeline/transform.py)
- `date`: `%Y-%m-%d`
- Domain specific: `open`, `high`, `low`, `close`, `volume`, `price`, etc.

Yahoo:
- `information.csv`: company profile (cleaned)
- `company_officers.csv`: officer roster
- `financials.csv`: date‑indexed statement entries

## 13. Testing

Pytest suite:
- API abstraction: [`tests/test_source.py`](tests/test_source.py)
- Extraction (stubbed): [`tests/test_extract.py`](tests/test_extract.py)
- Transformation (temp dirs): [`tests/test_transform.py`](tests/test_transform.py)
- Loading (DB init monkeypatched): [`tests/test_load.py`](tests/test_load.py)

Run:
```bash
pytest -q
```

CI workflow: [`.github/workflows/cicd.yml`](.github/workflows/cicd.yml).

## 14. Error Handling Strategy

- Input validation (missing config keys) raises early in [`Extract.__init__`](src/mononoke/pipeline/extract.py).
- Structured exceptions with contextual logging (e.g., unexpected API response blocks).
- Atomic JSON writes (`save_json` in [`common.py`](src/mononoke/utils/common.py)) to avoid partial files.

## 15. Extending the Pipeline

Add a new asset class:
1. Implement fetch method in [`source.py`](src/mononoke/pipeline/source.py).
2. Add extraction wrapper in [`extract.py`](src/mononoke/pipeline/extract.py).
3. Implement transform function in [`transform.py`](src/mononoke/pipeline/transform.py).
4. Update `config.yaml` targets.
5. Add unit test stubs under `tests/`.

## 16. Troubleshooting

| Symptom | Cause | Action |
|---------|-------|--------|
| Rate limit errors persist | Both keys exhausted | Wait daily reset / add keys |
| Empty processed CSV | Raw folder missing data | Verify extraction targets |
| Load failure (COPY) | Table schema misaligned | Drop table or adjust CSV cols |
| Airflow DAG not visible | Volume/path mismatch | Confirm `AIRFLOW_HOME` and mount |

## 17. Security Notes

- Store API keys only in `.env` (excluded by `.gitignore`).
- Avoid committing raw sensitive financial dumps if proprietary.
- Consider secrets management (Vault / AWS Secrets) for production.

## 18. Future Roadmap

- Incremental change detection
- Validation & quality metrics
- Data catalog integration
- REST serving layer
- ML feature store derivations
- Enhanced retry/backoff policy

## 19. Contributing

1. Fork & branch (`feature/...`).
2. Add / update tests.
3. Run `pytest`.
4. Open PR against `main`.