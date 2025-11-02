# Cupcake Mononoke

A comprehensive data pipeline for extracting, transforming, and loading financial market data from multiple sources including Alpha Vantage and Yahoo Finance.

## Project Overview

Cupcake Mononoke is an ETL (Extract, Transform, Load) pipeline designed to collect and process financial market data across multiple asset classes:
- **Commodities** (WTI, Brent, Coffee, Sugar, Copper, Natural Gas, Aluminum)
- **Cryptocurrencies** (BTC, ETH, LTC, XRP, ADA, DOGE)
- **Stocks** (AAPL, GOOGL, AMZN, NVDA, META, AMD, IBM)
- **Forex** (BRL, EUR, GBP, JPY, CAD, CNY vs USD)
- **Exchange Rates** (USD/EUR, USD/JPY, GBP/USD, etc.)

The pipeline automatically handles API rate limits by rotating between multiple API keys and stores both raw and processed data in structured CSV formats for analysis.

## Project Structure

```
.
├── artifacts/
│   ├── raw/                    # Raw JSON data from APIs
│   │   ├── commodities/
│   │   ├── cryptocurrencies/
│   │   ├── exchange_rates/
│   │   ├── forex/
│   │   ├── stocks/
│   │   └── yahoo_financials/
│   └── processed/              # Transformed CSV data
│       ├── commodities/
│       │   ├── instruments.csv
│       │   └── timeseries.csv
│       ├── cryptocurrencies/
│       ├── exchange_rates/
│       ├── forex/
│       └── stocks/
├── config/
│   └── config.yaml            # Pipeline configuration
├── logs/
│   └── running_logs.log       # Application logs
├── src/
│   └── mononoke/
│       ├── pipeline/
│       │   ├── source.py      # API client classes
│       │   ├── extract.py     # Data extraction logic
│       │   └── transform.py   # Data transformation logic
│       └── utils/
│           └── common.py      # Utility functions
├── tests/
│   └── test_source.py         # Unit tests
├── main.py                    # Pipeline entry point
└── requirements.txt           # Python dependencies
```

## Features

### API Integration
- **Alpha Vantage**: Daily stock data, cryptocurrency prices, forex rates, commodity prices, and real-time exchange rates
- **Yahoo Finance**: Company financials, industry data, and sector information
- **API Key Rotation**: Automatic failover between multiple API keys when rate limits are hit

### Data Pipeline Stages

#### 1. Extract ([`src/mononoke/pipeline/extract.py`](src/mononoke/pipeline/extract.py))
- Fetches data from Alpha Vantage and Yahoo Finance APIs
- Saves raw JSON responses to [`artifacts/raw/`](artifacts/raw/)
- Handles errors gracefully with comprehensive logging
- Supports batched extraction for multiple symbols

#### 2. Transform ([`src/mononoke/pipeline/transform.py`](src/mononoke/pipeline/transform.py))
- Normalizes raw JSON into structured DataFrames
- Generates unique hash IDs for each instrument
- Separates metadata (instruments) and time-series data
- Implements upsert logic to prevent duplicates
- Handles data type conversions and missing values

#### 3. Load ([`src/mononoke/pipeline/load.py`](src/mononoke/pipeline/load.py))
- Loads processed data from CSV files into a structured database.
- Initializes a connection to a PostgreSQL database using credentials from environment variables.
- Creates necessary schemas in the database if they do not exist.
- Scans the processed data directory for CSV files and maps them to their respective tables.
- Appends data to existing tables or creates new tables based on the CSV schema.
- Utilizes SQLAlchemy for database interactions and logging for tracking the loading process.

### Configuration

All extraction targets are defined in [`config/config.yaml`](config/config.yaml):

```yaml
extract_targets:
  commodities:
    - WTI
    - BRENT
    - COFFEE
  
  stock_symbols:
    - AAPL
    - GOOGL
  
  crypto_pairs:
    - [BTC, USD]
    - [ETH, USD]
  
  forex_pairs:
    - [EUR, USD]
    - [GBP, USD]
  
  outputsize: full  # 'compact' or 'full'
  ...
```

## Setup

### Prerequisites
- Python 3.10+
- Alpha Vantage API key(s)
- Internet connection

### Installation

1. Clone the repository:
```bash
git clone https://github.com/Archbaer/cupcake_mononoke
cd cupcake_mononoke
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Create a `.env` file in the project root:
```env
API_KEY1=your_first_api_key
API_KEY2=your_second_api_key
```

### Running the Pipeline

Execute the full ETL pipeline:
```bash
python main.py
```

The pipeline will:
1. Load configuration from [`config/config.yaml`](config/config.yaml)
2. Extract data from all configured sources
3. Save raw JSON to [`artifacts/raw/`](artifacts/raw/)
4. Transform data to structured CSVs in [`artifacts/processed/`](artifacts/processed/)

### Running Tests

```bash
pytest tests/
```

# Data Schema 

### Instruments Table
Each asset class has an `instruments.csv` with metadata:
- `instrument_id`: Unique MD5 hash identifier
- `source`: Data source (e.g., "Alpha Vantage")
- `data_type`: Asset class (e.g., "cryptocurrency", "stock")
- Asset-specific fields (symbol, currency_code, etc.)

### Time Series Table
Each asset class has a `timeseries.csv` with historical data:
- `instrument_id`: Foreign key to instruments table
- `date`: Trading date (YYYY-MM-DD format)
- `open`, `high`, `low`, `close`: Price data
- `volume`: Trading volume (where applicable)

## Logging

All operations are logged to [`logs/running_logs.log`](logs/running_logs.log) with timestamps, log levels, and module information.

## CI/CD

GitHub Actions workflow ([`.github/workflows/cicd.yml`](.github/workflows/cicd.yml)) runs tests on every push to `main`.

## Future Enhancements

- [ ] Machine learning model training
- [ ] REST API for data access
- [ ] Data visualization dashboard
- [ ] Additional data sources (Bloomberg, Quandl)
