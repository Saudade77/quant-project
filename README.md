# Multi-Factor Stock Selection and Backtest System

A Python research project for quant research / quant strategy internship portfolios. The project builds a small but complete pipeline for:

- preparing market and fundamentals data
- constructing cross-sectional factor scores
- selecting a portfolio on a monthly schedule
- running a daily backtest
- generating IC, group return, and performance reports

The current version supports both sample data and a free real-data workflow:

- prices and benchmark from Yahoo Finance via `yfinance`
- fundamentals derived from SEC EDGAR raw data

## What It Does

The pipeline currently includes:

- local `csv/parquet` data loading
- market / benchmark / fundamentals normalization
- tradable universe filtering
- factor calculation:
  - `bp`
  - `ep`
  - `mom_20`
  - `mom_60`
  - `roe`
- factor preprocessing and composite scoring
- monthly rebalancing and equal-weight portfolio construction
- daily close-to-close backtesting
- performance metrics:
  - `total_return`
  - `annual_return`
  - `annual_volatility`
  - `sharpe`
  - `max_drawdown`
- IC / RankIC analysis
- group return analysis
- report export to `outputs/reports/`

## Pipeline

```text
prices + fundamentals + benchmark
  -> data loading and cleaning
  -> universe filtering
  -> factor calculation
  -> factor preprocessing
  -> composite score generation
  -> portfolio construction
  -> backtest
  -> analysis and report export
```

## Project Structure

```text
quant-project/
├── config/
├── data/
│   ├── raw/
│   └── sample/
├── outputs/
│   └── reports/
├── scripts/
│   ├── fetch_sec_data.py
│   ├── make_sample_data.py
│   ├── prepare_real_data.py
│   ├── prepare_sec_fundamentals.py
│   ├── prepare_yahoo_prices.py
│   ├── run_backtest.py
│   └── run_pipeline.py
├── src/
│   ├── analysis/
│   ├── backtest/
│   ├── data/
│   ├── factors/
│   ├── strategy/
│   └── utils/
└── tests/
```

## Installation

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

## Running The Project

### Option 1: sample data

If the standard data tables do not exist, the pipeline can fall back to generated sample data:

```bash
python3 scripts/run_pipeline.py
```

### Option 2: free real-data workflow

Use the provided large-cap starter universe:

```bash
python3 scripts/prepare_real_data.py \
  --tickers-file data/sample/us_largecap_tickers.txt \
  --start-date 2020-01-01 \
  --end-date 2025-12-31 \
  --benchmark-ticker SPY \
  --sec-user-agent "your-name your-email@example.com"
```

Then run:

```bash
python3 scripts/run_pipeline.py
```

If real data was prepared successfully, the script should report:

```text
Input data source: existing_project_data
```

To print a backtest-only summary:

```bash
python3 scripts/run_backtest.py
```

## Data Sources

### Prices and benchmark

- Yahoo Finance via `yfinance`
- benchmark ticker defaults to `SPY`

Preparation script:

```bash
python3 scripts/prepare_yahoo_prices.py \
  --tickers-file data/sample/us_largecap_tickers.txt \
  --start-date 2020-01-01 \
  --end-date 2025-12-31 \
  --benchmark-ticker SPY
```

### SEC raw fundamentals

The project downloads:

- `company_tickers.json`
- `submissions.zip`
- `companyfacts.zip`

Preparation script:

```bash
python3 scripts/fetch_sec_data.py \
  --user-agent "your-name your-email@example.com"
```

### Standardized fundamentals table

The project derives a research-ready table with:

- `date`
- `ticker`
- `pe`
- `pb`
- `roe`

Preparation script:

```bash
python3 scripts/prepare_sec_fundamentals.py
```

## Standard Input Tables

The main pipeline reads only these three standardized tables:

- `data/raw/us_equity_prices.parquet`
- `data/raw/us_equity_fundamentals.parquet`
- `data/raw/us_benchmark.parquet`

That keeps the research pipeline separate from online downloading and raw-data handling.

## Outputs

Running `python3 scripts/run_pipeline.py` generates:

- `outputs/reports/scores.csv`
- `outputs/reports/portfolio.csv`
- `outputs/reports/backtest_result.csv`
- `outputs/reports/ic_result.csv`
- `outputs/reports/group_return_result.csv`
- `outputs/reports/analysis_report.txt`

## Configuration

Main config files:

- `config/default.yaml`
- `config/paths.yaml`

Current defaults include:

- market: `us`
- benchmark: `SPY`
- factor set: `bp`, `ep`, `mom_20`, `mom_60`, `roe`
- rebalance: `monthly`
- portfolio size: `top_n = 20`
- weighting: `equal`
- execution price: `close`
- commission: `10 bps`

## Included Starter Universe

The repo includes a starter ticker list at:

- `data/sample/us_largecap_tickers.txt`

This list is meant to be practical for a first real-data run:

- large-cap names
- broad sector coverage
- stable Yahoo / SEC coverage
- lower data-cleaning friction than a full-market universe

## Current Limitations

- daily-frequency research only
- simplified transaction cost model
- close-to-close execution assumption
- research-grade SEC alignment, not institutional point-in-time infrastructure
- banks and insurers are not the first-priority reporting template

## Research Use Notice

This repository is intended for research and educational purposes. Yahoo Finance access in this project is mediated through `yfinance`, and SEC data is sourced from public EDGAR resources. Review the terms and usage policies of each data source before using the project in other contexts.

## Example Result

One successful real-data run produced:

- `total_return`: `2.2712`
- `annual_return`: `0.2731`
- `annual_volatility`: `0.2270`
- `sharpe`: `1.2031`
- `max_drawdown`: `-0.3218`

The corresponding output files are included under `outputs/reports/`.
