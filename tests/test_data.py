"""Tests for data loading, cleaning, and universe construction."""

from pathlib import Path
import json
import zipfile
from http.client import IncompleteRead

import pandas as pd
import yaml

from scripts.fetch_sec_data import fetch_sec_source_data
from scripts.prepare_yahoo_prices import prepare_yahoo_price_data
from src.data.cleaner import clean_fundamentals_data, clean_price_data
from src.data.loader import load_market_data
from src.data.sec import (
    SEC_COMPANYFACTS_BULK_URL,
    SEC_SUBMISSIONS_BULK_URL,
    SEC_TICKERS_URL,
    download_sec_bulk_archive,
    list_zip_members,
    load_sec_ticker_mapping,
    normalize_sec_ticker_mapping,
    read_json_from_zip,
)
from src.data.universe import build_universe
from src.data.yahoo import build_benchmark_from_prices, download_yahoo_prices, read_tickers_file


def test_clean_price_data_normalizes_columns_and_filters_invalid_rows() -> None:
    raw = pd.DataFrame(
        {
            "Date": ["2024-01-02", "2024-01-02", "2024-01-03", None],
            "Symbol": ["aapl", "aapl", "msft", "nvda"],
            "Adj_Close": [185.0, 186.0, 0.0, 900.0],
            "Open": [184.0, 185.0, 380.0, 890.0],
            "High": [186.0, 187.0, 381.0, 910.0],
            "Low": [183.0, 184.0, 379.0, 880.0],
            "Volume": [100, 200, 300, 400],
        }
    )

    cleaned = clean_price_data(raw)

    assert list(cleaned.columns) == ["date", "ticker", "open", "high", "low", "close", "volume"]
    assert cleaned.shape[0] == 1
    assert cleaned.iloc[0]["ticker"] == "AAPL"
    assert cleaned.iloc[0]["close"] == 186.0


def test_clean_fundamentals_data_adds_missing_columns() -> None:
    raw = pd.DataFrame(
        {
            "date": ["2024-01-02"],
            "ticker": ["aapl"],
            "pb": [10.5],
        }
    )

    cleaned = clean_fundamentals_data(raw)

    assert list(cleaned.columns) == ["date", "ticker", "pe", "pb", "roe"]
    assert cleaned.iloc[0]["ticker"] == "AAPL"


def test_build_universe_applies_history_missingness_and_price_filters() -> None:
    prices = pd.DataFrame(
        {
            "date": pd.to_datetime(
                [
                    "2024-01-01",
                    "2024-01-02",
                    "2024-01-03",
                    "2024-01-01",
                    "2024-01-02",
                    "2024-01-03",
                ]
            ),
            "ticker": ["AAPL", "AAPL", "AAPL", "PENNY", "PENNY", "NEW"],
            "close": [180.0, 181.0, 182.0, 1.0, 1.2, 50.0],
            "open": [179.0, 180.0, 181.0, 1.0, 1.1, 49.0],
            "high": [181.0, 182.0, 183.0, 1.1, 1.3, 51.0],
            "low": [178.0, 179.0, 180.0, 0.9, 1.0, 48.0],
            "volume": [100, 100, 100, 200, 200, 50],
        }
    )

    universe = build_universe(prices, min_history_days=2, min_price=5.0, max_missing_ratio=0.1)

    assert set(universe["ticker"].unique()) == {"AAPL"}


def test_load_market_data_reads_configured_csv_files_and_filters_dates(tmp_path: Path) -> None:
    config_dir = tmp_path / "config"
    data_dir = tmp_path / "data"
    config_dir.mkdir()
    data_dir.mkdir()

    price_path = data_dir / "prices.csv"
    fundamentals_path = data_dir / "fundamentals.csv"
    benchmark_path = data_dir / "benchmark.csv"

    pd.DataFrame(
        {
            "date": ["2023-12-31", "2024-01-02"],
            "ticker": ["AAPL", "AAPL"],
            "open": [180.0, 181.0],
            "high": [182.0, 183.0],
            "low": [179.0, 180.0],
            "close": [181.0, 182.0],
            "volume": [1000, 1100],
        }
    ).to_csv(price_path, index=False)

    pd.DataFrame(
        {
            "date": ["2023-12-31", "2024-01-02"],
            "ticker": ["AAPL", "AAPL"],
            "pe": [25.0, 26.0],
            "pb": [10.0, 10.1],
            "roe": [0.2, 0.21],
        }
    ).to_csv(fundamentals_path, index=False)

    pd.DataFrame(
        {
            "date": ["2023-12-31", "2024-01-02"],
            "ticker": ["SPY", "SPY"],
            "close": [470.0, 471.0],
        }
    ).to_csv(benchmark_path, index=False)

    with (config_dir / "default.yaml").open("w", encoding="utf-8") as handle:
        yaml.safe_dump(
            {
                "start_date": "2024-01-01",
                "end_date": "2024-12-31",
            },
            handle,
        )

    with (config_dir / "paths.yaml").open("w", encoding="utf-8") as handle:
        yaml.safe_dump(
            {
                "paths": {
                    "market_data": str(price_path),
                    "fundamentals_data": str(fundamentals_path),
                    "benchmark_data": str(benchmark_path),
                }
            },
            handle,
        )

    bundle = load_market_data(
        config_path=str(config_dir / "default.yaml"),
        paths_path=str(config_dir / "paths.yaml"),
    )

    assert bundle.prices["date"].min() == pd.Timestamp("2024-01-02")
    assert bundle.fundamentals["date"].min() == pd.Timestamp("2024-01-02")
    assert bundle.benchmark["date"].min() == pd.Timestamp("2024-01-02")


def test_build_benchmark_from_prices_extracts_requested_ticker() -> None:
    prices = pd.DataFrame(
        {
            "date": pd.to_datetime(["2024-01-02", "2024-01-02"]),
            "ticker": ["SPY", "AAPL"],
            "open": [470.0, 183.0],
            "high": [471.0, 185.0],
            "low": [469.0, 182.0],
            "close": [470.5, 184.5],
            "volume": [1000000, 1200000],
        }
    )

    benchmark = build_benchmark_from_prices(prices, ticker="SPY")

    assert list(benchmark.columns) == ["date", "ticker", "close"]
    assert benchmark["ticker"].tolist() == ["SPY"]
    assert benchmark["close"].tolist() == [470.5]


def test_read_tickers_file_reads_one_symbol_per_line(tmp_path: Path) -> None:
    tickers_path = tmp_path / "tickers.txt"
    tickers_path.write_text("aapl\nmsft\n\nspy\n", encoding="utf-8")

    tickers = read_tickers_file(tickers_path)

    assert tickers == ["AAPL", "MSFT", "SPY"]


def test_download_yahoo_prices_normalizes_multiindex_download(monkeypatch) -> None:
    columns = pd.MultiIndex.from_product(
        [["Open", "High", "Low", "Adj Close", "Volume"], ["AAPL", "SPY"]],
        names=[None, "Ticker"],
    )
    downloaded = pd.DataFrame(
        [
            [183.0, 470.0, 185.0, 471.0, 182.0, 469.0, 184.5, 470.5, 1000, 2000],
            [184.0, 471.0, 186.0, 472.0, 183.0, 470.0, 185.5, 471.5, 1100, 2100],
        ],
        index=pd.to_datetime(["2024-01-02", "2024-01-03"]),
        columns=columns,
    )
    downloaded.index.name = "Date"

    class FakeYFinance:
        @staticmethod
        def download(**kwargs):
            return downloaded

    monkeypatch.setattr("src.data.yahoo._import_yfinance", lambda: FakeYFinance)

    prices = download_yahoo_prices(["AAPL", "SPY"], start_date="2024-01-01", end_date="2024-01-10")

    assert list(prices.columns) == ["date", "ticker", "open", "high", "low", "close", "volume"]
    assert set(prices["ticker"].unique()) == {"AAPL", "SPY"}
    assert prices.loc[(prices["ticker"] == "AAPL") & (prices["date"] == pd.Timestamp("2024-01-02")), "close"].iloc[0] == 184.5


def test_prepare_yahoo_price_data_writes_standard_tables(tmp_path: Path, monkeypatch) -> None:
    config_dir = tmp_path / "config"
    config_dir.mkdir(parents=True)

    with (config_dir / "paths.yaml").open("w", encoding="utf-8") as handle:
        yaml.safe_dump(
            {
                "paths": {
                    "market_data": "data/raw/us_equity_prices.parquet",
                    "fundamentals_data": "data/raw/us_equity_fundamentals.parquet",
                    "benchmark_data": "data/raw/us_benchmark.parquet",
                }
            },
            handle,
        )

    sample_prices = pd.DataFrame(
        {
            "date": pd.to_datetime(["2024-01-02", "2024-01-02"]),
            "ticker": ["AAPL", "SPY"],
            "open": [183.0, 470.0],
            "high": [185.0, 471.0],
            "low": [182.0, 469.0],
            "close": [184.5, 470.5],
            "volume": [1000, 2000],
        }
    )

    monkeypatch.setattr("scripts.prepare_yahoo_prices.download_yahoo_prices", lambda *args, **kwargs: sample_prices)

    written = prepare_yahoo_price_data(
        tickers=["AAPL"],
        start_date="2024-01-01",
        end_date="2024-01-10",
        benchmark_ticker="SPY",
        project_root=tmp_path,
    )

    prices = pd.read_parquet(written["market_data"])
    benchmark = pd.read_parquet(written["benchmark_data"])

    assert written["market_data"].exists()
    assert written["benchmark_data"].exists()
    assert set(prices["ticker"].unique()) == {"AAPL", "SPY"}
    assert benchmark["ticker"].tolist() == ["SPY"]


def test_normalize_sec_ticker_mapping_normalizes_ticker_and_cik() -> None:
    raw = {
        "0": {"ticker": "aapl", "cik_str": 320193, "title": "Apple Inc."},
        "1": {"ticker": "msft", "cik_str": 789019, "title": "Microsoft Corp"},
    }

    mapping = normalize_sec_ticker_mapping(raw)

    assert list(mapping.columns) == ["ticker", "cik", "company_name"]
    assert mapping["ticker"].tolist() == ["AAPL", "MSFT"]
    assert mapping["cik"].tolist() == [320193, 789019]


def test_load_sec_ticker_mapping_reads_downloaded_json(tmp_path: Path) -> None:
    mapping_path = tmp_path / "company_tickers.json"
    mapping_path.write_text(
        json.dumps({"0": {"ticker": "spy", "cik_str": 884394, "title": "SPDR S&P 500 ETF Trust"}}),
        encoding="utf-8",
    )

    mapping = load_sec_ticker_mapping(mapping_path)

    assert mapping.iloc[0]["ticker"] == "SPY"
    assert int(mapping.iloc[0]["cik"]) == 884394


def test_list_zip_members_and_read_json_from_zip(tmp_path: Path) -> None:
    archive_path = tmp_path / "companyfacts.zip"
    payload = {"cik": "0000320193", "entityName": "Apple Inc."}

    with zipfile.ZipFile(archive_path, "w") as archive:
        archive.writestr("companyfacts/CIK0000320193.json", json.dumps(payload))

    members = list_zip_members(archive_path)
    loaded = read_json_from_zip(archive_path, "companyfacts/CIK0000320193.json")

    assert members == ["companyfacts/CIK0000320193.json"]
    assert loaded["entityName"] == "Apple Inc."


def test_fetch_sec_source_data_downloads_expected_files(tmp_path: Path, monkeypatch) -> None:
    downloads: list[tuple[str, Path]] = []

    def fake_download_tickers(output_path: Path, user_agent: str = "unused") -> Path:
        downloads.append((SEC_TICKERS_URL, output_path))
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text("{}", encoding="utf-8")
        return output_path

    def fake_download_archive(url: str, output_path: Path, user_agent: str = "unused") -> Path:
        downloads.append((url, output_path))
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_bytes(b"zip")
        return output_path

    monkeypatch.setattr("scripts.fetch_sec_data.download_sec_company_tickers", fake_download_tickers)
    monkeypatch.setattr("scripts.fetch_sec_data.download_sec_bulk_archive", fake_download_archive)

    written = fetch_sec_source_data(output_dir="data/raw/sec_source", project_root=tmp_path)

    assert written["company_tickers"] == tmp_path / "data/raw/sec_source/company_tickers.json"
    assert written["submissions"] == tmp_path / "data/raw/sec_source/submissions.zip"
    assert written["companyfacts"] == tmp_path / "data/raw/sec_source/companyfacts.zip"
    assert downloads[0][0] == SEC_TICKERS_URL
    assert downloads[1][0] == SEC_SUBMISSIONS_BULK_URL
    assert downloads[2][0] == SEC_COMPANYFACTS_BULK_URL


def test_download_sec_bulk_archive_retries_after_incomplete_read(tmp_path: Path, monkeypatch) -> None:
    output_path = tmp_path / "submissions.zip"
    attempts = {"count": 0}
    valid_zip = tmp_path / "valid.zip"
    with zipfile.ZipFile(valid_zip, "w") as archive:
        archive.writestr("submissions/CIK0000000001.json", "{}")
    valid_zip_bytes = valid_zip.read_bytes()

    class FakeResponse:
        def __init__(self, chunks, fail=False, expected_bytes: int | None = None):
            self.chunks = list(chunks)
            self.fail = fail
            self.read_count = 0
            self.headers = {"Content-Length": str(expected_bytes)} if expected_bytes is not None else {}

        def read(self, _chunk_size):
            self.read_count += 1
            if self.fail and self.read_count == 2:
                raise IncompleteRead(b"abc", 3)
            if self.chunks:
                return self.chunks.pop(0)
            return b""

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    def fake_urlopen(_request, timeout=120):
        attempts["count"] += 1
        if attempts["count"] == 1:
            return FakeResponse([b"abc", b"def"], fail=True)
        return FakeResponse([valid_zip_bytes], fail=False, expected_bytes=len(valid_zip_bytes))

    monkeypatch.setattr("src.data.sec.urlopen", fake_urlopen)

    written = download_sec_bulk_archive("https://example.com/submissions.zip", output_path)

    assert attempts["count"] == 2
    assert written == output_path
    assert output_path.read_bytes() == valid_zip_bytes


def test_download_sec_bulk_archive_retries_when_zip_is_invalid(tmp_path: Path, monkeypatch) -> None:
    output_path = tmp_path / "companyfacts.zip"
    attempts = {"count": 0}

    class FakeResponse:
        def __init__(self, payload: bytes):
            self.payload = payload
            self.headers = {"Content-Length": str(len(payload))}
            self.sent = False

        def read(self, _chunk_size):
            if self.sent:
                return b""
            self.sent = True
            return self.payload

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    def fake_urlopen(_request, timeout=120):
        attempts["count"] += 1
        if attempts["count"] == 1:
            return FakeResponse(b"not-a-zip")

        valid_zip = tmp_path / "valid.zip"
        with zipfile.ZipFile(valid_zip, "w") as archive:
            archive.writestr("companyfacts/CIK0000000001.json", "{}")
        return FakeResponse(valid_zip.read_bytes())

    monkeypatch.setattr("src.data.sec.urlopen", fake_urlopen)

    written = download_sec_bulk_archive("https://example.com/companyfacts.zip", output_path)

    assert attempts["count"] == 2
    assert written == output_path
    assert output_path.exists()
