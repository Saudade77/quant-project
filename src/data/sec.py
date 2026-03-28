"""Utilities for fetching and normalizing free SEC EDGAR source data."""

from __future__ import annotations

import io
import json
import zipfile
from http.client import IncompleteRead
from pathlib import Path
from urllib.error import URLError
from urllib.request import Request, urlopen

import pandas as pd

SEC_TICKERS_URL = "https://www.sec.gov/files/company_tickers.json"
SEC_SUBMISSIONS_BULK_URL = "https://www.sec.gov/Archives/edgar/daily-index/bulkdata/submissions.zip"
SEC_COMPANYFACTS_BULK_URL = "https://www.sec.gov/Archives/edgar/daily-index/xbrl/companyfacts.zip"
DEFAULT_USER_AGENT = "quant-project/0.1 contact: local-research@example.com"


def _http_get_bytes(url: str, user_agent: str = DEFAULT_USER_AGENT) -> bytes:
    request = Request(url, headers={"User-Agent": user_agent, "Accept-Encoding": "identity"})
    with urlopen(request, timeout=120) as response:
        return response.read()


def _stream_http_get_to_path(
    url: str,
    output_path: str | Path,
    user_agent: str = DEFAULT_USER_AGENT,
    chunk_size: int = 1024 * 1024,
    max_retries: int = 3,
) -> Path:
    destination = Path(output_path)
    temp_path = destination.with_suffix(destination.suffix + ".part")

    for attempt in range(1, max_retries + 1):
        try:
            request = Request(url, headers={"User-Agent": user_agent, "Accept-Encoding": "identity"})
            with urlopen(request, timeout=120) as response, temp_path.open("wb") as handle:
                response_headers = getattr(response, "headers", {})
                expected_bytes = response_headers.get("Content-Length") if hasattr(response_headers, "get") else None
                expected_bytes = int(expected_bytes) if expected_bytes is not None else None
                bytes_written = 0
                while True:
                    chunk = response.read(chunk_size)
                    if not chunk:
                        break
                    handle.write(chunk)
                    bytes_written += len(chunk)

            if expected_bytes is not None and bytes_written != expected_bytes:
                raise IncompleteRead(b"", expected_bytes - bytes_written)

            temp_path.replace(destination)
            return destination
        except (IncompleteRead, TimeoutError, URLError, ConnectionError):
            if temp_path.exists():
                temp_path.unlink()
            if attempt == max_retries:
                raise

    return destination


def download_sec_company_tickers(output_path: str | Path, user_agent: str = DEFAULT_USER_AGENT) -> Path:
    """Download the SEC ticker-to-CIK mapping JSON."""
    destination = Path(output_path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_bytes(_http_get_bytes(SEC_TICKERS_URL, user_agent=user_agent))
    return destination


def download_sec_bulk_archive(url: str, output_path: str | Path, user_agent: str = DEFAULT_USER_AGENT) -> Path:
    """Download an SEC bulk ZIP archive to disk."""
    destination = Path(output_path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    max_retries = 3
    for attempt in range(1, max_retries + 1):
        downloaded = destination
        try:
            downloaded = _stream_http_get_to_path(url, destination, user_agent=user_agent, max_retries=1)
            with zipfile.ZipFile(downloaded) as archive:
                archive.namelist()
            return downloaded
        except (IncompleteRead, TimeoutError, URLError, ConnectionError, zipfile.BadZipFile):
            if downloaded.exists():
                downloaded.unlink()
            if attempt == max_retries:
                raise
    return destination


def normalize_sec_ticker_mapping(mapping_data: dict | list) -> pd.DataFrame:
    """Normalize SEC ticker mapping JSON into a tabular ticker/CIK mapping."""
    if isinstance(mapping_data, dict):
        records = list(mapping_data.values())
    else:
        records = list(mapping_data)

    frame = pd.DataFrame(records)
    if frame.empty:
        return pd.DataFrame(columns=["ticker", "cik", "company_name"])

    rename_map = {
        "ticker": "ticker",
        "cik_str": "cik",
        "title": "company_name",
    }
    frame = frame.rename(columns=rename_map)
    for column in ["ticker", "company_name"]:
        if column not in frame.columns:
            frame[column] = pd.NA
    if "cik" not in frame.columns:
        frame["cik"] = pd.NA

    frame["ticker"] = frame["ticker"].astype("string").str.upper().str.strip()
    frame["cik"] = pd.to_numeric(frame["cik"], errors="coerce").astype("Int64")
    return frame[["ticker", "cik", "company_name"]].dropna(subset=["ticker", "cik"]).drop_duplicates("ticker").reset_index(drop=True)


def load_sec_ticker_mapping(path: str | Path) -> pd.DataFrame:
    """Load a downloaded SEC ticker mapping JSON file."""
    with Path(path).open("r", encoding="utf-8") as handle:
        content = json.load(handle)
    return normalize_sec_ticker_mapping(content)


def list_zip_members(path: str | Path) -> list[str]:
    """List member file names in a ZIP archive."""
    with zipfile.ZipFile(path) as archive:
        return archive.namelist()


def read_json_from_zip(path: str | Path, member_name: str) -> dict:
    """Read a JSON member from a ZIP archive."""
    with zipfile.ZipFile(path) as archive:
        with archive.open(member_name) as handle:
            return json.load(io.TextIOWrapper(handle, encoding="utf-8"))
