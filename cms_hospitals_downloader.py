#!/usr/bin/env python3

from __future__ import annotations

import argparse
import csv
import json
import logging
import os
import re
import sys
import tempfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests

METASTORE_URL = "https://data.cms.gov/provider-data/api/1/metastore/schemas/dataset/items"
DEFAULT_THEME = "Hospitals"
DEFAULT_OUTPUT_DIR = Path("data/hospitals")
REQUEST_TIMEOUT = 60  # seconds
MAX_RETRIES = 3

logger = logging.getLogger("cms_hospitals_downloader")


# --------------------------------------------------------------------------
# Column name -> snake_case
# --------------------------------------------------------------------------
def to_snake_case(column_name: str) -> str:
    """Convert an arbitrary CSV header into snake_case.

    Examples
    --------
    "Patients' rating of the facility linear mean score"
        -> "patients_rating_of_the_facility_linear_mean_score"
    "ZIP Code"      -> "zip_code"
    "% of patients" -> "of_patients"
    """
    name = column_name.strip()
    # Drop apostrophes (straight + curly) so "Patients'" -> "Patients" (no stray underscore)
    name = re.sub(r"[\u2018\u2019'`]", "", name)
    # Any run of non-alphanumeric characters becomes a single underscore
    name = re.sub(r"[^0-9a-zA-Z]+", "_", name)
    return name.strip("_").lower()


def snake_case_csv_header(path: Path) -> None:
    """Rewrite the header row of a CSV file in place, converting column
    names to snake_case. Data rows are streamed through unchanged so this
    works on large files without loading them fully into memory."""
    fd, tmp_path = tempfile.mkstemp(dir=path.parent, suffix=".tmp")
    os.close(fd)
    try:
        with open(path, newline="", encoding="utf-8-sig") as src, open(
            tmp_path, "w", newline="", encoding="utf-8"
        ) as dst:
            reader = csv.reader(src)
            writer = csv.writer(dst)
            try:
                header = next(reader)
            except StopIteration:
                header = None
            if header is not None:
                writer.writerow([to_snake_case(col) for col in header])
            for row in reader:
                writer.writerow(row)
        os.replace(tmp_path, path)
    except Exception:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)
        raise


# --------------------------------------------------------------------------
# HTTP helpers (small retry wrapper, no extra dependencies)
# --------------------------------------------------------------------------
def _request_with_retry(method: str, url: str, **kwargs) -> requests.Response:
    last_exc: Exception | None = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.request(method, url, timeout=REQUEST_TIMEOUT, **kwargs)
            resp.raise_for_status()
            return resp
        except requests.RequestException as exc:
            last_exc = exc
            logger.warning("Request %s %s failed (attempt %d/%d): %s", method, url, attempt, MAX_RETRIES, exc)
    assert last_exc is not None
    raise last_exc


def fetch_metastore_items() -> list[dict[str, Any]]:
    logger.info("Fetching dataset metadata from %s", METASTORE_URL)
    resp = _request_with_retry("GET", METASTORE_URL)
    return resp.json()


# --------------------------------------------------------------------------
# State tracking (what was downloaded / when)
# --------------------------------------------------------------------------
@dataclass
class RunState:
    path: Path
    datasets: dict[str, dict[str, Any]] = field(default_factory=dict)

    @classmethod
    def load(cls, path: Path) -> "RunState":
        if path.exists():
            try:
                data = json.loads(path.read_text(encoding="utf-8"))
                return cls(path=path, datasets=data.get("datasets", {}))
            except (json.JSONDecodeError, OSError) as exc:
                logger.warning("Could not read state file %s (%s); starting fresh", path, exc)
        return cls(path=path)

    def last_modified(self, identifier: str) -> str | None:
        entry = self.datasets.get(identifier)
        return entry["modified"] if entry else None

    def record(self, identifier: str, modified: str, title: str, files: list[str]) -> None:
        self.datasets[identifier] = {
            "title": title,
            "modified": modified,
            "downloaded_at": datetime.now(timezone.utc).isoformat(),
            "files": files,
        }

    def save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp_path = tempfile.mkstemp(dir=self.path.parent, suffix=".tmp")
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump({"datasets": self.datasets}, f, indent=2, sort_keys=True)
        os.replace(tmp_path, self.path)


# --------------------------------------------------------------------------
# Download + process a single dataset
# --------------------------------------------------------------------------
def _safe_filename(url: str, fallback: str) -> str:
    name = url.rsplit("/", 1)[-1] or fallback
    return name.split("?", 1)[0]


def download_dataset(item: dict[str, Any], output_dir: Path) -> list[str]:
    """Download every CSV distribution of a dataset and snake_case its
    header. Returns the list of local file paths written."""
    identifier = item["identifier"]
    title = item.get("title", identifier)
    dataset_dir = output_dir / identifier
    dataset_dir.mkdir(parents=True, exist_ok=True)

    written: list[str] = []
    for dist in item.get("distribution", []):
        if dist.get("mediaType") != "text/csv":
            continue
        url = dist.get("downloadURL")
        if not url:
            continue
        filename = _safe_filename(url, fallback=f"{identifier}.csv")
        dest = dataset_dir / filename

        logger.info("Downloading %s (%s) -> %s", title, url, dest)
        resp = _request_with_retry("GET", url, stream=True)
        with open(dest, "wb") as f:
            for chunk in resp.iter_content(chunk_size=1024 * 256):
                if chunk:
                    f.write(chunk)

        snake_case_csv_header(dest)
        written.append(str(dest))

    return written


# --------------------------------------------------------------------------
# Main pipeline
# --------------------------------------------------------------------------
def select_hospital_datasets(items: list[dict[str, Any]], theme: str) -> list[dict[str, Any]]:
    return [item for item in items if theme in (item.get("theme") or [])]


def run(output_dir: Path, state_file: Path, theme: str, workers: int, force: bool) -> int:
    state = RunState.load(state_file)
    items = fetch_metastore_items()
    hospital_items = select_hospital_datasets(items, theme)
    logger.info("Found %d datasets tagged '%s'", len(hospital_items), theme)

    to_download = []
    skipped = 0
    for item in hospital_items:
        identifier = item.get("identifier")
        modified = item.get("modified", "")
        if not identifier:
            continue
        if not force and state.last_modified(identifier) == modified:
            skipped += 1
            continue
        to_download.append(item)

    logger.info("%d datasets unchanged since last run (skipped), %d to download", skipped, len(to_download))

    failures = 0
    with ThreadPoolExecutor(max_workers=workers) as pool:
        future_to_item = {
            pool.submit(download_dataset, item, output_dir): item for item in to_download
        }
        for future in as_completed(future_to_item):
            item = future_to_item[future]
            identifier = item["identifier"]
            try:
                files = future.result()
                state.record(identifier, item.get("modified", ""), item.get("title", identifier), files)
                logger.info("Completed dataset %s (%s): %d file(s)", identifier, item.get("title"), len(files))
            except Exception:
                failures += 1
                logger.exception("Failed to process dataset %s (%s)", identifier, item.get("title"))

    state.save()
    logger.info(
        "Run complete. Downloaded/updated: %d, skipped (unchanged): %d, failed: %d",
        len(to_download) - failures,
        skipped,
        failures,
    )
    return 1 if failures else 0


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--state-file", type=Path, default=None)
    parser.add_argument("--theme", default=DEFAULT_THEME)
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--force", action="store_true", help="Ignore state file, re-download everything")
    parser.add_argument("-v", "--verbose", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv if argv is not None else sys.argv[1:])
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)-8s %(message)s",
    )
    state_file = args.state_file or (args.output_dir / ".state.json")
    return run(args.output_dir, state_file, args.theme, args.workers, args.force)


if __name__ == "__main__":
    raise SystemExit(main())
