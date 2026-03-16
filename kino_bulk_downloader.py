#!/usr/bin/env python3
"""
kino_bulk_downloader.py
=======================
High-speed bulk downloader for large datasets (DS9, DS10, DS11).
Pulls directly from assets.getkino.com — no cookies, no auth, no age gate.

Designed for parallel execution: each job handles a specific EFTA slice
defined by SLICE_START and SLICE_END environment variables.

Uploads to: docketzero-files/Data Set {N}/EFTA########.pdf

Fully resumable — files already in DreamObjects are skipped.

Required environment variables:
    DATASET_NUMBER   Dataset to download (9, 10, or 11)
    SLICE_START      First EFTA number in this slice (integer)
    SLICE_END        Last EFTA number in this slice (integer)
    DO_ENDPOINT      https://s3.us-east-005.dream.io
    DO_ACCESS_KEY    DreamObjects access key
    DO_SECRET_KEY    DreamObjects secret key
    DO_BUCKET        docketzero-files
"""

import os
import sys
import time
import concurrent.futures
import threading

import boto3
import requests
from botocore.config import Config

# ── Environment ────────────────────────────────────────────────────────────────

DATASET_NUMBER = int(os.environ["DATASET_NUMBER"])
SLICE_START    = int(os.environ["SLICE_START"])
SLICE_END      = int(os.environ["SLICE_END"])
DO_ENDPOINT    = os.environ["DO_ENDPOINT"]
DO_ACCESS_KEY  = os.environ["DO_ACCESS_KEY"]
DO_SECRET_KEY  = os.environ["DO_SECRET_KEY"]
DO_BUCKET      = os.environ["DO_BUCKET"]

S3_PREFIX  = f"Data Set {DATASET_NUMBER}"
KINO_URL   = "https://assets.getkino.com/documents/EFTA{efta}.pdf"

# Concurrency — 10 parallel download threads
WORKERS    = 10
DELAY      = 0.1   # small delay per thread to be polite

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "application/pdf,*/*",
}


# ── S3 ─────────────────────────────────────────────────────────────────────────

def build_s3():
    return boto3.client(
        "s3",
        endpoint_url=DO_ENDPOINT,
        aws_access_key_id=DO_ACCESS_KEY,
        aws_secret_access_key=DO_SECRET_KEY,
        config=Config(
            connect_timeout=10,
            read_timeout=60,
            retries={"max_attempts": 3},
        ),
    )


def fetch_uploaded_keys(s3) -> set:
    print(f"  Fetching existing keys for slice {SLICE_START:08d}-{SLICE_END:08d} ...")
    existing = set()
    paginator = s3.get_paginator("list_objects_v2")
    # Use start-after to only list keys in our slice range — faster for large buckets
    start_key = f"{S3_PREFIX}/EFTA{SLICE_START:08d}.pdf"
    end_key   = f"{S3_PREFIX}/EFTA{SLICE_END:08d}.pdf"
    for page in paginator.paginate(Bucket=DO_BUCKET, Prefix=f"{S3_PREFIX}/EFTA",
                                   StartAfter=start_key):
        for obj in page.get("Contents", []):
            if obj["Key"] <= end_key:
                existing.add(obj["Key"])
            else:
                return existing  # past our slice range
    print(f"  {len(existing)} files already in this slice — will skip.")
    return existing


# ── Worker ─────────────────────────────────────────────────────────────────────

# Thread-safe counters
_lock             = threading.Lock()
_uploaded_count   = 0
_skipped_count    = 0
_failed_count     = 0
_not_found_count  = 0


def process_efta(efta_int: int, s3, uploaded: set) -> str:
    global _uploaded_count, _skipped_count, _failed_count, _not_found_count

    efta     = f"{efta_int:08d}"
    filename = f"EFTA{efta}.pdf"
    s3_key   = f"{S3_PREFIX}/{filename}"

    # Skip if already uploaded
    with _lock:
        if s3_key in uploaded:
            _skipped_count += 1
            return "skip"

    # Download from Kino
    url = KINO_URL.format(efta=efta)
    try:
        resp = requests.get(url, headers=HEADERS, timeout=60)
    except requests.RequestException:
        with _lock:
            _failed_count += 1
        return "error"

    if resp.status_code == 404:
        with _lock:
            _not_found_count += 1
        return "404"

    if resp.status_code != 200:
        with _lock:
            _failed_count += 1
        return "error"

    pdf_bytes = resp.content
    if len(pdf_bytes) < 512:
        with _lock:
            _not_found_count += 1
        return "404"

    # Upload to S3
    try:
        s3.put_object(
            Bucket=DO_BUCKET,
            Key=s3_key,
            Body=pdf_bytes,
            ContentType="application/pdf",
        )
    except Exception:
        with _lock:
            _failed_count += 1
        return "s3-error"

    with _lock:
        uploaded.add(s3_key)
        _uploaded_count += 1
        count = _uploaded_count

    if count % 100 == 0:
        print(f"  ✓ {count} uploaded  |  skipped: {_skipped_count}  |  "
              f"not found: {_not_found_count}  |  failed: {_failed_count}")

    time.sleep(DELAY)
    return "ok"


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    slice_size = SLICE_END - SLICE_START + 1

    print("=" * 60)
    print(f"  DocketZero — DS{DATASET_NUMBER} Bulk Kino Download")
    print("=" * 60)
    print(f"  Bucket     : {DO_BUCKET}")
    print(f"  Prefix     : {S3_PREFIX}/")
    print(f"  Slice      : {SLICE_START:08d} → {SLICE_END:08d}")
    print(f"  Slice size : {slice_size:,} files")
    print(f"  Workers    : {WORKERS}")
    print("=" * 60)

    s3       = build_s3()
    uploaded = fetch_uploaded_keys(s3)

    efta_range = range(SLICE_START, SLICE_END + 1)

    print(f"\n── Downloading slice ({slice_size:,} files, {WORKERS} workers) ──\n")

    with concurrent.futures.ThreadPoolExecutor(max_workers=WORKERS) as executor:
        futures = {
            executor.submit(process_efta, efta_int, s3, uploaded): efta_int
            for efta_int in efta_range
        }
        for future in concurrent.futures.as_completed(futures):
            try:
                future.result()
            except Exception as exc:
                with _lock:
                    global _failed_count
                    _failed_count += 1

    print("\n" + "=" * 60)
    print(f"  Slice complete — DS{DATASET_NUMBER} "
          f"{SLICE_START:08d}-{SLICE_END:08d}")
    print(f"  Uploaded    : {_uploaded_count:,}")
    print(f"  Skipped     : {_skipped_count:,}")
    print(f"  Not found   : {_not_found_count:,}")
    print(f"  Failed      : {_failed_count:,}")
    print("=" * 60)

    # Exit 1 only if zero files were uploaded AND there were S3 failures —
    # "not found" files are expected gaps (DOJ deletions Kino doesn't have).
    # Partial success with some S3 failures is still a successful run.
    if _failed_count > 0 and _uploaded_count == 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
