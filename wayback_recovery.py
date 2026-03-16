#!/usr/bin/env python3
"""
wayback_recovery.py
===================
Recovers missing Epstein Files PDFs from the Wayback Machine CDX API.

For each dataset, queries the CDX API to find every EFTA PDF that was
captured on or around January 31, 2026 — including files the DOJ has
since removed. Downloads each missing PDF and uploads to DreamObjects.

Uploads to: docketzero-files/Data Set {N}/EFTA########.pdf

Fully resumable — files already in DreamObjects are skipped.

Required environment variables (GitHub Secrets):
    DATASET_NUMBER   Which dataset to recover (1-12)
    DO_ENDPOINT      https://s3.us-east-005.dream.io
    DO_ACCESS_KEY    DreamObjects access key
    DO_SECRET_KEY    DreamObjects secret key
    DO_BUCKET        docketzero-files
"""

import os
import re
import sys
import time
import urllib.parse

import boto3
import requests
from botocore.config import Config

# ── Dataset registry ───────────────────────────────────────────────────────────

DATASETS = {
    1:  {"folder": "DataSet%201",   "prefix": "Data Set 1"},
    2:  {"folder": "DataSet%202",   "prefix": "Data Set 2"},
    3:  {"folder": "DataSet%203",   "prefix": "Data Set 3"},
    4:  {"folder": "DataSet%204",   "prefix": "Data Set 4"},
    5:  {"folder": "DataSet%205",   "prefix": "Data Set 5"},
    6:  {"folder": "DataSet%206",   "prefix": "Data Set 6"},
    7:  {"folder": "DataSet%207",   "prefix": "Data Set 7"},
    8:  {"folder": "DataSet%208",   "prefix": "Data Set 8"},
    9:  {"folder": "DataSet%209",   "prefix": "Data Set 9"},
    10: {"folder": "DataSet%2010",  "prefix": "Data Set 10"},
    11: {"folder": "DataSet%2011",  "prefix": "Data Set 11"},
    12: {"folder": "DataSet%2012",  "prefix": "Data Set 12"},
}

# ── Environment ────────────────────────────────────────────────────────────────

DATASET_NUMBER = int(os.environ["DATASET_NUMBER"])
DO_ENDPOINT    = os.environ["DO_ENDPOINT"]
DO_ACCESS_KEY  = os.environ["DO_ACCESS_KEY"]
DO_SECRET_KEY  = os.environ["DO_SECRET_KEY"]
DO_BUCKET      = os.environ["DO_BUCKET"]

if DATASET_NUMBER not in DATASETS:
    print(f"ERROR: DATASET_NUMBER {DATASET_NUMBER} not in registry (1-12)")
    sys.exit(1)

DS        = DATASETS[DATASET_NUMBER]
S3_PREFIX = DS["prefix"]
DOJ_FOLDER = DS["folder"]

# The DOJ URL pattern Wayback captured
DOJ_URL_PATTERN = (
    f"https://www.justice.gov/epstein/files/"
    f"{urllib.parse.unquote(DOJ_FOLDER)}/EFTA*.pdf"
)
DOJ_URL_ENCODED = (
    f"https://www.justice.gov/epstein/files/{DOJ_FOLDER}/EFTA"
)

# CDX API endpoint
CDX_API = "http://web.archive.org/cdx/search/cdx"

# Wayback raw file endpoint (if_ returns raw file, not toolbar)
WAYBACK_RAW = "https://web.archive.org/web/{timestamp}if_/{url}"

DELAY_BETWEEN  = 1.5   # be polite to Wayback
DELAY_ON_ERROR = 15.0

HEADERS = {
    "User-Agent": (
        "DocketZero/1.0 Research Archive "
        "(contact: docketzero.com; recovering public DOJ records)"
    ),
}


# ── S3 helpers ─────────────────────────────────────────────────────────────────

def build_s3():
    return boto3.client(
        "s3",
        endpoint_url=DO_ENDPOINT,
        aws_access_key_id=DO_ACCESS_KEY,
        aws_secret_access_key=DO_SECRET_KEY,
        config=Config(
            connect_timeout=10,
            read_timeout=120,
            retries={"max_attempts": 3},
        ),
    )


def fetch_uploaded_keys(s3) -> set:
    print(f"  Checking s3://{DO_BUCKET}/{S3_PREFIX}/ for already-uploaded files ...")
    existing = set()
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=DO_BUCKET, Prefix=f"{S3_PREFIX}/"):
        for obj in page.get("Contents", []):
            existing.add(obj["Key"])
    print(f"  {len(existing)} files already in DreamObjects — will skip these.")
    return existing


def upload(s3, key: str, data: bytes) -> bool:
    try:
        s3.put_object(
            Bucket=DO_BUCKET,
            Key=key,
            Body=data,
            ContentType="application/pdf",
        )
        return True
    except Exception as exc:
        print(f"    ✗  S3 upload failed: {exc}")
        return False


# ── CDX API: get all captured EFTA URLs ───────────────────────────────────────

def query_cdx(session: requests.Session) -> list:
    """
    Query the Wayback CDX API for all captured PDFs from this dataset.
    Returns a list of (efta_str, timestamp, original_url) tuples.
    Picks the earliest capture (closest to Jan 31, 2026) for each EFTA.
    """
    print(f"  Querying Wayback CDX API for DS{DATASET_NUMBER} PDFs ...")

    # CDX API parameters
    # matchType=prefix matches all URLs starting with the DOJ dataset folder
    # filter=statuscode:200 only gets successful captures
    # fl=original,timestamp gets just the URL and timestamp
    # output=text for simple parsing
    # from/to narrows to Jan 30 - Mar 31, 2026 window
    params = {
        "url":       f"www.justice.gov/epstein/files/{urllib.parse.unquote(DOJ_FOLDER)}/EFTA*.pdf",
        "matchType": "prefix",
        "filter":    "statuscode:200",
        "fl":        "original,timestamp",
        "output":    "text",
        "from":      "20260130",
        "to":        "20260331",
        "collapse":  "original",   # one entry per unique URL (earliest capture)
    }

    try:
        resp = session.get(CDX_API, params=params, timeout=120)
        if resp.status_code != 200:
            print(f"  CDX API returned HTTP {resp.status_code}")
            return []
    except requests.RequestException as exc:
        print(f"  CDX API error: {exc}")
        return []

    lines = resp.text.strip().split("\n")
    results = []

    for line in lines:
        if not line.strip():
            continue
        parts = line.strip().split(" ")
        if len(parts) < 2:
            continue
        original_url = parts[0]
        timestamp    = parts[1]

        # Extract EFTA number from URL
        match = re.search(r"EFTA(\d+)\.pdf", original_url, re.IGNORECASE)
        if not match:
            continue

        efta_str = f"{int(match.group(1)):08d}"
        results.append((efta_str, timestamp, original_url))

    print(f"  CDX API returned {len(results)} captured URLs")
    return results


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    print("=" * 60)
    print(f"  DocketZero — Dataset {DATASET_NUMBER} Wayback Recovery")
    print("=" * 60)
    print(f"  Bucket    : {DO_BUCKET}")
    print(f"  Prefix    : {S3_PREFIX}/")
    print(f"  Source    : Wayback Machine CDX API")
    print("=" * 60)

    session = requests.Session()
    session.headers.update(HEADERS)

    # Build S3 skip-list
    s3       = build_s3()
    uploaded = fetch_uploaded_keys(s3)

    # Query CDX for all captured PDFs
    print()
    captures = query_cdx(session)

    if not captures:
        print("\nNo captures found in Wayback for this dataset.")
        print("This dataset may not have been archived, or the CDX query needs adjustment.")
        sys.exit(0)

    # Filter to only files not already in DreamObjects
    missing = [
        (efta, ts, url)
        for efta, ts, url in captures
        if f"{S3_PREFIX}/EFTA{efta}.pdf" not in uploaded
    ]

    total_captured  = len(captures)
    total_missing   = len(missing)
    total_already   = total_captured - total_missing

    print(f"\n  Captured in Wayback   : {total_captured:,}")
    print(f"  Already in DreamObjects: {total_already:,}")
    print(f"  To recover            : {total_missing:,}")

    if total_missing == 0:
        print("\n  ✓ Nothing to recover — all Wayback captures already in DreamObjects!")
        sys.exit(0)

    print(f"\n── Recovering {total_missing:,} PDFs from Wayback ────────────────\n")

    recovered = 0
    failed    = []

    for idx, (efta, timestamp, original_url) in enumerate(missing, 1):
        filename = f"EFTA{efta}.pdf"
        s3_key   = f"{S3_PREFIX}/{filename}"

        # Build the Wayback raw URL
        wayback_url = WAYBACK_RAW.format(
            timestamp=timestamp,
            url=original_url
        )

        print(f"  [{idx}/{total_missing}]  {filename}  (captured {timestamp[:8]})", end="  ", flush=True)

        try:
            resp = session.get(wayback_url, timeout=90)
        except requests.RequestException as exc:
            print(f"✗  Request error: {exc}")
            failed.append(efta)
            time.sleep(DELAY_ON_ERROR)
            continue

        if resp.status_code == 404:
            print(f"✗  404 from Wayback")
            failed.append(efta)
            time.sleep(DELAY_BETWEEN)
            continue

        if resp.status_code != 200:
            print(f"✗  HTTP {resp.status_code} from Wayback")
            failed.append(efta)
            time.sleep(DELAY_ON_ERROR)
            continue

        # Check we got a PDF not a Wayback error page
        ctype = resp.headers.get("Content-Type", "")
        if "html" in ctype.lower():
            print(f"✗  Got HTML (Wayback may have returned error page)")
            failed.append(efta)
            time.sleep(DELAY_BETWEEN)
            continue

        pdf_bytes = resp.content
        if len(pdf_bytes) < 512:
            print(f"✗  Too small ({len(pdf_bytes)} bytes)")
            failed.append(efta)
            continue

        ok = upload(s3, s3_key, pdf_bytes)
        if ok:
            recovered += 1
            uploaded.add(s3_key)
            print(f"✓  {len(pdf_bytes):,} bytes")
        else:
            failed.append(efta)

        time.sleep(DELAY_BETWEEN)

    # Summary
    print("\n" + "=" * 60)
    print(f"  Recovery complete — DS{DATASET_NUMBER}")
    print(f"  Recovered this run : {recovered:,}")
    print(f"  Failed             : {len(failed):,}")
    print(f"  Total in bucket    : {len(uploaded):,}")
    print("=" * 60)

    if failed:
        print(f"\n  Failed EFTAs: {failed[:20]}{'...' if len(failed) > 20 else ''}")
        sys.exit(1)


if __name__ == "__main__":
    main()
