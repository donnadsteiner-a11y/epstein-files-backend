#!/usr/bin/env python3
"""
wayback_recovery.py
===================
Recovers missing Epstein Files PDFs from the Wayback Machine CDX API.

Queries Wayback for every EFTA PDF captured from the DOJ between
January 30 and March 31, 2026 — including files the DOJ has since
removed (~64,259 confirmed deleted per rhowardstone audit).

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
# DOJ folder names have SPACES — this is how Wayback captured the URLs

DATASETS = {
    1:  {"folder": "DataSet 1",   "prefix": "Data Set 1"},
    2:  {"folder": "DataSet 2",   "prefix": "Data Set 2"},
    3:  {"folder": "DataSet 3",   "prefix": "Data Set 3"},
    4:  {"folder": "DataSet 4",   "prefix": "Data Set 4"},
    5:  {"folder": "DataSet 5",   "prefix": "Data Set 5"},
    6:  {"folder": "DataSet 6",   "prefix": "Data Set 6"},
    7:  {"folder": "DataSet 7",   "prefix": "Data Set 7"},
    8:  {"folder": "DataSet 8",   "prefix": "Data Set 8"},
    9:  {"folder": "DataSet 9",   "prefix": "Data Set 9"},
    10: {"folder": "DataSet 10",  "prefix": "Data Set 10"},
    11: {"folder": "DataSet 11",  "prefix": "Data Set 11"},
    12: {"folder": "DataSet 12",  "prefix": "Data Set 12"},
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

DS         = DATASETS[DATASET_NUMBER]
S3_PREFIX  = DS["prefix"]
DOJ_FOLDER = DS["folder"]

# CDX API endpoint
CDX_API = "http://web.archive.org/cdx/search/cdx"

# Wayback raw file endpoint — if_ returns the raw file not the toolbar wrapper
WAYBACK_RAW = "https://web.archive.org/web/{timestamp}if_/{url}"

DELAY_BETWEEN  = 1.5
DELAY_ON_ERROR = 15.0

HEADERS = {
    "User-Agent": (
        "DocketZero/1.0 Research Archive "
        "(public DOJ records preservation; contact docketzero.com)"
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


# ── CDX API query ─────────────────────────────────────────────────────────────

def query_cdx(session: requests.Session) -> list:
    """
    Query Wayback CDX API for all captured PDFs from this dataset.
    
    The DOJ URLs contain literal spaces in folder names (e.g. "DataSet 12")
    which is how Wayback indexed them. We query using the exact URL pattern.
    
    Returns list of (efta_str, timestamp, original_url) tuples.
    """
    print(f"  Querying Wayback CDX API for DS{DATASET_NUMBER} PDFs ...")

    # The URL pattern as captured by Wayback — with literal space
    url_pattern = f"www.justice.gov/epstein/files/{DOJ_FOLDER}/EFTA*.pdf"

    # Try multiple query variations since Wayback may have indexed with
    # either spaces or %20 encoding
    all_results = {}

    for url_pat in [
        url_pattern,
        url_pattern.replace(" ", "%20"),
        url_pattern.replace(" ", "%2520"),  # double-encoded
    ]:
        params = {
            "url":       url_pat,
            "matchType": "prefix",
            "filter":    "statuscode:200",
            "fl":        "original,timestamp",
            "output":    "text",
            "from":      "20260130",
            "to":        "20260401",
            "collapse":  "original",
        }

        print(f"    Trying URL pattern: {url_pat[:60]}...")

        try:
            resp = session.get(CDX_API, params=params, timeout=120)
            if resp.status_code != 200:
                print(f"    CDX HTTP {resp.status_code} — trying next pattern")
                continue
        except requests.RequestException as exc:
            print(f"    CDX error: {exc} — trying next pattern")
            continue

        lines = [l for l in resp.text.strip().split("\n") if l.strip()]
        print(f"    Got {len(lines)} results")

        for line in lines:
            parts = line.strip().split(" ")
            if len(parts) < 2:
                continue
            original_url = parts[0]
            timestamp    = parts[1]
            match = re.search(r"EFTA(\d+)\.pdf", original_url, re.IGNORECASE)
            if not match:
                continue
            efta_str = f"{int(match.group(1)):08d}"
            # Keep earliest capture for each EFTA
            if efta_str not in all_results:
                all_results[efta_str] = (efta_str, timestamp, original_url)

        if all_results:
            break  # found results — no need to try other patterns

    results = list(all_results.values())
    print(f"  CDX API returned {len(results):,} unique captured URLs")
    return results


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    print("=" * 60)
    print(f"  DocketZero — Dataset {DATASET_NUMBER} Wayback Recovery")
    print("=" * 60)
    print(f"  Bucket    : {DO_BUCKET}")
    print(f"  Prefix    : {S3_PREFIX}/")
    print(f"  DOJ folder: {DOJ_FOLDER}")
    print(f"  Source    : Wayback Machine CDX API")
    print("=" * 60)

    session = requests.Session()
    session.headers.update(HEADERS)

    # Build S3 skip-list
    s3       = build_s3()
    uploaded = fetch_uploaded_keys(s3)

    # Query CDX
    print()
    captures = query_cdx(session)

    if not captures:
        print("\nNo captures found in Wayback for this dataset.")
        print("Wayback may not have archived these files, or they need")
        print("a different URL pattern. Try checking manually:")
        print(f"  https://web.archive.org/web/*/https://www.justice.gov/epstein/files/{DOJ_FOLDER}/EFTA*.pdf")
        sys.exit(0)

    # Filter to missing files only
    missing = [
        (efta, ts, url)
        for efta, ts, url in captures
        if f"{S3_PREFIX}/EFTA{efta}.pdf" not in uploaded
    ]

    total_captured = len(captures)
    total_missing  = len(missing)
    total_already  = total_captured - total_missing

    print(f"\n  Captured in Wayback    : {total_captured:,}")
    print(f"  Already in DreamObjects: {total_already:,}")
    print(f"  To recover             : {total_missing:,}")

    if total_missing == 0:
        print("\n  ✓ Nothing to recover — all Wayback captures already in DreamObjects!")
        sys.exit(0)

    print(f"\n── Recovering {total_missing:,} PDFs from Wayback ────────────────\n")

    recovered = 0
    failed    = []

    for idx, (efta, timestamp, original_url) in enumerate(missing, 1):
        filename = f"EFTA{efta}.pdf"
        s3_key   = f"{S3_PREFIX}/{filename}"

        # Build Wayback raw URL
        wayback_url = WAYBACK_RAW.format(
            timestamp=timestamp,
            url=original_url
        )

        print(f"  [{idx}/{total_missing}]  {filename}  ({timestamp[:8]})", end="  ", flush=True)

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
            print(f"✗  HTTP {resp.status_code}")
            failed.append(efta)
            time.sleep(DELAY_ON_ERROR)
            continue

        ctype = resp.headers.get("Content-Type", "")
        if "html" in ctype.lower():
            print(f"✗  Got HTML (Wayback error page)")
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
