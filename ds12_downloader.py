#!/usr/bin/env python3
"""
ds12_downloader.py
==================
Downloads all Dataset 12 PDFs from DOJ using the known EFTA number range
and uploads each one to DreamObjects under:

    docketzero-files/Data Set 12/EFTA02730265.pdf

EFTA range: 02730265 → 02731783 (1,519 files)

Fully resumable — files already in DreamObjects are skipped on every run.

Required environment variables (GitHub Secrets):
    DOJ_COOKIES      Cookie string copied from your browser
    DO_ENDPOINT      https://s3.us-east-005.dream.io
    DO_ACCESS_KEY    DreamObjects access key
    DO_SECRET_KEY    DreamObjects secret key
    DO_BUCKET        docketzero-files
"""

import json
import os
import sys
import time

import boto3
import requests
from botocore.config import Config

# ── Dataset config ─────────────────────────────────────────────────────────────

DATASET_NUMBER = 12
EFTA_START     = 2730265
EFTA_END       = 2731783
S3_PREFIX      = "Data Set 12"
DOJ_URL        = "https://www.justice.gov/epstein/files/DataSet%2012/EFTA{efta}.pdf"

# ── Environment ────────────────────────────────────────────────────────────────

DO_ENDPOINT     = os.environ["DO_ENDPOINT"]
DO_ACCESS_KEY   = os.environ["DO_ACCESS_KEY"]
DO_SECRET_KEY   = os.environ["DO_SECRET_KEY"]
DO_BUCKET       = os.environ["DO_BUCKET"]
DOJ_COOKIES_RAW = os.environ["DOJ_COOKIES"]

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept":          "application/pdf,*/*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer":         "https://www.justice.gov/epstein/doj-disclosures/data-set-12-files",
}

DELAY_BETWEEN  = 1.2
DELAY_ON_ERROR = 10.0


# ── Cookie parsing ─────────────────────────────────────────────────────────────

def parse_cookies(raw: str) -> dict:
    raw = raw.strip()
    if raw.startswith("{"):
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            pass
    cookies = {}
    for part in raw.split(";"):
        part = part.strip()
        if "=" in part:
            k, _, v = part.partition("=")
            cookies[k.strip()] = v.strip()
    return cookies


# ── S3 client ──────────────────────────────────────────────────────────────────

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
    print(f"  Checking s3://{DO_BUCKET}/{S3_PREFIX}/ for already-uploaded files ...")
    existing = set()
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=DO_BUCKET, Prefix=f"{S3_PREFIX}/"):
        for obj in page.get("Contents", []):
            existing.add(obj["Key"])
    print(f"  {len(existing)} files already uploaded — will skip these.")
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


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    total_files = EFTA_END - EFTA_START + 1

    print("=" * 60)
    print(f"  DocketZero — Dataset {DATASET_NUMBER} Downloader")
    print("=" * 60)
    print(f"  Bucket     : {DO_BUCKET}")
    print(f"  Prefix     : {S3_PREFIX}/")
    print(f"  EFTA range : {EFTA_START:08d} → {EFTA_END:08d}")
    print(f"  Total files: {total_files:,}")
    print("=" * 60)

    cookies = parse_cookies(DOJ_COOKIES_RAW)
    if not cookies:
        print("ERROR: DOJ_COOKIES is empty or could not be parsed.")
        sys.exit(1)
    print(f"\n  Cookies loaded: {list(cookies.keys())}\n")

    session = requests.Session()
    session.headers.update(HEADERS)
    session.cookies.update(cookies)

    s3 = build_s3()
    uploaded = fetch_uploaded_keys(s3)

    uploaded_count = 0
    skipped_count  = 0
    failed         = []

    print(f"\n── Downloading {total_files:,} PDFs ─────────────────────────────\n")

    for efta_int in range(EFTA_START, EFTA_END + 1):
        efta_str = f"{efta_int:08d}"
        filename = f"EFTA{efta_str}.pdf"
        s3_key   = f"{S3_PREFIX}/{filename}"
        url      = DOJ_URL.format(efta=efta_str)
        idx      = efta_int - EFTA_START + 1

        if s3_key in uploaded:
            skipped_count += 1
            continue

        print(f"  [{idx}/{total_files}]  {filename}", end="  ", flush=True)

        try:
            resp = session.get(url, timeout=60)
        except requests.RequestException as exc:
            print(f"✗  Request error: {exc}")
            failed.append(efta_str)
            time.sleep(DELAY_ON_ERROR)
            continue

        if resp.status_code == 404:
            print(f"✗  404 (not found)")
            time.sleep(DELAY_BETWEEN)
            continue

        if resp.status_code != 200:
            print(f"✗  HTTP {resp.status_code}")
            failed.append(efta_str)
            time.sleep(DELAY_ON_ERROR)
            continue

        ctype = resp.headers.get("Content-Type", "")
        if "html" in ctype.lower():
            print(f"✗  Got HTML — cookies may have expired. Stopping.")
            print("   Update DOJ_COOKIES secret and re-run.")
            print(f"   Stopped at EFTA {efta_str} ({idx}/{total_files})")
            sys.exit(2)

        pdf_bytes = resp.content
        if len(pdf_bytes) < 512:
            print(f"✗  Too small ({len(pdf_bytes)} bytes) — skipping")
            failed.append(efta_str)
            continue

        ok = upload(s3, s3_key, pdf_bytes)
        if ok:
            uploaded_count += 1
            uploaded.add(s3_key)
            print(f"✓  {len(pdf_bytes):,} bytes")
        else:
            failed.append(efta_str)

        time.sleep(DELAY_BETWEEN)

    print("\n" + "=" * 60)
    print(f"  Run complete")
    print(f"  Uploaded this run : {uploaded_count:,}")
    print(f"  Skipped (done)    : {skipped_count:,}")
    print(f"  Failed            : {len(failed):,}")
    print("=" * 60)

    if failed:
        print(f"\n  Failed EFTAs: {failed[:20]}{'...' if len(failed) > 20 else ''}")
        sys.exit(1)


if __name__ == "__main__":
    main()
