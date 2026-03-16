#!/usr/bin/env python3
"""
ds12_duplicates.py
==================
Checks DreamObjects Data Set 12 folder for duplicate files.
Also cross-references the DOJ listing page 0 against what's in the bucket
to identify any files on the DOJ site that we don't have yet.

Required GitHub Secrets:
    DOJ_COOKIES, DO_ENDPOINT, DO_ACCESS_KEY, DO_SECRET_KEY, DO_BUCKET
"""

import json
import os
import re
import sys
from collections import Counter

import boto3
import requests
from botocore.config import Config
from bs4 import BeautifulSoup

S3_PREFIX   = "Data Set 12"
DOJ_LISTING = "https://www.justice.gov/epstein/doj-disclosures/data-set-12-files"

DO_ENDPOINT     = os.environ["DO_ENDPOINT"]
DO_ACCESS_KEY   = os.environ["DO_ACCESS_KEY"]
DO_SECRET_KEY   = os.environ["DO_SECRET_KEY"]
DO_BUCKET       = os.environ["DO_BUCKET"]
DOJ_COOKIES_RAW = os.environ["DOJ_COOKIES"]

DOJ_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept":          "text/html,*/*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer":         DOJ_LISTING,
}


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


def fetch_all_s3_keys() -> list:
    """Return every key in Data Set 12/ including full path."""
    s3 = boto3.client(
        "s3",
        endpoint_url=DO_ENDPOINT,
        aws_access_key_id=DO_ACCESS_KEY,
        aws_secret_access_key=DO_SECRET_KEY,
        config=Config(connect_timeout=10, read_timeout=60, retries={"max_attempts": 3}),
    )
    keys = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=DO_BUCKET, Prefix=f"{S3_PREFIX}/"):
        for obj in page.get("Contents", []):
            keys.append(obj["Key"])
    return keys


def scrape_page0(session) -> set:
    """Scrape page 0 of the DOJ DS12 listing and return EFTA numbers found."""
    resp = session.get(DOJ_LISTING, timeout=30)
    if resp.status_code != 200:
        print(f"  WARNING: Page 0 returned HTTP {resp.status_code}")
        return set()
    soup = BeautifulSoup(resp.text, "html.parser")
    eftas = set()
    for tag in soup.find_all("a", href=True):
        href = tag["href"]
        if not href.lower().endswith(".pdf"):
            continue
        match = re.search(r"EFTA(\d+)", href, re.IGNORECASE)
        if match:
            eftas.add(f"{int(match.group(1)):08d}")
    return eftas


def main():
    print("=" * 60)
    print("  DocketZero — Dataset 12 Duplicate Check")
    print("=" * 60)

    # Fetch all S3 keys
    print(f"\n  Fetching all keys from s3://{DO_BUCKET}/{S3_PREFIX}/ ...")
    all_keys = fetch_all_s3_keys()
    print(f"  Total objects in bucket: {len(all_keys)}")

    # Extract filenames and check for duplicates
    filenames = [k.split("/")[-1] for k in all_keys]
    counts = Counter(filenames)
    duplicates = {f: c for f, c in counts.items() if c > 1}

    print(f"\n── Duplicate Check ──────────────────────────────────────")
    if duplicates:
        print(f"  Found {len(duplicates)} duplicate filenames:")
        for fname, count in sorted(duplicates.items()):
            print(f"    {fname} — appears {count} times")
    else:
        print("  No duplicates found in DreamObjects.")

    # Extract unique EFTA numbers
    bucket_eftas = set()
    for fname in filenames:
        match = re.search(r"EFTA(\d+)", fname, re.IGNORECASE)
        if match:
            bucket_eftas.add(f"{int(match.group(1)):08d}")

    print(f"\n── Page 0 Cross-Reference ───────────────────────────────")
    cookies = parse_cookies(DOJ_COOKIES_RAW)
    session = requests.Session()
    session.headers.update(DOJ_HEADERS)
    session.cookies.update(cookies)

    page0_eftas = scrape_page0(session)
    print(f"  Files on DOJ page 0       : {len(page0_eftas)}")
    print(f"  Files in DreamObjects     : {len(bucket_eftas)}")

    missing_from_bucket = page0_eftas - bucket_eftas
    if missing_from_bucket:
        print(f"  On page 0 but NOT in bucket ({len(missing_from_bucket)}):")
        for efta in sorted(missing_from_bucket):
            print(f"    EFTA{efta}.pdf")
    else:
        print("  All page 0 files are in the bucket. ✓")

    in_bucket_not_on_page0 = bucket_eftas - page0_eftas
    print(f"\n  In bucket but not on page 0: {len(in_bucket_not_on_page0)}")
    print(f"  (These came from the rhowardstone corpus)")

    print(f"\n── Summary ──────────────────────────────────────────────")
    print(f"  Total files in bucket     : {len(bucket_eftas)}")
    print(f"  Duplicates                : {len(duplicates)}")
    print(f"  Missing from page 0       : {len(missing_from_bucket)}")
    print("=" * 60)


if __name__ == "__main__":
    main()
