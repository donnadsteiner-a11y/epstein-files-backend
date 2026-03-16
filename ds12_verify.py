#!/usr/bin/env python3
"""
ds12_verify.py
==============
Compares what's in DreamObjects against what should be there
(rhowardstone corpus + DOJ listing scrape) and reports missing files.

Required GitHub Secrets:
    DOJ_COOKIES, DO_ENDPOINT, DO_ACCESS_KEY, DO_SECRET_KEY, DO_BUCKET
"""

import gzip
import io
import json
import os
import re
import sys
import time

import boto3
import requests
from botocore.config import Config
from bs4 import BeautifulSoup

# ── Config ─────────────────────────────────────────────────────────────────────

EFTA_RANGE_START = 2730265
EFTA_RANGE_END   = 2731783
S3_PREFIX        = "Data Set 12"
DOJ_LISTING      = "https://www.justice.gov/epstein/doj-disclosures/data-set-12-files"

CORPUS_URL = (
    "https://raw.githubusercontent.com/rhowardstone/"
    "Epstein-research-data/main/document_summary.csv.gz"
)

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
    "Accept":          "text/html,application/pdf,*/*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer":         DOJ_LISTING,
}


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


# ── Source 1: corpus ──────────────────────────────────────────────────────────

def fetch_corpus_eftas() -> set:
    print("  [Source 1] Downloading rhowardstone corpus ...")
    try:
        resp = requests.get(CORPUS_URL, timeout=120)
        if resp.status_code != 200:
            print(f"  WARNING: HTTP {resp.status_code} — skipping corpus")
            return set()
    except Exception as exc:
        print(f"  WARNING: {exc} — skipping corpus")
        return set()

    eftas = set()
    with gzip.open(io.BytesIO(resp.content), "rt", encoding="utf-8") as f:
        header = None
        efta_col = 0
        for line in f:
            line = line.rstrip("\n")
            if header is None:
                header = line.split(",")
                try:
                    efta_col = header.index("efta_number")
                except ValueError:
                    efta_col = 0
                continue
            parts = line.split(",")
            if len(parts) <= efta_col:
                continue
            efta = parts[efta_col].strip().strip('"')
            try:
                efta_clean = efta.upper().replace("EFTA", "").strip()
                efta_int = int(efta_clean)
                if EFTA_RANGE_START <= efta_int <= EFTA_RANGE_END:
                    eftas.add(f"{efta_int:08d}")
            except ValueError:
                continue

    print(f"  Found {len(eftas):,} EFTA numbers from corpus")
    return eftas


# ── Source 2: DOJ listing ─────────────────────────────────────────────────────

def scrape_listing_eftas(session: requests.Session) -> set:
    print("  [Source 2] Scraping DOJ DS12 listing pages ...")
    eftas = set()
    page_num = 0

    while True:
        url = DOJ_LISTING if page_num == 0 else f"{DOJ_LISTING}?page={page_num}"
        try:
            resp = session.get(url, timeout=30)
        except requests.RequestException as exc:
            print(f"    Request error: {exc} — stopping")
            break

        if resp.status_code not in (200, 304):
            print(f"    HTTP {resp.status_code} on page {page_num} — stopping")
            break

        if "Access Denied" in resp.text and len(resp.text) < 3000:
            print(f"    Access Denied on page {page_num} — stopping")
            break

        soup = BeautifulSoup(resp.text, "html.parser")
        page_eftas = set()
        for tag in soup.find_all("a", href=True):
            href = tag["href"]
            if not href.lower().endswith(".pdf"):
                continue
            match = re.search(r"EFTA(\d+)", href, re.IGNORECASE)
            if match:
                efta_int = int(match.group(1))
                page_eftas.add(f"{efta_int:08d}")

        print(f"    Page {page_num}: {len(page_eftas)} PDFs found")
        if not page_eftas:
            break

        eftas.update(page_eftas)
        page_num += 1
        time.sleep(1.5)

    print(f"  Found {len(eftas):,} EFTA numbers from listing")
    return eftas


# ── S3: what's in the bucket ──────────────────────────────────────────────────

def fetch_uploaded_eftas() -> set:
    print(f"  Fetching file list from s3://{DO_BUCKET}/{S3_PREFIX}/ ...")
    s3 = boto3.client(
        "s3",
        endpoint_url=DO_ENDPOINT,
        aws_access_key_id=DO_ACCESS_KEY,
        aws_secret_access_key=DO_SECRET_KEY,
        config=Config(connect_timeout=10, read_timeout=60, retries={"max_attempts": 3}),
    )
    eftas = set()
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=DO_BUCKET, Prefix=f"{S3_PREFIX}/"):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            match = re.search(r"EFTA(\d+)", key, re.IGNORECASE)
            if match:
                eftas.add(f"{int(match.group(1)):08d}")
    print(f"  {len(eftas):,} files found in DreamObjects")
    return eftas


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    print("=" * 60)
    print("  DocketZero — Dataset 12 Verification")
    print("=" * 60)

    cookies = parse_cookies(DOJ_COOKIES_RAW)
    session = requests.Session()
    session.headers.update(DOJ_HEADERS)
    session.cookies.update(cookies)

    print()
    corpus_eftas  = fetch_corpus_eftas()
    print()
    listing_eftas = scrape_listing_eftas(session)
    print()
    uploaded_eftas = fetch_uploaded_eftas()

    # Full expected set
    expected = corpus_eftas | listing_eftas
    missing  = expected - uploaded_eftas
    extra    = uploaded_eftas - expected

    print()
    print("=" * 60)
    print("  Verification Results")
    print("=" * 60)
    print(f"  Expected (corpus + listing) : {len(expected):,}")
    print(f"  In DreamObjects             : {len(uploaded_eftas):,}")
    print(f"  Missing                     : {len(missing):,}")
    print(f"  Extra (not in sources)      : {len(extra):,}")
    print("=" * 60)

    if not missing:
        print("\n  ✓ All expected files are present in DreamObjects!")
    else:
        print(f"\n  ✗ {len(missing)} files are missing:")
        for efta in sorted(missing):
            print(f"    EFTA{efta}.pdf")

    if extra:
        print(f"\n  Note: {len(extra)} files in DreamObjects not in either source")
        print("  (These may be files added by DOJ after corpus + listing snapshot)")
        for efta in sorted(extra):
            print(f"    EFTA{efta}.pdf")

    sys.exit(0 if not missing else 1)


if __name__ == "__main__":
    main()
