#!/usr/bin/env python3
"""
ds12_downloader.py
==================
Downloads all Dataset 12 PDFs using a two-source approach:

  1. rhowardstone corpus (document_summary.csv.gz) — bulk of known files
  2. Full pagination scrape of the DOJ DS12 listing — catches new files
     added after the corpus was last updated

Both sources are deduplicated before downloading.
Uploads to: docketzero-files/Data Set 12/EFTA02730265.pdf

Fully resumable — files already in DreamObjects are skipped.

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

# ── Dataset config ─────────────────────────────────────────────────────────────

DATASET_NUMBER   = 12
EFTA_RANGE_START = 2730265
EFTA_RANGE_END   = 2731783
S3_PREFIX        = "Data Set 12"
DOJ_LISTING      = "https://www.justice.gov/epstein/doj-disclosures/data-set-12-files"
DOJ_URL_TEMPLATE = "https://www.justice.gov/epstein/files/DataSet%2012/EFTA{efta}.pdf"
DOJ_BASE         = "https://www.justice.gov"

CORPUS_URL = (
    "https://raw.githubusercontent.com/rhowardstone/"
    "Epstein-research-data/main/document_summary.csv.gz"
)

# ── Environment ────────────────────────────────────────────────────────────────

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


# ── Source 1: rhowardstone corpus ─────────────────────────────────────────────

def fetch_corpus_eftas() -> set:
    """
    Pull verified EFTA numbers from the rhowardstone corpus,
    filtered to the DS12 EFTA range.
    """
    print("  [Source 1] Downloading rhowardstone corpus ...")
    try:
        resp = requests.get(CORPUS_URL, timeout=120)
        if resp.status_code != 200:
            print(f"  WARNING: Corpus download failed (HTTP {resp.status_code}) — skipping")
            return set()
    except Exception as exc:
        print(f"  WARNING: Corpus download error ({exc}) — skipping")
        return set()

    print(f"  Downloaded {len(resp.content):,} bytes — parsing ...")

    eftas = set()
    with gzip.open(io.BytesIO(resp.content), "rt", encoding="utf-8") as f:
        header = None
        efta_col = 0
        for line in f:
            line = line.rstrip("\n")
            if header is None:
                header = line.split(",")
                print(f"  CSV columns: {header}")
                try:
                    efta_col = header.index("efta_number")
                except ValueError:
                    efta_col = 0
                continue

            parts = line.split(",")
            if len(parts) <= efta_col:
                continue

            efta = parts[efta_col].strip().strip('"')
            if not efta:
                continue

            try:
                efta_int = int(efta)
                if EFTA_RANGE_START <= efta_int <= EFTA_RANGE_END:
                    eftas.add(f"{efta_int:08d}")
            except ValueError:
                continue

    print(f"  Found {len(eftas):,} EFTA numbers from corpus")
    return eftas


# ── Source 2: DOJ listing page scraper ────────────────────────────────────────

def scrape_listing_eftas(session: requests.Session) -> set:
    """
    Scrape every page of the DOJ DS12 listing using authenticated session.
    Returns a set of EFTA number strings found on the listing pages.
    """
    print("  [Source 2] Scraping DOJ DS12 listing pages ...")
    eftas = set()
    page_num = 0

    while True:
        url = DOJ_LISTING if page_num == 0 else f"{DOJ_LISTING}?page={page_num}"
        print(f"    Page {page_num}: {url}")

        try:
            resp = session.get(url, timeout=30)
        except requests.RequestException as exc:
            print(f"    Request error: {exc} — stopping pagination")
            break

        if resp.status_code not in (200, 304):
            print(f"    HTTP {resp.status_code} — stopping pagination")
            break

        # Detect soft Access Denied
        if "Access Denied" in resp.text and len(resp.text) < 3000:
            print(f"    Access Denied — stopping pagination")
            break

        soup = BeautifulSoup(resp.text, "html.parser")

        # Extract EFTA numbers from all PDF links on this page
        page_eftas = set()
        for tag in soup.find_all("a", href=True):
            href = tag["href"]
            if not href.lower().endswith(".pdf"):
                continue
            match = re.search(r"EFTA(\d+)", href, re.IGNORECASE)
            if match:
                efta_int = int(match.group(1))
                page_eftas.add(f"{efta_int:08d}")

        print(f"    Found {len(page_eftas)} PDFs on page {page_num}")

        if not page_eftas:
            print(f"    No PDFs found — pagination complete")
            break

        eftas.update(page_eftas)
        page_num += 1
        time.sleep(1.5)

    print(f"  Found {len(eftas):,} EFTA numbers from listing scrape")
    return eftas


# ── S3 helpers ─────────────────────────────────────────────────────────────────

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
    print("=" * 60)
    print(f"  DocketZero — Dataset {DATASET_NUMBER} Downloader")
    print("=" * 60)
    print(f"  Bucket  : {DO_BUCKET}")
    print(f"  Prefix  : {S3_PREFIX}/")
    print("=" * 60)

    cookies = parse_cookies(DOJ_COOKIES_RAW)
    if not cookies:
        print("ERROR: DOJ_COOKIES is empty or could not be parsed.")
        sys.exit(1)
    print(f"\n  Cookies loaded: {list(cookies.keys())}\n")

    session = requests.Session()
    session.headers.update(DOJ_HEADERS)
    session.cookies.update(cookies)

    # ── Gather EFTA numbers from both sources ──────────────────────────────────
    corpus_eftas  = fetch_corpus_eftas()
    print()
    listing_eftas = scrape_listing_eftas(session)
    print()

    # Merge and sort — listing catches anything newer than the corpus
    all_eftas = sorted(corpus_eftas | listing_eftas)
    total     = len(all_eftas)

    print(f"  Combined unique EFTA numbers : {total:,}")
    print(f"  (corpus: {len(corpus_eftas):,}  |  listing-only new: {len(listing_eftas - corpus_eftas):,})")

    if total == 0:
        print("ERROR: No EFTA numbers found from either source.")
        sys.exit(1)

    # ── Build S3 skip-list ─────────────────────────────────────────────────────
    s3       = build_s3()
    uploaded = fetch_uploaded_keys(s3)

    # ── Download loop ──────────────────────────────────────────────────────────
    uploaded_count = 0
    skipped_count  = 0
    failed         = []

    print(f"\n── Downloading {total:,} PDFs ─────────────────────────────\n")

    for idx, efta in enumerate(all_eftas, 1):
        filename = f"EFTA{efta}.pdf"
        s3_key   = f"{S3_PREFIX}/{filename}"
        url      = DOJ_URL_TEMPLATE.format(efta=efta)

        if s3_key in uploaded:
            skipped_count += 1
            continue

        print(f"  [{idx}/{total}]  {filename}", end="  ", flush=True)

        try:
            resp = session.get(url, timeout=60)
        except requests.RequestException as exc:
            print(f"✗  Request error: {exc}")
            failed.append(efta)
            time.sleep(DELAY_ON_ERROR)
            continue

        if resp.status_code == 404:
            print(f"✗  404 (not found)")
            time.sleep(DELAY_BETWEEN)
            continue

        if resp.status_code != 200:
            print(f"✗  HTTP {resp.status_code}")
            failed.append(efta)
            time.sleep(DELAY_ON_ERROR)
            continue

        ctype = resp.headers.get("Content-Type", "")
        if "html" in ctype.lower():
            print(f"✗  Got HTML — cookies may have expired. Stopping.")
            print("   Update DOJ_COOKIES secret and re-run.")
            print(f"   Stopped at EFTA {efta} ({idx}/{total})")
            sys.exit(2)

        pdf_bytes = resp.content
        if len(pdf_bytes) < 512:
            print(f"✗  Too small ({len(pdf_bytes)} bytes) — skipping")
            failed.append(efta)
            continue

        ok = upload(s3, s3_key, pdf_bytes)
        if ok:
            uploaded_count += 1
            uploaded.add(s3_key)
            print(f"✓  {len(pdf_bytes):,} bytes")
        else:
            failed.append(efta)

        time.sleep(DELAY_BETWEEN)

    # ── Summary ────────────────────────────────────────────────────────────────
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
