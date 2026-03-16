#!/usr/bin/env python3
"""
doj_downloader.py
=================
Generic DOJ dataset downloader with Kino fallback.

Download strategy for each EFTA file:
  1. Try DOJ directly (freshest source, includes newest files)
  2. If DOJ returns 404 or blocks → try assets.getkino.com
     (1,401,320 files including DOJ-deleted documents)
  3. If both fail → log as failed for next run

Two-source URL discovery:
  1. rhowardstone corpus — verified EFTA numbers
  2. DOJ listing page 0 scrape — catches new files

Uploads to: docketzero-files/Data Set {N}/EFTA########.pdf

Fully resumable — files already in DreamObjects are skipped.

Required environment variables (GitHub Secrets):
    DATASET_NUMBER   Which dataset to download (1-12)
    DOJ_COOKIES      Browser cookie string
    DO_ENDPOINT      https://s3.us-east-005.dream.io
    DO_ACCESS_KEY    DreamObjects access key
    DO_SECRET_KEY    DreamObjects secret key
    DO_BUCKET        docketzero-files
"""

import gzip
import io
import json
import os
import re
import sys
import time
import urllib.parse

import boto3
import requests
from botocore.config import Config
from bs4 import BeautifulSoup

# ── Dataset registry ───────────────────────────────────────────────────────────

DATASETS = {
    1:  {"start": 1,        "end": 3158,    "url_folder": "DataSet%201"},
    2:  {"start": 3159,     "end": 3857,    "url_folder": "DataSet%202"},
    3:  {"start": 3858,     "end": 5586,    "url_folder": "DataSet%203"},
    4:  {"start": 5705,     "end": 8320,    "url_folder": "DataSet%204"},
    5:  {"start": 8409,     "end": 8528,    "url_folder": "DataSet%205"},
    6:  {"start": 8529,     "end": 8998,    "url_folder": "DataSet%206"},
    7:  {"start": 9016,     "end": 9664,    "url_folder": "DataSet%207"},
    8:  {"start": 9676,     "end": 39023,   "url_folder": "DataSet%208"},
    9:  {"start": 39025,    "end": 1262781, "url_folder": "DataSet%209"},
    10: {"start": 1262782,  "end": 2205654, "url_folder": "DataSet%2010"},
    11: {"start": 2205655,  "end": 2730264, "url_folder": "DataSet%2011"},
    12: {"start": 2730265,  "end": 2858497, "url_folder": "DataSet%2012"},
}

CORPUS_URL = (
    "https://raw.githubusercontent.com/rhowardstone/"
    "Epstein-research-data/main/document_summary.csv.gz"
)

# Kino mirror — 1,401,320 files including DOJ-deleted documents
KINO_URL = "https://assets.getkino.com/documents/EFTA{efta}.pdf"

# ── Environment ────────────────────────────────────────────────────────────────

DATASET_NUMBER  = int(os.environ["DATASET_NUMBER"])
DO_ENDPOINT     = os.environ["DO_ENDPOINT"]
DO_ACCESS_KEY   = os.environ["DO_ACCESS_KEY"]
DO_SECRET_KEY   = os.environ["DO_SECRET_KEY"]
DO_BUCKET       = os.environ["DO_BUCKET"]
DOJ_COOKIES_RAW = os.environ["DOJ_COOKIES"]

if DATASET_NUMBER not in DATASETS:
    print(f"ERROR: DATASET_NUMBER {DATASET_NUMBER} not in registry (1-12)")
    sys.exit(1)

DS          = DATASETS[DATASET_NUMBER]
EFTA_START  = DS["start"]
EFTA_END    = DS["end"]
URL_FOLDER  = DS["url_folder"]
S3_PREFIX   = f"Data Set {DATASET_NUMBER}"
DOJ_LISTING = f"https://www.justice.gov/epstein/doj-disclosures/data-set-{DATASET_NUMBER}-files"
DOJ_URL     = f"https://www.justice.gov/epstein/files/{URL_FOLDER}/EFTA{{efta}}.pdf"

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

KINO_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "application/pdf,*/*",
}

DELAY_BETWEEN  = 1.0
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
    print(f"  [Source 1] Downloading rhowardstone corpus ...")
    try:
        resp = requests.get(CORPUS_URL, timeout=120)
        if resp.status_code != 200:
            print(f"  WARNING: Corpus HTTP {resp.status_code} — skipping")
            return set()
    except Exception as exc:
        print(f"  WARNING: Corpus error ({exc}) — skipping")
        return set()

    print(f"  Downloaded {len(resp.content):,} bytes — parsing ...")

    eftas = set()
    with gzip.open(io.BytesIO(resp.content), "rt", encoding="utf-8") as f:
        header   = None
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
                efta_int   = int(efta_clean)
                if EFTA_START <= efta_int <= EFTA_END:
                    eftas.add(f"{efta_int:08d}")
            except ValueError:
                continue

    print(f"  Found {len(eftas):,} EFTA numbers from corpus")
    return eftas


# ── Source 2: DOJ listing scraper ─────────────────────────────────────────────

def scrape_listing_eftas(session: requests.Session) -> set:
    print(f"  [Source 2] Scraping DOJ DS{DATASET_NUMBER} listing pages ...")
    eftas    = set()
    page_num = 0

    while True:
        url = DOJ_LISTING if page_num == 0 else f"{DOJ_LISTING}?page={page_num}"
        print(f"    Page {page_num}: {url}")
        try:
            resp = session.get(url, timeout=30)
        except requests.RequestException as exc:
            print(f"    Request error: {exc} — stopping")
            break

        if resp.status_code not in (200, 304):
            print(f"    HTTP {resp.status_code} — stopping pagination")
            break

        if "Access Denied" in resp.text and len(resp.text) < 3000:
            print(f"    Access Denied — stopping pagination")
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

        print(f"    Found {len(page_eftas)} PDFs on page {page_num}")
        if not page_eftas:
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


# ── Download: try DOJ then Kino ────────────────────────────────────────────────

def download_pdf(efta: str, doj_session: requests.Session,
                 kino_session: requests.Session) -> tuple[bytes | None, str]:
    """
    Try DOJ first, then Kino mirror.
    Returns (pdf_bytes, source) or (None, reason).
    """
    doj_url  = DOJ_URL.format(efta=efta)
    kino_url = KINO_URL.format(efta=efta)

    # ── Attempt 1: DOJ ────────────────────────────────────────────────────────
    try:
        resp = doj_session.get(doj_url, timeout=60)

        if resp.status_code == 200:
            ctype = resp.headers.get("Content-Type", "")
            if "html" not in ctype.lower() and len(resp.content) >= 512:
                return resp.content, "DOJ"

        if resp.status_code not in (404, 403, 401):
            # Unexpected status — still try Kino
            pass

    except requests.RequestException:
        pass  # fall through to Kino

    # ── Attempt 2: Kino mirror ────────────────────────────────────────────────
    try:
        resp = kino_session.get(kino_url, timeout=60)

        if resp.status_code == 200:
            ctype = resp.headers.get("Content-Type", "")
            if "html" not in ctype.lower() and len(resp.content) >= 512:
                return resp.content, "Kino"

        if resp.status_code == 404:
            return None, "404-both"

    except requests.RequestException as exc:
        return None, f"error:{exc}"

    return None, "failed"


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    print("=" * 60)
    print(f"  DocketZero — Dataset {DATASET_NUMBER} Downloader")
    print("=" * 60)
    print(f"  Bucket      : {DO_BUCKET}")
    print(f"  Prefix      : {S3_PREFIX}/")
    print(f"  EFTA range  : {EFTA_START:08d} → {EFTA_END:08d}")
    print(f"  Sources     : DOJ → Kino fallback")
    print("=" * 60)

    cookies = parse_cookies(DOJ_COOKIES_RAW)
    if not cookies:
        print("ERROR: DOJ_COOKIES is empty or could not be parsed.")
        sys.exit(1)
    print(f"\n  Cookies loaded: {list(cookies.keys())}\n")

    # DOJ session (with cookies)
    doj_session = requests.Session()
    doj_session.headers.update(DOJ_HEADERS)
    doj_session.cookies.update(cookies)

    # Kino session (no auth needed)
    kino_session = requests.Session()
    kino_session.headers.update(KINO_HEADERS)

    # Gather EFTA numbers
    corpus_eftas  = fetch_corpus_eftas()
    print()
    listing_eftas = scrape_listing_eftas(doj_session)
    print()

    all_eftas = sorted(corpus_eftas | listing_eftas)
    total     = len(all_eftas)

    print(f"  Combined unique EFTA numbers : {total:,}")
    print(f"  (corpus: {len(corpus_eftas):,}  |  listing-only new: {len(listing_eftas - corpus_eftas):,})")

    if total == 0:
        print("ERROR: No EFTA numbers found from either source.")
        sys.exit(1)

    # S3 skip-list
    s3       = build_s3()
    uploaded = fetch_uploaded_keys(s3)

    # Download + upload loop
    uploaded_count = 0
    skipped_count  = 0
    doj_count      = 0
    kino_count     = 0
    failed         = []

    print(f"\n── Downloading {total:,} PDFs (DOJ → Kino fallback) ──────────\n")

    for idx, efta in enumerate(all_eftas, 1):
        filename = f"EFTA{efta}.pdf"
        s3_key   = f"{S3_PREFIX}/{filename}"

        if s3_key in uploaded:
            skipped_count += 1
            continue

        print(f"  [{idx}/{total}]  {filename}", end="  ", flush=True)

        pdf_bytes, source = download_pdf(efta, doj_session, kino_session)

        if pdf_bytes is None:
            print(f"✗  Not found ({source})")
            if source != "404-both":
                failed.append(efta)
            time.sleep(DELAY_BETWEEN)
            continue

        ok = upload(s3, s3_key, pdf_bytes)
        if ok:
            uploaded_count += 1
            uploaded.add(s3_key)
            if source == "DOJ":
                doj_count += 1
            else:
                kino_count += 1
            print(f"✓  {len(pdf_bytes):,} bytes  [{source}]")
        else:
            failed.append(efta)

        time.sleep(DELAY_BETWEEN)

    # Summary
    print("\n" + "=" * 60)
    print(f"  Run complete — DS{DATASET_NUMBER}")
    print(f"  Uploaded this run : {uploaded_count:,}")
    print(f"    from DOJ        : {doj_count:,}")
    print(f"    from Kino       : {kino_count:,}")
    print(f"  Skipped (done)    : {skipped_count:,}")
    print(f"  Failed            : {len(failed):,}")
    print("=" * 60)

    if failed:
        print(f"\n  Failed EFTAs: {failed[:20]}{'...' if len(failed) > 20 else ''}")
        sys.exit(1)


if __name__ == "__main__":
    main()
