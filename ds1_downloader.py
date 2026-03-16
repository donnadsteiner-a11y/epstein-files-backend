#!/usr/bin/env python3
"""
ds1_downloader.py
=================
Downloads every PDF from the DOJ Dataset 1 listing page and uploads
each one to DreamObjects under:

    docketzero-files/Data Set 1/EFTA########.pdf

Fully resumable — files already in DreamObjects are skipped.

Required environment variables (GitHub Secrets):
    DOJ_COOKIES      Cookie string copied from your browser
    DO_ENDPOINT      https://s3.us-east-005.dream.io
    DO_ACCESS_KEY    DreamObjects access key
    DO_SECRET_KEY    DreamObjects secret key
    DO_BUCKET        docketzero-files
"""

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

# ── Config ─────────────────────────────────────────────────────────────────────

DOJ_LISTING   = "https://www.justice.gov/epstein/doj-disclosures/data-set-1-files"
DOJ_BASE      = "https://www.justice.gov"
S3_PREFIX     = "Data Set 1"

DO_ENDPOINT   = os.environ["DO_ENDPOINT"]
DO_ACCESS_KEY = os.environ["DO_ACCESS_KEY"]
DO_SECRET_KEY = os.environ["DO_SECRET_KEY"]
DO_BUCKET     = os.environ["DO_BUCKET"]

DOJ_COOKIES_RAW = os.environ["DOJ_COOKIES"]

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept":          "text/html,application/xhtml+xml,application/pdf,*/*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer":         DOJ_LISTING,
}

DELAY_BETWEEN_REQUESTS = 1.5
DELAY_ON_ERROR         = 10.0


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


# ── DreamObjects helpers ───────────────────────────────────────────────────────

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
    print(f"  Fetching existing keys from s3://{DO_BUCKET}/{S3_PREFIX}/ ...")
    existing = set()
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=DO_BUCKET, Prefix=f"{S3_PREFIX}/"):
        for obj in page.get("Contents", []):
            existing.add(obj["Key"])
    print(f"  {len(existing)} files already uploaded — will skip these.")
    return existing


def upload_to_s3(s3, key: str, data: bytes) -> bool:
    try:
        s3.put_object(
            Bucket=DO_BUCKET,
            Key=key,
            Body=data,
            ContentType="application/pdf",
        )
        return True
    except Exception as exc:
        print(f"    ✗  S3 upload failed for {key}: {exc}")
        return False


# ── DOJ scraping ───────────────────────────────────────────────────────────────

def scrape_pdf_links_from_page(session: requests.Session, url: str) -> tuple[list[str], bool]:
    try:
        resp = session.get(url, timeout=30)
    except requests.RequestException as exc:
        print(f"    Request error fetching {url}: {exc}")
        return [], False

    if resp.status_code != 200:
        print(f"    HTTP {resp.status_code} on {url}")
        return [], False

    if "Access Denied" in resp.text and len(resp.text) < 3000:
        print(f"    Access Denied response on {url} — stopping pagination.")
        return [], False

    soup = BeautifulSoup(resp.text, "html.parser")

    pdf_urls = []
    for tag in soup.find_all("a", href=True):
        href = tag["href"]
        if not href.lower().endswith(".pdf"):
            continue
        if href.startswith("http"):
            abs_url = href
        elif href.startswith("/"):
            abs_url = DOJ_BASE + href
        else:
            abs_url = DOJ_BASE + "/" + href
        pdf_urls.append(abs_url)

    has_next = bool(
        soup.find("a", rel="next")
        or soup.find("li", class_=re.compile(r"next"))
        or soup.find("a", string=re.compile(r"next", re.IGNORECASE))
    )

    return pdf_urls, has_next


def scrape_all_pdf_links(session: requests.Session) -> list[str]:
    all_urls = []
    seen     = set()
    page_num = 0

    print(f"\n── Scraping DS1 listing pages ───────────────────────────")

    while True:
        url = DOJ_LISTING if page_num == 0 else f"{DOJ_LISTING}?page={page_num}"
        print(f"  Page {page_num}: {url}")

        links, has_next = scrape_pdf_links_from_page(session, url)

        new_links = [u for u in links if u not in seen]
        seen.update(new_links)
        all_urls.extend(new_links)

        print(f"    Found {len(links)} PDF links ({len(new_links)} new) — total so far: {len(all_urls)}")

        if not links:
            print("    No PDFs found on this page — done.")
            break

        if not has_next:
            print("    No next-page link — done.")
            break

        page_num += 1
        time.sleep(DELAY_BETWEEN_REQUESTS)

    print(f"\n  Total unique PDF URLs found: {len(all_urls)}")
    return all_urls


# ── URL → S3 key ──────────────────────────────────────────────────────────────

def url_to_s3_key(url: str) -> str:
    decoded  = urllib.parse.unquote(url)
    filename = decoded.split("/")[-1]
    return f"{S3_PREFIX}/{filename}"


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    print("=" * 60)
    print("  DocketZero — Dataset 1 Downloader")
    print("=" * 60)
    print(f"  Bucket  : {DO_BUCKET}")
    print(f"  Prefix  : {S3_PREFIX}/")
    print(f"  Listing : {DOJ_LISTING}")
    print("=" * 60)

    cookies = parse_cookies(DOJ_COOKIES_RAW)
    if not cookies:
        print("ERROR: DOJ_COOKIES is empty or could not be parsed.")
        sys.exit(1)
    print(f"\n  Cookies loaded: {list(cookies.keys())}")

    session = requests.Session()
    session.headers.update(HEADERS)
    session.cookies.update(cookies)

    s3 = build_s3()

    uploaded = fetch_uploaded_keys(s3)

    pdf_urls = scrape_all_pdf_links(session)

    if not pdf_urls:
        print("\nNo PDF URLs found. Check your cookies or the listing page.")
        sys.exit(1)

    total          = len(pdf_urls)
    skipped        = 0
    uploaded_count = 0
    failed         = []

    print(f"\n── Downloading {total} PDFs ──────────────────────────────────")

    for idx, url in enumerate(pdf_urls, 1):
        s3_key   = url_to_s3_key(url)
        filename = s3_key.split("/")[-1]

        if s3_key in uploaded:
            skipped += 1
            continue

        print(f"  [{idx}/{total}]  {filename}", end="  ", flush=True)

        try:
            resp = session.get(url, timeout=60, stream=False)
        except requests.RequestException as exc:
            print(f"✗  Download error: {exc}")
            failed.append(url)
            time.sleep(DELAY_ON_ERROR)
            continue

        if resp.status_code == 404:
            print(f"✗  404 — file missing on DOJ server, skipping")
            failed.append(url)
            time.sleep(DELAY_BETWEEN_REQUESTS)
            continue

        if resp.status_code != 200:
            print(f"✗  HTTP {resp.status_code} — will retry next run")
            failed.append(url)
            time.sleep(DELAY_ON_ERROR)
            continue

        content_type = resp.headers.get("Content-Type", "")
        if "html" in content_type.lower():
            print(f"✗  Got HTML instead of PDF (possible auth wall) — stopping")
            print("     Refresh your DOJ_COOKIES secret and re-run.")
            sys.exit(2)

        pdf_bytes = resp.content
        if len(pdf_bytes) < 512:
            print(f"✗  Response too small ({len(pdf_bytes)} bytes) — skipping")
            failed.append(url)
            continue

        ok = upload_to_s3(s3, s3_key, pdf_bytes)
        if ok:
            uploaded_count += 1
            uploaded.add(s3_key)
            print(f"✓  {len(pdf_bytes):,} bytes")
        else:
            failed.append(url)

        time.sleep(DELAY_BETWEEN_REQUESTS)

    print("\n" + "=" * 60)
    print("  Run complete")
    print(f"  Uploaded this run : {uploaded_count}")
    print(f"  Skipped (done)    : {skipped}")
    print(f"  Failed            : {len(failed)}")
    print("=" * 60)

    if failed:
        print("\nFailed URLs (will be retried on next scheduled run):")
        for u in failed:
            print(f"  {u}")
        sys.exit(1)


if __name__ == "__main__":
    main()
