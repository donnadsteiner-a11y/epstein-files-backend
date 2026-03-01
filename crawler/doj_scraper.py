"""
DOJ Scraper — Crawls all 12 DOJ EFTA data set pages and collects
download links for PDFs, JPGs, MOVs, and other evidence files.
"""
import os
import sys
import time
import logging
import re
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.settings import (
    DOJ_BASE, DOJ_DISCLOSURES_URL, DOJ_DATA_SET_URLS,
    ALLOWED_EXTENSIONS, REQUEST_DELAY, REQUEST_TIMEOUT,
    USER_AGENT, MAX_RETRIES, LOG_DIR, LOG_LEVEL
)
from db.database import init_db, mark_url_scraped, url_already_scraped, get_db, query_val

os.makedirs(LOG_DIR, exist_ok=True)
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.StreamHandler(),
    ]
)
logger = logging.getLogger("doj_scraper")

session = requests.Session()
session.headers.update({"User-Agent": USER_AGENT})


def fetch_page(url: str, retries: int = MAX_RETRIES) -> str | None:
    for attempt in range(retries):
        try:
            logger.debug(f"Fetching: {url} (attempt {attempt+1})")
            resp = session.get(url, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
            time.sleep(REQUEST_DELAY)
            return resp.text
        except requests.RequestException as e:
            logger.warning(f"Attempt {attempt+1} failed for {url}: {e}")
            time.sleep(REQUEST_DELAY * (attempt + 1))
    logger.error(f"All {retries} attempts failed for {url}")
    return None


def extract_file_links(html: str, base_url: str) -> list[dict]:
    soup = BeautifulSoup(html, "html.parser")
    links = []
    seen = set()

    for a_tag in soup.find_all("a", href=True):
        href = a_tag["href"].strip()
        full_url = urljoin(base_url, href)
        parsed = urlparse(full_url)
        path_lower = parsed.path.lower()

        ext = os.path.splitext(path_lower)[1]
        if ext not in ALLOWED_EXTENSIONS:
            if "/dl" in path_lower or "/download" in path_lower:
                ext = ".pdf"
            else:
                continue

        if full_url in seen:
            continue
        seen.add(full_url)

        filename = os.path.basename(parsed.path)
        if not filename or filename == "dl":
            filename = a_tag.get_text(strip=True)[:80] or "unknown"
            filename = re.sub(r'[^\w\s\-.]', '', filename).strip()
            if not filename.endswith(ext):
                filename += ext

        links.append({
            "url": full_url,
            "filename": filename,
            "file_type": ext.lstrip(".").lower(),
            "link_text": a_tag.get_text(strip=True)[:200],
        })

    return links


def scrape_dataset_page(dataset_id: int, url: str) -> list[dict]:
    logger.info(f"Scraping Data Set {dataset_id}: {url}")
    all_files = []

    html = fetch_page(url)
    if not html:
        logger.error(f"Could not fetch Data Set {dataset_id} page")
        return []

    files = extract_file_links(html, url)
    logger.info(f"  Found {len(files)} file links on main page")

    soup = BeautifulSoup(html, "html.parser")
    page_links = soup.find_all("a", href=True)
    next_pages = []
    for link in page_links:
        href = link["href"]
        if "page=" in href or "?page" in href:
            next_url = urljoin(url, href)
            if next_url not in next_pages and next_url != url:
                next_pages.append(next_url)

    for page_url in next_pages:
        logger.info(f"  Scraping pagination: {page_url}")
        page_html = fetch_page(page_url)
        if page_html:
            page_files = extract_file_links(page_html, page_url)
            files.extend(page_files)
            logger.info(f"    Found {len(page_files)} more file links")

    new_count = 0
    for f in files:
        if not url_already_scraped(f["url"]):
            mark_url_scraped(
                url=f["url"],
                dataset_id=dataset_id,
                file_type=f["file_type"],
                downloaded=False,
            )
            new_count += 1

    logger.info(f"  Data Set {dataset_id}: {len(files)} total links, {new_count} new")
    all_files.extend(files)
    return all_files


def scrape_all_datasets():
    logger.info("=" * 60)
    logger.info("Starting full DOJ scrape of all 12 data sets")
    logger.info("=" * 60)

    total_files = 0
    for ds_id, ds_url in DOJ_DATA_SET_URLS.items():
        files = scrape_dataset_page(ds_id, ds_url)
        total_files += len(files)

    logger.info(f"Scrape complete. Total file links found: {total_files}")
    return total_files


def scrape_main_disclosures_page():
    logger.info(f"Scraping main disclosures page: {DOJ_DISCLOSURES_URL}")
    html = fetch_page(DOJ_DISCLOSURES_URL)
    if not html:
        return 0

    files = extract_file_links(html, DOJ_DISCLOSURES_URL)
    new_count = 0
    for f in files:
        if not url_already_scraped(f["url"]):
            mark_url_scraped(f["url"], file_type=f["file_type"], downloaded=False)
            new_count += 1

    logger.info(f"Main page: {len(files)} links found, {new_count} new")
    return new_count


if __name__ == "__main__":
    init_db()
    scrape_main_disclosures_page()
    scrape_all_datasets()

    with get_db() as conn:
        total = query_val(conn, "SELECT COUNT(*) FROM scraped_urls")
        pending = query_val(conn, "SELECT COUNT(*) FROM scraped_urls WHERE downloaded=0")

    print(f"\n{'='*50}")
    print(f"Scrape Summary")
    print(f"{'='*50}")
    print(f"Total URLs in database:  {total}")
    print(f"Pending download:        {pending}")
