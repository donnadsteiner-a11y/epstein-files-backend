"""
URL Generator — Generates direct EFTA file URLs and checks if they exist.
Bypasses the DOJ's 403-blocked listing pages by constructing URLs directly.

The EFTA files follow the pattern:
https://www.justice.gov/epstein/files/DataSet%20{N}/EFTA{8-digit-number}.pdf

This module discovers files by trying sequential EFTA numbers with HEAD requests.
"""
import os
import sys
import time
import logging
import requests

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.settings import REQUEST_DELAY, REQUEST_TIMEOUT, USER_AGENT, LOG_DIR
from db.database import init_db, get_db, mark_url_scraped, url_already_scraped, query_val, query_rows

os.makedirs(LOG_DIR, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger("url_generator")

DOJ_FILE_BASE = "https://www.justice.gov/epstein/files"

# Browser-like headers to avoid 403
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Connection": "keep-alive",
}

session = requests.Session()
session.headers.update(HEADERS)

# Approximate EFTA number ranges per dataset (derived from DOJ structure)
# These will be refined as we discover actual files
DATASET_RANGES = {
    1:  (1, 400000),
    2:  (400001, 800000),
    3:  (800001, 1100000),
    4:  (1100001, 1400000),
    5:  (1400001, 1600000),
    6:  (1600001, 1800000),
    7:  (1800001, 2000000),
    8:  (2000001, 2200000),
    9:  (2200001, 2400000),
    10: (2400001, 2550000),
    11: (2550001, 2700000),
    12: (2700001, 2800000),
}


def ensure_progress_table():
    """Create a table to track scanning progress per dataset."""
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS scan_progress (
                dataset_id INTEGER PRIMARY KEY,
                last_checked INTEGER DEFAULT 0,
                files_found INTEGER DEFAULT 0,
                consecutive_misses INTEGER DEFAULT 0,
                completed BOOLEAN DEFAULT FALSE
            )
        """)
        # Seed initial progress rows
        for ds_id, (start, end) in DATASET_RANGES.items():
            cur.execute("""
                INSERT INTO scan_progress (dataset_id, last_checked)
                VALUES (%s, %s)
                ON CONFLICT (dataset_id) DO NOTHING
            """, (ds_id, start - 1))
        cur.close()


def get_scan_progress():
    """Get current scan progress for all datasets."""
    with get_db() as conn:
        return query_rows(conn, "SELECT * FROM scan_progress ORDER BY dataset_id")


def update_progress(dataset_id, last_checked, files_found_delta=0, consecutive_misses=0, completed=False):
    """Update scan progress for a dataset."""
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute("""
            UPDATE scan_progress 
            SET last_checked = %s, 
                files_found = files_found + %s,
                consecutive_misses = %s,
                completed = %s
            WHERE dataset_id = %s
        """, (last_checked, files_found_delta, consecutive_misses, completed, dataset_id))
        cur.close()


def check_url_exists(url):
    """Use HEAD request to check if a URL exists. Returns True/False."""
    try:
        resp = session.head(url, timeout=15, allow_redirects=True)
        return resp.status_code == 200
    except requests.RequestException:
        return False


def generate_and_check_batch(dataset_id, batch_size=200, max_consecutive_misses=500):
    """
    Generate EFTA URLs for a dataset and check which exist.
    
    Returns: (urls_found, urls_checked, is_completed)
    """
    start_range, end_range = DATASET_RANGES.get(dataset_id, (1, 100000))
    
    # Get current progress
    with get_db() as conn:
        progress = query_rows(conn, 
            "SELECT * FROM scan_progress WHERE dataset_id = %s", (dataset_id,))
    
    if not progress:
        return 0, 0, False
    
    progress = progress[0]
    if progress.get("completed"):
        return 0, 0, True
    
    last_checked = progress.get("last_checked", start_range - 1)
    consecutive_misses = progress.get("consecutive_misses", 0)
    
    urls_found = 0
    urls_checked = 0
    current_num = last_checked + 1
    
    logger.info(f"Dataset {dataset_id}: Scanning from EFTA{current_num:08d} (batch of {batch_size})")
    
    while urls_checked < batch_size and current_num <= end_range:
        efta_num = f"EFTA{current_num:08d}"
        url = f"{DOJ_FILE_BASE}/DataSet%20{dataset_id}/{efta_num}.pdf"
        
        if url_already_scraped(url):
            current_num += 1
            urls_checked += 1
            consecutive_misses = 0  # Reset since this was a known file
            continue
        
        exists = check_url_exists(url)
        urls_checked += 1
        
        if exists:
            mark_url_scraped(url, dataset_id=dataset_id, file_type="pdf", downloaded=False)
            urls_found += 1
            consecutive_misses = 0
            logger.info(f"  FOUND: {efta_num}.pdf")
        else:
            consecutive_misses += 1
        
        # If we've had too many misses in a row, this range might be empty
        # But DOJ files can have gaps, so be generous
        if consecutive_misses >= max_consecutive_misses:
            logger.info(f"  Dataset {dataset_id}: {max_consecutive_misses} consecutive misses, marking complete")
            update_progress(dataset_id, current_num, urls_found, consecutive_misses, completed=True)
            return urls_found, urls_checked, True
        
        current_num += 1
        
        # Small delay to be polite
        if urls_checked % 10 == 0:
            time.sleep(0.5)
    
    # Check if we've reached the end of the range
    completed = current_num > end_range
    update_progress(dataset_id, current_num - 1, urls_found, consecutive_misses, completed)
    
    logger.info(f"  Dataset {dataset_id}: Checked {urls_checked}, found {urls_found}, misses: {consecutive_misses}")
    return urls_found, urls_checked, completed


def scan_all_datasets(batch_per_dataset=200):
    """
    Scan all datasets for new files.
    Each dataset gets a batch of checks per run.
    Over many cron runs, this covers the full EFTA range.
    """
    ensure_progress_table()
    
    total_found = 0
    total_checked = 0
    datasets_completed = 0
    
    progress = get_scan_progress()
    
    for p in progress:
        ds_id = p["dataset_id"]
        if p.get("completed"):
            datasets_completed += 1
            continue
        
        found, checked, completed = generate_and_check_batch(ds_id, batch_size=batch_per_dataset)
        total_found += found
        total_checked += checked
        if completed:
            datasets_completed += 1
    
    logger.info(f"\nScan complete: {total_found} new files found, {total_checked} URLs checked, {datasets_completed}/12 datasets complete")
    return total_found, total_checked, datasets_completed


def get_scan_summary():
    """Get a summary of scanning progress."""
    ensure_progress_table()
    progress = get_scan_progress()
    
    summary = []
    for p in progress:
        ds_id = p["dataset_id"]
        start, end = DATASET_RANGES.get(ds_id, (0, 0))
        total_range = end - start + 1
        checked = p.get("last_checked", start) - start + 1
        pct = (checked / total_range * 100) if total_range > 0 else 0
        
        summary.append({
            "dataset": ds_id,
            "range": f"EFTA{start:08d} - EFTA{end:08d}",
            "last_checked": p.get("last_checked", 0),
            "files_found": p.get("files_found", 0),
            "progress_pct": round(pct, 1),
            "completed": p.get("completed", False),
        })
    
    return summary


if __name__ == "__main__":
    init_db()
    ensure_progress_table()
    found, checked, completed = scan_all_datasets(batch_per_dataset=100)
    
    print(f"\n{'='*50}")
    print(f"Scan Summary")
    print(f"{'='*50}")
    print(f"New files found:    {found}")
    print(f"URLs checked:       {checked}")
    print(f"Datasets complete:  {completed}/12")
    
    print(f"\nProgress per dataset:")
    for s in get_scan_summary():
        status = "✓ DONE" if s["completed"] else f"{s['progress_pct']}%"
        print(f"  DS{s['dataset']:2d}: {s['files_found']:6d} files found | {status}")
