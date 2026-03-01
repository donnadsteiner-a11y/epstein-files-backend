"""
Daily Monitor — Designed to run as a cron job.
1. Scans for new EFTA file URLs by sequential number checking
2. Downloads any new files to DreamObjects
3. Extracts text and tags persons
4. Logs activity
"""
import os
import sys
import logging
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.settings import LOG_DIR
from db.database import init_db, get_db, insert_monitor_log, query_val
from crawler.url_generator import scan_all_datasets, get_scan_summary
from crawler.downloader import process_downloads
from crawler.metadata_extractor import (
    process_unindexed_documents, process_images, process_videos
)

os.makedirs(LOG_DIR, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.StreamHandler(),
    ]
)
logger = logging.getLogger("daily_monitor")


def run_daily_check():
    logger.info("=" * 60)
    logger.info(f"DAILY DOJ MONITOR — {datetime.now().isoformat()}")
    logger.info("=" * 60)

    init_db()

    # ── STEP 1: Scan for new EFTA file URLs ──
    logger.info("STEP 1: Scanning for new EFTA file URLs...")
    try:
        new_found, urls_checked, datasets_done = scan_all_datasets(batch_per_dataset=2000)
        logger.info(f"Scan: {new_found} new files found, {urls_checked} URLs checked, {datasets_done}/12 datasets complete")
    except Exception as e:
        logger.error(f"URL scanning failed: {e}")
        insert_monitor_log("doj_efta", "error", f"Scan failed: {e}")
        return

    # Check pending downloads
    with get_db() as conn:
        pending = query_val(conn, "SELECT COUNT(*) FROM scraped_urls WHERE downloaded=0")

    logger.info(f"Pending downloads: {pending}")

    # ── STEP 2: Download new files ──
    downloaded = 0
    skipped = 0
    errors = 0
    if pending > 0:
        logger.info("STEP 2: Downloading new files...")
        try:
            downloaded, skipped, errors = process_downloads(limit=500)
        except Exception as e:
            logger.error(f"Download failed: {e}")
            insert_monitor_log("doj_efta", "error", f"Download failed: {e}")
            return
        logger.info(f"Downloaded: {downloaded}, Skipped: {skipped}, Errors: {errors}")
    else:
        logger.info("STEP 2: No new files to download")

    # ── STEP 3: Index new files ──
    logger.info("STEP 3: Extracting metadata from new files...")
    processed = 0
    extract_errors = 0
    try:
        processed, extract_errors = process_unindexed_documents(limit=200)
        process_images(limit=200)
        process_videos(limit=200)
    except Exception as e:
        logger.error(f"Extraction failed: {e}")
        insert_monitor_log("doj_efta", "error", f"Extraction failed: {e}")
        return

    # ── STEP 4: Log results ──
    if downloaded > 0 or new_found > 10:
        status = "major" if downloaded > 50 else "update"
        result = f"{downloaded} new files downloaded, {processed} indexed"
    elif new_found > 0:
        status = "update"
        result = f"{new_found} new URLs found, {downloaded} downloaded"
    else:
        status = "checked"
        result = f"Scan: {urls_checked} checked, {new_found} new. {datasets_done}/12 datasets scanned."

    scan_summary = get_scan_summary()
    total_files_found = sum(s["files_found"] for s in scan_summary)

    insert_monitor_log(
        source="doj_efta",
        status=status,
        result=result,
        new_files=downloaded,
        details={
            "new_urls_found": new_found,
            "urls_checked": urls_checked,
            "datasets_complete": datasets_done,
            "total_files_discovered": total_files_found,
            "downloaded": downloaded,
            "skipped": skipped,
            "indexed": processed,
            "errors": errors,
        }
    )

    logger.info(f"\nDaily monitor complete: {result}")
    logger.info(f"Total EFTA files discovered so far: {total_files_found}")
    logger.info("=" * 60)


if __name__ == "__main__":
    run_daily_check()
