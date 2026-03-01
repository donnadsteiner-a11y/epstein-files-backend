"""
Daily Monitor — Designed to run as a cron job.
Checks the DOJ EFTA disclosures page for new files,
downloads any new additions, and logs activity.
"""
import os
import sys
import logging
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.settings import LOG_DIR
from db.database import init_db, get_db, insert_monitor_log, query_val
from crawler.doj_scraper import scrape_all_datasets, scrape_main_disclosures_page
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
    """
    Full daily monitoring pipeline:
    1. Scrape DOJ for new file links
    2. Download any new files
    3. Extract text & tag persons
    4. Log results
    """
    logger.info("=" * 60)
    logger.info(f"DAILY DOJ MONITOR — {datetime.now().isoformat()}")
    logger.info("=" * 60)

    init_db()

    # Count files before
    with get_db() as conn:
        count_before = query_val(conn, "SELECT COUNT(*) FROM scraped_urls")

    # ── STEP 1: Scrape for new file links ──
    logger.info("STEP 1: Scraping DOJ for new file links...")
    try:
        scrape_main_disclosures_page()
        scrape_all_datasets()
    except Exception as e:
        logger.error(f"Scraping failed: {e}")
        insert_monitor_log("doj_efta", "error", f"Scrape failed: {e}")
        return

    # Count new links
    with get_db() as conn:
        count_after = query_val(conn, "SELECT COUNT(*) FROM scraped_urls")
        pending = query_val(conn, "SELECT COUNT(*) FROM scraped_urls WHERE downloaded=0")

    new_links = count_after - count_before
    logger.info(f"Found {new_links} new file links ({pending} pending download)")

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
    if downloaded > 0 or new_links > 10:
        status = "major" if downloaded > 50 else "update"
        result = f"{downloaded} new files downloaded, {processed} indexed"
    elif new_links > 0:
        status = "update"
        result = f"{new_links} new links found, {downloaded} downloaded"
    else:
        status = "checked"
        result = "No new files"

    insert_monitor_log(
        source="doj_efta",
        status=status,
        result=result,
        new_files=downloaded,
        details={
            "new_links": new_links,
            "downloaded": downloaded,
            "skipped": skipped,
            "indexed": processed,
            "errors": errors,
        }
    )

    logger.info(f"\nDaily monitor complete: {result}")
    logger.info("=" * 60)


if __name__ == "__main__":
    run_daily_check()
