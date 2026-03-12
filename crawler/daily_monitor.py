"""
Daily Monitor — Designed to run as a cron job.
1. Optionally scans for new EFTA file URLs by sequential number checking
2. Downloads any new files to DreamObjects
3. Optionally extracts text and tags persons
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
from crawler.metadata_extractor import process_unindexed_documents, process_images, process_videos

os.makedirs(LOG_DIR, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger("daily_monitor")

SKIP_SCAN = os.environ.get("SKIP_SCAN", "false").lower() == "true"
DOWNLOAD_ONLY = os.environ.get("DOWNLOAD_ONLY", "false").lower() == "true"
DOWNLOAD_BATCH = int(os.environ.get("DOWNLOAD_BATCH", "2000"))
DATASET_FOCUS = os.environ.get("DATASET_FOCUS", "").strip()
DATASET_FOCUS = int(DATASET_FOCUS) if DATASET_FOCUS.isdigit() else None


def run_daily_check():
    logger.info("=" * 60)
    logger.info(f"DAILY DOJ MONITOR — {datetime.now().isoformat()}")
    logger.info("=" * 60)
    logger.info(
        f"Config: SKIP_SCAN={SKIP_SCAN}, DOWNLOAD_ONLY={DOWNLOAD_ONLY}, "
        f"DOWNLOAD_BATCH={DOWNLOAD_BATCH}, DATASET_FOCUS={DATASET_FOCUS}"
    )

    init_db()

    new_found = 0
    urls_checked = 0
    datasets_done = 0

    if not SKIP_SCAN:
        logger.info("STEP 1: Scanning for new EFTA file URLs...")
        try:
            if DATASET_FOCUS is not None:
                logger.info(
                    "Dataset focus is enabled. Scanner may still scan broadly depending on "
                    "url_generator implementation, but downloader will only process the focus dataset."
                )
            new_found, urls_checked, datasets_done = scan_all_datasets(batch_per_dataset=100)
            logger.info(
                f"Scan: {new_found} new files found, {urls_checked} URLs checked, "
                f"{datasets_done}/12 datasets complete"
            )
        except Exception as e:
            logger.exception("URL scanning failed")
            insert_monitor_log("doj_efta", "error", f"Scan failed: {e}")
            return
  else:
        logger.info("Skipping URL scan — SKIP_SCAN is enabled.")

    logger.info("STEP 2: Downloading new files to DreamObjects...")
    try:
        process_downloads(batch=DOWNLOAD_BATCH, dataset_focus=DATASET_FOCUS)
    except Exception as e:
        logger.exception("Download step failed")
        insert_monitor_log("doj_efta", "error", f"Download failed: {e}")
        return

    if not DOWNLOAD_ONLY:
        logger.info("STEP 3: Extracting metadata...")
        try:
            process_unindexed_documents()
            process_images()
            process_videos()
        except Exception as e:
            logger.exception("Metadata extraction failed")
            insert_monitor_log("doj_efta", "error", f"Metadata failed: {e}")

    insert_monitor_log("doj_efta", "success", "Daily monitor complete")
    logger.info("Daily monitor complete.")


run_daily_check()
