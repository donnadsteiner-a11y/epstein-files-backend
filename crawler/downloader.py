"""
Downloader — Downloads scraped file URLs from the DOJ and uploads
them to DreamObjects (S3-compatible cloud storage).

Concurrency upgrades:
- Uses ThreadPoolExecutor with per-worker sessions
- Optional dataset focus filter (ex: DATASET_FOCUS=10)
- Small per-worker jitter instead of one global post-download sleep

Protections preserved:
- Detect DOJ age-verify redirects (/age-verify) and treat as NOT a PDF.
- Detect "block/WAF PDFs" and do NOT ingest.
- Only upload + insert documents when content is a real target PDF.
"""
import os
import sys
import time
import random
import hashlib
import logging
from datetime import datetime, timezone
from urllib.parse import urlparse
import re
from collections import defaultdict
import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
import boto3
from botocore.config import Config as BotoConfig
import psycopg

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.settings import (  # noqa: E402
    DATABASE_URL,
    S3_ENDPOINT, S3_ACCESS_KEY, S3_SECRET_KEY, S3_BUCKET, S3_PUBLIC_URL,
    TEMP_DOWNLOAD_DIR, REQUEST_DELAY, REQUEST_TIMEOUT, MAX_RETRIES,
    USER_AGENT, CHUNK_SIZE, LOG_DIR, MAX_CONCURRENT_DOWNLOADS,
)
from db.database import (  # noqa: E402
    init_db, get_undownloaded_urls, mark_url_scraped,
    insert_document, document_exists, update_scraped_dataset_id,
)
from crawler.doj_session import build_doj_session, PDF_MAGIC  # noqa: E402


MISSING_STREAK_TRIGGER = 25
NUMERIC_SUFFIX_DROP = 2
BLOCK_SIG_PHRASES = (
    "Product ID:",
    "Source IP:",
    "Source Country:",
    "Source Region:",
    "Type: PRTT/CCC/17/",
)
SNIFF_CHARS = 2500
DOJ_WARMUP_URL = "https://www.justice.gov/epstein/files/DataSet%2010/EFTA01602154.pdf"

# Optional env overrides
DOWNLOAD_WORKERS = max(1, int(os.environ.get("DOWNLOADER_WORKERS", str(MAX_CONCURRENT_DOWNLOADS or 3))))
DATASET_FOCUS = os.environ.get("DATASET_FOCUS", "").strip()
DATASET_FOCUS = int(DATASET_FOCUS) if DATASET_FOCUS.isdigit() else None
    print(f"Errors:       {errors}")
