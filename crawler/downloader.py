"""
Downloader — Downloads scraped file URLs from the DOJ and uploads
them to DreamObjects (S3-compatible cloud storage).
"""
import os
import sys
import time
import hashlib
import logging
from datetime import datetime, timezone
from urllib.parse import urlparse

import requests
import boto3
from botocore.config import Config as BotoConfig

import re

def normalize_doj_url(url: str) -> str:
    """
    DOJ sometimes lists files as /epstein/files/EFTAxxxxx.pdf
    but the real file lives under /epstein/files/DataSet 10/.
    """
    m = re.match(r"https://www\.justice\.gov/epstein/files/(EFTA[^/]+\.pdf)", url)
    if m:
        filename = m.group(1)
        return f"https://www.justice.gov/epstein/files/DataSet%2010/{filename}"
    return url

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.settings import (  # noqa: E402
    S3_ENDPOINT, S3_ACCESS_KEY, S3_SECRET_KEY, S3_BUCKET, S3_PUBLIC_URL,
    TEMP_DOWNLOAD_DIR, REQUEST_DELAY, REQUEST_TIMEOUT, MAX_RETRIES,
    USER_AGENT, CHUNK_SIZE, LOG_DIR
)
from db.database import (  # noqa: E402
    init_db, get_undownloaded_urls, mark_url_scraped,
    insert_document, document_exists
)

# DOJ session helpers (age gate + QueueIT priming + PDF checks)
from crawler.doj_session import build_doj_session, PDF_MAGIC  # noqa: E402

os.makedirs(LOG_DIR, exist_ok=True)
os.makedirs(TEMP_DOWNLOAD_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger("downloader")

# Use DOJ-hardened session (should include browser-like headers + cookies priming)
session = build_doj_session()
session.headers.update({"User-Agent": USER_AGENT})


def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY,
        config=BotoConfig(
            signature_version="s3v4",
            retries={"max_attempts": 3},
        ),
    )


def ensure_bucket_exists(s3):
    try:
        s3.head_bucket(Bucket=S3_BUCKET)
        logger.info(f"Bucket '{S3_BUCKET}' exists")
    except Exception:
        logger.info(f"Creating bucket '{S3_BUCKET}'")
        s3.create_bucket(Bucket=S3_BUCKET)


def get_content_type(file_type: str) -> str:
    types = {
        "pdf": "application/pdf",
        "jpg": "image/jpeg",
        "jpeg": "image/jpeg",
        "png": "image/png",
        "mov": "video/quicktime",
        "mp4": "video/mp4",
        "tif": "image/tiff",
        "tiff": "image/tiff",
    }
    return types.get(file_type, "application/octet-stream")

def download_file(url: str, dest_path: str) -> tuple[str, int, str]:
    """
    Download a DOJ file and verify it is a real PDF.

    Returns:
        ("ok", size, sha256)
        ("missing", 0, "")
        ("error", 0, "")
    """

    for attempt in range(MAX_RETRIES):
        try:
            resp = session.get(
                url,
                stream=True,
                timeout=REQUEST_TIMEOUT,
                allow_redirects=True,
            )

            if resp.status_code == 404:
                return "missing", 0, ""

            ct = (resp.headers.get("Content-Type") or "").lower()

            if "text/html" in ct:
                raise requests.RequestException(
                    f"HTML response status={resp.status_code} ct={ct}"
                )

            resp.raise_for_status()

            sha256 = hashlib.sha256()
            total_size = 0

            magic = resp.raw.read(len(PDF_MAGIC))

            if magic != PDF_MAGIC:
                raise requests.RequestException(
                    f"NOT_PDF magic={magic!r} status={resp.status_code}"
                )

            with open(dest_path, "wb") as f:
                f.write(magic)
                sha256.update(magic)
                total_size += len(magic)

                for chunk in resp.iter_content(chunk_size=CHUNK_SIZE):
                    if chunk:
                        f.write(chunk)
                        sha256.update(chunk)
                        total_size += len(chunk)

            return "ok", total_size, sha256.hexdigest()

        except requests.RequestException as e:
            logger.warning(f"Download attempt {attempt+1} failed for {url}: {e}")
            time.sleep(REQUEST_DELAY * (attempt + 1))

            if os.path.exists(dest_path):
                try:
                    os.remove(dest_path)
                except Exception:
                    pass



def upload_to_s3(s3, local_path: str, s3_key: str, content_type: str | None = None) -> str:
    extra_args = {}
    if content_type:
        extra_args["ContentType"] = content_type

    s3.upload_file(local_path, S3_BUCKET, s3_key, ExtraArgs=extra_args)
    return f"{S3_PUBLIC_URL}/{s3_key}"


def process_downloads(limit: int = 500):
    s3 = get_s3_client()
    ensure_bucket_exists(s3)

    pending = get_undownloaded_urls(limit=limit)
    logger.info(f"Found {len(pending)} files to download")

    downloaded = 0
    missing = 0
    skipped_dupe = 0
    errors = 0

    for i, entry in enumerate(pending):
    url = entry["url"]
    url = normalize_doj_url(url)
    dataset_id = entry.get("dataset_id")
    file_type = entry.get("file_type", "pdf")

        logger.info(f"[{i+1}/{len(pending)}] Downloading: {url}")

        parsed = urlparse(url)
        filename = os.path.basename(parsed.path) or f"file_{i}.{file_type}"
        filename = "".join(c for c in filename if c.isalnum() or c in ".-_ ").strip() or f"file_{i}.{file_type}"

        temp_path = os.path.join(TEMP_DOWNLOAD_DIR, filename)

        result, file_size, sha256 = download_file(url, temp_path)

        if result == "missing":
            logger.info(f"  NOT_FOUND (404): {url}")
            # IMPORTANT: don't keep retrying forever
            mark_url_scraped(url, downloaded=True)  # treat as terminal; adjust if you add a 'missing' status column
            missing += 1
            continue

        if result != "ok":
            logger.error(f"  Failed to download: {url} (result={result})")
            errors += 1
            continue

        if document_exists(sha256_hash=sha256):
            skipped_dupe += 1
            logger.debug(f"  Duplicate SHA256 (still storing unique filename): {filename}")

        ds_prefix = f"dataset_{dataset_id:02d}" if dataset_id else "other"
        s3_key = f"{ds_prefix}/{file_type}/{filename}"

        try:
            s3_url = upload_to_s3(s3, temp_path, s3_key, get_content_type(file_type))
            logger.info(f"  Uploaded to: {s3_key} ({file_size} bytes)")
        except Exception as e:
            logger.error(f"  S3 upload failed: {e}")
            errors += 1
            if os.path.exists(temp_path):
                os.remove(temp_path)
            continue

        file_id = f"DS{dataset_id or 0}-{sha256[:8]}"

        insert_document({
            "file_id": file_id,
            "filename": filename,
            "title": filename.rsplit(".", 1)[0].replace("_", " ").replace("-", " "),
            "file_type": file_type,
            "file_size": file_size,
            "dataset_id": dataset_id,
            "source_url": url,
            "s3_key": s3_key,
            "s3_url": s3_url,
            "sha256_hash": sha256,
            "date_downloaded": datetime.now(timezone.utc).isoformat(),
            "source": "doj_efta",
            "status": "downloaded",
        })

        mark_url_scraped(url, downloaded=True)

        if os.path.exists(temp_path):
            os.remove(temp_path)

        downloaded += 1
        time.sleep(REQUEST_DELAY)

    logger.info(
        f"\nDownload complete: {downloaded} downloaded, {missing} missing(404), "
        f"{skipped_dupe} dupes, {errors} errors"
    )
    return downloaded, missing, skipped_dupe, errors


if __name__ == "__main__":
    init_db()
    downloaded, missing, dupes, errors = process_downloads()

    print(f"\n{'='*50}")
    print("Download Summary")
    print(f"{'='*50}")
    print(f"Downloaded:  {downloaded}")
    print(f"Missing:     {missing} (404)")
    print(f"Duplicates:  {dupes} (sha256)")
    print(f"Errors:      {errors}")
