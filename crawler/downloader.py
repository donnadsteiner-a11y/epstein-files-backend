"""
Downloader — Downloads scraped file URLs from the DOJ and uploads
them to DreamObjects (S3-compatible cloud storage).
"""
import os
import sys
import time
import hashlib
import logging
from datetime import datetime
from urllib.parse import urlparse

import requests
import boto3
from botocore.config import Config as BotoConfig

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.settings import (
    S3_ENDPOINT, S3_ACCESS_KEY, S3_SECRET_KEY, S3_BUCKET, S3_PUBLIC_URL,
    TEMP_DOWNLOAD_DIR, REQUEST_DELAY, REQUEST_TIMEOUT, MAX_RETRIES,
    USER_AGENT, CHUNK_SIZE, LOG_DIR
)
from db.database import (
    init_db, get_undownloaded_urls, mark_url_scraped,
    insert_document, document_exists, get_db
)

os.makedirs(LOG_DIR, exist_ok=True)
os.makedirs(TEMP_DOWNLOAD_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(os.path.join(LOG_DIR, "downloader.log")),
    ]
)
logger = logging.getLogger("downloader")

session = requests.Session()
session.headers.update({"User-Agent": USER_AGENT})


def get_s3_client():
    """Create a boto3 S3 client configured for DreamObjects."""
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
    """Create the S3 bucket if it doesn't exist."""
    try:
        s3.head_bucket(Bucket=S3_BUCKET)
        logger.info(f"Bucket '{S3_BUCKET}' exists")
    except s3.exceptions.ClientError:
        logger.info(f"Creating bucket '{S3_BUCKET}'")
        s3.create_bucket(Bucket=S3_BUCKET)
        # Set public-read policy so frontend can access files
        policy = f'''{{
            "Version": "2012-10-17",
            "Statement": [{{
                "Sid": "PublicReadGetObject",
                "Effect": "Allow",
                "Principal": "*",
                "Action": "s3:GetObject",
                "Resource": "arn:aws:s3:::{S3_BUCKET}/*"
            }}]
        }}'''
        s3.put_bucket_policy(Bucket=S3_BUCKET, Policy=policy)


def download_file(url: str, dest_path: str) -> tuple[bool, int, str]:
    """
    Download a file from the given URL to dest_path.
    Returns (success, file_size_bytes, sha256_hash).
    """
    for attempt in range(MAX_RETRIES):
        try:
            resp = session.get(url, stream=True, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()

            sha256 = hashlib.sha256()
            total_size = 0

            with open(dest_path, "wb") as f:
                for chunk in resp.iter_content(chunk_size=CHUNK_SIZE):
                    if chunk:
                        f.write(chunk)
                        sha256.update(chunk)
                        total_size += len(chunk)

            return True, total_size, sha256.hexdigest()

        except requests.RequestException as e:
            logger.warning(f"Download attempt {attempt+1} failed for {url}: {e}")
            time.sleep(REQUEST_DELAY * (attempt + 1))
            if os.path.exists(dest_path):
                os.remove(dest_path)

    return False, 0, ""


def upload_to_s3(s3, local_path: str, s3_key: str, content_type: str = None) -> str:
    """Upload a file to DreamObjects. Returns the public URL."""
    extra_args = {"ACL": "public-read"}
    if content_type:
        extra_args["ContentType"] = content_type

    s3.upload_file(local_path, S3_BUCKET, s3_key, ExtraArgs=extra_args)
    return f"{S3_PUBLIC_URL}/{s3_key}"


def get_content_type(file_type: str) -> str:
    """Map file extension to MIME type."""
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


def process_downloads(limit: int = 500):
    """
    Main download pipeline:
    1. Get pending URLs from the database
    2. Download each file to temp storage
    3. Check for duplicates via SHA256
    4. Upload to DreamObjects
    5. Record in the documents table
    """
    s3 = get_s3_client()
    ensure_bucket_exists(s3)

    pending = get_undownloaded_urls(limit=limit)
    logger.info(f"Found {len(pending)} files to download")

    downloaded = 0
    skipped = 0
    errors = 0

    for i, entry in enumerate(pending):
        url = entry["url"]
        dataset_id = entry.get("dataset_id")
        file_type = entry.get("file_type", "pdf")

        logger.info(f"[{i+1}/{len(pending)}] Downloading: {url}")

        # Temp filename
        parsed = urlparse(url)
        filename = os.path.basename(parsed.path) or f"file_{i}.{file_type}"
        # Sanitize
        filename = "".join(c for c in filename if c.isalnum() or c in ".-_ ").strip()
        if not filename:
            filename = f"file_{i}.{file_type}"

        temp_path = os.path.join(TEMP_DOWNLOAD_DIR, filename)

        # Download
        success, file_size, sha256 = download_file(url, temp_path)
        if not success:
            logger.error(f"  Failed to download: {url}")
            errors += 1
            continue

        # Check for duplicate
        if document_exists(sha256_hash=sha256):
            logger.info(f"  Duplicate (SHA256 match), skipping: {filename}")
            mark_url_scraped(url, downloaded=True)
            os.remove(temp_path)
            skipped += 1
            continue

        # Build S3 key: dataset_XX/file_type/filename
        ds_prefix = f"dataset_{dataset_id:02d}" if dataset_id else "other"
        s3_key = f"{ds_prefix}/{file_type}/{filename}"

        # Upload to DreamObjects
        try:
            s3_url = upload_to_s3(s3, temp_path, s3_key, get_content_type(file_type))
            logger.info(f"  Uploaded to: {s3_key} ({file_size} bytes)")
        except Exception as e:
            logger.error(f"  S3 upload failed: {e}")
            errors += 1
            if os.path.exists(temp_path):
                os.remove(temp_path)
            continue

        # Generate a file_id
        file_id = f"DS{dataset_id or 0}-{sha256[:8]}"

        # Insert into database
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
            "date_downloaded": datetime.utcnow().isoformat(),
            "source": "doj_efta",
            "status": "downloaded",
        })

        # Mark URL as downloaded
        mark_url_scraped(url, downloaded=True)

        # Clean up temp file
        os.remove(temp_path)
        downloaded += 1

        # Polite delay
        time.sleep(REQUEST_DELAY)

    logger.info(f"\nDownload complete: {downloaded} downloaded, {skipped} skipped (dupes), {errors} errors")
    return downloaded, skipped, errors


if __name__ == "__main__":
    init_db()
    downloaded, skipped, errors = process_downloads()

    print(f"\n{'='*50}")
    print(f"Download Summary")
    print(f"{'='*50}")
    print(f"Downloaded:  {downloaded}")
    print(f"Skipped:     {skipped} (duplicates)")
    print(f"Errors:      {errors}")
    print(f"\nRun 'python -m crawler.metadata_extractor' to index file contents")
