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
import re

import requests
import boto3
from botocore.config import Config as BotoConfig

import psycopg

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.settings import (  # noqa: E402
    DATABASE_URL,
    S3_ENDPOINT, S3_ACCESS_KEY, S3_SECRET_KEY, S3_BUCKET, S3_PUBLIC_URL,
    TEMP_DOWNLOAD_DIR, REQUEST_DELAY, REQUEST_TIMEOUT, MAX_RETRIES,
    USER_AGENT, CHUNK_SIZE, LOG_DIR
)
from db.database import (  # noqa: E402
    init_db, get_undownloaded_urls, mark_url_scraped,
    insert_document, document_exists, update_scraped_dataset_id
)

# DOJ session helpers (age gate + QueueIT priming + PDF checks)
from crawler.doj_session import build_doj_session, PDF_MAGIC  # noqa: E402


# =============================
# Tunables
# =============================

# How many sequential "true missing" (after relocation attempts) we tolerate
# for the same (dataset + prefix) before bulk-marking the rest.
# Keep this conservative to avoid false negatives.
MISSING_STREAK_TRIGGER = 25

# Prefix definition: drop last N digits from the numeric tail (usually 2 works well)
# E.g. EFTA00800101 -> prefix EFTA008001
NUMERIC_SUFFIX_DROP = 2


os.makedirs(LOG_DIR, exist_ok=True)
os.makedirs(TEMP_DOWNLOAD_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger("downloader")

# Use DOJ-hardened session (browser-like headers + cookies priming)
session = build_doj_session()
session.headers.update({"User-Agent": USER_AGENT})


# -----------------------------
# DOJ URL normalization + relocation
# -----------------------------

_DOJ_DATASET_RE = re.compile(
    r"^https://www\.justice\.gov/epstein/files/DataSet%20(\d+)/((?:EFTA|EFTR)[^/]+\.pdf)$"
)
_DOJ_BARE_RE = re.compile(
    r"^https://www\.justice\.gov/epstein/files/((?:EFTA|EFTR)[^/]+\.pdf)$"
)


def normalize_doj_url(url: str, dataset_id: int | None = None) -> str:
    """
    If URL is a bare DOJ epstein file URL (no DataSet path) and we have dataset_id,
    rewrite to DataSet%20{dataset_id}/filename.pdf.

    If dataset_id is unknown, leave as-is (we may relocate by probing later).
    """
    if "/DataSet%20" in url:
        return url

    m = _DOJ_BARE_RE.match(url)
    if m and dataset_id:
        fn = m.group(1)
        return f"https://www.justice.gov/epstein/files/DataSet%20{int(dataset_id)}/{fn}"

    return url


def _probe_pdf_head(url: str) -> bool:
    """
    Lightweight check: request stream and verify:
      - HTTP 200
      - Content-Type includes application/pdf
      - First bytes match PDF magic
    """
    try:
        r = session.get(url, stream=True, timeout=REQUEST_TIMEOUT, allow_redirects=True)
        ct = (r.headers.get("Content-Type") or "").lower()

        if r.status_code != 200:
            r.close()
            return False

        if "application/pdf" not in ct:
            # Often DOJ gate pages come back as HTML
            r.close()
            return False

        magic = r.raw.read(len(PDF_MAGIC))
        r.close()
        return magic == PDF_MAGIC
    except requests.RequestException:
        return False


def relocate_dataset_pdf(url: str, max_offset: int = 6, max_dataset: int = 99) -> str | None:
    """
    If url is .../DataSet XX/EFTA....pdf and returns 404 (or bad content),
    try nearby datasets to find where the file actually lives.
    Returns the working URL or None.

    We probe: ds, ds-1, ds+1, ds-2, ds+2, ... up to max_offset.
    """
    m = _DOJ_DATASET_RE.match(url)
    if not m:
        return None

    ds = int(m.group(1))
    fn = m.group(2)

    candidates: list[int] = [ds]
    for k in range(1, max_offset + 1):
        for d in (ds - k, ds + k):
            if 1 <= d <= max_dataset:
                candidates.append(d)

    for d in candidates:
        test_url = f"https://www.justice.gov/epstein/files/DataSet%20{d}/{fn}"
        if _probe_pdf_head(test_url):
            return test_url

    return None


def locate_dataset_for_bare_url(url: str, max_dataset: int = 99) -> str | None:
    """
    If url is https://www.justice.gov/epstein/files/EFTAxxxx.pdf (no dataset),
    try datasets 1..max_dataset and return the first that serves a real PDF.
    """
    m = _DOJ_BARE_RE.match(url)
    if not m:
        return None

    fn = m.group(1)
    for d in range(1, max_dataset + 1):
        test_url = f"https://www.justice.gov/epstein/files/DataSet%20{d}/{fn}"
        if _probe_pdf_head(test_url):
            return test_url
    return None


def _dataset_and_filename(url: str) -> tuple[int | None, str | None]:
    m = _DOJ_DATASET_RE.match(url)
    if not m:
        return None, None
    return int(m.group(1)), m.group(2)


def _prefix_for_filename(filename: str) -> str:
    """
    Build a conservative prefix for bulk-missing.
    Example:
      EFTA00800101.pdf -> stem EFTA00800101 -> prefix EFTA008001 (drop last 2 digits)
    """
    stem = filename[:-4] if filename.lower().endswith(".pdf") else filename
    if len(stem) <= NUMERIC_SUFFIX_DROP:
        return stem
    return stem[:-NUMERIC_SUFFIX_DROP]


# -----------------------------
# S3 helpers
# -----------------------------

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


# -----------------------------
# Bulk-missing helper
# -----------------------------

def bulk_mark_missing_by_prefix(dataset_id: int, prefix: str, reason: str) -> int:
    """
    Mark ALL pending scraped_urls matching this dataset+prefix pattern as missing.
    This is the acceleration trick.
    """
    pattern = f"https://www.justice.gov/epstein/files/DataSet%20{dataset_id}/{prefix}%.pdf"
    with psycopg.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE scraped_urls
                SET status='missing',
                    downloaded=1,
                    last_checked=NOW(),
                    last_error=%s
                WHERE status='pending'
                  AND url LIKE %s
                """,
                (reason, pattern),
            )
            updated = cur.rowcount or 0
        conn.commit()
    return updated


# -----------------------------
# Download + verify
# -----------------------------

def download_file(url: str, dest_path: str) -> tuple[str, int, str, str]:
    """
    Download a DOJ file and verify it is a real PDF.

    Returns:
        ("ok", size, sha256, final_url)
        ("missing", 0, "", final_url)
        ("error", 0, "", final_url)
    """
    final_url = url

    for attempt in range(MAX_RETRIES):
        try:
            resp = session.get(
                final_url,
                stream=True,
                timeout=REQUEST_TIMEOUT,
                allow_redirects=True,
            )

            # Handle 404 with relocation if possible
            if resp.status_code == 404:
                resp.close()

                relocated = relocate_dataset_pdf(final_url)
                if relocated:
                    logger.info(f"  Relocated dataset URL: {final_url} -> {relocated}")
                    final_url = relocated
                    continue

                # If it was a bare URL, try locating dataset
                bare_loc = locate_dataset_for_bare_url(final_url)
                if bare_loc:
                    logger.info(f"  Located dataset for bare URL: {final_url} -> {bare_loc}")
                    final_url = bare_loc
                    continue

                return "missing", 0, "", final_url

            ct = (resp.headers.get("Content-Type") or "").lower()
            if "text/html" in ct:
                resp.close()
                relocated = relocate_dataset_pdf(final_url)
                if relocated and relocated != final_url:
                    logger.info(f"  HTML response; relocating: {final_url} -> {relocated}")
                    final_url = relocated
                    continue
                raise requests.RequestException(f"HTML response status={resp.status_code} ct={ct}")

            resp.raise_for_status()

            sha256 = hashlib.sha256()
            total_size = 0

            # Verify magic bytes first
            magic = resp.raw.read(len(PDF_MAGIC))
            if magic != PDF_MAGIC:
                resp.close()
                relocated = relocate_dataset_pdf(final_url)
                if relocated and relocated != final_url:
                    logger.info(f"  NOT_PDF; relocating: {final_url} -> {relocated}")
                    final_url = relocated
                    continue
                raise requests.RequestException(
                    f"NOT_PDF magic={magic!r} status={resp.status_code} ct={ct}"
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

            resp.close()
            return "ok", total_size, sha256.hexdigest(), final_url

        except requests.RequestException as e:
            logger.warning(f"Download attempt {attempt+1} failed for {final_url}: {e}")
            time.sleep(REQUEST_DELAY * (attempt + 1))

            if os.path.exists(dest_path):
                try:
                    os.remove(dest_path)
                except Exception:
                    pass

    return "error", 0, "", final_url


def upload_to_s3(s3, local_path: str, s3_key: str, content_type: str | None = None) -> str:
    extra_args = {}
    if content_type:
        extra_args["ContentType"] = content_type

    s3.upload_file(local_path, S3_BUCKET, s3_key, ExtraArgs=extra_args)
    return f"{S3_PUBLIC_URL}/{s3_key}"


# -----------------------------
# Main processing loop
# -----------------------------

def process_downloads(limit: int = 500):
    s3 = get_s3_client()
    ensure_bucket_exists(s3)

    pending = get_undownloaded_urls(limit=limit)
    logger.info(f"Found {len(pending)} files to download")

    downloaded = 0
    missing = 0
    skipped_dupe = 0
    errors = 0
    bulk_skipped = 0

    # 404 streak tracking
    streak_key: tuple[int, str] | None = None  # (dataset_id, prefix)
    streak_count = 0

    # if we bulk-mark (dataset,prefix), skip further urls matching it during this run
    bulk_blocked: set[tuple[int, str]] = set()

    for i, entry in enumerate(pending):
        original_url = entry["url"]
        dataset_id = entry.get("dataset_id")
        file_type = entry.get("file_type", "pdf")

        url = normalize_doj_url(original_url, dataset_id)

        # fast-skip if already bulk-blocked (avoid wasting requests in this same run)
        ds_u, fn_u = _dataset_and_filename(url)
        if ds_u and fn_u:
            pref_u = _prefix_for_filename(fn_u)
            if (ds_u, pref_u) in bulk_blocked:
                bulk_skipped += 1
                continue

        logger.info(f"[{i+1}/{len(pending)}] Downloading: {url}")

        parsed = urlparse(url)
        filename = os.path.basename(parsed.path) or f"file_{i}.{file_type}"
        filename = "".join(c for c in filename if c.isalnum() or c in ".-_ ").strip() or f"file_{i}.{file_type}"
        temp_path = os.path.join(TEMP_DOWNLOAD_DIR, filename)

        result, file_size, sha256, final_url = download_file(url, temp_path)

        # If we relocated to another dataset, trust final_url and fix dataset_id
        m = _DOJ_DATASET_RE.match(final_url)
        if m:
            final_ds = int(m.group(1))
            if dataset_id != final_ds:
                logger.info(f"  Dataset corrected: {dataset_id} -> {final_ds}")
                dataset_id = final_ds
                try:
                    update_scraped_dataset_id(original_url, final_ds)
                except Exception as e:
                    logger.warning(f"  Could not update scraped_urls.dataset_id for {original_url}: {e}")

        if result == "missing":
            logger.info(f"  NOT_FOUND (404): {final_url}")
            mark_url_scraped(original_url, downloaded=True, status="missing")
            missing += 1

            # streak logic uses final_url (post-relocation) so we only bulk-mark when
            # relocation ALSO failed repeatedly.
            ds_f, fn_f = _dataset_and_filename(final_url)
            if ds_f and fn_f:
                prefix = _prefix_for_filename(fn_f)
                key = (ds_f, prefix)

                if streak_key == key:
                    streak_count += 1
                else:
                    streak_key = key
                    streak_count = 1

                if streak_count >= MISSING_STREAK_TRIGGER and key not in bulk_blocked:
                    reason = f"bulk_missing_streak:{ds_f}:{prefix}"
                    try:
                        updated = bulk_mark_missing_by_prefix(ds_f, prefix, reason)
                        bulk_blocked.add(key)
                        logger.warning(
                            f"  BULK-MISSING triggered for DataSet {ds_f} prefix {prefix}% "
                            f"(streak={streak_count}) updated_rows={updated}"
                        )
                    except Exception as e:
                        logger.warning(f"  BULK-MISSING failed for {key}: {e}")
                    finally:
                        # reset streak after attempting bulk action
                        streak_key = None
                        streak_count = 0

            continue

        # reset streak on any non-missing outcome
        streak_key = None
        streak_count = 0

        if result != "ok":
            logger.error(f"  Failed to download: {final_url} (result={result})")
            errors += 1
            # optional: mark error
            try:
                mark_url_scraped(original_url, downloaded=False, status="error", last_error=f"download_error:{result}")
            except Exception:
                pass
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
            try:
                mark_url_scraped(original_url, downloaded=False, status="error", last_error=f"s3_upload_failed:{e}")
            except Exception:
                pass
            if os.path.exists(temp_path):
                try:
                    os.remove(temp_path)
                except Exception:
                    pass
            continue

        file_id = f"DS{dataset_id or 0}-{sha256[:8]}"

        insert_document({
            "file_id": file_id,
            "filename": filename,
            "title": filename.rsplit(".", 1)[0].replace("_", " ").replace("-", " "),
            "file_type": file_type,
            "file_size": file_size,
            "dataset_id": dataset_id,
            "source_url": final_url,  # store the URL that actually worked
            "s3_key": s3_key,
            "s3_url": s3_url,
            "sha256_hash": sha256,
            "date_downloaded": datetime.now(timezone.utc).isoformat(),
            "source": "doj_efta",
            "status": "downloaded",
        })

        mark_url_scraped(original_url, downloaded=True, status="downloaded")

        if os.path.exists(temp_path):
            try:
                os.remove(temp_path)
            except Exception:
                pass

        downloaded += 1
        time.sleep(REQUEST_DELAY)

    logger.info(
        f"\nDownload complete: {downloaded} downloaded, {missing} missing(404), "
        f"{skipped_dupe} dupes, {errors} errors, {bulk_skipped} bulk-skipped"
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
