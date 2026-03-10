# DocketZero replacement files

## 1) `crawler/downloader.py`

```python
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
JITTER_MIN = float(os.environ.get("DOWNLOADER_JITTER_MIN", "0.05"))
JITTER_MAX = float(os.environ.get("DOWNLOADER_JITTER_MAX", "0.20"))

os.makedirs(LOG_DIR, exist_ok=True)
os.makedirs(TEMP_DOWNLOAD_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger("downloader")


def _set_age_cookie(s: requests.Session) -> None:
    try:
        s.cookies.set("justiceGovAgeVerified", "true", domain="www.justice.gov", path="/")
        s.cookies.set("justiceGovAgeVerified", "true", domain=".justice.gov", path="/")
    except Exception:
        pass


def _warmup_queueit(s: requests.Session) -> None:
    try:
        r = s.get(DOJ_WARMUP_URL, timeout=REQUEST_TIMEOUT, allow_redirects=True, stream=True)
        _ = r.raw.read(8) if hasattr(r, "raw") else None
        r.close()
    except Exception:
        pass


def new_session() -> requests.Session:
    s = build_doj_session()
    if USER_AGENT:
        s.headers.update({"User-Agent": USER_AGENT})
    _set_age_cookie(s)
    _warmup_queueit(s)
    return s


_DOJ_DATASET_RE = re.compile(
    r"^https://www\.justice\.gov/epstein/files/DataSet%20(\d+)/((?:EFTA|EFTR)[^/]+\.pdf)$"
)
_DOJ_BARE_RE = re.compile(
    r"^https://www\.justice\.gov/epstein/files/((?:EFTA|EFTR)[^/]+\.pdf)$"
)


def normalize_doj_url(url: str, dataset_id: int | None = None) -> str:
    if "/DataSet%20" in url:
        return url
    m = _DOJ_BARE_RE.match(url)
    if m and dataset_id:
        fn = m.group(1)
        return f"https://www.justice.gov/epstein/files/DataSet%20{int(dataset_id)}/{fn}"
    return url


def _is_age_verify(resp: requests.Response) -> bool:
    if resp.url and "/age-verify" in resp.url:
        return True
    ct = (resp.headers.get("Content-Type") or "").lower()
    return "text/html" in ct


def _probe_pdf_head_with_session(s: requests.Session, url: str) -> bool:
    try:
        r = s.get(url, stream=True, timeout=REQUEST_TIMEOUT, allow_redirects=True)
        if _is_age_verify(r):
            r.close()
            return False
        ct = (r.headers.get("Content-Type") or "").lower()
        if r.status_code != 200 or "application/pdf" not in ct:
            r.close()
            return False
        magic = r.raw.read(len(PDF_MAGIC))
        r.close()
        return magic == PDF_MAGIC
    except requests.RequestException:
        return False


def relocate_dataset_pdf(s: requests.Session, url: str, max_offset: int = 6, max_dataset: int = 99) -> str | None:
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
        if _probe_pdf_head_with_session(s, test_url):
            return test_url
    return None


def locate_dataset_for_bare_url(s: requests.Session, url: str, max_dataset: int = 99) -> str | None:
    m = _DOJ_BARE_RE.match(url)
    if not m:
        return None

    fn = m.group(1)
    search_range = [DATASET_FOCUS] if DATASET_FOCUS else range(1, max_dataset + 1)
    for d in search_range:
        test_url = f"https://www.justice.gov/epstein/files/DataSet%20{d}/{fn}"
        if _probe_pdf_head_with_session(s, test_url):
            return test_url
    return None


def _dataset_and_filename(url: str) -> tuple[int | None, str | None]:
    m = _DOJ_DATASET_RE.match(url)
    if not m:
        return None, None
    return int(m.group(1)), m.group(2)


def _prefix_for_filename(filename: str) -> str:
    base = os.path.basename(filename)
    stem = base[:-4] if base.lower().endswith(".pdf") else base
    if len(stem) <= NUMERIC_SUFFIX_DROP:
        return stem
    return stem[:-NUMERIC_SUFFIX_DROP]


def reconcile_scraped_url(original_url: str, final_url: str, dataset_id: int | None, file_type: str | None) -> str:
    if final_url == original_url:
        return original_url

    try:
        with psycopg.connect(DATABASE_URL) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1 FROM public.scraped_urls WHERE url=%s LIMIT 1", (final_url,))
                exists = cur.fetchone() is not None

                if not exists:
                    cur.execute(
                        """
                        UPDATE public.scraped_urls
                        SET url=%s,
                            dataset_id=COALESCE(%s, dataset_id),
                            file_type=COALESCE(%s, file_type),
                            last_checked=NOW(),
                            last_error=COALESCE(last_error,'') || %s
                        WHERE url=%s
                        """,
                        (
                            final_url,
                            dataset_id,
                            file_type,
                            f"|relocated_from:{original_url}",
                            original_url,
                        ),
                    )
                    conn.commit()
                    if (cur.rowcount or 0) > 0:
                        return final_url

                cur.execute(
                    """
                    UPDATE public.scraped_urls
                    SET last_checked=NOW(),
                        last_error=COALESCE(last_error,'') || %s
                    WHERE url=%s
                    """,
                    (f"|resolved_to:{final_url}", original_url),
                )
                conn.commit()
    except Exception as e:
        logger.warning(f"reconcile_scraped_url failed: {type(e).__name__}: {e}")

    return original_url


def upsert_document_by_filename(doc: dict) -> None:
    try:
        with psycopg.connect(DATABASE_URL) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT id
                    FROM public.documents
                    WHERE filename=%s
                    ORDER BY updated_at DESC NULLS LAST, id DESC
                    LIMIT 1
                    """,
                    (doc["filename"],),
                )
                row = cur.fetchone()
                if row:
                    doc_id = row[0]
                    cur.execute(
                        """
                        UPDATE public.documents
                        SET
                            file_id=%s,
                            title=%s,
                            file_type=%s,
                            file_size=%s,
                            dataset_id=%s,
                            source_url=%s,
                            s3_key=%s,
                            s3_url=%s,
                            sha256_hash=%s,
                            date_downloaded=%s,
                            source=%s,
                            status=%s,
                            extracted_text=NULL,
                            updated_at=NOW()
                        WHERE id=%s
                        """,
                        (
                            doc.get("file_id"),
                            doc.get("title"),
                            doc.get("file_type"),
                            doc.get("file_size"),
                            doc.get("dataset_id"),
                            doc.get("source_url"),
                            doc.get("s3_key"),
                            doc.get("s3_url"),
                            doc.get("sha256_hash"),
                            doc.get("date_downloaded"),
                            doc.get("source"),
                            doc.get("status"),
                            doc_id,
                        ),
                    )
                    conn.commit()
                    return
    except Exception as e:
        logger.warning(f"upsert_document_by_filename UPDATE failed, fallback to insert_document(): {type(e).__name__}: {e}")

    insert_document(doc)


def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY,
        config=BotoConfig(signature_version="s3v4", retries={"max_attempts": 3}),
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


def upload_to_s3(s3, local_path: str, s3_key: str, content_type: str | None = None) -> str:
    extra_args = {}
    if content_type:
        extra_args["ContentType"] = content_type
    s3.upload_file(local_path, S3_BUCKET, s3_key, ExtraArgs=extra_args)
    return f"{S3_PUBLIC_URL}/{s3_key}"


def bulk_mark_missing_by_prefix(dataset_id: int, prefix: str, reason: str) -> int:
    pattern = f"https://www.justice.gov/epstein/files/DataSet%20{int(dataset_id)}/{prefix}%.pdf"
    with psycopg.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE public.scraped_urls
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


def _sniff_first_page_text_is_block_pdf(pdf_path: str) -> bool:
    try:
        proc = subprocess.run(
            ["pdftotext", "-f", "1", "-l", "1", "-layout", pdf_path, "-"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
        )
        txt = (proc.stdout or b"").decode("utf-8", errors="replace")
        head = txt[:SNIFF_CHARS]
        hits = sum(1 for p in BLOCK_SIG_PHRASES if p in head)
        return hits >= 2
    except Exception:
        return False


def download_file(s: requests.Session, url: str, dest_path: str) -> tuple[str, int, str, str, str]:
    final_url = url

    for attempt in range(MAX_RETRIES):
        try:
            resp = s.get(final_url, stream=True, timeout=REQUEST_TIMEOUT, allow_redirects=True)

            if _is_age_verify(resp):
                note = f"age_verify:{resp.url}"
                resp.close()
                logger.warning("Age-verify gate hit. Rebuilding worker session + warmup.")
                s = new_session()
                time.sleep(REQUEST_DELAY)
                if attempt == 0:
                    continue
                return "age_verify", 0, "", final_url, note

            if resp.status_code == 404:
                resp.close()

                relocated = relocate_dataset_pdf(s, final_url)
                if relocated:
                    final_url = relocated
                    continue

                bare_loc = locate_dataset_for_bare_url(s, final_url)
                if bare_loc:
                    final_url = bare_loc
                    continue

                return "missing", 0, "", final_url, "404_not_found"

            ct = (resp.headers.get("Content-Type") or "").lower()
            if "application/pdf" not in ct:
                note = f"non_pdf_ct:{ct} status={resp.status_code}"
                resp.close()

                relocated = relocate_dataset_pdf(s, final_url)
                if relocated and relocated != final_url:
                    final_url = relocated
                    continue

                raise requests.RequestException(note)

            resp.raise_for_status()

            sha256 = hashlib.sha256()
            total_size = 0

            magic = resp.raw.read(len(PDF_MAGIC))
            if magic != PDF_MAGIC:
                note = f"not_pdf_magic:{magic!r} status={resp.status_code} ct={ct}"
                resp.close()

                relocated = relocate_dataset_pdf(s, final_url)
                if relocated and relocated != final_url:
                    final_url = relocated
                    continue

                raise requests.RequestException(note)

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

            if _sniff_first_page_text_is_block_pdf(dest_path):
                return "blocked_pdf", total_size, sha256.hexdigest(), final_url, "block_pdf_signature"

            return "ok", total_size, sha256.hexdigest(), final_url, "ok"

        except requests.RequestException as e:
            logger.warning(f"Download attempt {attempt+1} failed for {final_url}: {e}")
            time.sleep(max(0.1, REQUEST_DELAY * (attempt + 1) * 0.25))
            if os.path.exists(dest_path):
                try:
                    os.remove(dest_path)
                except Exception:
                    pass

    return "error", 0, "", final_url, "max_retries_exceeded"


def _process_one(entry: dict) -> dict:
    s3 = get_s3_client()
    session = new_session()

    original_url = entry["url"]
    dataset_id = entry.get("dataset_id")
    file_type = entry.get("file_type", "pdf")

    if DATASET_FOCUS is not None and dataset_id != DATASET_FOCUS:
        return {"result": "ignored", "reason": f"dataset_focus_{DATASET_FOCUS}"}

    url = normalize_doj_url(original_url, dataset_id)
    parsed = urlparse(url)
    filename = os.path.basename(parsed.path) or f"file_{entry.get('id', 'x')}.{file_type}"
    filename = "".join(c for c in filename if c.isalnum() or c in ".-_ ").strip() or f"file_{entry.get('id', 'x')}.{file_type}"
    temp_path = os.path.join(TEMP_DOWNLOAD_DIR, f"{entry.get('id', 'x')}_{filename}")

    try:
        result, file_size, sha256, final_url, note = download_file(session, url, temp_path)

        if result in ("ok", "blocked_pdf"):
            m = _DOJ_DATASET_RE.match(final_url)
            if m:
                final_ds = int(m.group(1))
                if dataset_id != final_ds:
                    dataset_id = final_ds
                    try:
                        update_scraped_dataset_id(original_url, final_ds)
                    except Exception as e:
                        logger.warning(f"Could not update scraped_urls.dataset_id for {original_url}: {e}")

        url_for_mark = reconcile_scraped_url(original_url, final_url, dataset_id, file_type)

        if result == "missing":
            mark_url_scraped(url_for_mark, dataset_id=dataset_id, file_type=file_type, downloaded=True, status="missing", last_error=note)
            return {"result": "missing", "dataset_id": dataset_id, "final_url": final_url}

        if result == "age_verify":
            mark_url_scraped(url_for_mark, dataset_id=dataset_id, file_type=file_type, downloaded=False, status="age_verify", last_error=note)
            return {"result": "age_verify", "dataset_id": dataset_id}

        if result == "blocked_pdf":
            mark_url_scraped(url_for_mark, dataset_id=dataset_id, file_type=file_type, downloaded=False, status="blocked_pdf", last_error=note)
            return {"result": "blocked_pdf", "dataset_id": dataset_id}

        if result != "ok":
            mark_url_scraped(url_for_mark, dataset_id=dataset_id, file_type=file_type, downloaded=False, status="error", last_error=f"download_error:{note}")
            return {"result": "error", "dataset_id": dataset_id, "error": note}

        dupe = document_exists(sha256_hash=sha256)
        ds_prefix = f"dataset_{dataset_id:02d}" if dataset_id else "other"
        s3_key = f"{ds_prefix}/{file_type}/{filename}"

        try:
            s3_url = upload_to_s3(s3, temp_path, s3_key, get_content_type(file_type))
        except Exception as e:
            mark_url_scraped(url_for_mark, dataset_id=dataset_id, file_type=file_type, downloaded=False, status="error", last_error=f"s3_upload_failed:{type(e).__name__}")
            return {"result": "error", "dataset_id": dataset_id, "error": str(e)}

        file_id = f"DS{dataset_id or 0}-{sha256[:8]}"
        doc = {
            "file_id": file_id,
            "filename": filename,
            "title": filename.rsplit(".", 1)[0].replace("_", " ").replace("-", " "),
            "file_type": file_type,
            "file_size": file_size,
            "dataset_id": dataset_id,
            "source_url": final_url,
            "s3_key": s3_key,
            "s3_url": s3_url,
            "sha256_hash": sha256,
            "date_downloaded": datetime.now(timezone.utc).date(),
            "source": "doj_efta",
            "status": "downloaded",
        }
        upsert_document_by_filename(doc)

        mark_url_scraped(url_for_mark, dataset_id=dataset_id, file_type=file_type, downloaded=True, status="downloaded", last_error=None)

        time.sleep(random.uniform(JITTER_MIN, JITTER_MAX))
        return {"result": "downloaded", "dataset_id": dataset_id, "dupe": dupe}

    finally:
        if os.path.exists(temp_path):
            try:
                os.remove(temp_path)
            except Exception:
                pass


def process_downloads(limit: int = 500):
    s3 = get_s3_client()
    ensure_bucket_exists(s3)

    pending = get_undownloaded_urls(limit=limit)
    if DATASET_FOCUS is not None:
        pending = [p for p in pending if p.get("dataset_id") == DATASET_FOCUS]
        logger.info(f"Dataset focus enabled: DataSet {DATASET_FOCUS}")

    logger.info(f"Found {len(pending)} files to download")
    if not pending:
        return 0, 0, 0, 0, 0, 0

    downloaded = 0
    missing = 0
    blocked = 0
    age_gated = 0
    skipped_dupe = 0
    errors = 0

    missing_counts: dict[tuple[int, str], int] = defaultdict(int)
    bulk_blocked: set[tuple[int, str]] = set()

    with ThreadPoolExecutor(max_workers=DOWNLOAD_WORKERS) as executor:
        futures = [executor.submit(_process_one, entry) for entry in pending]

        for future in as_completed(futures):
            try:
                outcome = future.result()
            except Exception as e:
                logger.exception(f"Worker failed: {e}")
                errors += 1
                continue

            result = outcome.get("result")

            if result == "downloaded":
                downloaded += 1
                if outcome.get("dupe"):
                    skipped_dupe += 1
            elif result == "missing":
                missing += 1
                final_url = outcome.get("final_url") or ""
                ds_f, fn_f = _dataset_and_filename(final_url)
                if ds_f and fn_f:
                    prefix = _prefix_for_filename(fn_f)
                    key = (ds_f, prefix)
                    missing_counts[key] += 1
                    if missing_counts[key] >= MISSING_STREAK_TRIGGER and key not in bulk_blocked:
                        reason = f"bulk_missing_streak:{ds_f}:{prefix}"
                        try:
                            updated = bulk_mark_missing_by_prefix(ds_f, prefix, reason)
                            bulk_blocked.add(key)
                            logger.warning(
                                f"BULK-MISSING triggered for DataSet {ds_f} prefix {prefix}% "
                                f"(count={missing_counts[key]}) updated_rows={updated}"
                            )
                        except Exception as e:
                            logger.warning(f"BULK-MISSING failed for {key}: {e}")
            elif result == "blocked_pdf":
                blocked += 1
            elif result == "age_verify":
                age_gated += 1
            elif result == "ignored":
                continue
            else:
                errors += 1

    logger.info(
        "Download complete: "
        f"{downloaded} downloaded, "
        f"{missing} missing(404), "
        f"{blocked} blocked_pdf, "
        f"{age_gated} age_verify, "
        f"{skipped_dupe} dupes, "
        f"{errors} errors"
    )
    return downloaded, missing, blocked, age_gated, skipped_dupe, errors


if __name__ == "__main__":
    init_db()
    downloaded, missing, blocked, age_gated, dupes, errors = process_downloads()
    print(f"\n{'='*50}")
    print("Download Summary")
    print(f"{'='*50}")
    print(f"Downloaded:   {downloaded}")
    print(f"Missing:      {missing} (404)")
    print(f"Blocked PDFs: {blocked}")
    print(f"Age Verify:   {age_gated}")
    print(f"Duplicates:   {dupes} (sha256)")
    print(f"Errors:       {errors}")
```

## 2) `crawler/daily_monitor.py`

```python
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
        logger.info("STEP 1: Scan skipped by SKIP_SCAN=true")

    with get_db() as conn:
        if DATASET_FOCUS is not None:
            pending = query_val(
                conn,
                "SELECT COUNT(*) FROM scraped_urls WHERE status='pending' AND dataset_id=%s",
                (DATASET_FOCUS,),
            )
        else:
            pending = query_val(conn, "SELECT COUNT(*) FROM scraped_urls WHERE status='pending'")

    logger.info(f"Pending downloads: {pending}")

    downloaded = 0
    missing = 0
    blocked = 0
    age_gated = 0
    dupes = 0
    errors = 0

    if pending and pending > 0:
        logger.info("STEP 2: Downloading new files...")
        try:
            downloaded, missing, blocked, age_gated, dupes, errors = process_downloads(limit=DOWNLOAD_BATCH)
            logger.info(
                f"Downloaded: {downloaded}, Missing: {missing}, Blocked: {blocked}, "
                f"Age-gated: {age_gated}, Dupes: {dupes}, Errors: {errors}"
            )
        except Exception as e:
            logger.exception("Download failed")
            insert_monitor_log("doj_efta", "error", f"Download failed: {e}")
            return
    else:
        logger.info("STEP 2: No pending files to download")

    processed = 0
    extract_errors = 0
    if not DOWNLOAD_ONLY:
        logger.info("STEP 3: Extracting metadata from new files...")
        try:
            processed, extract_errors = process_unindexed_documents(limit=200)
            process_images(limit=200)
            process_videos(limit=200)
            logger.info(f"Indexed: {processed}, Extract errors: {extract_errors}")
        except Exception as e:
            logger.exception("Extraction failed")
            insert_monitor_log("doj_efta", "error", f"Extraction failed: {e}")
            return
    else:
        logger.info("STEP 3: Extraction skipped by DOWNLOAD_ONLY=true")

    if downloaded > 0:
        status = "major" if downloaded > 50 else "update"
        result = f"{downloaded} new files downloaded"
        if not DOWNLOAD_ONLY:
            result += f", {processed} indexed"
    elif new_found > 0:
        status = "update"
        result = f"{new_found} new URLs found, {downloaded} downloaded"
    else:
        status = "checked"
        result = f"Scan: {urls_checked} checked, {new_found} new. {datasets_done}/12 datasets scanned."

    scan_summary = []
    total_files_found = 0
    try:
        scan_summary = get_scan_summary()
        total_files_found = sum(s.get("files_found", 0) for s in scan_summary)
    except Exception:
        pass

    insert_monitor_log(
        source="doj_efta",
        status=status,
        result=result,
        new_files=downloaded,
        details={
            "dataset_focus": DATASET_FOCUS,
            "new_urls_found": new_found,
            "urls_checked": urls_checked,
            "datasets_complete": datasets_done,
            "total_files_discovered": total_files_found,
            "downloaded": downloaded,
            "missing": missing,
            "blocked": blocked,
            "age_gated": age_gated,
            "duplicates": dupes,
            "indexed": processed,
            "extract_errors": extract_errors,
            "errors": errors,
            "skip_scan": SKIP_SCAN,
            "download_only": DOWNLOAD_ONLY,
            "download_batch": DOWNLOAD_BATCH,
        },
    )

    logger.info(f"Daily monitor complete: {result}")
    logger.info(f"Total EFTA files discovered so far: {total_files_found}")
    logger.info("=" * 60)


if __name__ == "__main__":
    run_daily_check()
```

## Render env vars to add now

```bash
DATASET_FOCUS=10
SKIP_SCAN=true
DOWNLOAD_ONLY=true
DOWNLOADER_WORKERS=8
DOWNLOAD_BATCH=5000
DOWNLOADER_JITTER_MIN=0.05
DOWNLOADER_JITTER_MAX=0.20
```
