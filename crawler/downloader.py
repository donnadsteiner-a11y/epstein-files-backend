"""
Downloader — Downloads scraped file URLs from the DOJ and uploads
them to DreamObjects (S3-compatible cloud storage).

Protections added:
- Detect DOJ age-verify redirects (/age-verify) and treat as NOT a PDF.
- Detect "block/WAF PDFs" (real %PDF but contains "Product ID / Source IP ...") and do NOT ingest.
- Only upload + insert documents when content is a real target PDF.

Data integrity fixes:
- If DOJ relocation changes the URL, reconcile scraped_urls.url safely (avoid key collisions).
- Upsert documents by filename when re-downloading (avoid duplicates, keep continuity).
"""
import os
import sys
import time
import hashlib
import logging
from datetime import datetime, timezone
from urllib.parse import urlparse
import re
from collections import defaultdict
import subprocess

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
from crawler.doj_session import build_doj_session, PDF_MAGIC  # noqa: E402


# =============================
# Tunables
# =============================
MISSING_STREAK_TRIGGER = 25
NUMERIC_SUFFIX_DROP = 2

# Block-PDF signature (matches your junk extracted_text pattern)
BLOCK_SIG_PHRASES = (
    "Product ID:",
    "Source IP:",
    "Source Country:",
    "Source Region:",
    "Type: PRTT/CCC/17/",
)
SNIFF_CHARS = 2500  # how much to sniff from first page text

# Warmup URL: any stable PDF path (use one you know exists)
DOJ_WARMUP_URL = "https://www.justice.gov/epstein/files/DataSet%2010/EFTA01602154.pdf"


os.makedirs(LOG_DIR, exist_ok=True)
os.makedirs(TEMP_DOWNLOAD_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger("downloader")


# -----------------------------
# DOJ session helpers
# -----------------------------
def _set_age_cookie(s: requests.Session) -> None:
    """
    Force the age cookie that your shell test shows.
    Requests sometimes won’t persist it across certain redirects unless set explicitly.
    """
    try:
        # Try both host and dot-domain; harmless if redundant
        s.cookies.set("justiceGovAgeVerified", "true", domain="www.justice.gov", path="/")
        s.cookies.set("justiceGovAgeVerified", "true", domain=".justice.gov", path="/")
    except Exception:
        pass


def _warmup_queueit(s: requests.Session) -> None:
    """
    Warm up QueueIT cookie (QueueITAccepted-...).
    Your shell test showed DOJ responds with Set-Cookie for QueueITAccepted on a PDF GET.
    """
    try:
        r = s.get(DOJ_WARMUP_URL, timeout=REQUEST_TIMEOUT, allow_redirects=True, stream=True)
        # Drain a tiny bit if it’s a PDF, then close
        _ = r.raw.read(8) if hasattr(r, "raw") else None
        r.close()
    except Exception:
        pass


def new_session() -> requests.Session:
    """Create a DOJ-hardened session (cookies primed, browser-ish) + warmup."""
    s = build_doj_session()
    if USER_AGENT:
        s.headers.update({"User-Agent": USER_AGENT})
    _set_age_cookie(s)
    _warmup_queueit(s)
    return s


# Global session (recreated when age-gated)
session = new_session()


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
    if "/DataSet%20" in url:
        return url
    m = _DOJ_BARE_RE.match(url)
    if m and dataset_id:
        fn = m.group(1)
        return f"https://www.justice.gov/epstein/files/DataSet%20{int(dataset_id)}/{fn}"
    return url


def _is_age_verify(resp: requests.Response) -> bool:
    # DOJ redirects to /age-verify?destination=...
    if resp.url and "/age-verify" in resp.url:
        return True
    ct = (resp.headers.get("Content-Type") or "").lower()
    # Only treat HTML as a gate if it’s not a PDF
    if "text/html" in ct:
        return True
    return False


def _probe_pdf_head(url: str) -> bool:
    """
    Lightweight check:
      - 200
      - Content-Type includes application/pdf
      - PDF magic
      - not age-verify
    """
    try:
        r = session.get(url, stream=True, timeout=REQUEST_TIMEOUT, allow_redirects=True)
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


def relocate_dataset_pdf(url: str, max_offset: int = 6, max_dataset: int = 99) -> str | None:
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
    base = os.path.basename(filename)
    stem = base[:-4] if base.lower().endswith(".pdf") else base
    if len(stem) <= NUMERIC_SUFFIX_DROP:
        return stem
    return stem[:-NUMERIC_SUFFIX_DROP]


# -----------------------------
# DB helpers (safe URL reconcile + document upsert)
# -----------------------------
def reconcile_scraped_url(original_url: str, final_url: str, dataset_id: int | None, file_type: str | None) -> str:
    """
    Ensure mark_url_scraped() can find the right row even if final_url differs.

    Returns the URL that should be used as the key when calling mark_url_scraped():
      - final_url if we successfully updated scraped_urls.url to final_url
      - otherwise original_url
    """
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

                # collision path: keep original, but record resolved in last_error
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
        logger.warning(f"  reconcile_scraped_url failed: {type(e).__name__}: {e}")

    return original_url


def upsert_document_by_filename(doc: dict) -> None:
    """
    Prefer updating an existing documents row by filename (especially for needs_redownload),
    instead of creating duplicates.
    """
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
        logger.warning(f"  upsert_document_by_filename UPDATE failed, fallback to insert_document(): {type(e).__name__}: {e}")

    insert_document(doc)


# -----------------------------
# S3 helpers
# -----------------------------
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


# -----------------------------
# Bulk-missing helper
# -----------------------------
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


# -----------------------------
# Download + verify
# -----------------------------
def _sniff_first_page_text_is_block_pdf(pdf_path: str) -> bool:
    """
    Extract first page text and look for the block signature.
    Require >=2 phrase hits to reduce false positives.
    """
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


def download_file(url: str, dest_path: str) -> tuple[str, int, str, str, str]:
    """
    Returns:
      ("ok", size, sha256, final_url, note)
      ("missing", 0, "", final_url, note)
      ("age_verify", 0, "", final_url, note)
      ("blocked_pdf", size, sha256, final_url, note)
      ("error", 0, "", final_url, note)
    """
    global session
    final_url = url

    for attempt in range(MAX_RETRIES):
        try:
            resp = session.get(final_url, stream=True, timeout=REQUEST_TIMEOUT, allow_redirects=True)

            # age verify gate
            if _is_age_verify(resp):
                note = f"age_verify:{resp.url}"
                resp.close()

                # rebuild + warmup once, retry once, then stop burning retries
                logger.warning("Age-verify gate hit. Rebuilding session + warmup.")
                session = new_session()
                time.sleep(REQUEST_DELAY)

                if attempt == 0:
                    continue
                return "age_verify", 0, "", resp.url or final_url, note

            # 404 relocation logic
            if resp.status_code == 404:
                resp.close()

                relocated = relocate_dataset_pdf(final_url)
                if relocated:
                    logger.info(f"  Relocated dataset URL: {final_url} -> {relocated}")
                    final_url = relocated
                    continue

                bare_loc = locate_dataset_for_bare_url(final_url)
                if bare_loc:
                    logger.info(f"  Located dataset for bare URL: {final_url} -> {bare_loc}")
                    final_url = bare_loc
                    continue

                return "missing", 0, "", final_url, "404_not_found"

            ct = (resp.headers.get("Content-Type") or "").lower()
            if "application/pdf" not in ct:
                note = f"non_pdf_ct:{ct} status={resp.status_code}"
                resp.close()

                relocated = relocate_dataset_pdf(final_url)
                if relocated and relocated != final_url:
                    logger.info(f"  Non-PDF response; relocating: {final_url} -> {relocated}")
                    final_url = relocated
                    continue

                raise requests.RequestException(note)

            resp.raise_for_status()

            sha256 = hashlib.sha256()
            total_size = 0

            # verify PDF magic
            magic = resp.raw.read(len(PDF_MAGIC))
            if magic != PDF_MAGIC:
                note = f"not_pdf_magic:{magic!r} status={resp.status_code} ct={ct}"
                resp.close()

                relocated = relocate_dataset_pdf(final_url)
                if relocated and relocated != final_url:
                    logger.info(f"  NOT_PDF magic; relocating: {final_url} -> {relocated}")
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

            # detect block PDF
            if _sniff_first_page_text_is_block_pdf(dest_path):
                return "blocked_pdf", total_size, sha256.hexdigest(), final_url, "block_pdf_signature"

            return "ok", total_size, sha256.hexdigest(), final_url, "ok"

        except requests.RequestException as e:
            logger.warning(f"Download attempt {attempt+1} failed for {final_url}: {e}")
            time.sleep(REQUEST_DELAY * (attempt + 1))

            if os.path.exists(dest_path):
                try:
                    os.remove(dest_path)
                except Exception:
                    pass

    return "error", 0, "", final_url, "max_retries_exceeded"


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
    blocked = 0
    age_gated = 0
    skipped_dupe = 0
    errors = 0
    bulk_skipped = 0

    missing_counts: dict[tuple[int, str], int] = defaultdict(int)
    bulk_blocked: set[tuple[int, str]] = set()

    for i, entry in enumerate(pending):
        original_url = entry["url"]
        dataset_id = entry.get("dataset_id")
        file_type = entry.get("file_type", "pdf")

        url = normalize_doj_url(original_url, dataset_id)

        # fast-skip if already bulk-blocked within this run
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

        result, file_size, sha256, final_url, note = download_file(url, temp_path)

        # Only trust relocation dataset changes if we actually got a PDF file (ok or blocked_pdf).
        if result in ("ok", "blocked_pdf"):
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

        # Ensure we can update the right scraped_urls row even if URL changed
        url_for_mark = reconcile_scraped_url(original_url, final_url, dataset_id, file_type)

        if result == "missing":
            logger.info(f"  NOT_FOUND (404): {final_url}")
            mark_url_scraped(
                url_for_mark,
                dataset_id=dataset_id,
                file_type=file_type,
                downloaded=True,
                status="missing",
                last_error=note,
            )
            missing += 1

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
                            f"  BULK-MISSING triggered for DataSet {ds_f} prefix {prefix}% "
                            f"(count={missing_counts[key]}) updated_rows={updated}"
                        )
                    except Exception as e:
                        logger.warning(f"  BULK-MISSING failed for {key}: {e}")

            if os.path.exists(temp_path):
                try:
                    os.remove(temp_path)
                except Exception:
                    pass
            continue

        if result == "age_verify":
            logger.warning(f"  AGE_VERIFY gate: {final_url}")
            age_gated += 1
            mark_url_scraped(
                url_for_mark,
                dataset_id=dataset_id,
                file_type=file_type,
                downloaded=False,
                status="age_verify",
                last_error=note,
            )
            if os.path.exists(temp_path):
                try:
                    os.remove(temp_path)
                except Exception:
                    pass
            continue

        if result == "blocked_pdf":
            logger.warning(f"  BLOCKED_PDF detected (not ingesting): {final_url}")
            blocked += 1
            mark_url_scraped(
                url_for_mark,
                dataset_id=dataset_id,
                file_type=file_type,
                downloaded=False,
                status="blocked_pdf",
                last_error=note,
            )
            if os.path.exists(temp_path):
                try:
                    os.remove(temp_path)
                except Exception:
                    pass
            continue

        if result != "ok":
            logger.error(f"  Failed to download: {final_url} (result={result})")
            errors += 1
            mark_url_scraped(
                url_for_mark,
                dataset_id=dataset_id,
                file_type=file_type,
                downloaded=False,
                status="error",
                last_error=f"download_error:{note}",
            )
            if os.path.exists(temp_path):
                try:
                    os.remove(temp_path)
                except Exception:
                    pass
            continue

        # OK PDF only from here
        if document_exists(sha256_hash=sha256):
            skipped_dupe += 1
            logger.debug(f"  Duplicate SHA256: {filename}")

        ds_prefix = f"dataset_{dataset_id:02d}" if dataset_id else "other"
        s3_key = f"{ds_prefix}/{file_type}/{filename}"

        try:
            s3_url = upload_to_s3(s3, temp_path, s3_key, get_content_type(file_type))
            logger.info(f"  Uploaded to: {s3_key} ({file_size} bytes)")
        except Exception as e:
            logger.error(f"  S3 upload failed: {e}")
            errors += 1
            mark_url_scraped(
                url_for_mark,
                dataset_id=dataset_id,
                file_type=file_type,
                downloaded=False,
                status="error",
                last_error=f"s3_upload_failed:{type(e).__name__}",
            )
            if os.path.exists(temp_path):
                try:
                    os.remove(temp_path)
                except Exception:
                    pass
            continue

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
            "date_downloaded": datetime.now(timezone.utc).isoformat(),
            "source": "doj_efta",
            "status": "downloaded",
        }

        upsert_document_by_filename(doc)

        mark_url_scraped(
            url_for_mark,
            dataset_id=dataset_id,
            file_type=file_type,
            downloaded=True,
            status="downloaded",
            last_error=None,
        )

        if os.path.exists(temp_path):
            try:
                os.remove(temp_path)
            except Exception:
                pass

        downloaded += 1
        time.sleep(REQUEST_DELAY)

    logger.info(
        "\nDownload complete: "
        f"{downloaded} downloaded, "
        f"{missing} missing(404), "
        f"{blocked} blocked_pdf, "
        f"{age_gated} age_verify, "
        f"{skipped_dupe} dupes, "
        f"{errors} errors, "
        f"{bulk_skipped} bulk-skipped"
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
