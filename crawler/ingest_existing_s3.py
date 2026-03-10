"""
Ingest existing PDFs already stored in DreamObjects into the documents table.

Actual key format in this bucket:
  dataset_10/pdf/EFTA01262782.pdf

Creates/updates rows in public.documents without re-downloading files.
Skips non-PDF files like zip archives.

Optional env vars:
  DATASET_FOCUS=10
  INGEST_PREFIX=dataset_
  INGEST_LIMIT=0
"""

import os
import re
import sys
import logging
from datetime import datetime, timezone

import boto3
from botocore.config import Config as BotoConfig
import psycopg

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.settings import (  # noqa: E402
    DATABASE_URL,
    S3_ENDPOINT,
    S3_ACCESS_KEY,
    S3_SECRET_KEY,
    S3_BUCKET,
    S3_PUBLIC_URL,
)
from db.database import init_db  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("ingest_existing_s3")

INGEST_PREFIX = os.environ.get("INGEST_PREFIX", "dataset_")
DATASET_FOCUS_RAW = os.environ.get("DATASET_FOCUS", "").strip()
DATASET_FOCUS = int(DATASET_FOCUS_RAW) if DATASET_FOCUS_RAW.isdigit() else None
INGEST_LIMIT = int(os.environ.get("INGEST_LIMIT", "0"))

# Actual object key format:
# dataset_10/pdf/EFTA01262782.pdf
DATASET_KEY_RE = re.compile(r"^dataset_(\d+)/pdf/(.+)$", re.IGNORECASE)
PDF_NAME_RE = re.compile(r"^(EFTA|EFTR).+\.pdf$", re.IGNORECASE)


def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY,
        config=BotoConfig(signature_version="s3v4", retries={"max_attempts": 3}),
    )


def s3_public_url(key: str) -> str:
    return f"{S3_PUBLIC_URL}/{key}"


def title_from_filename(filename: str) -> str:
    stem = filename.rsplit(".", 1)[0]
    return stem.replace("_", " ").replace("-", " ")


def iter_s3_objects(s3, bucket: str, prefix: str):
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            yield obj


def upsert_document(conn, doc: dict) -> str:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT id FROM public.documents WHERE s3_key = %s LIMIT 1",
            (doc["s3_key"],),
        )
        row = cur.fetchone()
        if row:
            cur.execute(
                """
                UPDATE public.documents
                SET
                    file_id=%s,
                    filename=%s,
                    title=%s,
                    file_type=%s,
                    file_size=%s,
                    dataset_id=%s,
                    s3_url=%s,
                    source=%s,
                    status=%s,
                    updated_at=NOW()
                WHERE id=%s
                """,
                (
                    doc["file_id"],
                    doc["filename"],
                    doc["title"],
                    doc["file_type"],
                    doc["file_size"],
                    doc["dataset_id"],
                    doc["s3_url"],
                    doc["source"],
                    doc["status"],
                    row[0],
                ),
            )
            return "updated"

        cur.execute(
            """
            SELECT id
            FROM public.documents
            WHERE filename = %s AND dataset_id = %s
            ORDER BY id DESC
            LIMIT 1
            """,
            (doc["filename"], doc["dataset_id"]),
        )
        row = cur.fetchone()
        if row:
            cur.execute(
                """
                UPDATE public.documents
                SET
                    file_id=%s,
                    title=%s,
                    file_type=%s,
                    file_size=%s,
                    s3_key=%s,
                    s3_url=%s,
                    source=%s,
                    status=%s,
                    updated_at=NOW()
                WHERE id=%s
                """,
                (
                    doc["file_id"],
                    doc["title"],
                    doc["file_type"],
                    doc["file_size"],
                    doc["s3_key"],
                    doc["s3_url"],
                    doc["source"],
                    doc["status"],
                    row[0],
                ),
            )
            return "updated"

        cur.execute(
            """
            INSERT INTO public.documents
            (
                file_id, filename, title, file_type, file_size, dataset_id,
                source_url, s3_key, s3_url, sha256_hash, date_on_doc,
                date_downloaded, source, status, metadata_json
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                doc["file_id"],
                doc["filename"],
                doc["title"],
                doc["file_type"],
                doc["file_size"],
                doc["dataset_id"],
                doc["source_url"],
                doc["s3_key"],
                doc["s3_url"],
                doc["sha256_hash"],
                doc["date_on_doc"],
                doc["date_downloaded"],
                doc["source"],
                doc["status"],
                doc["metadata_json"],
            ),
        )
        return "inserted"


def main():
    init_db()
    s3 = get_s3_client()

    inserted = 0
    updated = 0
    skipped = 0
    scanned = 0

    logger.info(f"Bucket: {S3_BUCKET}")
    logger.info(f"Prefix: {INGEST_PREFIX}")
    if DATASET_FOCUS is not None:
        logger.info(f"Dataset focus: {DATASET_FOCUS}")
    if INGEST_LIMIT > 0:
        logger.info(f"Ingest limit: {INGEST_LIMIT}")

    with psycopg.connect(DATABASE_URL) as conn:
        conn.autocommit = False

        for obj in iter_s3_objects(s3, S3_BUCKET, INGEST_PREFIX):
            key = obj["Key"]
            size = int(obj.get("Size", 0))
            scanned += 1

            if key.endswith("/"):
                skipped += 1
                continue

            m = DATASET_KEY_RE.match(key)
            if not m:
                skipped += 1
                continue

            dataset_id = int(m.group(1))
            filename = os.path.basename(m.group(2))

            if DATASET_FOCUS is not None and dataset_id != DATASET_FOCUS:
                skipped += 1
                continue

            if not filename.lower().endswith(".pdf"):
                skipped += 1
                continue

            if not PDF_NAME_RE.match(filename):
                skipped += 1
                continue

            if size < 100:
                skipped += 1
                continue

            stem = filename.rsplit(".", 1)[0]
            file_id = f"DS{dataset_id:02d}-{stem}"

            doc = {
                "file_id": file_id,
                "filename": filename,
                "title": title_from_filename(filename),
                "file_type": "pdf",
                "file_size": size,
                "dataset_id": dataset_id,
                "source_url": None,
                "s3_key": key,
                "s3_url": s3_public_url(key),
                "sha256_hash": None,
                "date_on_doc": None,
                "date_downloaded": datetime.now(timezone.utc).date(),
                "source": "dreamobjects_import",
                "status": "downloaded",
                "metadata_json": "{}",
            }

            result = upsert_document(conn, doc)
            if result == "inserted":
                inserted += 1
            elif result == "updated":
                updated += 1
            else:
                skipped += 1

            if (inserted + updated) % 500 == 0 and (inserted + updated) > 0:
                conn.commit()
                logger.info(
                    f"Progress: scanned={scanned}, inserted={inserted}, "
                    f"updated={updated}, skipped={skipped}"
                )

            if INGEST_LIMIT > 0 and (inserted + updated) >= INGEST_LIMIT:
                break

        conn.commit()

    logger.info("Ingest complete")
    logger.info(
        f"Final: scanned={scanned}, inserted={inserted}, "
        f"updated={updated}, skipped={skipped}"
    )


if __name__ == "__main__":
    main()
