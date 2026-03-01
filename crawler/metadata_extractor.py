"""
Metadata Extractor — Reads downloaded PDFs, extracts text content,
and auto-tags documents with persons of interest found in the text.
"""
import os
import sys
import re
import logging
from io import BytesIO

import boto3
from botocore.config import Config as BotoConfig

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.settings import (
    S3_ENDPOINT, S3_ACCESS_KEY, S3_SECRET_KEY, S3_BUCKET,
    TRACKED_PERSONS, LOG_DIR
)
from db.database import init_db, get_db, update_document_text, query_rows, query_val

os.makedirs(LOG_DIR, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.StreamHandler(),
    ]
)
logger = logging.getLogger("metadata_extractor")


def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY,
        config=BotoConfig(signature_version="s3v4"),
    )


def extract_text_from_pdf(pdf_bytes: bytes) -> str:
    try:
        import fitz
        doc = fitz.open(stream=pdf_bytes, filetype="pdf")
        text_parts = []
        for page_num in range(len(doc)):
            page = doc[page_num]
            text_parts.append(page.get_text())
        doc.close()
        return "\n".join(text_parts)
    except ImportError:
        logger.warning("PyMuPDF not installed. Trying pdfplumber...")
        try:
            import pdfplumber
            with pdfplumber.open(BytesIO(pdf_bytes)) as pdf:
                text_parts = []
                for page in pdf.pages:
                    text = page.extract_text()
                    if text:
                        text_parts.append(text)
            return "\n".join(text_parts)
        except ImportError:
            logger.error("No PDF library available.")
            return ""
    except Exception as e:
        logger.error(f"PDF extraction error: {e}")
        return ""


def find_persons_in_text(text: str) -> list[str]:
    if not text:
        return []
    found = []
    text_lower = text.lower()
    for person in TRACKED_PERSONS:
        if person.lower() in text_lower:
            found.append(person)
            continue
        parts = person.split()
        if len(parts) >= 2:
            last_name = parts[-1].lower()
            if len(last_name) > 4 and last_name in text_lower:
                first_name = parts[0].lower()
                if first_name in text_lower:
                    if person not in found:
                        found.append(person)
    return found


def extract_date_from_text(text: str) -> str | None:
    if not text:
        return None
    patterns = [
        r'(\d{1,2}/\d{1,2}/\d{4})',
        r'(\d{4}-\d{2}-\d{2})',
        r'(\w+ \d{1,2},? \d{4})',
        r'(\d{1,2} \w+ \d{4})',
    ]
    for pattern in patterns:
        match = re.search(pattern, text[:5000])
        if match:
            return match.group(1)
    return None


def process_unindexed_documents(limit: int = 200):
    s3 = get_s3_client()

    with get_db() as conn:
        docs = query_rows(conn,
            "SELECT id, file_id, filename, file_type, s3_key FROM documents "
            "WHERE status = 'downloaded' AND file_type = 'pdf' LIMIT %s",
            (limit,))

    logger.info(f"Found {len(docs)} unindexed PDFs to process")
    processed = 0
    errors = 0

    for doc in docs:
        doc_id = doc["id"]
        s3_key = doc["s3_key"]
        filename = doc["filename"]
        logger.info(f"Processing [{doc_id}]: {filename}")

        try:
            response = s3.get_object(Bucket=S3_BUCKET, Key=s3_key)
            pdf_bytes = response["Body"].read()
            text = extract_text_from_pdf(pdf_bytes)
            if not text:
                logger.warning(f"  No text extracted from {filename}")
                with get_db() as conn:
                    cur = conn.cursor()
                    cur.execute(
                        "UPDATE documents SET status='indexed', extracted_text='[no text extracted]' WHERE id=%s",
                        (doc_id,))
                    cur.close()
                continue

            persons_found = find_persons_in_text(text)
            logger.info(f"  Extracted {len(text)} chars, found {len(persons_found)} persons: {persons_found}")

            doc_date = extract_date_from_text(text)
            if doc_date:
                with get_db() as conn:
                    cur = conn.cursor()
                    cur.execute(
                        "UPDATE documents SET date_on_doc=%s WHERE id=%s AND date_on_doc IS NULL",
                        (doc_date, doc_id))
                    cur.close()

            update_document_text(doc_id, text[:50000], persons_found)
            processed += 1

        except Exception as e:
            logger.error(f"  Error processing {filename}: {e}")
            errors += 1

    logger.info(f"\nExtraction complete: {processed} processed, {errors} errors")
    return processed, errors


def process_images(limit: int = 200):
    with get_db() as conn:
        docs = query_rows(conn,
            "SELECT id, filename, file_type FROM documents "
            "WHERE status = 'downloaded' AND file_type IN ('jpg','jpeg','png','tif','tiff') LIMIT %s",
            (limit,))

    logger.info(f"Found {len(docs)} unindexed images to process")
    for doc in docs:
        with get_db() as conn:
            cur = conn.cursor()
            cur.execute(
                "UPDATE documents SET status='indexed', extracted_text='[image file]' WHERE id=%s",
                (doc["id"],))
            cur.close()
    logger.info(f"Marked {len(docs)} images as indexed")


def process_videos(limit: int = 200):
    with get_db() as conn:
        docs = query_rows(conn,
            "SELECT id FROM documents "
            "WHERE status = 'downloaded' AND file_type IN ('mov','mp4') LIMIT %s",
            (limit,))

    for doc in docs:
        with get_db() as conn:
            cur = conn.cursor()
            cur.execute(
                "UPDATE documents SET status='indexed', extracted_text='[video file]' WHERE id=%s",
                (doc["id"],))
            cur.close()
    logger.info(f"Marked {len(docs)} videos as indexed")


if __name__ == "__main__":
    init_db()
    processed, errors = process_unindexed_documents()
    process_images()
    process_videos()

    with get_db() as conn:
        total_indexed = query_val(conn, "SELECT COUNT(*) FROM documents WHERE status='indexed'")

    print(f"\n{'='*50}")
    print(f"Extraction Summary")
    print(f"{'='*50}")
    print(f"PDFs processed:   {processed}")
    print(f"Errors:           {errors}")
    print(f"Total indexed:    {total_indexed}")
