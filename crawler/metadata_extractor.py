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
from db.database import init_db, get_db, update_document_text

os.makedirs(LOG_DIR, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(os.path.join(LOG_DIR, "extractor.log")),
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
    """Extract text from a PDF using PyMuPDF (fitz)."""
    try:
        import fitz  # PyMuPDF
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
            logger.error("No PDF library available. Install PyMuPDF or pdfplumber.")
            return ""
    except Exception as e:
        logger.error(f"PDF extraction error: {e}")
        return ""


def find_persons_in_text(text: str) -> list[str]:
    """Search document text for tracked persons of interest."""
    if not text:
        return []

    found = []
    text_lower = text.lower()

    for person in TRACKED_PERSONS:
        # Simple case-insensitive name search
        if person.lower() in text_lower:
            found.append(person)
            continue

        # Also check last name only (for partial mentions)
        parts = person.split()
        if len(parts) >= 2:
            last_name = parts[-1].lower()
            # Only match last name if it's reasonably unique (>4 chars)
            if len(last_name) > 4 and last_name in text_lower:
                # Verify it's likely a reference to this person
                # by checking for nearby first name or context
                first_name = parts[0].lower()
                if first_name in text_lower:
                    if person not in found:
                        found.append(person)

    return found


def extract_date_from_text(text: str) -> str | None:
    """Try to extract a date from document text."""
    if not text:
        return None

    # Common date patterns
    patterns = [
        r'(\d{1,2}/\d{1,2}/\d{4})',           # MM/DD/YYYY
        r'(\d{4}-\d{2}-\d{2})',                 # YYYY-MM-DD
        r'(\w+ \d{1,2},? \d{4})',               # Month DD, YYYY
        r'(\d{1,2} \w+ \d{4})',                 # DD Month YYYY
    ]

    for pattern in patterns:
        match = re.search(pattern, text[:5000])  # Only check first 5000 chars
        if match:
            return match.group(1)

    return None


def process_unindexed_documents(limit: int = 200):
    """
    Process all downloaded-but-not-yet-indexed documents:
    1. Download PDF from S3
    2. Extract text
    3. Find person mentions
    4. Update database
    """
    s3 = get_s3_client()

    with get_db() as conn:
        docs = conn.execute(
            "SELECT id, file_id, filename, file_type, s3_key FROM documents "
            "WHERE status = 'downloaded' AND file_type = 'pdf' LIMIT ?",
            (limit,)
        ).fetchall()

    logger.info(f"Found {len(docs)} unindexed PDFs to process")

    processed = 0
    errors = 0

    for doc in docs:
        doc_id = doc["id"]
        s3_key = doc["s3_key"]
        filename = doc["filename"]

        logger.info(f"Processing [{doc_id}]: {filename}")

        try:
            # Download from S3
            response = s3.get_object(Bucket=S3_BUCKET, Key=s3_key)
            pdf_bytes = response["Body"].read()

            # Extract text
            text = extract_text_from_pdf(pdf_bytes)
            if not text:
                logger.warning(f"  No text extracted from {filename}")
                # Still mark as indexed so we don't retry
                with get_db() as conn:
                    conn.execute(
                        "UPDATE documents SET status='indexed', extracted_text='[no text extracted]' WHERE id=?",
                        (doc_id,)
                    )
                continue

            # Find persons
            persons_found = find_persons_in_text(text)
            logger.info(f"  Extracted {len(text)} chars, found {len(persons_found)} persons: {persons_found}")

            # Try to find a date in the document
            doc_date = extract_date_from_text(text)
            if doc_date:
                with get_db() as conn:
                    conn.execute(
                        "UPDATE documents SET date_on_doc=? WHERE id=? AND date_on_doc IS NULL",
                        (doc_date, doc_id)
                    )

            # Update database with text and person tags
            # Truncate text to ~50K chars for storage efficiency
            update_document_text(doc_id, text[:50000], persons_found)
            processed += 1

        except Exception as e:
            logger.error(f"  Error processing {filename}: {e}")
            errors += 1

    logger.info(f"\nExtraction complete: {processed} processed, {errors} errors")
    return processed, errors


def process_images(limit: int = 200):
    """
    For image files (JPG, PNG, TIF), we can't extract text,
    but we mark them as indexed and could add EXIF metadata.
    """
    with get_db() as conn:
        docs = conn.execute(
            "SELECT id, filename, file_type FROM documents "
            "WHERE status = 'downloaded' AND file_type IN ('jpg','jpeg','png','tif','tiff') LIMIT ?",
            (limit,)
        ).fetchall()

    logger.info(f"Found {len(docs)} unindexed images to process")

    for doc in docs:
        with get_db() as conn:
            conn.execute(
                "UPDATE documents SET status='indexed', extracted_text='[image file]' WHERE id=?",
                (doc["id"],)
            )

    logger.info(f"Marked {len(docs)} images as indexed")


def process_videos(limit: int = 200):
    """Mark video files as indexed."""
    with get_db() as conn:
        docs = conn.execute(
            "SELECT id FROM documents "
            "WHERE status = 'downloaded' AND file_type IN ('mov','mp4') LIMIT ?",
            (limit,)
        ).fetchall()

    for doc in docs:
        with get_db() as conn:
            conn.execute(
                "UPDATE documents SET status='indexed', extracted_text='[video file]' WHERE id=?",
                (doc["id"],)
            )

    logger.info(f"Marked {len(docs)} videos as indexed")


if __name__ == "__main__":
    init_db()
    processed, errors = process_unindexed_documents()
    process_images()
    process_videos()

    with get_db() as conn:
        total_indexed = conn.execute(
            "SELECT COUNT(*) as c FROM documents WHERE status='indexed'"
        ).fetchone()["c"]

    print(f"\n{'='*50}")
    print(f"Extraction Summary")
    print(f"{'='*50}")
    print(f"PDFs processed:   {processed}")
    print(f"Errors:           {errors}")
    print(f"Total indexed:    {total_indexed}")
