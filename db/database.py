"""
PostgreSQL database layer for the Epstein Files Platform.
Uses Render PostgreSQL for persistent storage that survives restarts.
Uses psycopg 3 driver.
"""
import os
import sys
import json
import logging
from datetime import datetime
from contextlib import contextmanager

import psycopg
from psycopg.rows import dict_row

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.settings import DATABASE_URL  # noqa: E402

logger = logging.getLogger(__name__)


@contextmanager
def get_db():
    """Context manager for database connections."""
    conn = psycopg.connect(DATABASE_URL)
    conn.autocommit = False
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def query_rows(conn, sql, params=None):
    """Execute query and return list of dicts."""
    cur = conn.cursor(row_factory=dict_row)
    cur.execute(sql, params or ())
    rows = cur.fetchall()
    cur.close()
    return rows


def query_one(conn, sql, params=None):
    """Execute query and return single dict."""
    cur = conn.cursor(row_factory=dict_row)
    cur.execute(sql, params or ())
    row = cur.fetchone()
    cur.close()
    return row


def query_val(conn, sql, params=None):
    """Execute query and return single value."""
    cur = conn.cursor()
    cur.execute(sql, params or ())
    row = cur.fetchone()
    cur.close()
    return row[0] if row else None


def init_db():
    """Create all tables if they don't exist, and apply safe schema upgrades."""
    with get_db() as conn:
        cur = conn.cursor()

        # Base tables
        cur.execute("""
        CREATE TABLE IF NOT EXISTS documents (
            id              SERIAL PRIMARY KEY,
            file_id         TEXT UNIQUE NOT NULL,
            filename        TEXT NOT NULL,
            title           TEXT,
            file_type       TEXT NOT NULL,
            file_size       BIGINT DEFAULT 0,
            dataset_id      INTEGER,
            source_url      TEXT,
            s3_key          TEXT,
            s3_url          TEXT,
            sha256_hash     TEXT,
            date_on_doc     DATE,
            date_downloaded DATE NOT NULL,
            date_modified   DATE,
            source          TEXT DEFAULT 'doj_efta',
            status          TEXT DEFAULT 'downloaded',
            extracted_text  TEXT,
            metadata_json   TEXT,
            created_at      TIMESTAMP DEFAULT NOW(),
            updated_at      TIMESTAMP DEFAULT NOW()
        );

        -- Basic indexes
        CREATE INDEX IF NOT EXISTS idx_documents_file_type ON documents(file_type);
        CREATE INDEX IF NOT EXISTS idx_documents_dataset ON documents(dataset_id);
        CREATE INDEX IF NOT EXISTS idx_documents_date ON documents(date_on_doc);
        CREATE INDEX IF NOT EXISTS idx_documents_source ON documents(source);
        CREATE INDEX IF NOT EXISTS idx_documents_hash ON documents(sha256_hash);
        CREATE INDEX IF NOT EXISTS idx_documents_status ON documents(status);

        -- Composite indexes for scale (common filters + sorts)
        CREATE INDEX IF NOT EXISTS idx_documents_dataset_type_date
            ON documents(dataset_id, file_type, date_on_doc DESC NULLS LAST);
        CREATE INDEX IF NOT EXISTS idx_documents_source_date
            ON documents(source, date_on_doc DESC NULLS LAST);

        CREATE TABLE IF NOT EXISTS persons (
            id            SERIAL PRIMARY KEY,
            name          TEXT UNIQUE NOT NULL,
            role          TEXT,
            status        TEXT,
            category      TEXT,
            mentions      INTEGER DEFAULT 0,
            metadata_json TEXT,
            created_at    TIMESTAMP DEFAULT NOW(),
            updated_at    TIMESTAMP DEFAULT NOW()
        );
        CREATE INDEX IF NOT EXISTS idx_persons_category ON persons(category);
        CREATE INDEX IF NOT EXISTS idx_persons_mentions ON persons(mentions DESC);
        CREATE INDEX IF NOT EXISTS idx_persons_name ON persons(name);

        CREATE TABLE IF NOT EXISTS document_persons (
            id          SERIAL PRIMARY KEY,
            document_id INTEGER NOT NULL REFERENCES documents(id),
            person_id   INTEGER NOT NULL REFERENCES persons(id),
            confidence  REAL DEFAULT 1.0,
            context     TEXT,
            UNIQUE(document_id, person_id)
        );
        CREATE INDEX IF NOT EXISTS idx_docpersons_doc ON document_persons(document_id);
        CREATE INDEX IF NOT EXISTS idx_docpersons_person ON document_persons(person_id);

        CREATE TABLE IF NOT EXISTS timeline_events (
            id            SERIAL PRIMARY KEY,
            date          DATE NOT NULL,
            type          TEXT NOT NULL,
            title         TEXT NOT NULL,
            description   TEXT,
            location      TEXT,
            persons_json  TEXT,
            source_doc_id INTEGER REFERENCES documents(id),
            source        TEXT,
            created_at    TIMESTAMP DEFAULT NOW()
        );
        CREATE INDEX IF NOT EXISTS idx_timeline_date ON timeline_events(date);
        CREATE INDEX IF NOT EXISTS idx_timeline_type ON timeline_events(type);

        CREATE TABLE IF NOT EXISTS monitor_log (
            id           SERIAL PRIMARY KEY,
            timestamp    TIMESTAMP NOT NULL DEFAULT NOW(),
            source       TEXT NOT NULL,
            status       TEXT NOT NULL,
            result       TEXT,
            new_files    INTEGER DEFAULT 0,
            details_json TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_monitor_timestamp ON monitor_log(timestamp DESC);

        CREATE TABLE IF NOT EXISTS scraped_urls (
            id           SERIAL PRIMARY KEY,
            url          TEXT UNIQUE NOT NULL,
            dataset_id   INTEGER,
            first_seen   TIMESTAMP DEFAULT NOW(),
            last_checked TIMESTAMP DEFAULT NOW(),
            file_type    TEXT,
            downloaded   INTEGER DEFAULT 0,

            -- Step 1: real statuses for robust ingestion
            status       TEXT,
            fail_count   INTEGER NOT NULL DEFAULT 0,
            last_error   TEXT
        );

        CREATE INDEX IF NOT EXISTS idx_scraped_downloaded ON scraped_urls(downloaded);
        CREATE INDEX IF NOT EXISTS idx_scraped_dataset_downloaded ON scraped_urls(dataset_id, downloaded);

        -- Step 1 indexes
        CREATE INDEX IF NOT EXISTS idx_scraped_status ON scraped_urls(status);
        CREATE INDEX IF NOT EXISTS idx_scraped_status_dataset ON scraped_urls(status, dataset_id);
        """)

        # Safe upgrades if table existed before Step 1
        cur.execute("""
        ALTER TABLE scraped_urls
          ADD COLUMN IF NOT EXISTS status TEXT;
        ALTER TABLE scraped_urls
          ADD COLUMN IF NOT EXISTS fail_count INTEGER NOT NULL DEFAULT 0;
        ALTER TABLE scraped_urls
          ADD COLUMN IF NOT EXISTS last_error TEXT;
        """)

        # Backfill status if missing
        cur.execute("""
        UPDATE scraped_urls
        SET status = CASE WHEN downloaded = 1 THEN 'downloaded' ELSE 'pending' END
        WHERE status IS NULL;
        """)

        cur.close()

    logger.info("PostgreSQL database initialized")


# ═══════════════════════════════════════════════════════════════════════════
# DOCUMENT OPERATIONS
# ═══════════════════════════════════════════════════════════════════════════

def insert_document(doc_data: dict) -> int:
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO documents
            (file_id, filename, title, file_type, file_size, dataset_id,
             source_url, s3_key, s3_url, sha256_hash, date_on_doc,
             date_downloaded, source, status, metadata_json)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (file_id) DO NOTHING
            RETURNING id
        """, (
            doc_data.get("file_id"),
            doc_data.get("filename"),
            doc_data.get("title"),
            doc_data.get("file_type"),
            doc_data.get("file_size", 0),
            doc_data.get("dataset_id"),
            doc_data.get("source_url"),
            doc_data.get("s3_key"),
            doc_data.get("s3_url"),
            doc_data.get("sha256_hash"),
            doc_data.get("date_on_doc"),
            doc_data.get("date_downloaded", datetime.utcnow().date()),
            doc_data.get("source", "doj_efta"),
            doc_data.get("status", "downloaded"),
            json.dumps(doc_data.get("metadata", {})),
        ))
        row = cur.fetchone()
        cur.close()
        return row[0] if row else 0


def get_document_by_file_id(file_id: str):
    with get_db() as conn:
        return query_one(conn, "SELECT * FROM documents WHERE file_id = %s", (file_id,))


def get_documents(file_type=None, dataset_id=None, source=None,
                  sort="date_desc", search=None, limit=100, offset=0):
    """
    Document listing should stay fast at tens of thousands+ rows.
    IMPORTANT: we intentionally do NOT search extracted_text here (ILIKE on big text fields will not scale).
    Deep text search should be implemented later using Postgres FTS (tsvector) or OpenSearch.
    """
    query = "SELECT * FROM documents WHERE 1=1"
    params = []

    if file_type and file_type != "all":
        query += " AND file_type = %s"
        params.append(file_type)

    if dataset_id:
        query += " AND dataset_id = %s"
        params.append(dataset_id)

    if source:
        query += " AND source = %s"
        params.append(source)

    if search:
        query += " AND (title ILIKE %s OR filename ILIKE %s OR file_id ILIKE %s)"
        s = f"%{search}%"
        params.extend([s, s, s])

    sort_map = {
        "date_desc": "date_on_doc DESC NULLS LAST, id DESC",
        "date_asc": "date_on_doc ASC NULLS LAST, id ASC",
        "name": "title ASC NULLS LAST, id DESC",
        "size": "file_size DESC, id DESC",
        "newest_download": "date_downloaded DESC, id DESC",
    }

    query += f" ORDER BY {sort_map.get(sort, 'date_downloaded DESC, id DESC')}"
    query += " LIMIT %s OFFSET %s"
    params.extend([limit, offset])

    with get_db() as conn:
        return query_rows(conn, query, params)


def get_document_count(file_type=None, dataset_id=None):
    query = "SELECT COUNT(*) FROM documents WHERE 1=1"
    params = []

    if file_type and file_type != "all":
        query += " AND file_type = %s"
        params.append(file_type)

    if dataset_id:
        query += " AND dataset_id = %s"
        params.append(dataset_id)

    with get_db() as conn:
        return query_val(conn, query, params)


def document_exists(sha256_hash=None, source_url=None):
    with get_db() as conn:
        if sha256_hash:
            r = query_val(conn, "SELECT id FROM documents WHERE sha256_hash = %s", (sha256_hash,))
            if r:
                return True
        if source_url:
            r = query_val(conn, "SELECT id FROM documents WHERE source_url = %s", (source_url,))
            if r:
                return True
    return False


def update_document_text(doc_id: int, extracted_text: str, persons_found: list):
    """
    Marks a document indexed once extracted_text exists.
    Links persons found to the document and increments mentions.
    """
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute(
            "UPDATE documents SET extracted_text = %s, status = 'indexed', updated_at = NOW() WHERE id = %s",
            (extracted_text, doc_id)
        )

        for person_name in persons_found:
            person_id = query_val(conn, "SELECT id FROM persons WHERE name = %s", (person_name,))
            if person_id:
                cur.execute(
                    "INSERT INTO document_persons (document_id, person_id) VALUES (%s, %s) ON CONFLICT DO NOTHING",
                    (doc_id, person_id)
                )
                cur.execute(
                    "UPDATE persons SET mentions = mentions + 1, updated_at = NOW() WHERE id = %s",
                    (person_id,)
                )
        cur.close()


# ═══════════════════════════════════════════════════════════════════════════
# PERSON OPERATIONS
# ═══════════════════════════════════════════════════════════════════════════

def insert_person(name, role=None, status=None, category=None, mentions=0):
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO persons (name, role, status, category, mentions)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT(name) DO UPDATE SET
                role=COALESCE(EXCLUDED.role, persons.role),
                status=COALESCE(EXCLUDED.status, persons.status),
                category=COALESCE(EXCLUDED.category, persons.category),
                updated_at=NOW()
        """, (name, role, status, category, mentions))
        cur.close()


def get_persons(category=None, search=None, sort="mentions_desc", limit=100):
    query = "SELECT * FROM persons WHERE 1=1"
    params = []

    if category and category != "all":
        query += " AND category = %s"
        params.append(category)

    if search:
        query += " AND (name ILIKE %s OR role ILIKE %s)"
        s = f"%{search}%"
        params.extend([s, s])

    sort_map = {
        "mentions_desc": "mentions DESC, name ASC",
        "mentions_asc": "mentions ASC, name ASC",
        "name": "name ASC",
    }
    query += f" ORDER BY {sort_map.get(sort, 'mentions DESC, name ASC')} LIMIT %s"
    params.append(limit)

    with get_db() as conn:
        return query_rows(conn, query, params)


# ═══════════════════════════════════════════════════════════════════════════
# TIMELINE OPERATIONS
# ═══════════════════════════════════════════════════════════════════════════

def get_timeline(person=None, event_type=None, year_start=None, year_end=None,
                 search=None, limit=200):
    query = "SELECT * FROM timeline_events WHERE 1=1"
    params = []

    if event_type and event_type != "all":
        query += " AND type = %s"
        params.append(event_type)

    if year_start:
        query += " AND EXTRACT(YEAR FROM date) >= %s"
        params.append(int(year_start))
    if year_end:
        query += " AND EXTRACT(YEAR FROM date) <= %s"
        params.append(int(year_end))

    if person and person != "all":
        query += " AND persons_json ILIKE %s"
        params.append(f"%{person}%")

    if search:
        query += " AND (title ILIKE %s OR description ILIKE %s)"
        s = f"%{search}%"
        params.extend([s, s])

    query += " ORDER BY date ASC, id ASC LIMIT %s"
    params.append(limit)

    with get_db() as conn:
        rows = query_rows(conn, query, params)
        for r in rows:
            r["persons"] = json.loads(r.get("persons_json", "[]") or "[]")
        return rows


# ═══════════════════════════════════════════════════════════════════════════
# MONITOR LOG OPERATIONS
# ═══════════════════════════════════════════════════════════════════════════

def insert_monitor_log(source, status, result, new_files=0, details=None):
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO monitor_log (source, status, result, new_files, details_json)
            VALUES (%s, %s, %s, %s, %s)
        """, (source, status, result, new_files, json.dumps(details or {})))
        cur.close()


def get_monitor_log(limit=50):
    with get_db() as conn:
        rows = query_rows(conn, "SELECT * FROM monitor_log ORDER BY timestamp DESC LIMIT %s", (limit,))
        for r in rows:
            if r.get("timestamp"):
                r["timestamp"] = r["timestamp"].isoformat() if hasattr(r["timestamp"], "isoformat") else str(r["timestamp"])
        return rows


# ═══════════════════════════════════════════════════════════════════════════
# SCRAPED URL TRACKING (Step 1)
# ═══════════════════════════════════════════════════════════════════════════

def url_already_scraped(url):
    with get_db() as conn:
        r = query_val(conn, "SELECT status FROM scraped_urls WHERE url = %s", (url,))
        return r in ("downloaded", "missing")


def mark_url_scraped(url, dataset_id=None, file_type=None, downloaded=True, status=None, last_error=None):
    """
    status: pending | downloaded | missing | error
    downloaded: legacy boolean-ish flag kept for compatibility
    """
    if status is None:
        status = "downloaded" if downloaded else "pending"

    with get_db() as conn:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO scraped_urls (url, dataset_id, file_type, downloaded, status, last_error, last_checked)
            VALUES (%s, %s, %s, %s, %s, %s, NOW())
            ON CONFLICT(url) DO UPDATE SET
                last_checked=NOW(),
                dataset_id=COALESCE(EXCLUDED.dataset_id, scraped_urls.dataset_id),
                file_type=COALESCE(EXCLUDED.file_type, scraped_urls.file_type),
                downloaded=EXCLUDED.downloaded,
                status=EXCLUDED.status,
                last_error=COALESCE(EXCLUDED.last_error, scraped_urls.last_error)
        """, (url, dataset_id, file_type, 1 if downloaded else 0, status, last_error))
        cur.close()


def update_scraped_dataset_id(url: str, dataset_id: int) -> None:
    """Persist corrected dataset_id for a scraped URL (used by downloader relocation logic)."""
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute(
            "UPDATE scraped_urls SET dataset_id = %s, last_checked = NOW() WHERE url = %s",
            (int(dataset_id), url),
        )
        cur.close()


def get_undownloaded_urls(limit=500):
    with get_db() as conn:
        return query_rows(
            conn,
            "SELECT * FROM scraped_urls WHERE status = 'pending' ORDER BY first_seen ASC LIMIT %s",
            (limit,)
        )


# ═══════════════════════════════════════════════════════════════════════════
# STATS
# ═══════════════════════════════════════════════════════════════════════════

def get_stats():
    """
    Stats must be consistent with archive language:
    - documents = downloaded/mirrored records (rows in documents)
    - indexed_documents = documents with extracted_text processed (status='indexed')
    - discovered/downloaded/pending = scraped_urls counters
    """
    with get_db() as conn:
        total_docs = query_val(conn, "SELECT COUNT(*) FROM documents")
        total_pdfs = query_val(conn, "SELECT COUNT(*) FROM documents WHERE file_type='pdf'")
        total_images = query_val(conn, "SELECT COUNT(*) FROM documents WHERE file_type IN ('jpg','jpeg','png','tif','tiff')")
        total_videos = query_val(conn, "SELECT COUNT(*) FROM documents WHERE file_type IN ('mov','mp4')")
        total_persons = query_val(conn, "SELECT COUNT(*) FROM persons")
        indexed_docs = query_val(conn, "SELECT COUNT(*) FROM documents WHERE status='indexed'")
        total_size = query_val(conn, "SELECT COALESCE(SUM(file_size),0) FROM documents")

        discovered_urls = query_val(conn, "SELECT COUNT(*) FROM scraped_urls")
        downloaded_urls = query_val(conn, "SELECT COUNT(*) FROM scraped_urls WHERE status='downloaded'")
        pending_urls = query_val(conn, "SELECT COUNT(*) FROM scraped_urls WHERE status='pending'")
        missing_urls = query_val(conn, "SELECT COUNT(*) FROM scraped_urls WHERE status='missing'")
        error_urls = query_val(conn, "SELECT COUNT(*) FROM scraped_urls WHERE status='error'")

        last_log = query_one(conn, "SELECT * FROM monitor_log ORDER BY timestamp DESC LIMIT 1")
        recent_new = query_val(conn, "SELECT COALESCE(SUM(new_files),0) FROM monitor_log WHERE timestamp > NOW() - INTERVAL '7 days'")

    if last_log and last_log.get("timestamp"):
        last_log["timestamp"] = last_log["timestamp"].isoformat() if hasattr(last_log["timestamp"], "isoformat") else str(last_log["timestamp"])

    return {
        "total_documents": total_docs,
        "downloaded_documents": total_docs,
        "indexed_documents": indexed_docs,

        "total_pdfs": total_pdfs,
        "total_images": total_images,
        "total_videos": total_videos,

        "total_persons": total_persons,

        "total_size_bytes": total_size,
        "total_size_human": _human_size(total_size),

        "discovered_urls": discovered_urls,
        "downloaded_urls": downloaded_urls,
        "pending_urls": pending_urls,
        "missing_urls": missing_urls,
        "error_urls": error_urls,

        "new_files_this_week": recent_new,
        "last_check": last_log,
    }


def _human_size(size_bytes):
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if size_bytes < 1024:
            return f"{size_bytes:.1f} {unit}"
        size_bytes /= 1024
    return f"{size_bytes:.1f} PB"


# ═══════════════════════════════════════════════════════════════════════════
# SEED DATA
# ═══════════════════════════════════════════════════════════════════════════

def seed_persons():
    with get_db() as conn:
        count = query_val(conn, "SELECT COUNT(*) FROM persons")
        if count > 0:
            logger.info("Persons table already seeded, skipping")
            return

    persons_data = [
        ("Jeffrey Epstein", "Principal Subject", "Deceased", "principal", 6000000),
        ("Ghislaine Maxwell", "Co-Conspirator (Convicted)", "Incarcerated", "principal", 1200000),
        ("Jean-Luc Brunel", "Co-Conspirator", "Deceased", "principal", 89000),
        ("Les Wexner", "Named Co-Conspirator (FBI)", "Unredacted Feb 2026", "associate", 45000),
        ("Prince Andrew", "Named Individual", "Titles Stripped", "associate", 500000),
        ("Bill Clinton", "Named Individual", "Public", "political", 320000),
        ("Donald Trump", "Named Individual", "Public", "political", 280000),
        ("Alan Dershowitz", "Legal Advisor to Epstein", "Public", "legal", 210000),
        ("Elon Musk", "Named Individual", "Public", "associate", 95000),
        ("Bill Gates", "Named Individual", "Public", "associate", 185000),
        ("Richard Branson", "Named Individual", "Public", "associate", 120000),
        ("Kevin Spacey", "Named Individual (Photos)", "Public", "associate", 34000),
        ("Darren Indyke", "Personal Lawyer / Estate Executor", "Public", "inner_circle", 67000),
        ("Richard Kahn", "Accountant / Estate Co-Executor", "Public", "inner_circle", 43000),
        ("Sarah Kellen", "Personal Assistant", "Named Co-Conspirator", "inner_circle", 89000),
        ("Nadia Marcinkova", "Associate", "Named Co-Conspirator", "inner_circle", 56000),
        ("Steve Tisch", "Named Individual", "400+ mentions", "associate", 78000),
        ("Casey Wasserman", "Named Individual", "Public", "associate", 25000),
        ("Sultan Ahmed bin Sulayem", "Identified by Rep. Khanna", "Unredacted Feb 2026", "associate", 18000),
        ("Miroslav Lajčák", "Named Individual", "300+ mentions", "political", 32000),
        ("Alexander Acosta", "Non-Prosecution Agreement", "Public", "legal", 145000),
        ("Martin Nowak", "Harvard Professor / Beneficiary", "Public", "academic", 28000),
        ("Steven Pinker", "Named Individual (Video)", "Public", "academic", 15000),
        ("Boris Nikolic", "Backup Estate Executor", "Public", "associate", 22000),
        ("Stacey Plaskett", "Named Individual (Texts)", "Public", "political", 19000),
        ("Mark Epstein", "Brother / Beneficiary", "Public", "family", 67000),
        ("John Phelan", "Flight Manifest (2006)", "Public", "associate", 12000),
        ("Maria Farmer", "Survivor / FBI Complainant (1996)", "Public", "survivor", 95000),
        ("Virginia Giuffre", "Survivor / Key Witness", "Deceased", "survivor", 450000),
    ]
    for name, role, status, category, mentions in persons_data:
        insert_person(name, role, status, category, mentions)
    logger.info(f"Seeded {len(persons_data)} persons")


def seed_timeline():
    with get_db() as conn:
        count = query_val(conn, "SELECT COUNT(*) FROM timeline_events")
        if count > 0:
            logger.info("Timeline already seeded, skipping")
            return

    events = [
        ("1996-09-01","fbi","Maria Farmer FBI Complaint","First known FBI complaint about Epstein's crimes. FBI fails to investigate.","New York, NY",["Maria Farmer","Jeffrey Epstein"]),
        ("2002-01-01","travel","Clinton Flights on Lolita Express","Flight logs show multiple trips on Epstein's private jet.",None,["Bill Clinton","Jeffrey Epstein","Ghislaine Maxwell"]),
        ("2003-06-01","financial","$6.5M Harvard Donation","Epstein donates to establish Harvard's Program for Evolutionary Dynamics.","Cambridge, MA",["Jeffrey Epstein","Martin Nowak"]),
        ("2005-03-01","legal","Palm Beach Police Investigation Begins","Investigation launched after parent reports abuse of 14-year-old daughter.","Palm Beach, FL",["Jeffrey Epstein"]),
        ("2005-06-01","evidence","Epstein's 'Black Book' Taken","Former employee takes 97-page contact book from Epstein's home.","Palm Beach, FL",["Jeffrey Epstein"]),
        ("2006-05-01","legal","FBI Opens Federal Investigation","FBI investigation initiated based on Palm Beach PD evidence.","Palm Beach, FL",["Jeffrey Epstein"]),
        ("2007-09-01","legal","Non-Prosecution Agreement Signed","US Attorney Acosta signs agreement granting immunity to Epstein and co-conspirators.","Miami, FL",["Alexander Acosta","Jeffrey Epstein","Alan Dershowitz"]),
        ("2008-06-30","legal","Epstein Pleads Guilty (State)","Pleads to two state charges. Sentenced to 18 months; receives work release.","Palm Beach, FL",["Jeffrey Epstein"]),
        ("2019-07-06","legal","Epstein Arrested (SDNY)","Arrested on federal sex trafficking charges.","Teterboro, NJ",["Jeffrey Epstein"]),
        ("2019-08-10","legal","Epstein Death in Custody","Found dead in Metropolitan Correctional Center. Ruled suicide.","New York, NY",["Jeffrey Epstein"]),
        ("2021-12-29","legal","Maxwell Found Guilty","Convicted on 5 of 6 counts including sex trafficking.","New York, NY",["Ghislaine Maxwell"]),
        ("2025-11-19","legal","EFTA Signed into Law","Epstein Files Transparency Act (H.R.4405) signed by President Trump.","Washington, DC",["Donald Trump"]),
        ("2025-12-19","disclosure","Massive DOJ Release","3+ million pages, 180,000 images, 2,000 videos released.","Washington, DC",["Jeffrey Epstein","Ghislaine Maxwell"]),
        ("2026-01-30","disclosure","January 2026 Document Release","Additional 3 million documents released by DOJ.","Washington, DC",["Jeffrey Epstein"]),
        ("2026-02-10","disclosure","Wexner Unredacted as Co-Conspirator","Les Wexner's name unredacted from FBI documents labeling him co-conspirator.","Washington, DC",["Les Wexner"]),
    ]
    with get_db() as conn:
        cur = conn.cursor()
        for date, etype, title, desc, loc, persons in events:
            cur.execute("""
                INSERT INTO timeline_events (date, type, title, description, location, persons_json)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (date, etype, title, desc, loc, json.dumps(persons)))
        cur.close()
    logger.info(f"Seeded {len(events)} timeline events")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    init_db()
    seed_persons()
    seed_timeline()
    print("PostgreSQL database initialized and seeded!")
