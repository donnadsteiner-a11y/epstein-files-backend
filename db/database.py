"""
SQLite database layer for the Epstein Files Platform.
Stores metadata about every downloaded file, persons, timeline events,
and monitor activity logs.
"""
import sqlite3
import os
import json
import logging
from datetime import datetime
from contextlib import contextmanager

import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.settings import DB_PATH

logger = logging.getLogger(__name__)


@contextmanager
def get_db():
    """Context manager for database connections."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def init_db():
    """Create all tables if they don't exist."""
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

    with get_db() as conn:
        conn.executescript("""
        -- ═══ DOCUMENTS ═══
        -- Every file downloaded from DOJ or other sources
        CREATE TABLE IF NOT EXISTS documents (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            file_id         TEXT UNIQUE NOT NULL,       -- e.g. "DS1-001" or hash-based
            filename        TEXT NOT NULL,               -- original filename
            title           TEXT,                        -- human-readable title
            file_type       TEXT NOT NULL,               -- pdf, jpg, mov, etc.
            file_size       INTEGER DEFAULT 0,           -- bytes
            dataset_id      INTEGER,                     -- 1-12 for DOJ data sets
            source_url      TEXT,                        -- original DOJ URL
            s3_key          TEXT,                        -- path in DreamObjects
            s3_url          TEXT,                        -- public DreamObjects URL
            sha256_hash     TEXT,                        -- for deduplication
            date_on_doc     TEXT,                        -- date extracted from document
            date_downloaded  TEXT NOT NULL,               -- when we grabbed it
            date_modified   TEXT,                        -- last modified on source
            source          TEXT DEFAULT 'doj_efta',     -- doj_efta, house_oversight, foia, court
            status          TEXT DEFAULT 'downloaded',   -- downloaded, indexed, error
            extracted_text  TEXT,                        -- full text (for PDFs)
            metadata_json   TEXT,                        -- any extra metadata as JSON
            created_at      TEXT DEFAULT (datetime('now')),
            updated_at      TEXT DEFAULT (datetime('now'))
        );

        CREATE INDEX IF NOT EXISTS idx_documents_file_type ON documents(file_type);
        CREATE INDEX IF NOT EXISTS idx_documents_dataset ON documents(dataset_id);
        CREATE INDEX IF NOT EXISTS idx_documents_date ON documents(date_on_doc);
        CREATE INDEX IF NOT EXISTS idx_documents_source ON documents(source);
        CREATE INDEX IF NOT EXISTS idx_documents_hash ON documents(sha256_hash);
        CREATE INDEX IF NOT EXISTS idx_documents_status ON documents(status);

        -- ═══ PERSONS ═══
        -- All tracked individuals
        CREATE TABLE IF NOT EXISTS persons (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            name        TEXT UNIQUE NOT NULL,
            role        TEXT,
            status      TEXT,
            category    TEXT,        -- principal, inner_circle, associate, political, legal, academic, family, survivor
            mentions    INTEGER DEFAULT 0,
            metadata_json TEXT,
            created_at  TEXT DEFAULT (datetime('now')),
            updated_at  TEXT DEFAULT (datetime('now'))
        );

        CREATE INDEX IF NOT EXISTS idx_persons_category ON persons(category);
        CREATE INDEX IF NOT EXISTS idx_persons_mentions ON persons(mentions DESC);

        -- ═══ DOCUMENT <-> PERSON TAGS ═══
        -- Which persons appear in which documents
        CREATE TABLE IF NOT EXISTS document_persons (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            document_id INTEGER NOT NULL REFERENCES documents(id),
            person_id   INTEGER NOT NULL REFERENCES persons(id),
            confidence  REAL DEFAULT 1.0,  -- 0.0-1.0 confidence of match
            context     TEXT,              -- snippet of text where name appeared
            UNIQUE(document_id, person_id)
        );

        CREATE INDEX IF NOT EXISTS idx_docpersons_doc ON document_persons(document_id);
        CREATE INDEX IF NOT EXISTS idx_docpersons_person ON document_persons(person_id);

        -- ═══ TIMELINE EVENTS ═══
        CREATE TABLE IF NOT EXISTS timeline_events (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            date        TEXT NOT NULL,
            type        TEXT NOT NULL,     -- legal, fbi, correspondence, travel, financial, evidence, media, disclosure
            title       TEXT NOT NULL,
            description TEXT,
            location    TEXT,
            persons_json TEXT,             -- JSON array of person names
            source_doc_id INTEGER REFERENCES documents(id),
            source      TEXT,             -- where this event info came from
            created_at  TEXT DEFAULT (datetime('now'))
        );

        CREATE INDEX IF NOT EXISTS idx_timeline_date ON timeline_events(date);
        CREATE INDEX IF NOT EXISTS idx_timeline_type ON timeline_events(type);

        -- ═══ MONITOR LOG ═══
        -- Every time we check DOJ for new files
        CREATE TABLE IF NOT EXISTS monitor_log (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp   TEXT NOT NULL DEFAULT (datetime('now')),
            source      TEXT NOT NULL,     -- doj_efta, house_oversight, foia, court, web_scan
            status      TEXT NOT NULL,     -- checked, update, alert, major, error
            result      TEXT,              -- human-readable result
            new_files   INTEGER DEFAULT 0, -- number of new files found
            details_json TEXT              -- any extra details as JSON
        );

        CREATE INDEX IF NOT EXISTS idx_monitor_timestamp ON monitor_log(timestamp DESC);

        -- ═══ SCRAPED URLS ═══
        -- Track every URL we've seen to avoid re-downloading
        CREATE TABLE IF NOT EXISTS scraped_urls (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            url         TEXT UNIQUE NOT NULL,
            dataset_id  INTEGER,
            first_seen  TEXT DEFAULT (datetime('now')),
            last_checked TEXT DEFAULT (datetime('now')),
            file_type   TEXT,
            downloaded  INTEGER DEFAULT 0  -- 0=not yet, 1=downloaded
        );

        CREATE INDEX IF NOT EXISTS idx_scraped_downloaded ON scraped_urls(downloaded);
        """)

    logger.info(f"Database initialized at {DB_PATH}")


# ═══════════════════════════════════════════════════════════════════════════
# DOCUMENT OPERATIONS
# ═══════════════════════════════════════════════════════════════════════════

def insert_document(doc_data: dict) -> int:
    """Insert a new document record. Returns the new row id."""
    with get_db() as conn:
        cursor = conn.execute("""
            INSERT OR IGNORE INTO documents
            (file_id, filename, title, file_type, file_size, dataset_id,
             source_url, s3_key, s3_url, sha256_hash, date_on_doc,
             date_downloaded, source, status, metadata_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
            doc_data.get("date_downloaded", datetime.utcnow().isoformat()),
            doc_data.get("source", "doj_efta"),
            doc_data.get("status", "downloaded"),
            json.dumps(doc_data.get("metadata", {})),
        ))
        return cursor.lastrowid


def get_documents(file_type=None, dataset_id=None, source=None,
                  sort="date_desc", search=None, limit=100, offset=0):
    """Query documents with filters."""
    query = "SELECT * FROM documents WHERE 1=1"
    params = []

    if file_type and file_type != "all":
        query += " AND file_type = ?"
        params.append(file_type)
    if dataset_id:
        query += " AND dataset_id = ?"
        params.append(dataset_id)
    if source:
        query += " AND source = ?"
        params.append(source)
    if search:
        query += " AND (title LIKE ? OR filename LIKE ? OR extracted_text LIKE ?)"
        s = f"%{search}%"
        params.extend([s, s, s])

    sort_map = {
        "date_desc": "date_on_doc DESC",
        "date_asc": "date_on_doc ASC",
        "name": "title ASC",
        "size": "file_size DESC",
        "newest_download": "date_downloaded DESC",
    }
    query += f" ORDER BY {sort_map.get(sort, 'date_downloaded DESC')}"
    query += " LIMIT ? OFFSET ?"
    params.extend([limit, offset])

    with get_db() as conn:
        rows = conn.execute(query, params).fetchall()
        return [dict(r) for r in rows]


def get_document_count(file_type=None, dataset_id=None):
    """Get total document count with optional filters."""
    query = "SELECT COUNT(*) as cnt FROM documents WHERE 1=1"
    params = []
    if file_type and file_type != "all":
        query += " AND file_type = ?"
        params.append(file_type)
    if dataset_id:
        query += " AND dataset_id = ?"
        params.append(dataset_id)

    with get_db() as conn:
        return conn.execute(query, params).fetchone()["cnt"]


def document_exists(sha256_hash=None, source_url=None):
    """Check if a document already exists (by hash or URL)."""
    with get_db() as conn:
        if sha256_hash:
            row = conn.execute(
                "SELECT id FROM documents WHERE sha256_hash = ?", (sha256_hash,)
            ).fetchone()
            if row:
                return True
        if source_url:
            row = conn.execute(
                "SELECT id FROM documents WHERE source_url = ?", (source_url,)
            ).fetchone()
            if row:
                return True
    return False


def update_document_text(doc_id: int, extracted_text: str, persons_found: list):
    """Update a document with extracted text and person tags."""
    with get_db() as conn:
        conn.execute(
            "UPDATE documents SET extracted_text = ?, status = 'indexed', updated_at = datetime('now') WHERE id = ?",
            (extracted_text, doc_id)
        )
        for person_name in persons_found:
            person = conn.execute(
                "SELECT id FROM persons WHERE name = ?", (person_name,)
            ).fetchone()
            if person:
                conn.execute(
                    "INSERT OR IGNORE INTO document_persons (document_id, person_id) VALUES (?, ?)",
                    (doc_id, person["id"])
                )
                conn.execute(
                    "UPDATE persons SET mentions = mentions + 1, updated_at = datetime('now') WHERE id = ?",
                    (person["id"],)
                )


# ═══════════════════════════════════════════════════════════════════════════
# PERSON OPERATIONS
# ═══════════════════════════════════════════════════════════════════════════

def insert_person(name, role=None, status=None, category=None, mentions=0):
    """Insert or update a person."""
    with get_db() as conn:
        conn.execute("""
            INSERT INTO persons (name, role, status, category, mentions)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(name) DO UPDATE SET
                role=COALESCE(excluded.role, persons.role),
                status=COALESCE(excluded.status, persons.status),
                category=COALESCE(excluded.category, persons.category),
                updated_at=datetime('now')
        """, (name, role, status, category, mentions))


def get_persons(category=None, search=None, sort="mentions_desc", limit=100):
    """Get persons with optional filters."""
    query = "SELECT * FROM persons WHERE 1=1"
    params = []

    if category and category != "all":
        query += " AND category = ?"
        params.append(category)
    if search:
        query += " AND (name LIKE ? OR role LIKE ?)"
        s = f"%{search}%"
        params.extend([s, s])

    sort_map = {"mentions_desc": "mentions DESC", "name": "name ASC"}
    query += f" ORDER BY {sort_map.get(sort, 'mentions DESC')} LIMIT ?"
    params.append(limit)

    with get_db() as conn:
        return [dict(r) for r in conn.execute(query, params).fetchall()]


# ═══════════════════════════════════════════════════════════════════════════
# TIMELINE OPERATIONS
# ═══════════════════════════════════════════════════════════════════════════

def get_timeline(person=None, event_type=None, year_start=None, year_end=None,
                 search=None, limit=200):
    """Get timeline events with filters."""
    query = "SELECT * FROM timeline_events WHERE 1=1"
    params = []

    if event_type and event_type != "all":
        query += " AND type = ?"
        params.append(event_type)
    if year_start:
        query += " AND CAST(SUBSTR(date, 1, 4) AS INTEGER) >= ?"
        params.append(int(year_start))
    if year_end:
        query += " AND CAST(SUBSTR(date, 1, 4) AS INTEGER) <= ?"
        params.append(int(year_end))
    if person and person != "all":
        query += " AND persons_json LIKE ?"
        params.append(f"%{person}%")
    if search:
        query += " AND (title LIKE ? OR description LIKE ?)"
        s = f"%{search}%"
        params.extend([s, s])

    query += " ORDER BY date ASC LIMIT ?"
    params.append(limit)

    with get_db() as conn:
        rows = conn.execute(query, params).fetchall()
        result = []
        for r in rows:
            d = dict(r)
            d["persons"] = json.loads(d.get("persons_json", "[]"))
            result.append(d)
        return result


# ═══════════════════════════════════════════════════════════════════════════
# MONITOR LOG OPERATIONS
# ═══════════════════════════════════════════════════════════════════════════

def insert_monitor_log(source, status, result, new_files=0, details=None):
    """Log a monitoring check."""
    with get_db() as conn:
        conn.execute("""
            INSERT INTO monitor_log (source, status, result, new_files, details_json)
            VALUES (?, ?, ?, ?, ?)
        """, (source, status, result, new_files, json.dumps(details or {})))


def get_monitor_log(limit=50):
    """Get recent monitor activity."""
    with get_db() as conn:
        return [dict(r) for r in conn.execute(
            "SELECT * FROM monitor_log ORDER BY timestamp DESC LIMIT ?", (limit,)
        ).fetchall()]


# ═══════════════════════════════════════════════════════════════════════════
# SCRAPED URL TRACKING
# ═══════════════════════════════════════════════════════════════════════════

def url_already_scraped(url):
    """Check if we've already scraped this URL."""
    with get_db() as conn:
        row = conn.execute(
            "SELECT downloaded FROM scraped_urls WHERE url = ?", (url,)
        ).fetchone()
        return row and row["downloaded"] == 1


def mark_url_scraped(url, dataset_id=None, file_type=None, downloaded=True):
    """Record that we've seen/downloaded a URL."""
    with get_db() as conn:
        conn.execute("""
            INSERT INTO scraped_urls (url, dataset_id, file_type, downloaded, last_checked)
            VALUES (?, ?, ?, ?, datetime('now'))
            ON CONFLICT(url) DO UPDATE SET
                last_checked=datetime('now'),
                downloaded=excluded.downloaded
        """, (url, dataset_id, file_type, 1 if downloaded else 0))


def get_undownloaded_urls(limit=500):
    """Get URLs we've seen but not yet downloaded."""
    with get_db() as conn:
        return [dict(r) for r in conn.execute(
            "SELECT * FROM scraped_urls WHERE downloaded = 0 ORDER BY first_seen ASC LIMIT ?",
            (limit,)
        ).fetchall()]


# ═══════════════════════════════════════════════════════════════════════════
# STATS
# ═══════════════════════════════════════════════════════════════════════════

def get_stats():
    """Get dashboard statistics."""
    with get_db() as conn:
        total_docs = conn.execute("SELECT COUNT(*) as c FROM documents").fetchone()["c"]
        total_pdfs = conn.execute("SELECT COUNT(*) as c FROM documents WHERE file_type='pdf'").fetchone()["c"]
        total_images = conn.execute("SELECT COUNT(*) as c FROM documents WHERE file_type IN ('jpg','jpeg','png','tif','tiff')").fetchone()["c"]
        total_videos = conn.execute("SELECT COUNT(*) as c FROM documents WHERE file_type IN ('mov','mp4')").fetchone()["c"]
        total_persons = conn.execute("SELECT COUNT(*) as c FROM persons").fetchone()["c"]
        total_indexed = conn.execute("SELECT COUNT(*) as c FROM documents WHERE status='indexed'").fetchone()["c"]
        total_size = conn.execute("SELECT COALESCE(SUM(file_size),0) as s FROM documents").fetchone()["s"]

        last_log = conn.execute(
            "SELECT * FROM monitor_log ORDER BY timestamp DESC LIMIT 1"
        ).fetchone()

        recent_new = conn.execute(
            "SELECT COALESCE(SUM(new_files),0) as n FROM monitor_log WHERE timestamp > datetime('now', '-7 days')"
        ).fetchone()["n"]

    return {
        "total_documents": total_docs,
        "total_pdfs": total_pdfs,
        "total_images": total_images,
        "total_videos": total_videos,
        "total_persons": total_persons,
        "total_indexed": total_indexed,
        "total_size_bytes": total_size,
        "total_size_human": _human_size(total_size),
        "new_files_this_week": recent_new,
        "last_check": dict(last_log) if last_log else None,
    }


def _human_size(size_bytes):
    """Convert bytes to human-readable string."""
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if size_bytes < 1024:
            return f"{size_bytes:.1f} {unit}"
        size_bytes /= 1024
    return f"{size_bytes:.1f} PB"


# ═══════════════════════════════════════════════════════════════════════════
# SEED DATA
# ═══════════════════════════════════════════════════════════════════════════

def seed_persons():
    """Populate the persons table with known individuals."""
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
    """Populate timeline with known events."""
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
        for date, etype, title, desc, loc, persons in events:
            conn.execute("""
                INSERT OR IGNORE INTO timeline_events (date, type, title, description, location, persons_json)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (date, etype, title, desc, loc, json.dumps(persons)))
    logger.info(f"Seeded {len(events)} timeline events")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    init_db()
    seed_persons()
    seed_timeline()
    print(f"Database created and seeded at: {DB_PATH}")
