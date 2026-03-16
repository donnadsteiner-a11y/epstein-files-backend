"""
Configuration for Epstein Files Platform
Edit these values for your environment.
"""
import os

# ─── PATHS ───────────────────────────────────────────────────────────────────
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LOG_DIR = os.path.join(BASE_DIR, "logs")
STATIC_DIR = os.path.join(BASE_DIR, "static")
TEMP_DOWNLOAD_DIR = os.path.join(BASE_DIR, "temp_downloads")

# ─── DATABASE ────────────────────────────────────────────────────────────────
# Render provides this automatically when you create a PostgreSQL database
DATABASE_URL = os.environ.get("DATABASE_URL", "")

# ─── DREAMOBJECTS (S3-COMPATIBLE) ───────────────────────────────────────────
S3_ENDPOINT = os.environ.get("S3_ENDPOINT", "https://s3.us-east-005.dream.io")
S3_BUCKET = os.environ.get("S3_BUCKET", "docketzero-files")
S3_PUBLIC_URL = f"{S3_ENDPOINT}/{S3_BUCKET}"

# ─── DOJ SOURCE URLS ────────────────────────────────────────────────────────
DOJ_BASE = "https://www.justice.gov"
DOJ_DISCLOSURES_URL = f"{DOJ_BASE}/epstein/doj-disclosures"
DOJ_SEARCH_URL = f"{DOJ_BASE}/epstein/search"

DOJ_DATA_SET_URLS = {
    1: f"{DOJ_BASE}/epstein/doj-disclosures/data-set-1-files",
    2: f"{DOJ_BASE}/epstein/doj-disclosures/data-set-2-files",
    3: f"{DOJ_BASE}/epstein/doj-disclosures/data-set-3-files",
    4: f"{DOJ_BASE}/epstein/doj-disclosures/data-set-4-files",
    5: f"{DOJ_BASE}/epstein/doj-disclosures/data-set-5-files",
    6: f"{DOJ_BASE}/epstein/doj-disclosures/data-set-6-files",
    7: f"{DOJ_BASE}/epstein/doj-disclosures/data-set-7-files",
    8: f"{DOJ_BASE}/epstein/doj-disclosures/data-set-8-files",
    9: f"{DOJ_BASE}/epstein/doj-disclosures/data-set-9-files",
    10: f"{DOJ_BASE}/epstein/doj-disclosures/data-set-10-files",
    11: f"{DOJ_BASE}/epstein/doj-disclosures/data-set-11-files",
    12: f"{DOJ_BASE}/epstein/doj-disclosures/data-set-12-files",
}

HOUSE_OVERSIGHT_URL = "https://oversight.house.gov/release/oversight-committee-releases-epstein-records-provided-by-the-department-of-justice/"
EFTA_BILL_URL = "https://www.congress.gov/bill/119th-congress/house-bill/4405"

# ─── CRAWLER SETTINGS ───────────────────────────────────────────────────────
REQUEST_DELAY = 2.0
REQUEST_TIMEOUT = 60
MAX_RETRIES = 3
CHUNK_SIZE = 8192
USER_AGENT = (
    "EpsteinFilesReviewPlatform/2.1 "
    "(Legal Research; Contact: admin@docketzero.com)"
)
MAX_CONCURRENT_DOWNLOADS = 3

ALLOWED_EXTENSIONS = {".pdf", ".jpg", ".jpeg", ".png", ".mov", ".mp4", ".tif", ".tiff"}

# ─── API SERVER ──────────────────────────────────────────────────────────────
API_HOST = "0.0.0.0"
API_PORT = int(os.environ.get("PORT", os.environ.get("API_PORT", 5000)))
API_DEBUG = os.environ.get("API_DEBUG", "false").lower() == "true"
CORS_ORIGINS = ["https://docketzero.com", "https://www.docketzero.com"]

# ─── PERSONS OF INTEREST ────────────────────────────────────────────────────
TRACKED_PERSONS = [
    "Jeffrey Epstein", "Ghislaine Maxwell", "Jean-Luc Brunel",
    "Les Wexner", "Prince Andrew", "Andrew Windsor",
    "Bill Clinton", "Donald Trump", "Alan Dershowitz",
    "Elon Musk", "Bill Gates", "Richard Branson",
    "Kevin Spacey", "Darren Indyke", "Richard Kahn",
    "Sarah Kellen", "Nadia Marcinkova", "Steve Tisch",
    "Casey Wasserman", "Sultan Ahmed bin Sulayem",
    "Miroslav Lajčák", "Alexander Acosta", "Martin Nowak",
    "Steven Pinker", "Boris Nikolic", "Stacey Plaskett",
    "Mark Epstein", "John Phelan", "Maria Farmer",
    "Virginia Giuffre", "Chris Tucker", "Mick Jagger",
    "Michael Jackson", "Diana Ross", "Phil Collins",
    "Sarah Ferguson", "Peter Mandelson",
]

# ─── LOGGING ─────────────────────────────────────────────────────────────────
LOG_LEVEL = "INFO"
LOG_FORMAT = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
