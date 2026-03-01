"""
Configuration for Epstein Files Platform
Edit these values for your Dreamhost environment.
"""
import os

# ─── PATHS ───────────────────────────────────────────────────────────────────
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(BASE_DIR, "db", "epstein_files.db")
LOG_DIR = os.path.join(BASE_DIR, "logs")
STATIC_DIR = os.path.join(BASE_DIR, "static")
TEMP_DOWNLOAD_DIR = os.path.join(BASE_DIR, "temp_downloads")

# ─── DREAMOBJECTS (S3-COMPATIBLE) ───────────────────────────────────────────
# Get these from Dreamhost Panel → Cloud Services → DreamObjects
S3_ENDPOINT = os.environ.get("S3_ENDPOINT", "https://objects-us-east-1.dream.io")
S3_ACCESS_KEY = os.environ.get("S3_ACCESS_KEY", "YOUR_ACCESS_KEY_HERE")
S3_SECRET_KEY = os.environ.get("S3_SECRET_KEY", "YOUR_SECRET_KEY_HERE")
S3_BUCKET = os.environ.get("S3_BUCKET", "epstein-files")
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

# Additional sources
HOUSE_OVERSIGHT_URL = "https://oversight.house.gov/release/oversight-committee-releases-epstein-records-provided-by-the-department-of-justice/"
EFTA_BILL_URL = "https://www.congress.gov/bill/119th-congress/house-bill/4405"

# ─── CRAWLER SETTINGS ───────────────────────────────────────────────────────
REQUEST_DELAY = 2.0            # Seconds between requests (be polite to DOJ servers)
REQUEST_TIMEOUT = 60           # Timeout per request in seconds
MAX_RETRIES = 3                # Retry failed downloads
CHUNK_SIZE = 8192              # Download chunk size in bytes
USER_AGENT = (
    "EpsteinFilesReviewPlatform/2.1 "
    "(Legal Research; Contact: your-email@example.com)"
)
MAX_CONCURRENT_DOWNLOADS = 3   # Parallel downloads

# File types we care about
ALLOWED_EXTENSIONS = {".pdf", ".jpg", ".jpeg", ".png", ".mov", ".mp4", ".tif", ".tiff"}

# ─── API SERVER ──────────────────────────────────────────────────────────────
API_HOST = "0.0.0.0"
API_PORT = int(os.environ.get("API_PORT", 5000))
API_DEBUG = os.environ.get("API_DEBUG", "false").lower() == "true"
CORS_ORIGINS = ["*"]  # Lock this down in production to your domain

# ─── PERSONS OF INTEREST (for tagging/extraction) ───────────────────────────
# These names are searched for in document text to auto-tag files
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
