# crawler/doj_session.py
import time
import requests

AGE_VERIFY_URL = "https://www.justice.gov/age-verify?destination=/epstein/files/"
PDF_MAGIC = b"%PDF"

DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "*/*",
    "Accept-Language": "en-US,en;q=0.9",
    "Connection": "keep-alive",
}

def build_doj_session() -> requests.Session:
    s = requests.Session()
    s.headers.update(DEFAULT_HEADERS)

    # Age gate cookie (what you were manually setting)
    s.cookies.set("justiceGovAgeVerified", "true", domain=".justice.gov", path="/")

    # Prime QueueIT / Akamai cookies (what your curl did)
    try:
        s.get(AGE_VERIFY_URL, timeout=30, allow_redirects=True)
    except Exception:
        pass

    return s

def ensure_pdf_response(resp: requests.Response) -> bool:
    # Content-type isn't always reliable; verify file magic.
    try:
        first = resp.raw.read(4, decode_content=True)
        return first == PDF_MAGIC
    except Exception:
        return False
