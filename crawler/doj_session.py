# crawler/doj_session.py
import requests

AGE_VERIFY_URL = "https://www.justice.gov/age-verify?destination=/epstein/files/"
DOJ_HOME_URL = "https://www.justice.gov/"
DOJ_EPSTEIN_URL = "https://www.justice.gov/epstein/doj-disclosures"
DOJ_WARMUP_PDF = "https://www.justice.gov/epstein/files/DataSet%2010/EFTA01602154.pdf"

PDF_MAGIC = b"%PDF"

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": (
        "text/html,application/xhtml+xml,application/xml;q=0.9,"
        "image/avif,image/webp,image/apng,*/*;q=0.8"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
}


def build_doj_session() -> requests.Session:
    s = requests.Session()
    s.headers.update(DEFAULT_HEADERS)

    # Force both host and dot-domain cookie variants
    for domain in ("www.justice.gov", ".justice.gov"):
        try:
            s.cookies.set("justiceGovAgeVerified", "true", domain=domain, path="/")
        except Exception:
            pass

    try:
        # Step 1: land on the main DOJ site
        s.get(DOJ_HOME_URL, timeout=30, allow_redirects=True)
    except Exception:
        pass

    try:
        # Step 2: land on the Epstein disclosures page with a realistic referer
        s.get(
            DOJ_EPSTEIN_URL,
            timeout=30,
            allow_redirects=True,
            headers={"Referer": DOJ_HOME_URL},
        )
    except Exception:
        pass

    try:
        # Step 3: hit the age verify URL explicitly
        s.get(
            AGE_VERIFY_URL,
            timeout=30,
            allow_redirects=True,
            headers={"Referer": DOJ_EPSTEIN_URL},
        )
    except Exception:
        pass

    try:
        # Step 4: warm a real PDF path to pick up any additional cookies
        r = s.get(
            DOJ_WARMUP_PDF,
            timeout=30,
            allow_redirects=True,
            stream=True,
            headers={"Referer": DOJ_EPSTEIN_URL},
        )
        try:
            _ = r.raw.read(8)
        except Exception:
            pass
        r.close()
    except Exception:
        pass

    return s


def ensure_pdf_response(resp: requests.Response) -> bool:
    try:
        first = resp.raw.read(4, decode_content=True)
        return first == PDF_MAGIC
    except Exception:
        return False
