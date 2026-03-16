# Dataset 12 Downloader — Setup Guide

## How it works

1. GitHub Actions runs on a schedule every 4 hours
2. The script scrapes every page of the DS12 listing using your browser cookies
3. Each PDF is downloaded and uploaded to `docketzero-files/Data Set 12/`
4. Already-uploaded files are skipped — every run is safe to re-run
5. Runs until everything is downloaded, then goes quiet

---

## Step 1 — Capture your DOJ cookies

You need to grab the cookies from an active DOJ session in your browser after
passing the age gate manually.

1. Open **Chrome** and go to https://www.justice.gov/epstein/doj-disclosures/data-set-12-files
2. Pass the age gate (click through the confirmation)
3. Open **DevTools** → **Application** tab → **Cookies** → `https://www.justice.gov`
4. You need the cookies as a string. The easiest way:
   - Open DevTools → **Network** tab → refresh the page
   - Click any request to `www.justice.gov`
   - In the **Headers** pane, find the `Cookie:` request header
   - Copy the entire value (looks like: `SSESSION=abc123; disclaimer=1; ...`)

That copied string is your `DOJ_COOKIES` value.

**Alternative — JSON format also works:**
```json
{"SSESSION": "abc123", "disclaimer_seen": "1", "has_js": "1"}
```

---

## Step 2 — Add GitHub Secrets

Go to your repo → **Settings** → **Secrets and variables** → **Actions** → **New repository secret**

Add these five secrets:

| Secret name    | Value                              |
|----------------|------------------------------------|
| `DOJ_COOKIES`  | The cookie string from Step 1      |
| `DO_ENDPOINT`  | `https://s3.us-east-005.dream.io`  |
| `DO_ACCESS_KEY`| Your DreamObjects access key       |
| `DO_SECRET_KEY`| Your DreamObjects secret key       |
| `DO_BUCKET`    | `docketzero-files`                 |

---

## Step 3 — Add the files to your repo

```
your-repo/
├── ds12_downloader.py
└── .github/
    └── workflows/
        └── ds12_download.yml
```

Commit and push both files to `main`.

---

## Step 4 — Trigger a first run

Go to **Actions** → **Dataset 12 — PDF Downloader** → **Run workflow**

Watch the logs. You should see:

```
── Scraping DS12 listing pages ─────────────────────────
  Page 0: https://www.justice.gov/epstein/doj-disclosures/data-set-12-files
    Found 50 PDF links (50 new) — total so far: 50
  Page 1: ...
    ...
── Downloading 1234 PDFs ──────────────────────────────
  [1/1234]  EFTA02730265.pdf  ✓  142,830 bytes
  [2/1234]  EFTA02730266.pdf  ✓  98,512 bytes
  ...
```

---

## Monitoring

- **GitHub Actions tab** → see each scheduled run and its logs
- **DreamObjects panel** → watch the file count grow under `Data Set 12/`
- If a run fails (e.g. cookies expired), you'll get a GitHub email notification

---

## When cookies expire

DOJ session cookies typically last a few days to a week. When the script
hits an auth wall it exits with a clear message:

```
✗  Got HTML instead of PDF (possible auth wall) — stopping
   Refresh your DOJ_COOKIES secret and re-run.
```

When that happens:
1. Go back to Chrome, re-visit the DOJ page (re-pass the age gate if needed)
2. Copy the fresh cookie string
3. Update the `DOJ_COOKIES` secret in GitHub
4. Manually trigger the workflow again

---

## S3 layout

```
docketzero-files/
└── Data Set 12/
    ├── EFTA02730265.pdf
    ├── EFTA02730266.pdf
    └── ...
```
