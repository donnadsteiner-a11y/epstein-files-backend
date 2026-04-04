/**
 * resolve-route.js
 * ================
 * Express route: GET /api/resolve/:efta
 *
 * Checks whether a given EFTA document is still live on the DOJ site.
 * If yes  → returns the DOJ URL (primary source)
 * If no   → returns a pre-signed DreamObjects URL (private bucket, 1hr expiry)
 * Logs every removal detection to PostgreSQL for provenance tracking.
 *
 * Scraper protections:
 *   - Tight per-IP rate limit (20 req / 15 min anonymous, 60 authenticated)
 *   - Sequential EFTA detection (blocks IPs requesting sequential IDs)
 *   - Pre-signed URLs expire after 1 hour (useless to share/scrape)
 *   - Direct S3 URLs never exposed (bucket is private)
 */

const express    = require('express');
const router     = express.Router();
const rateLimit  = require('express-rate-limit');
const { S3Client, HeadObjectCommand } = require('@aws-sdk/client-s3');
const crypto = require('crypto');
const { Pool } = require('pg');

// ── Config ────────────────────────────────────────────────────────────────────
const DO_ENDPOINT  = process.env.DO_ENDPOINT  || 'https://s3.us-east-005.dream.io';
const DO_BUCKET    = process.env.DO_BUCKET    || 'docketzero-files';
const DATABASE_URL = process.env.DATABASE_URL;

// Pre-signed URL expiry — 1 hour. Short enough to limit sharing, long enough
// for a researcher to open and read a document.
const SIGNED_URL_EXPIRY_SECONDS = 3600;

// ── S3 client (DreamObjects) ──────────────────────────────────────────────────
const s3 = new S3Client({
  endpoint:    DO_ENDPOINT,
  region:      'us-east-1',
  credentials: {
    accessKeyId:     process.env.DO_ACCESS_KEY,
    secretAccessKey: process.env.DO_SECRET_KEY,
  },
  forcePathStyle: true,
});

// ── PostgreSQL ────────────────────────────────────────────────────────────────
const db = DATABASE_URL ? new Pool({ connectionString: DATABASE_URL }) : null;
let tableReady = false;

async function ensureTable() {
  if (!db || tableReady) return;
  await db.query(`
    CREATE TABLE IF NOT EXISTS doj_removals (
      id           SERIAL PRIMARY KEY,
      efta         VARCHAR(20)  NOT NULL,
      dataset      INTEGER      NOT NULL,
      detected_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
      served_from  VARCHAR(10)  NOT NULL DEFAULT 'archive'
    );
    CREATE INDEX IF NOT EXISTS idx_doj_removals_efta ON doj_removals(efta);
  `);
  tableReady = true;
}

// ── Dataset registry ──────────────────────────────────────────────────────────
const DATASETS = [
  { num: 1,  start: 1,       end: 3158,    folder: 'Data Set 1'  },
  { num: 2,  start: 3159,    end: 3857,    folder: 'Data Set 2'  },
  { num: 3,  start: 3858,    end: 5586,    folder: 'Data Set 3'  },
  { num: 4,  start: 5705,    end: 8320,    folder: 'Data Set 4'  },
  { num: 5,  start: 8409,    end: 8528,    folder: 'Data Set 5'  },
  { num: 6,  start: 8529,    end: 8998,    folder: 'Data Set 6'  },
  { num: 7,  start: 9016,    end: 9664,    folder: 'Data Set 7'  },
  { num: 8,  start: 9676,    end: 39023,   folder: 'Data Set 8'  },
  { num: 9,  start: 39025,   end: 1262781, folder: 'Data Set 9'  },
  { num: 10, start: 1262782, end: 2205654, folder: 'Data Set 10' },
  { num: 11, start: 2205655, end: 2730264, folder: 'Data Set 11' },
  { num: 12, start: 2730265, end: 2858497, folder: 'Data Set 12' },
];

function findDataset(eftaNum) {
  return DATASETS.find(ds => eftaNum >= ds.start && eftaNum <= ds.end) || null;
}

// ── In-memory cache ───────────────────────────────────────────────────────────
const cache = new Map();
const DOJ_CACHE_TTL     = 60 * 60 * 1000;        // 1 hour
const ARCHIVE_CACHE_TTL = 24 * 60 * 60 * 1000;   // 24 hours

function getCached(key) {
  const entry = cache.get(key);
  if (!entry) return null;
  if (Date.now() > entry.expires) { cache.delete(key); return null; }
  return entry.value;
}
function setCache(key, value, ttl) {
  cache.set(key, { value, expires: Date.now() + ttl });
}

// ── Scraper detection — sequential EFTA tracking ─────────────────────────────
// Tracks recent EFTA requests per IP. If an IP requests N sequential EFTAs
// within a short window, it's almost certainly a scraper.

const SCRAPER_WINDOW_MS  = 5 * 60 * 1000;  // 5 minute window
const SCRAPER_SEQ_THRESH = 8;               // 8+ sequential requests = block
const ipEftaHistory      = new Map();       // ip -> [{ num, ts }, ...]

function isLikelyScraper(ip, eftaNum) {
  const now = Date.now();
  if (!ipEftaHistory.has(ip)) ipEftaHistory.set(ip, []);

  const history = ipEftaHistory.get(ip)
    .filter(e => now - e.ts < SCRAPER_WINDOW_MS);  // trim old entries

  history.push({ num: eftaNum, ts: now });
  ipEftaHistory.set(ip, history.slice(-20));        // keep last 20

  if (history.length < SCRAPER_SEQ_THRESH) return false;

  // Check if the last N requests are numerically sequential (ascending or descending)
  const recent  = history.slice(-SCRAPER_SEQ_THRESH).map(e => e.num);
  const sorted  = [...recent].sort((a, b) => a - b);
  let sequential = 0;
  for (let i = 1; i < sorted.length; i++) {
    if (sorted[i] - sorted[i - 1] <= 5) sequential++;  // allow small gaps
  }
  return sequential >= SCRAPER_SEQ_THRESH - 2;
}

// ── Rate limiters ─────────────────────────────────────────────────────────────

// Anonymous: 20 resolve requests per 15 minutes per IP
const anonymousResolveLimit = rateLimit({
  windowMs: 15 * 60 * 1000,
  max: 20,
  standardHeaders: true,
  legacyHeaders: false,
  keyGenerator: (req) => req.ip,
  skip: (req) => !!req.headers.authorization,  // skip if authenticated
  message: {
    error: 'Too many document requests. Please wait 15 minutes or create a free account for higher limits.',
    upgrade_url: 'https://docketzero.com/index.html#signup',
  },
});

// Authenticated: 60 resolve requests per 15 minutes per IP
const authenticatedResolveLimit = rateLimit({
  windowMs: 15 * 60 * 1000,
  max: 60,
  standardHeaders: true,
  legacyHeaders: false,
  keyGenerator: (req) => req.ip,
  skip: (req) => !req.headers.authorization,  // skip if anonymous (handled above)
  message: {
    error: 'Too many document requests. Please wait 15 minutes.',
  },
});

// ── DOJ availability check ────────────────────────────────────────────────────
async function isOnDOJ(ds, eftaPadded) {
  const dojUrl  = `https://www.justice.gov/epstein/files/DataSet%20${ds.num}/EFTA${eftaPadded}.pdf`;
  const cacheKey = `doj:${eftaPadded}`;
  const cached  = getCached(cacheKey);
  if (cached !== null) return cached;

  try {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), 8000);
    const res = await fetch(dojUrl, {
      method: 'HEAD',
      headers: {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Accept':     'application/pdf,*/*',
      },
      redirect: 'follow',
      signal:   controller.signal,
    });
    clearTimeout(timer);

    if (res.status === 403) return null;

    const contentType = res.headers.get('content-type') || '';
    const live = res.status === 200 && contentType.includes('pdf');
    setCache(cacheKey, live, DOJ_CACHE_TTL);
    return live;
  } catch {
    return null;
  }
}

// ── DreamObjects availability check ──────────────────────────────────────────
async function isInArchive(ds, eftaPadded) {
  const s3Key    = `${ds.folder}/EFTA${eftaPadded}.pdf`;
  const cacheKey = `do:${eftaPadded}`;
  const cached   = getCached(cacheKey);
  if (cached !== null) return cached;

  try {
    await s3.send(new HeadObjectCommand({ Bucket: DO_BUCKET, Key: s3Key }));
    setCache(cacheKey, true, ARCHIVE_CACHE_TTL);
    return true;
  } catch (err) {
    if (err.name === 'NotFound' || err.$metadata?.httpStatusCode === 404) {
      setCache(cacheKey, false, ARCHIVE_CACHE_TTL);
      return false;
    }
    return null;
  }
}

// ── Generate pre-signed URL — DreamObjects v2 signature (HMAC-SHA1) ──────────
// DreamObjects does not support AWS SDK v4 pre-signed URLs. We use the older
// v2 query-string signing method (HMAC-SHA1) which DreamObjects fully supports.
// Note: DreamObjects prohibits response-content-disposition and
// response-content-type override params on pre-signed requests.
function getPresignedUrl(ds, eftaPadded) {
  const s3Key   = `${ds.folder}/EFTA${eftaPadded}.pdf`;
  const expires = Math.floor(Date.now() / 1000) + SIGNED_URL_EXPIRY_SECONDS;
  const bucket  = DO_BUCKET;

  // String to sign: METHOD\nContent-MD5\nContent-Type\nExpires\n/bucket/key
  const stringToSign = [
    'GET',
    '',   // Content-MD5 (empty)
    '',   // Content-Type (empty for GET)
    expires,
    `/${bucket}/${s3Key}`,
  ].join('\n');

  const signature = crypto
    .createHmac('sha1', process.env.DO_SECRET_KEY)
    .update(stringToSign)
    .digest('base64');

  // Build URL — no response-content-* overrides, DreamObjects prohibits them
  const encodedKey = s3Key.split('/').map(p => encodeURIComponent(p)).join('/');
  const params = new URLSearchParams({
    AWSAccessKeyId: process.env.DO_ACCESS_KEY,
    Expires:        expires,
    Signature:      signature,
  });

  return `${DO_ENDPOINT}/${bucket}/${encodedKey}?${params.toString()}`;
}

// ── Log DOJ removal ───────────────────────────────────────────────────────────
async function logRemoval(efta, datasetNum) {
  if (!db) return;
  try {
    await ensureTable();
    await db.query(
      `INSERT INTO doj_removals (efta, dataset, served_from)
       SELECT $1, $2, 'archive'
       WHERE NOT EXISTS (
         SELECT 1 FROM doj_removals
         WHERE efta = $1
         AND detected_at > NOW() - INTERVAL '24 hours'
       )`,
      [efta, datasetNum]
    );
  } catch (err) {
    console.error('[resolve] DB log error:', err.message);
  }
}

// ── GET /api/resolve/:efta ────────────────────────────────────────────────────
router.get(
  '/resolve/:efta',
  anonymousResolveLimit,
  authenticatedResolveLimit,
  async (req, res) => {
    res.setHeader('Access-Control-Allow-Origin', 'https://docketzero.com');
    res.setHeader('Access-Control-Allow-Methods', 'GET');

    const ip       = req.ip;
    const raw      = req.params.efta.toUpperCase().replace(/^EFTA/, '');
    const eftaNum  = parseInt(raw, 10);

    if (isNaN(eftaNum) || eftaNum < 1) {
      return res.status(400).json({ error: 'Invalid EFTA number' });
    }

    // ── Scraper detection ────────────────────────────────────────────────────
    if (isLikelyScraper(ip, eftaNum)) {
      console.warn(`[resolve] Scraper detected: IP ${ip} requesting sequential EFTAs`);
      return res.status(429).json({
        error: 'Automated access detected. DocketZero is for human researchers. Please contact us if you have a legitimate research need.',
        contact: 'https://docketzero.com/contact.html',
      });
    }

    const eftaPadded = String(eftaNum).padStart(8, '0');
    const ds         = findDataset(eftaNum);

    if (!ds) {
      return res.status(404).json({ error: 'EFTA number outside known dataset ranges' });
    }

    const dojUrl = `https://www.justice.gov/epstein/files/DataSet%20${ds.num}/EFTA${eftaPadded}.pdf`;

    // ── Step 1: Check DOJ ────────────────────────────────────────────────────
    const onDOJ = await isOnDOJ(ds, eftaPadded);

    if (onDOJ === true) {
      return res.json({
        efta:    `EFTA${eftaPadded}`,
        dataset: ds.num,
        source:  'doj',
        url:     dojUrl,
      });
    }

    // ── Step 2: Check DreamObjects ───────────────────────────────────────────
    const inArchive = await isInArchive(ds, eftaPadded);

    if (inArchive === true) {
      if (onDOJ === false) {
        logRemoval(`EFTA${eftaPadded}`, ds.num);
      }

      // Generate a pre-signed URL — expires in 1 hour, no direct S3 exposure
      let signedUrl;
      try {
        signedUrl = getPresignedUrl(ds, eftaPadded);
      } catch (err) {
        console.error('[resolve] Pre-sign error:', err.message);
        return res.status(500).json({ error: 'Could not generate archive URL' });
      }

      return res.json({
        efta:             `EFTA${eftaPadded}`,
        dataset:          ds.num,
        source:           'archive',
        url:              signedUrl,
        url_expires_in:   SIGNED_URL_EXPIRY_SECONDS,
        removed_from_doj: onDOJ === false,
      });
    }

    // ── Step 3: DOJ inconclusive — return DOJ URL as best guess ─────────────
    if (onDOJ === null) {
      return res.json({
        efta:    `EFTA${eftaPadded}`,
        dataset: ds.num,
        source:  'doj',
        url:     dojUrl,
        note:    'DOJ status unknown — serving DOJ URL as fallback',
      });
    }

    return res.status(404).json({
      error:   'File not found on DOJ or in DocketZero archive',
      efta:    `EFTA${eftaPadded}`,
      dataset: ds.num,
    });
  }
);

// ── GET /api/resolve/:efta/status (admin/audit) ───────────────────────────────
router.get('/resolve/:efta/status', async (req, res) => {
  res.setHeader('Access-Control-Allow-Origin', 'https://docketzero.com');

  const raw     = req.params.efta.toUpperCase().replace(/^EFTA/, '');
  const eftaNum = parseInt(raw, 10);
  if (isNaN(eftaNum)) return res.status(400).json({ error: 'Invalid EFTA' });

  const eftaPadded = String(eftaNum).padStart(8, '0');
  const ds = findDataset(eftaNum);
  if (!ds) return res.status(404).json({ error: 'Outside known ranges' });

  const [onDOJ, inArchive] = await Promise.all([
    isOnDOJ(ds, eftaPadded),
    isInArchive(ds, eftaPadded),
  ]);

  res.json({
    efta:       `EFTA${eftaPadded}`,
    dataset:    ds.num,
    on_doj:     onDOJ,
    in_archive: inArchive,
    status:     onDOJ === true          ? 'live'
              : onDOJ === false && inArchive ? 'removed_preserved'
              : onDOJ === false              ? 'removed_lost'
              : 'unknown',
    doj_url:    `https://www.justice.gov/epstein/files/DataSet%20${ds.num}/EFTA${eftaPadded}.pdf`,
  });
});

module.exports = router;
