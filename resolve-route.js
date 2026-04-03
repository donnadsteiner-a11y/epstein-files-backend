/**
 * resolve-route.js
 * ================
 * Express route: GET /api/resolve/:efta
 *
 * Checks whether a given EFTA document is still live on the DOJ site.
 * If yes  → returns the DOJ URL (primary source)
 * If no   → returns the DreamObjects URL (preserved archive copy)
 * Logs every removal detection to PostgreSQL for provenance tracking.
 *
 * Integration — add to your main server.js / app.js:
 *
 *   const resolveRoute = require('./resolve-route');
 *   app.use('/api', resolveRoute);
 *
 * Required Railway environment variables:
 *   DO_ENDPOINT     https://s3.us-east-005.dream.io
 *   DO_ACCESS_KEY   DreamObjects access key
 *   DO_SECRET_KEY   DreamObjects secret key
 *   DO_BUCKET       docketzero-files
 *   DATABASE_URL    (already set by Railway Postgres — used for removal logging)
 *
 * npm dependencies to add:
 *   npm install @aws-sdk/client-s3 node-fetch
 *   (express and pg are already in your project)
 */

const express = require('express');
const router  = express.Router();
const { S3Client, HeadObjectCommand } = require('@aws-sdk/client-s3');
const { Pool } = require('pg');

// ── Config ────────────────────────────────────────────────────────────────────

const DO_ENDPOINT  = process.env.DO_ENDPOINT  || 'https://s3.us-east-005.dream.io';
const DO_BUCKET    = process.env.DO_BUCKET    || 'docketzero-files';
const DATABASE_URL = process.env.DATABASE_URL;

// ── S3 client (DreamObjects) ──────────────────────────────────────────────────

const s3 = new S3Client({
  endpoint:        DO_ENDPOINT,
  region:          'us-east-1',           // DreamObjects requires a region value
  credentials: {
    accessKeyId:     process.env.DO_ACCESS_KEY,
    secretAccessKey: process.env.DO_SECRET_KEY,
  },
  forcePathStyle: true,                   // Required for DreamObjects
});

// ── PostgreSQL pool (optional — for removal logging) ─────────────────────────

const db = DATABASE_URL ? new Pool({ connectionString: DATABASE_URL }) : null;

// Ensure the removals table exists on first use
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
// Maps EFTA numeric ranges to dataset number and S3 folder name.

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
// Avoids hammering DOJ or DreamObjects on repeated views of the same file.
// DOJ results cached for 1 hour; archive results cached for 24 hours.

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

// ── DOJ availability check ────────────────────────────────────────────────────

async function isOnDOJ(ds, eftaPadded) {
  const dojUrl = `https://www.justice.gov/epstein/files/DataSet%20${ds.num}/EFTA${eftaPadded}.pdf`;
  const cacheKey = `doj:${eftaPadded}`;
  const cached = getCached(cacheKey);
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

    if (res.status === 403) return null;  // rate limited — don't cache

    // DOJ returns 200 + text/html for the age gate even when the PDF is gone.
    // Only treat as live if Content-Type is actually a PDF.
    const contentType = res.headers.get('content-type') || '';
    const live = res.status === 200 && contentType.includes('pdf');

    setCache(cacheKey, live, DOJ_CACHE_TTL);
    return live;
  } catch {
    return null;  // network error — fall back to archive
  }
}

// ── DreamObjects availability check ───────────────────────────────────────────

async function isInArchive(ds, eftaPadded) {
  const s3Key   = `${ds.folder}/EFTA${eftaPadded}.pdf`;
  const cacheKey = `do:${eftaPadded}`;
  const cached  = getCached(cacheKey);
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
    // Unknown S3 error — don't cache, return null
    return null;
  }
}

// ── Log DOJ removal to DB ─────────────────────────────────────────────────────

async function logRemoval(efta, datasetNum) {
  if (!db) return;
  try {
    await ensureTable();
    // Only log if not already recorded (avoid duplicate rows per session)
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

// ── Route: GET /api/resolve/:efta ─────────────────────────────────────────────

router.get('/resolve/:efta', async (req, res) => {
  // CORS — allow docketzero.com frontend
  res.setHeader('Access-Control-Allow-Origin', 'https://docketzero.com');
  res.setHeader('Access-Control-Allow-Methods', 'GET');

  // Parse and validate EFTA parameter
  const raw      = req.params.efta.toUpperCase().replace(/^EFTA/, '');
  const eftaNum  = parseInt(raw, 10);

  if (isNaN(eftaNum) || eftaNum < 1) {
    return res.status(400).json({ error: 'Invalid EFTA number' });
  }

  const eftaPadded = String(eftaNum).padStart(8, '0');
  const ds = findDataset(eftaNum);

  if (!ds) {
    return res.status(404).json({ error: 'EFTA number outside known dataset ranges' });
  }

  const dojUrl     = `https://www.justice.gov/epstein/files/DataSet%20${ds.num}/EFTA${eftaPadded}.pdf`;
  const archiveUrl = `https://s3.us-east-005.dream.io/${DO_BUCKET}/${encodeURIComponent(ds.folder)}/EFTA${eftaPadded}.pdf`;

  // ── Step 1: Check DOJ ──────────────────────────────────────────────────────
  const onDOJ = await isOnDOJ(ds, eftaPadded);

  if (onDOJ === true) {
    // File is live on DOJ — serve from there
    return res.json({
      efta:    `EFTA${eftaPadded}`,
      dataset: ds.num,
      source:  'doj',
      url:     dojUrl,
      cached:  false,
    });
  }

  // ── Step 2: DOJ unavailable — check DreamObjects ───────────────────────────
  const inArchive = await isInArchive(ds, eftaPadded);

  if (inArchive === true) {
    // File removed from DOJ but preserved in our archive
    // Log this as a detected removal
    if (onDOJ === false) {
      logRemoval(`EFTA${eftaPadded}`, ds.num);   // async, don't await
    }

    return res.json({
      efta:    `EFTA${eftaPadded}`,
      dataset: ds.num,
      source:  'archive',
      url:     archiveUrl,
      removed_from_doj: onDOJ === false,
    });
  }

  // ── Step 3: Not found anywhere ─────────────────────────────────────────────
  // If DOJ check was inconclusive (null), still try returning DOJ URL as best guess
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
});

// ── Route: GET /api/resolve/:efta/status (for admin/audit use) ────────────────
// Returns full status without redirecting — useful for building deletion reports.

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
    status:     onDOJ === true  ? 'live'
              : onDOJ === false && inArchive ? 'removed_preserved'
              : onDOJ === false              ? 'removed_lost'
              : 'unknown',
    doj_url:     `https://www.justice.gov/epstein/files/DataSet%20${ds.num}/EFTA${eftaPadded}.pdf`,
    archive_url: `https://s3.us-east-005.dream.io/${DO_BUCKET}/${encodeURIComponent(ds.folder)}/EFTA${eftaPadded}.pdf`,
  });
});

module.exports = router;
