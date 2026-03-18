#!/usr/bin/env node
/**
 * doj-sync.js
 * -----------
 * Crawls a DOJ Epstein dataset listing page, compares against DreamObjects
 * bucket, and downloads + uploads any missing files.
 *
 * Designed to run inside GitHub Actions where the IP is not flagged by Akamai.
 *
 * Env vars:
 *   DATASET        — dataset number 1-12
 *   DO_ACCESS_KEY  — DreamObjects access key
 *   DO_SECRET_KEY  — DreamObjects secret key
 *   DO_BUCKET      — bucket name (default: docketzero-files)
 *   DO_ENDPOINT    — S3 endpoint (default: https://s3.us-east-005.dream.io)
 *   DRY_RUN        — if "true", report only, no downloads
 */

const fetch = require('node-fetch');
const { S3Client, ListObjectsV2Command, PutObjectCommand, HeadObjectCommand } = require('@aws-sdk/client-s3');

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------
const DATASET    = parseInt(process.env.DATASET || '1', 10);
const BUCKET     = process.env.DO_BUCKET    || 'docketzero-files';
const ENDPOINT   = process.env.DO_ENDPOINT  || 'https://s3.us-east-005.dream.io';
const DRY_RUN    = process.env.DRY_RUN === 'true';
const DOJ_BASE   = 'https://www.justice.gov';
const BUCKET_FOLDER = `Data Set ${DATASET}`;

// Rotate User-Agent strings to reduce bot detection
const USER_AGENTS = [
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
  'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
  'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
];

const s3 = new S3Client({
  region: 'us-east-1',
  endpoint: ENDPOINT,
  credentials: {
    accessKeyId: process.env.DO_ACCESS_KEY,
    secretAccessKey: process.env.DO_SECRET_KEY,
  },
  forcePathStyle: true,
});

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------
function sleep(ms) {
  return new Promise(r => setTimeout(r, ms));
}

function randomUA() {
  return USER_AGENTS[Math.floor(Math.random() * USER_AGENTS.length)];
}

async function fetchWithRetry(url, attempt = 1) {
  try {
    const res = await fetch(url, {
      headers: {
        'User-Agent': randomUA(),
        'Accept': 'text/html,application/xhtml+xml',
        'Accept-Language': 'en-US,en;q=0.9',
        'Cache-Control': 'no-cache',
        'Referer': 'https://www.justice.gov/epstein/doj-disclosures',
        // Age gate cookie — DOJ sets this when user clicks "Yes" on the age verification modal
        'Cookie': 'age_verified=1; EFTA_age_gate=1; Drupal.visitor.doj_age_gate=1',
      },
      timeout: 30000,
    });
    if (res.status === 403 || res.status === 429) {
      throw new Error(`HTTP ${res.status} — blocked`);
    }
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    return res;
  } catch (err) {
    if (attempt < 4) {
      const delay = attempt * 3000;
      console.log(`  Retry ${attempt}/3 in ${delay}ms: ${err.message}`);
      await sleep(delay);
      return fetchWithRetry(url, attempt + 1);
    }
    throw err;
  }
}

// Extract EFTA numbers from HTML text
function extractEFTAs(html) {
  const matches = html.match(/EFTA\d{8}/g);
  return matches ? [...new Set(matches)] : [];
}

// ---------------------------------------------------------------------------
// Step 1: Crawl DOJ listing pages for this dataset
// ---------------------------------------------------------------------------
async function crawlDOJ() {
  console.log(`\n=== Crawling DOJ DataSet ${DATASET} ===`);
  const allEFTAs = new Set();
  let page = 0;
  let emptyPages = 0;

  while (emptyPages < 2) {
    const url = `${DOJ_BASE}/epstein/doj-disclosures/data-set-${DATASET}-files?page=${page}`;
    console.log(`  Fetching page ${page}...`);

    try {
      const res  = await fetchWithRetry(url);
      const html = await res.text();
      const eftas = extractEFTAs(html);

      // Detect age gate — if page contains this, the cookie didn't work
      if (html.includes('Are you 18 years of age') && eftas.length === 0) {
        console.log(`  WARNING: Age gate detected on page ${page} — cookie bypass failed`);
        console.log(`  This dataset may return 0 results. Check cookie name in script.`);
      }

      if (eftas.length === 0) {
        emptyPages++;
        console.log(`  Page ${page}: 0 files (empty page ${emptyPages}/2)`);
      } else {
        emptyPages = 0;
        eftas.forEach(e => allEFTAs.add(e));
        console.log(`  Page ${page}: ${eftas.length} files (running total: ${allEFTAs.size})`);
      }
    } catch (err) {
      console.log(`  Page ${page} FAILED: ${err.message}`);
      emptyPages++;
    }

    page++;
    await sleep(1500);
  }

  console.log(`\nDOJ total for DataSet ${DATASET}: ${allEFTAs.size} files`);
  return [...allEFTAs];
}

// ---------------------------------------------------------------------------
// Step 2: List existing files in DreamObjects bucket for this dataset
// ---------------------------------------------------------------------------
async function listBucket() {
  console.log(`\n=== Listing bucket: ${BUCKET_FOLDER} ===`);
  const keys = new Set();
  let token  = undefined;
  let pages  = 0;

  do {
    const res = await s3.send(new ListObjectsV2Command({
      Bucket: BUCKET,
      Prefix: `${BUCKET_FOLDER}/`,
      MaxKeys: 1000,
      ContinuationToken: token,
    }));
    pages++;
    for (const obj of (res.Contents || [])) {
      const filename = obj.Key.split('/').pop();
      if (filename) keys.add(filename.replace('.pdf', '').toUpperCase());
    }
    token = res.IsTruncated ? res.NextContinuationToken : undefined;
    if (pages % 100 === 0) console.log(`  Listed ${keys.size} bucket objects so far...`);
  } while (token);

  console.log(`Bucket total for ${BUCKET_FOLDER}: ${keys.size} files`);
  return keys;
}

// ---------------------------------------------------------------------------
// Step 3: Download missing file from DOJ and upload to DreamObjects
// ---------------------------------------------------------------------------
async function downloadAndUpload(efta) {
  const filename  = `${efta}.pdf`;
  const dojUrl    = `${DOJ_BASE}/epstein/files/DataSet%20${DATASET}/${filename}`;
  const bucketKey = `${BUCKET_FOLDER}/${filename}`;

  try {
    const res = await fetchWithRetry(dojUrl);
    const buffer = await res.buffer();

    if (buffer.length < 500) {
      console.log(`  SKIP ${filename} — suspiciously small (${buffer.length} bytes), may be error page`);
      return 'skipped';
    }

    await s3.send(new PutObjectCommand({
      Bucket: BUCKET,
      Key: bucketKey,
      Body: buffer,
      ContentType: 'application/pdf',
    }));

    console.log(`  OK ${filename} (${(buffer.length / 1024).toFixed(1)} KB)`);
    return 'ok';
  } catch (err) {
    console.log(`  FAILED ${filename}: ${err.message}`);
    return 'failed';
  }
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------
async function main() {
  console.log(`DocketZero DOJ Sync — DataSet ${DATASET}`);
  console.log(`Bucket: ${BUCKET} / ${BUCKET_FOLDER}`);
  console.log(`Dry run: ${DRY_RUN}`);
  console.log('');

  // Crawl DOJ
  const dojEFTAs = await crawlDOJ();

  // List bucket
  const bucketEFTAs = await listBucket();

  // Diff
  const missing    = dojEFTAs.filter(e => !bucketEFTAs.has(e.toUpperCase()));
  const preserved  = [...bucketEFTAs].filter(e => !dojEFTAs.map(x => x.toUpperCase()).includes(e));

  console.log(`\n=== DataSet ${DATASET} Report ===`);
  console.log(`  DOJ has:                  ${dojEFTAs.length}`);
  console.log(`  Bucket has:               ${bucketEFTAs.size}`);
  console.log(`  Missing from bucket:      ${missing.length}`);
  console.log(`  Preserved (DOJ removed):  ${preserved.length}`);

  // Write summary to GitHub Actions step summary
  const summary = [
    `## DataSet ${DATASET} Sync Report`,
    `| | Count |`,
    `|---|---|`,
    `| DOJ files | ${dojEFTAs.length} |`,
    `| Bucket files | ${bucketEFTAs.size} |`,
    `| Missing from bucket | ${missing.length} |`,
    `| Preserved (DOJ removed) | ${preserved.length} |`,
  ].join('\n');

  const fs = require('fs');
  if (process.env.GITHUB_STEP_SUMMARY) {
    fs.appendFileSync(process.env.GITHUB_STEP_SUMMARY, summary + '\n\n');
  }

  if (missing.length === 0) {
    console.log(`\n✓ DataSet ${DATASET} is complete — nothing to download.`);
    return;
  }

  if (DRY_RUN) {
    console.log(`\nDRY RUN — would download ${missing.length} files:`);
    missing.forEach(e => console.log(`  ${e}.pdf`));
    return;
  }

  // Download missing files with concurrency limit
  console.log(`\nDownloading ${missing.length} missing files...`);
  const CONCURRENCY = 5;
  const results = { ok: 0, failed: 0, skipped: 0 };
  const failedFiles = [];

  for (let i = 0; i < missing.length; i += CONCURRENCY) {
    const batch = missing.slice(i, i + CONCURRENCY);
    const batchResults = await Promise.all(batch.map(e => downloadAndUpload(e)));
    batchResults.forEach((r, j) => {
      results[r]++;
      if (r === 'failed') failedFiles.push(batch[j]);
    });
    await sleep(500);
  }

  console.log(`\n=== DataSet ${DATASET} Done ===`);
  console.log(`  Downloaded: ${results.ok}`);
  console.log(`  Skipped:    ${results.skipped}`);
  console.log(`  Failed:     ${results.failed}`);

  if (failedFiles.length > 0) {
    console.log(`\nFailed files (may have been deleted from DOJ):`);
    failedFiles.forEach(e => console.log(`  ${e}.pdf`));
    fs.writeFileSync(`ds${DATASET}-failed.txt`, failedFiles.join('\n') + '\n');
  }
}

main().catch(err => {
  console.error('Fatal:', err);
  process.exit(1);
});
