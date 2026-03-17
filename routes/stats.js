/**
 * Stats route
 *   GET /api/stats   — returns archive stats for home page counters
 *
 * Returns live counts from the database.
 * Falls back to hardcoded values if DB is unavailable.
 */

const express = require('express');
const pool    = require('../db/pool');

const router = express.Router();

// Cache stats for 5 minutes to avoid hammering DB on every page load
let cachedStats    = null;
let cacheTimestamp = 0;
const CACHE_TTL    = 5 * 60 * 1000; // 5 minutes

router.get('/', async (req, res) => {
  try {
    const now = Date.now();

    // Return cached stats if fresh
    if (cachedStats && (now - cacheTimestamp) < CACHE_TTL) {
      return res.json(cachedStats);
    }

    // Query DB for live counts
    const [usersResult] = await Promise.all([
      pool.query('SELECT COUNT(*) FROM users WHERE is_active = TRUE'),
    ]);

    const stats = {
      // Archive stats (static for now — will be dynamic once archive API is built)
      preserved_count:   1401320,
      removed_count:     64259,
      altered_count:     212730,
      total_documents:   null,   // populated once document indexing is live
      total_persons:     null,   // populated once entity extraction is live
      datasets_count:    12,
      // Site stats
      registered_users:  parseInt(usersResult.rows[0].count),
      // Timestamp
      last_check: {
        timestamp: new Date().toISOString(),
      },
    };

    cachedStats    = stats;
    cacheTimestamp = now;

    res.json(stats);

  } catch (err) {
    console.error('[STATS] Error:', err.message);

    // Return fallback static stats so the page still loads
    res.json({
      preserved_count: 1401320,
      removed_count:   64259,
      altered_count:   212730,
      total_documents: null,
      total_persons:   null,
      datasets_count:  12,
      last_check: {
        timestamp: new Date().toISOString(),
      },
    });
  }
});

module.exports = router;
