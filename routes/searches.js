/**
 * routes/searches.js
 * ==================
 * Saved searches API for DocketZero dashboard.
 *
 * Endpoints:
 *   GET    /api/users/me/saved-searches                    — list (optionally filtered by investigation)
 *   GET    /api/users/me/saved-searches/grouped            — list grouped by investigation
 *   POST   /api/users/me/saved-searches                    — save a new search
 *   DELETE /api/users/me/saved-searches/:id                — delete a saved search
 *   PATCH  /api/users/me/saved-searches/:id/pin            — toggle pin
 *   PATCH  /api/users/me/saved-searches/:id/investigation  — move to different investigation
 *
 * Mount in server.js:
 *   const searchesRoute = require('./routes/searches');
 *   app.use('/api/users', searchesRoute);
 */

const express         = require('express');
const router          = express.Router();
const pool            = require('../db/pool');
const { requireAuth } = require('../middleware/auth');
const {
  buildUsageSnapshot,
  getUserAccessProfile,
  isUnlimited,
} = require('../lib/access');

// Valid investigation slugs — extend as new investigations are added
const VALID_INVESTIGATIONS = ['epstein'];

// ── GET /api/users/me/saved-searches ─────────────────────────────────────────
// Optional query param: ?investigation=epstein
router.get('/me/saved-searches', requireAuth, async (req, res) => {
  const { investigation } = req.query;

  try {
    const params = [req.user.id];
    let where = 'WHERE user_id = $1';

    if (investigation) {
      params.push(investigation);
      where += ` AND investigation = $${params.length}`;
    }

    const result = await pool.query(
      `SELECT id, name, query, dataset, filters, result_count,
              pinned, investigation, created_at, updated_at
       FROM saved_searches
       ${where}
       ORDER BY pinned DESC, created_at DESC
       LIMIT 50`,
      params
    );
    res.json(result.rows);
  } catch (err) {
    console.error('[searches] GET error:', err.message);
    res.status(500).json({ error: 'Failed to load saved searches' });
  }
});

// ── GET /api/users/me/saved-searches/grouped ─────────────────────────────────
// Returns searches grouped by investigation:
// { epstein: [...], maxwell: [...], ... }
router.get('/me/saved-searches/grouped', requireAuth, async (req, res) => {
  try {
    const result = await pool.query(
      `SELECT id, name, query, dataset, filters, result_count,
              pinned, investigation, created_at, updated_at
       FROM saved_searches
       WHERE user_id = $1
       ORDER BY investigation ASC, pinned DESC, created_at DESC
       LIMIT 50`,
      [req.user.id]
    );

    // Group by investigation
    const grouped = {};
    for (const row of result.rows) {
      const inv = row.investigation || 'epstein';
      if (!grouped[inv]) grouped[inv] = [];
      grouped[inv].push(row);
    }

    // Return metadata alongside groups
    res.json({
      total:  result.rows.length,
      groups: grouped,
      // Investigation display metadata (add new investigations here)
      investigations: {
        epstein: { label: 'Epstein Files', icon: '⚖️', href: '/epstein.html' },
      },
    });
  } catch (err) {
    console.error('[searches] GET grouped error:', err.message);
    res.status(500).json({ error: 'Failed to load grouped searches' });
  }
});

// ── POST /api/users/me/saved-searches ────────────────────────────────────────
router.post('/me/saved-searches', requireAuth, async (req, res) => {
  const { name, query, dataset, filters, result_count, investigation } = req.body;

  if (!name || typeof name !== 'string' || !name.trim()) {
    return res.status(400).json({ error: 'Search name is required' });
  }

  const inv = VALID_INVESTIGATIONS.includes(investigation) ? investigation : 'epstein';

  try {
    const access = await getUserAccessProfile(req.user.id);
    if (!access) {
      return res.status(404).json({ error: 'User not found.' });
    }

    if (!access.entitlements.can_save_searches) {
      return res.status(403).json({ error: 'Your account cannot save searches.' });
    }

    const countRes = await pool.query(
      'SELECT COUNT(*) FROM saved_searches WHERE user_id = $1',
      [req.user.id]
    );
    const savedSearchCount = parseInt(countRes.rows[0].count, 10);

    if (!isUnlimited(access.entitlements.saved_search_limit) && savedSearchCount >= access.entitlements.saved_search_limit) {
      return res.status(400).json({
        error: `${access.entitlements.label} includes up to ${access.entitlements.saved_search_limit} saved searches. Delete one before saving another.`,
      });
    }

    const result = await pool.query(
      `INSERT INTO saved_searches
         (user_id, name, query, dataset, filters, result_count, investigation)
       VALUES ($1, $2, $3, $4, $5, $6, $7)
       RETURNING id, name, query, dataset, filters, result_count,
                 pinned, investigation, created_at`,
      [
        req.user.id,
        name.trim().slice(0, 200),
        query        || null,
        dataset      || null,
        JSON.stringify(filters || {}),
        result_count || 0,
        inv,
      ]
    );
    res.status(201).json({
      ...result.rows[0],
      usage: buildUsageSnapshot(access.entitlements, {
        saved_searches: savedSearchCount + 1,
      }),
    });
  } catch (err) {
    console.error('[searches] POST error:', err.message);
    res.status(500).json({ error: 'Failed to save search' });
  }
});

// ── DELETE /api/users/me/saved-searches/:id ──────────────────────────────────
router.delete('/me/saved-searches/:id', requireAuth, async (req, res) => {
  const id = parseInt(req.params.id);
  if (isNaN(id)) return res.status(400).json({ error: 'Invalid search ID' });

  try {
    const result = await pool.query(
      'DELETE FROM saved_searches WHERE id = $1 AND user_id = $2 RETURNING id',
      [id, req.user.id]
    );
    if (!result.rows.length) {
      return res.status(404).json({ error: 'Search not found' });
    }
    res.json({ deleted: true, id });
  } catch (err) {
    console.error('[searches] DELETE error:', err.message);
    res.status(500).json({ error: 'Failed to delete search' });
  }
});

// ── PATCH /api/users/me/saved-searches/:id/pin ───────────────────────────────
router.patch('/me/saved-searches/:id/pin', requireAuth, async (req, res) => {
  const id = parseInt(req.params.id);
  if (isNaN(id)) return res.status(400).json({ error: 'Invalid search ID' });

  try {
    const result = await pool.query(
      `UPDATE saved_searches
       SET pinned = NOT pinned, updated_at = NOW()
       WHERE id = $1 AND user_id = $2
       RETURNING id, pinned`,
      [id, req.user.id]
    );
    if (!result.rows.length) {
      return res.status(404).json({ error: 'Search not found' });
    }
    res.json(result.rows[0]);
  } catch (err) {
    console.error('[searches] PATCH pin error:', err.message);
    res.status(500).json({ error: 'Failed to update pin' });
  }
});

// ── PATCH /api/users/me/saved-searches/:id/investigation ─────────────────────
// Move a search to a different investigation
router.patch('/me/saved-searches/:id/investigation', requireAuth, async (req, res) => {
  const id  = parseInt(req.params.id);
  if (isNaN(id)) return res.status(400).json({ error: 'Invalid search ID' });

  const { investigation } = req.body;
  if (!VALID_INVESTIGATIONS.includes(investigation)) {
    return res.status(400).json({
      error: `Invalid investigation. Valid values: ${VALID_INVESTIGATIONS.join(', ')}`,
    });
  }

  try {
    const result = await pool.query(
      `UPDATE saved_searches
       SET investigation = $1, updated_at = NOW()
       WHERE id = $2 AND user_id = $3
       RETURNING id, investigation`,
      [investigation, id, req.user.id]
    );
    if (!result.rows.length) {
      return res.status(404).json({ error: 'Search not found' });
    }
    res.json(result.rows[0]);
  } catch (err) {
    console.error('[searches] PATCH investigation error:', err.message);
    res.status(500).json({ error: 'Failed to update investigation' });
  }
});

module.exports = router;
