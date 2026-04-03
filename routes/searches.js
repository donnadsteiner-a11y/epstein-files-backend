/**
 * routes/searches.js
 * ==================
 * Saved searches API for DocketZero dashboard.
 *
 * Endpoints:
 *   GET    /api/users/me/saved-searches          — list user's saved searches
 *   POST   /api/users/me/saved-searches          — save a new search
 *   DELETE /api/users/me/saved-searches/:id      — delete a saved search
 *   PATCH  /api/users/me/saved-searches/:id/pin  — toggle pin
 *
 * Mount in server.js:
 *   const searchesRoute = require('./routes/searches');
 *   app.use('/api/users', searchesRoute);
 */

const express      = require('express');
const router       = express.Router();
const pool         = require('../db/pool');
const { requireAuth } = require('../middleware/auth');

// ── GET /api/users/me/saved-searches ─────────────────────────────────────────
router.get('/me/saved-searches', requireAuth, async (req, res) => {
  try {
    const result = await pool.query(
      `SELECT id, name, query, dataset, filters, result_count, pinned, created_at
       FROM saved_searches
       WHERE user_id = $1
       ORDER BY pinned DESC, created_at DESC
       LIMIT 50`,
      [req.user.id]
    );
    res.json(result.rows);
  } catch (err) {
    console.error('[searches] GET error:', err.message);
    res.status(500).json({ error: 'Failed to load saved searches' });
  }
});

// ── POST /api/users/me/saved-searches ────────────────────────────────────────
router.post('/me/saved-searches', requireAuth, async (req, res) => {
  const { name, query, dataset, filters, result_count } = req.body;

  if (!name || typeof name !== 'string' || !name.trim()) {
    return res.status(400).json({ error: 'Search name is required' });
  }

  try {
    // Cap at 50 saved searches per user
    const countRes = await pool.query(
      'SELECT COUNT(*) FROM saved_searches WHERE user_id = $1',
      [req.user.id]
    );
    if (parseInt(countRes.rows[0].count) >= 50) {
      return res.status(400).json({ error: 'Maximum 50 saved searches reached. Please delete some to save new ones.' });
    }

    const result = await pool.query(
      `INSERT INTO saved_searches (user_id, name, query, dataset, filters, result_count)
       VALUES ($1, $2, $3, $4, $5, $6)
       RETURNING id, name, query, dataset, filters, result_count, pinned, created_at`,
      [
        req.user.id,
        name.trim().slice(0, 200),
        query  || null,
        dataset || null,
        JSON.stringify(filters || {}),
        result_count || 0,
      ]
    );
    res.status(201).json(result.rows[0]);
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

module.exports = router;
