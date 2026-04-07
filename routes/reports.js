const express = require('express');
const pool = require('../db/pool');
const { requireAuth } = require('../middleware/auth');
const {
  buildUsageSnapshot,
  getUserAccessProfile,
  isUnlimited,
} = require('../lib/access');

const router = express.Router();

const VALID_INVESTIGATIONS = ['epstein'];
const VALID_REPORT_TYPES = ['search_snapshot'];

function sanitizeText(value, maxLength) {
  if (typeof value !== 'string') return null;
  const trimmed = value.trim();
  if (!trimmed) return null;
  return trimmed.slice(0, maxLength);
}

function sanitizeFilters(filters) {
  if (!filters || typeof filters !== 'object' || Array.isArray(filters)) {
    return {};
  }
  return filters;
}

function sanitizeSnapshot(snapshot) {
  if (!snapshot || typeof snapshot !== 'object' || Array.isArray(snapshot)) {
    return {};
  }

  const next = {};

  if (snapshot.meta && typeof snapshot.meta === 'object' && !Array.isArray(snapshot.meta)) {
    next.meta = {
      heading: sanitizeText(snapshot.meta.heading, 200) || '',
      subheading: sanitizeText(snapshot.meta.subheading, 300) || '',
      mode: sanitizeText(snapshot.meta.mode, 50) || '',
      page_info: sanitizeText(snapshot.meta.page_info, 200) || '',
      generated_on: sanitizeText(snapshot.meta.generated_on, 100) || '',
    };
  }

  if (Array.isArray(snapshot.rows)) {
    next.rows = snapshot.rows.slice(0, 25).map((row) => ({
      title: sanitizeText(row?.title, 200) || '',
      subtitle: sanitizeText(row?.subtitle, 280) || '',
      dataset: sanitizeText(row?.dataset, 100) || '',
      source: sanitizeText(row?.source, 120) || '',
      count: sanitizeText(row?.count, 120) || '',
      href: sanitizeText(row?.href, 300) || '',
      access_label: sanitizeText(row?.access_label, 80) || '',
    }));
  } else {
    next.rows = [];
  }

  return next;
}

function rowToReport(row, usage = null) {
  const report = {
    id: row.id,
    user_id: row.user_id,
    title: row.title,
    summary: row.summary,
    investigation: row.investigation,
    report_type: row.report_type,
    query: row.query,
    dataset: row.dataset,
    filters: row.filters || {},
    result_count: row.result_count || 0,
    snapshot: row.snapshot || {},
    created_at: row.created_at,
    updated_at: row.updated_at,
  };

  if (usage) {
    report.usage = usage;
  }

  return report;
}

router.get('/me/reports', requireAuth, async (req, res) => {
  try {
    const access = await getUserAccessProfile(req.user.id);
    if (!access) {
      return res.status(404).json({ error: 'User not found.' });
    }

    const [reportsRes, countRes] = await Promise.all([
      pool.query(
        `SELECT id, user_id, title, summary, investigation, report_type, query,
                dataset, filters, result_count, snapshot, created_at, updated_at
           FROM saved_reports
          WHERE user_id = $1
          ORDER BY created_at DESC
          LIMIT 100`,
        [req.user.id]
      ),
      pool.query(
        'SELECT COUNT(*) FROM saved_reports WHERE user_id = $1',
        [req.user.id]
      ),
    ]);

    const usage = buildUsageSnapshot(access.entitlements, {
      reports: countRes.rows[0].count,
    });

    res.json({
      reports: reportsRes.rows.map((row) => rowToReport(row)),
      usage,
    });
  } catch (err) {
    console.error('[reports] GET list error:', err.message);
    res.status(500).json({ error: 'Failed to load saved reports.' });
  }
});

router.get('/me/reports/:id', requireAuth, async (req, res) => {
  const id = parseInt(req.params.id, 10);
  if (Number.isNaN(id)) {
    return res.status(400).json({ error: 'Invalid report ID.' });
  }

  try {
    const result = await pool.query(
      `SELECT id, user_id, title, summary, investigation, report_type, query,
              dataset, filters, result_count, snapshot, created_at, updated_at
         FROM saved_reports
        WHERE id = $1 AND user_id = $2`,
      [id, req.user.id]
    );

    if (!result.rows.length) {
      return res.status(404).json({ error: 'Report not found.' });
    }

    res.json({ report: rowToReport(result.rows[0]) });
  } catch (err) {
    console.error('[reports] GET detail error:', err.message);
    res.status(500).json({ error: 'Failed to load report.' });
  }
});

router.post('/me/reports', requireAuth, async (req, res) => {
  const access = await getUserAccessProfile(req.user.id);
  if (!access) {
    return res.status(404).json({ error: 'User not found.' });
  }

  if (!access.entitlements.can_create_reports) {
    return res.status(403).json({ error: 'Your account cannot create reports.' });
  }

  const title = sanitizeText(req.body.title, 200);
  if (!title) {
    return res.status(400).json({ error: 'Report title is required.' });
  }

  const summary = sanitizeText(req.body.summary, 600);
  const reportType = VALID_REPORT_TYPES.includes(req.body.report_type)
    ? req.body.report_type
    : 'search_snapshot';
  const investigation = VALID_INVESTIGATIONS.includes(req.body.investigation)
    ? req.body.investigation
    : 'epstein';
  const query = sanitizeText(req.body.query, 200);
  const dataset = sanitizeText(req.body.dataset, 50);
  const resultCount = Number.isFinite(Number(req.body.result_count))
    ? Math.max(0, parseInt(req.body.result_count, 10))
    : 0;
  const filters = sanitizeFilters(req.body.filters);
  const snapshot = sanitizeSnapshot(req.body.snapshot);

  try {
    const countRes = await pool.query(
      'SELECT COUNT(*) FROM saved_reports WHERE user_id = $1',
      [req.user.id]
    );
    const reportCount = parseInt(countRes.rows[0].count, 10);

    if (!isUnlimited(access.entitlements.report_limit) && reportCount >= access.entitlements.report_limit) {
      return res.status(400).json({
        error: `${access.entitlements.label} includes up to ${access.entitlements.report_limit} saved reports. Delete one before creating another.`,
      });
    }

    const result = await pool.query(
      `INSERT INTO saved_reports
         (user_id, title, summary, investigation, report_type, query, dataset, filters, result_count, snapshot)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
       RETURNING id, user_id, title, summary, investigation, report_type, query,
                 dataset, filters, result_count, snapshot, created_at, updated_at`,
      [
        req.user.id,
        title,
        summary,
        investigation,
        reportType,
        query,
        dataset,
        JSON.stringify(filters),
        resultCount,
        JSON.stringify(snapshot),
      ]
    );

    const usage = buildUsageSnapshot(access.entitlements, {
      reports: reportCount + 1,
    });

    res.status(201).json({
      report: rowToReport(result.rows[0], usage),
    });
  } catch (err) {
    console.error('[reports] POST error:', err.message);
    res.status(500).json({ error: 'Failed to save report.' });
  }
});

router.patch('/me/reports/:id', requireAuth, async (req, res) => {
  const id = parseInt(req.params.id, 10);
  if (Number.isNaN(id)) {
    return res.status(400).json({ error: 'Invalid report ID.' });
  }

  const updates = [];
  const values = [];

  const title = sanitizeText(req.body.title, 200);
  if (req.body.title !== undefined) {
    if (!title) {
      return res.status(400).json({ error: 'Report title is required.' });
    }
    updates.push(`title = $${updates.length + 1}`);
    values.push(title);
  }

  if (req.body.summary !== undefined) {
    updates.push(`summary = $${updates.length + 1}`);
    values.push(sanitizeText(req.body.summary, 600));
  }

  if (req.body.snapshot !== undefined) {
    updates.push(`snapshot = $${updates.length + 1}`);
    values.push(JSON.stringify(sanitizeSnapshot(req.body.snapshot)));
  }

  if (!updates.length) {
    return res.status(400).json({ error: 'No valid fields provided.' });
  }

  values.push(id, req.user.id);

  try {
    const result = await pool.query(
      `UPDATE saved_reports
          SET ${updates.join(', ')}, updated_at = NOW()
        WHERE id = $${values.length - 1} AND user_id = $${values.length}
      RETURNING id, user_id, title, summary, investigation, report_type, query,
                dataset, filters, result_count, snapshot, created_at, updated_at`,
      values
    );

    if (!result.rows.length) {
      return res.status(404).json({ error: 'Report not found.' });
    }

    res.json({ report: rowToReport(result.rows[0]) });
  } catch (err) {
    console.error('[reports] PATCH error:', err.message);
    res.status(500).json({ error: 'Failed to update report.' });
  }
});

router.delete('/me/reports/:id', requireAuth, async (req, res) => {
  const id = parseInt(req.params.id, 10);
  if (Number.isNaN(id)) {
    return res.status(400).json({ error: 'Invalid report ID.' });
  }

  try {
    const result = await pool.query(
      'DELETE FROM saved_reports WHERE id = $1 AND user_id = $2 RETURNING id',
      [id, req.user.id]
    );

    if (!result.rows.length) {
      return res.status(404).json({ error: 'Report not found.' });
    }

    res.json({ deleted: true, id });
  } catch (err) {
    console.error('[reports] DELETE error:', err.message);
    res.status(500).json({ error: 'Failed to delete report.' });
  }
});

module.exports = router;
