/**
 * Auth routes
 *   POST /api/auth/register   — create account
 *   POST /api/auth/login      — sign in
 *   PATCH /api/auth/me        — update age_verified, disclaimer_accepted, role etc.
 *   GET  /api/auth/me         — get current user profile
 */

const express    = require('express');
const bcrypt     = require('bcrypt');
const jwt        = require('jsonwebtoken');
const rateLimit  = require('express-rate-limit');
const pool       = require('../db/pool');
const { requireAuth } = require('../middleware/auth');

const router = express.Router();

// Tighter rate limit on auth endpoints — 10 attempts per 15 min
const authLimit = rateLimit({
  windowMs: 15 * 60 * 1000,
  max: 10,
  message: { error: 'Too many attempts. Please wait 15 minutes and try again.' },
  skipSuccessfulRequests: true,
});

// ── Helpers ───────────────────────────────────────────────────────────────────

function isValidEmail(email) {
  return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test((email || '').trim());
}

function generateAccessToken(user) {
  return jwt.sign(
    {
      id:         user.id,
      email:      user.email,
      first_name: user.first_name,
      last_name:  user.last_name,
    },
    process.env.JWT_SECRET,
    { expiresIn: process.env.JWT_EXPIRES_IN || '7d' }
  );
}

function safeUser(row) {
  return {
    id:                  row.id,
    email:               row.email,
    first_name:          row.first_name,
    last_name:           row.last_name,
    role:                row.role,
    interest:            row.interest,
    referral_source:     row.referral_source,
    age_verified:        row.age_verified,
    disclaimer_accepted: row.disclaimer_accepted,
    created_at:          row.created_at,
  };
}

// ── POST /api/auth/register ───────────────────────────────────────────────────
router.post('/register', authLimit, async (req, res) => {
  try {
    const {
      first_name, last_name, email, password,
      role, interest, referral_source,
    } = req.body;

    // Validate required fields
    if (!first_name?.trim()) return res.status(400).json({ error: 'First name is required.' });
    if (!last_name?.trim())  return res.status(400).json({ error: 'Last name is required.' });
    if (!email || !isValidEmail(email)) return res.status(400).json({ error: 'A valid email address is required.' });
    if (!password || password.length < 8) return res.status(400).json({ error: 'Password must be at least 8 characters.' });

    const emailLower = email.trim().toLowerCase();

    // Check for existing account
    const existing = await pool.query(
      'SELECT id FROM users WHERE email = $1',
      [emailLower]
    );
    if (existing.rows.length > 0) {
      return res.status(409).json({ error: 'An account with this email already exists.' });
    }

    // Hash password
    const SALT_ROUNDS = 12;
    const password_hash = await bcrypt.hash(password, SALT_ROUNDS);

    // Insert user
    const result = await pool.query(
      `INSERT INTO users
         (email, password_hash, first_name, last_name, role, interest, referral_source)
       VALUES ($1, $2, $3, $4, $5, $6, $7)
       RETURNING *`,
      [
        emailLower,
        password_hash,
        first_name.trim(),
        last_name.trim(),
        role || null,
        interest || null,
        referral_source || null,
      ]
    );

    const user = result.rows[0];
    const access_token = generateAccessToken(user);

    console.log(`[AUTH] New registration: ${emailLower}`);

    res.status(201).json({
      access_token,
      user: safeUser(user),
    });

  } catch (err) {
    console.error('[AUTH] Register error:', err.message);
    res.status(500).json({ error: 'Registration failed. Please try again.' });
  }
});

// ── POST /api/auth/login ──────────────────────────────────────────────────────
router.post('/login', authLimit, async (req, res) => {
  try {
    const { email, password } = req.body;

    if (!email || !password) {
      return res.status(400).json({ error: 'Email and password are required.' });
    }

    const emailLower = email.trim().toLowerCase();

    // Find user
    const result = await pool.query(
      'SELECT * FROM users WHERE email = $1 AND is_active = TRUE',
      [emailLower]
    );

    if (result.rows.length === 0) {
      // Use same message for missing email or wrong password (security best practice)
      return res.status(401).json({ error: 'Incorrect email or password.' });
    }

    const user = result.rows[0];

    // Check password
    const match = await bcrypt.compare(password, user.password_hash);
    if (!match) {
      return res.status(401).json({ error: 'Incorrect email or password.' });
    }

    // Update last login
    await pool.query(
      'UPDATE users SET last_login = NOW() WHERE id = $1',
      [user.id]
    );

    const access_token = generateAccessToken(user);

    console.log(`[AUTH] Login: ${emailLower}`);

    res.json({
      access_token,
      user: safeUser(user),
    });

  } catch (err) {
    console.error('[AUTH] Login error:', err.message);
    res.status(500).json({ error: 'Login failed. Please try again.' });
  }
});

// ── GET /api/auth/me ──────────────────────────────────────────────────────────
router.get('/me', requireAuth, async (req, res) => {
  try {
    const result = await pool.query(
      'SELECT * FROM users WHERE id = $1 AND is_active = TRUE',
      [req.user.id]
    );

    if (result.rows.length === 0) {
      return res.status(404).json({ error: 'User not found.' });
    }

    res.json({ user: safeUser(result.rows[0]) });

  } catch (err) {
    console.error('[AUTH] Get me error:', err.message);
    res.status(500).json({ error: 'Could not retrieve profile.' });
  }
});

// ── PATCH /api/auth/me ────────────────────────────────────────────────────────
// Updates age_verified, disclaimer_accepted, role, interest, referral_source
router.patch('/me', requireAuth, async (req, res) => {
  try {
    const allowed = ['age_verified', 'disclaimer_accepted', 'role', 'interest', 'referral_source'];
    const updates = [];
    const values  = [];
    let   idx     = 1;

    for (const field of allowed) {
      if (req.body[field] !== undefined) {
        updates.push(`${field} = $${idx}`);
        values.push(req.body[field]);
        idx++;
      }
    }

    if (updates.length === 0) {
      return res.status(400).json({ error: 'No valid fields provided.' });
    }

    values.push(req.user.id);

    const result = await pool.query(
      `UPDATE users SET ${updates.join(', ')} WHERE id = $${idx} RETURNING *`,
      values
    );

    if (result.rows.length === 0) {
      return res.status(404).json({ error: 'User not found.' });
    }

    res.json({ user: safeUser(result.rows[0]) });

  } catch (err) {
    console.error('[AUTH] Patch me error:', err.message);
    res.status(500).json({ error: 'Could not update profile.' });
  }
});

module.exports = router;
