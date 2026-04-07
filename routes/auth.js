/**
 * Auth routes
 *   POST /api/auth/register   â€” create account
 *   POST /api/auth/login      â€” sign in
 *   PATCH /api/auth/me        â€” update age_verified, disclaimer_accepted, role etc.
 *   GET  /api/auth/me         â€” get current user profile
 */

const express    = require('express');
const bcrypt     = require('bcrypt');
const jwt        = require('jsonwebtoken');
const nodemailer = require('nodemailer');
const rateLimit  = require('express-rate-limit');
const pool       = require('../db/pool');
const { requireAuth } = require('../middleware/auth');
const { getPlanEntitlements } = require('../lib/access');

const router = express.Router();

// Tighter rate limit on auth endpoints â€” 10 attempts per 15 min
const authLimit = rateLimit({
  windowMs: 15 * 60 * 1000,
  max: 10,
  message: { error: 'Too many attempts. Please wait 15 minutes and try again.' },
  skipSuccessfulRequests: true,
});

// â”€â”€ Helpers â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

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
  const membership = getPlanEntitlements(row.plan_tier);
  return {
    id:                  row.id,
    email:               row.email,
    first_name:          row.first_name,
    last_name:           row.last_name,
    plan_tier:           membership.plan_tier,
    membership,
    role:                row.role,
    interest:            row.interest,
    referral_source:     row.referral_source,
    age_verified:        row.age_verified,
    disclaimer_accepted: row.disclaimer_accepted,
    created_at:          row.created_at,
  };
}

// ── Welcome email ────────────────────────────────────────────────────────────
async function sendWelcomeEmail(user) {
  const transporter = nodemailer.createTransport({
    host:   process.env.SMTP_HOST,
    port:   parseInt(process.env.SMTP_PORT || '587'),
    secure: process.env.SMTP_PORT === '465',
    auth: {
      user: process.env.SMTP_USER,
      pass: process.env.SMTP_PASS,
    },
    tls: { rejectUnauthorized: process.env.NODE_ENV === 'production' },
  });

  const html = `
    <div style="font-family:Arial,sans-serif;max-width:600px;color:#0f1720">
      <div style="background:#102131;padding:20px 28px;border-radius:8px 8px 0 0">
        <img src="https://docketzero.com/assets/docketzero_logo_horizontal.png"
             alt="DocketZero" style="height:36px;margin-bottom:4px" />
      </div>
      <div style="background:#f9fbfd;padding:28px;border:1px solid #dbe4ec;border-top:none">
        <h2 style="margin:0 0 12px;font-size:20px;color:#0f1720">
          Welcome to DocketZero, ${user.first_name}.
        </h2>
        <p style="margin:0 0 16px;font-size:15px;line-height:1.7;color:#304355">
          Your account has been created. You now have access to the full DocketZero
          research archive — including documents the DOJ has quietly removed from
          their servers.
        </p>
        <div style="margin-bottom:20px">
          <a href="https://docketzero.com/dashboard.html"
             style="display:inline-block;background:#102131;color:#fff;
                    padding:12px 24px;border-radius:10px;font-weight:700;
                    font-size:14px;text-decoration:none">
            Go to your dashboard →
          </a>
        </div>
        <p style="margin:0 0 10px;font-size:14px;font-weight:700;color:#0f1720">
          What you can do:
        </p>
        <ul style="margin:0 0 20px;padding-left:20px;font-size:14px;
                   line-height:1.9;color:#304355">
          <li>Search 1.4 million preserved documents by name, EFTA number, or dataset</li>
          <li>Browse 1,614 named individuals identified across the archive</li>
          <li>Save searches to your dashboard for quick access</li>
          <li>Track files the DOJ has removed — DocketZero serves preserved copies</li>
        </ul>
        <p style="margin:0;font-size:14px;color:#304355;line-height:1.7">
          Questions? Reach us at
          <a href="mailto:support@docketzero.com"
             style="color:#193146;font-weight:700">support@docketzero.com</a>.
        </p>
      </div>
      <div style="background:#eef3f8;padding:14px 28px;border-radius:0 0 8px 8px;
                  border:1px solid #dbe4ec;border-top:none">
        <p style="margin:0;font-size:12px;color:#7a96ae">
          DocketZero · Preserving the public record ·
          <a href="https://docketzero.com" style="color:#7a96ae">docketzero.com</a>
        </p>
        <p style="margin:6px 0 0;font-size:11px;color:#9ab0c4">
          Inclusion in these records does not imply criminal conduct or legal liability.
          All individuals are presumed innocent unless proven guilty in a court of law.
        </p>
      </div>
    </div>
  `;

  await transporter.sendMail({
    from:    `"DocketZero" <${process.env.SMTP_USER}>`,
    to:      user.email,
    subject: `Welcome to DocketZero — your account is ready`,
    html,
  });

  console.log(`[AUTH] Welcome email sent to ${user.email}`);
}

// â”€â”€ POST /api/auth/register â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
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

    // Send welcome email — fire and forget, don't block the response
    sendWelcomeEmail(user).catch(err =>
      console.error('[AUTH] Welcome email failed:', err.message)
    );

    res.status(201).json({
      access_token,
      user: safeUser(user),
    });

  } catch (err) {
    console.error('[AUTH] Register error:', err.message);
    res.status(500).json({ error: 'Registration failed. Please try again.' });
  }
});

// â”€â”€ POST /api/auth/login â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
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

// â”€â”€ GET /api/auth/me â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
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

// â”€â”€ PATCH /api/auth/me â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
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
