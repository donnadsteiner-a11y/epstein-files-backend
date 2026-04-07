/**
 * DocketZero API Server
 */

const express    = require('express');
const cors       = require('cors');
const helmet     = require('helmet');
const rateLimit  = require('express-rate-limit');
const pool       = require('./db/pool');

const authRouter     = require('./routes/auth');
const contactRouter  = require('./routes/contact');
const statsRouter    = require('./routes/stats');
const searchesRouter = require('./routes/searches');
const resolveRoute   = require('./resolve-route');

const app  = express();
const PORT = process.env.PORT || 5000;

// ── Auto-migrate on startup ──────────────────────────────────────────────────
async function runMigrations() {
  try {
    await pool.query(`
      CREATE TABLE IF NOT EXISTS users (
        id                   SERIAL PRIMARY KEY,
        email                VARCHAR(255) NOT NULL UNIQUE,
        password_hash        VARCHAR(255) NOT NULL,
        first_name           VARCHAR(100) NOT NULL,
        last_name            VARCHAR(100) NOT NULL,
        role                 VARCHAR(100),
        interest             VARCHAR(200),
        referral_source      VARCHAR(200),
        age_verified         BOOLEAN NOT NULL DEFAULT FALSE,
        disclaimer_accepted  BOOLEAN NOT NULL DEFAULT FALSE,
        created_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        last_login           TIMESTAMPTZ,
        is_active            BOOLEAN NOT NULL DEFAULT TRUE
      );
      CREATE INDEX IF NOT EXISTS idx_users_email ON users(email);

      CREATE TABLE IF NOT EXISTS contact_submissions (
        id           SERIAL PRIMARY KEY,
        first_name   VARCHAR(100) NOT NULL,
        last_name    VARCHAR(100) NOT NULL,
        email        VARCHAR(255) NOT NULL,
        role         VARCHAR(100),
        subject      VARCHAR(100) NOT NULL,
        message      TEXT NOT NULL,
        file_count   INTEGER DEFAULT 0,
        ip_address   VARCHAR(45),
        created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
      );

      CREATE TABLE IF NOT EXISTS refresh_tokens (
        id          SERIAL PRIMARY KEY,
        user_id     INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
        token_hash  VARCHAR(255) NOT NULL UNIQUE,
        created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        expires_at  TIMESTAMPTZ NOT NULL,
        revoked     BOOLEAN NOT NULL DEFAULT FALSE
      );
      CREATE INDEX IF NOT EXISTS idx_refresh_tokens_user ON refresh_tokens(user_id);

      CREATE TABLE IF NOT EXISTS saved_searches (
        id           SERIAL PRIMARY KEY,
        user_id      INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
        name         VARCHAR(200) NOT NULL,
        query        TEXT,
        dataset      VARCHAR(50),
        filters      JSONB DEFAULT '{}',
        result_count INTEGER DEFAULT 0,
        pinned       BOOLEAN NOT NULL DEFAULT FALSE,
        created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
      );
      CREATE INDEX IF NOT EXISTS idx_saved_searches_user_id ON saved_searches(user_id);
    `);
    console.log('[DB] Tables ready');
  } catch (err) {
    console.error('[DB] Migration error:', err.message);
  }
}

// ── Trust proxy (required for Railway/Heroku deployments) ────────────────────
app.set('trust proxy', 1);

// ── Security headers ─────────────────────────────────────────────────────────
app.use(helmet());

// ── CORS ─────────────────────────────────────────────────────────────────────
const ALLOWED_ORIGINS = [
  'https://docketzero.com',
  'https://www.docketzero.com',
  'http://docketzero.com',
  'http://www.docketzero.com',
  'https://docketzero.dreamhosters.com',
  'http://docketzero.dreamhosters.com',
  'http://localhost:3000',
  'http://localhost:5000',
  'http://127.0.0.1:5500',
];

function isAllowedOrigin(origin) {
  return !origin || ALLOWED_ORIGINS.includes(origin);
}

app.use((req, res, next) => {
  const origin = req.get('Origin');

  if (isAllowedOrigin(origin)) {
    return next();
  }

  // Return a clean policy denial instead of sending blocked CORS requests
  // through the generic 500 error handler.
  if (req.method === 'OPTIONS') {
    return res.status(403).json({ error: 'Origin not allowed by CORS policy.' });
  }

  return res.status(403).json({ error: 'Origin not allowed by CORS policy.' });
});

app.use(cors({
  origin: (origin, callback) => {
    callback(null, isAllowedOrigin(origin));
  },
  credentials: true,
}));

// ── Body parsing ─────────────────────────────────────────────────────────────
app.use(express.json({ limit: '10mb' }));
app.use(express.urlencoded({ extended: true, limit: '10mb' }));

// ── Global rate limiting ─────────────────────────────────────────────────────
// Covers all non-resolve endpoints. /api/resolve has its own tighter limits.
app.use(rateLimit({
  windowMs: 15 * 60 * 1000,
  max: 150,
  standardHeaders: true,
  legacyHeaders: false,
  skip: (req) => req.path.startsWith('/api/resolve'),  // resolve has own limiter
  message: { error: 'Too many requests. Please try again in a few minutes.' },
}));

// ── Health check ──────────────────────────────────────────────────────────────
app.get('/health', (req, res) => {
  res.json({ status: 'ok', timestamp: new Date().toISOString() });
});

// ── Routes ────────────────────────────────────────────────────────────────────
// IMPORTANT: specific routes must come before the catch-all resolveRoute
app.use('/api/auth',    authRouter);
app.use('/api/contact', contactRouter);
app.use('/api/stats',   statsRouter);
app.use('/api/users',   searchesRouter);
app.use('/api',         resolveRoute);   // catch-all — must be last

// ── 404 ───────────────────────────────────────────────────────────────────────
app.use((req, res) => {
  res.status(404).json({ error: 'Route not found' });
});

// ── Error handler ─────────────────────────────────────────────────────────────
app.use((err, req, res, next) => {
  console.error('[ERROR]', err.message);
  res.status(err.status || 500).json({
    error: process.env.NODE_ENV === 'production'
      ? 'An unexpected error occurred.'
      : err.message,
  });
});

// ── Start ─────────────────────────────────────────────────────────────────────
app.listen(PORT, async () => {
  console.log(`DocketZero API running on port ${PORT}`);
  console.log(`Environment: ${process.env.NODE_ENV || 'development'}`);
  await runMigrations();
});
