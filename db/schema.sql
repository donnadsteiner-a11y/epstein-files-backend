-- DocketZero Database Schema
-- Run this once against your Render PostgreSQL database
-- psql $DATABASE_URL -f db/schema.sql

-- ── Users table ───────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS users (
  id                   SERIAL PRIMARY KEY,
  email                VARCHAR(255) NOT NULL UNIQUE,
  password_hash        VARCHAR(255) NOT NULL,
  first_name           VARCHAR(100) NOT NULL,
  last_name            VARCHAR(100) NOT NULL,
  plan_tier            VARCHAR(20) NOT NULL DEFAULT 'tier_1',

  -- Profile fields (from signup form - all optional)
  role                 VARCHAR(100),
  interest             VARCHAR(200),
  referral_source      VARCHAR(200),

  -- Gate flags
  age_verified         BOOLEAN NOT NULL DEFAULT FALSE,
  disclaimer_accepted  BOOLEAN NOT NULL DEFAULT FALSE,

  -- Metadata
  created_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  last_login           TIMESTAMPTZ,
  is_active            BOOLEAN NOT NULL DEFAULT TRUE
);

-- Index for fast login lookups
CREATE INDEX IF NOT EXISTS idx_users_email ON users(email);

-- ── Contact submissions table ─────────────────────────────────────────────────
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

-- ── Refresh tokens table (for JWT refresh flow) ───────────────────────────────
CREATE TABLE IF NOT EXISTS refresh_tokens (
  id          SERIAL PRIMARY KEY,
  user_id     INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  token_hash  VARCHAR(255) NOT NULL UNIQUE,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  expires_at  TIMESTAMPTZ NOT NULL,
  revoked     BOOLEAN NOT NULL DEFAULT FALSE
);

CREATE INDEX IF NOT EXISTS idx_refresh_tokens_user ON refresh_tokens(user_id);

-- Saved searches
CREATE TABLE IF NOT EXISTS saved_searches (
  id             SERIAL PRIMARY KEY,
  user_id        INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  name           VARCHAR(200) NOT NULL,
  query          TEXT,
  dataset        VARCHAR(50),
  filters        JSONB DEFAULT '{}',
  result_count   INTEGER DEFAULT 0,
  pinned         BOOLEAN NOT NULL DEFAULT FALSE,
  investigation  VARCHAR(50) NOT NULL DEFAULT 'epstein',
  created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_saved_searches_user_id ON saved_searches(user_id);

-- User-created reports
CREATE TABLE IF NOT EXISTS saved_reports (
  id             SERIAL PRIMARY KEY,
  user_id        INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  title          VARCHAR(200) NOT NULL,
  summary        TEXT,
  investigation  VARCHAR(50) NOT NULL DEFAULT 'epstein',
  report_type    VARCHAR(50) NOT NULL DEFAULT 'search_snapshot',
  query          TEXT,
  dataset        VARCHAR(50),
  filters        JSONB DEFAULT '{}',
  result_count   INTEGER DEFAULT 0,
  snapshot       JSONB DEFAULT '{}',
  created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_saved_reports_user_id ON saved_reports(user_id);
