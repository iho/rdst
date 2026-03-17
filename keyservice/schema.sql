-- RDST Key Service - D1 Database Schema
-- Run: wrangler d1 execute rdst-keyservice-db --file=schema.sql

CREATE TABLE IF NOT EXISTS users (
    email TEXT PRIMARY KEY,
    token TEXT UNIQUE NOT NULL,
    verification_token TEXT,
    verified INTEGER DEFAULT 0,
    usage_cents INTEGER DEFAULT 0,
    limit_cents INTEGER DEFAULT 500,
    created_at TEXT NOT NULL,
    verified_at TEXT,
    last_used_at TEXT,
    ip_address TEXT,
    status TEXT DEFAULT 'pending',  -- pending | active | exhausted
    email_tier TEXT DEFAULT 'business'  -- personal ($1.50) | business ($5.00)
);

CREATE INDEX IF NOT EXISTS idx_users_token ON users(token);

CREATE TABLE IF NOT EXISTS usage_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    email TEXT NOT NULL,
    model TEXT,
    input_tokens INTEGER,
    output_tokens INTEGER,
    cost_cents REAL,
    created_at TEXT NOT NULL,
    ip_address TEXT
);

CREATE INDEX IF NOT EXISTS idx_usage_email ON usage_log(email);
CREATE INDEX IF NOT EXISTS idx_usage_ip_created ON usage_log(ip_address, created_at);

CREATE TABLE IF NOT EXISTS registration_attempts (
    ip_address TEXT NOT NULL,
    attempted_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_reg_ip ON registration_attempts(ip_address);

CREATE TABLE IF NOT EXISTS settings (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

-- Default: 100 trial user slots
INSERT OR IGNORE INTO settings (key, value) VALUES ('max_trial_users', '100');
