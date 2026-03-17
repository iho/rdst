# RDST Key Service

Trial credit proxy for RDST. Gives new users $5 of free Anthropic API credits so they can try RDST without creating an Anthropic account or buying credits first.

## How It Works

```
RDST CLI                     Key Service (Cloudflare Worker + D1)         Anthropic API
   |                                      |                                    |
   |  1. rdst init                        |                                    |
   |     POST /register {email}           |                                    |
   |  ----------------------------------> |                                    |
   |     "Check your email"               | --> Resend: verification email      |
   |  <---------------------------------- |                                    |
   |                                      |                                    |
   |  2. User clicks email link           |                                    |
   |              GET /verify?token=xxx   |                                    |
   |                                      | assigns trial token                |
   |                                      | shows token on HTML page           |
   |                                      |                                    |
   |  3. User pastes token in rdst init   |                                    |
   |     Token saved to ~/.rdst/config.toml                                    |
   |                                      |                                    |
   |  4. rdst analyze / help / ask / etc  |                                    |
   |     POST /v1/messages                |                                    |
   |     x-api-key: <trial-token>         |                                    |
   |     X-RDST-Client: rdst             |                                    |
   |     X-RDST-Signature: <hmac>        |                                    |
   |  ----------------------------------> |  validate token + HMAC             |
   |                                      |  check $5 cap                      |
   |                                      |  swap key with real Anthropic key  |
   |                                      |  --------------------------------> |
   |                                      |  <-------------------------------- |
   |                                      |  count tokens, update usage        |
   |     Anthropic response               |  X-RDST-Trial-Remaining-Cents      |
   |  <---------------------------------- |                                    |
```

### Key-Type Routing (in RDST)

The RDST client determines routing based on where the API key came from. There is no `ANTHROPIC_BASE_URL` manipulation — it's purely key-type based:

```
ANTHROPIC_API_KEY set?        --yes-->  Direct to api.anthropic.com
         | no
RDST_TRIAL_TOKEN set?         --yes-->  Route to keyservice proxy
         | no
Trial token in config.toml?   --yes-->  Route to keyservice proxy
         | no
         +-->  Error: "No API key. Run rdst init or set ANTHROPIC_API_KEY"
```

Own-key users never touch the proxy. Zero latency impact for paying customers.

### Client Attestation

Trial tokens are protected by HMAC-based attestation headers so they can't be trivially used outside RDST:

- `X-RDST-Client: rdst` — presence check
- `X-RDST-Signature: <timestamp>.<hmac>` — HMAC-SHA256 of `<timestamp>.<trial_token>` using a shared secret

The shared secret (`CLIENT_SECRET`) is embedded in both `keyservice/src/index.py` and `lib/llm_manager/key_resolution.py`. This is defense-in-depth, not cryptographic security — someone with source access could extract it, but the $5 cap limits damage.

### Usage Tracking

Every proxied request logs token counts from Anthropic's response to a `usage_log` table. Cost is calculated using hardcoded model pricing (same pricing table exists in the keyservice and in RDST's `lib/functions/llm_analysis.py`). When a user's cumulative cost hits their limit (default $5), their status flips to `exhausted` and subsequent requests are rejected with a `TRIAL_EXHAUSTED` error.

### Safety Limits

- **Per-user credit cap**: $5.00 default (configurable per-user via admin)
- **Max trial users**: 100 default (configurable via admin). Limits total exposure to $500.
- **IP rate limiting**: Max 3 registration attempts per IP per hour
- **Email verification**: Must click email link before trial activates

## Architecture

| Component | Technology | Purpose |
|-----------|-----------|---------|
| Worker | Cloudflare Workers (Python/FastAPI) | HTTP routing, proxy, auth |
| Database | Cloudflare D1 (SQLite) | Users, usage logs, settings |
| Email | Resend API | Verification emails |
| DNS | `rdst-keyservice.readysetio.workers.dev` | Worker endpoint |

### Database Tables

| Table | Purpose |
|-------|---------|
| `users` | Email, trial token, usage (cents), limit (cents), status, timestamps |
| `usage_log` | Per-request log: model, input/output tokens, cost, timestamp |
| `registration_attempts` | IP + timestamp for rate limiting |
| `settings` | Key-value store (currently: `max_trial_users`) |

### API Endpoints

| Method | Path | Auth | Purpose |
|--------|------|------|---------|
| POST | `/register` | None | Register for trial (sends verification email) |
| GET | `/verify?token=xxx` | None | Verify email, show trial token |
| POST | `/v1/messages` | Trial token + HMAC | Proxy to Anthropic |
| GET | `/admin` | None (page), Bearer (API calls) | Admin web dashboard |
| GET | `/admin/status` | Bearer | Aggregate stats |
| GET | `/admin/users` | Bearer | List all users with usage |
| PUT | `/admin/users` | Bearer | Update user limit/status |
| GET | `/admin/users/log?email=...` | Bearer | Per-user request history |
| PUT | `/admin/settings` | Bearer | Update max_trial_users, etc. |
| GET | `/health` | None | Health check |

### Secrets (Cloudflare Wrangler Secrets)

| Secret | Purpose |
|--------|---------|
| `ANTHROPIC_API_KEY` | Company Anthropic API key (swapped in for trial tokens) |
| `RESEND_API_KEY` | Resend API key for sending verification emails |
| `ADMIN_SECRET` | Bearer token for admin endpoints and dashboard login |

These are set via `wrangler secret put <NAME>` and are never visible in code or logs.

## Admin Dashboard

**URL**: `https://rdst-keyservice.readysetio.workers.dev/admin`

Login with the `ADMIN_SECRET` bearer token. The token is stored in browser `sessionStorage` (clears when tab closes).

**Features**:
- Stats overview: users, active, exhausted, total spend, API requests, tokens, 24h signups
- Model usage breakdown: per-model request counts, tokens, cost
- Settings: change max trial users on the fly
- User table: email, status, request count, usage with progress bar, limit, tokens (in/out), timestamps
- Search: filter users by email
- Pagination: 25 per page default, configurable 15/25/50/100
- Edit: change any user's credit limit or status (reactivates exhausted users when limit is increased)
- Request log: expand inline (last 20) or modal view (last 100) of a user's request history

## RDST Client-Side Integration

The key resolution logic lives in `lib/llm_manager/key_resolution.py`. Key files involved:

| File | Role |
|------|------|
| `lib/llm_manager/key_resolution.py` | Shared resolver: env vars > trial token, HMAC attestation |
| `lib/llm_manager/claude_provider.py` | `base_url` + `extra_headers` params, proxy-down handling, trial exhaustion detection |
| `lib/llm_manager/llm_manager.py` | Uses resolver, passes routing to provider, propagates trial balance |
| `lib/agent/chat_agent.py` | Uses resolver for Anthropic SDK `base_url` + `default_headers` |
| `lib/llm_manager/trial_display.py` | Low-balance and exhaustion warnings |
| `lib/cli/configuration_wizard.py` | Trial registration flow in `rdst init` |
| `lib/cli/rdst_cli.py` | `TargetsConfig` trial helpers (get/set/is_active) |

### Config File (`~/.rdst/config.toml`)

```toml
[trial]
token = "550e8400-e29b-41d4-a716-446655440000"
email = "user@example.com"
status = "active"    # active | exhausted
```

## Deployment

### Current: Manual via Wrangler

```bash
cd rdst/keyservice

# Deploy code
uv run pywrangler deploy

# Update secrets
echo "sk-ant-..." | npx wrangler secret put ANTHROPIC_API_KEY
echo "re_..." | npx wrangler secret put RESEND_API_KEY
echo "your-token" | npx wrangler secret put ADMIN_SECRET

# Run database migrations
npx wrangler d1 execute rdst-keyservice-db --remote --file=schema.sql

# Check admin status
curl -H 'Authorization: Bearer <ADMIN_SECRET>' \
  https://rdst-keyservice.readysetio.workers.dev/admin/status
```

### Local Development

```bash
cd rdst/keyservice
cp .dev.vars.example .dev.vars   # Fill in local secrets
wrangler dev                     # Starts on localhost:8787 with local D1
```

### Future: Buildkite CI Integration

The keyservice can be added as an app in the `web-apps/.buildkite/` pipeline, which already deploys Cloudflare Workers (admin, cloud, docs, marketing). The pattern:

1. **Cloudflare auth** is handled via AWS Secrets Manager:
   - Secret: `frontend/cloudflare` (or create `rdst/keyservice/cloudflare`)
   - Keys: `.api_token` (Cloudflare API token) and `.account_id`
   - Injected by the `seek-oss/aws-sm` Buildkite plugin

2. **Deployment** is just `wrangler deploy` — wrangler auto-reads `CLOUDFLARE_API_TOKEN` and `CLOUDFLARE_ACCOUNT_ID` from environment.

3. **What's needed** (one-time setup):
   - Create a Cloudflare API token with Workers permissions (or reuse the existing `frontend/cloudflare` token if scoped correctly)
   - Store it in AWS Secrets Manager
   - Add `rdst-admin` as another app entry in the web-apps pipeline YAML
   - The only difference from existing JS apps: skip the `pnpm build:cf` step (Python Worker doesn't need a JS build)

4. **Recommended approach**: Keep as a separate deploy target from RDST PyPI releases. The keyservice is a Cloudflare Worker; RDST is a pip package. You'll want to hotfix the keyservice independently (adjust user limits, fix bugs) without cutting a whole RDST release.

### D1 Database Migrations

Schema changes go in `schema.sql`. All statements use `CREATE TABLE IF NOT EXISTS` and `INSERT OR IGNORE` so the file is idempotent — safe to re-run.

```bash
# Apply migrations to production
npx wrangler d1 execute rdst-keyservice-db --remote --file=schema.sql

# Query production database directly
npx wrangler d1 execute rdst-keyservice-db --remote \
  --command="SELECT email, usage_cents, status FROM users"
```

## Pricing Sync

Model pricing is hardcoded in two places that must stay in sync:

| Location | Format |
|----------|--------|
| `keyservice/src/index.py` — `CLAUDE_PRICING` | `{model_id: {"input": $/MTok, "output": $/MTok}}` |
| `lib/functions/llm_analysis.py` — `CLAUDE_PRICING` | Same format |

When Anthropic changes prices, update both. The keyservice pricing determines billing; the RDST pricing is for cost estimation display only.

## Operations

### Give a user more credits

Via the admin dashboard (Edit button), or via API:

```bash
curl -X PUT https://rdst-keyservice.readysetio.workers.dev/admin/users \
  -H 'Authorization: Bearer <ADMIN_SECRET>' \
  -H 'Content-Type: application/json' \
  -d '{"email": "user@example.com", "limit_cents": 1000}'
```

Setting `limit_cents` above their current `usage_cents` automatically reactivates an exhausted user.

### Increase the trial user cap

Via the admin dashboard (Settings section), or via API:

```bash
curl -X PUT https://rdst-keyservice.readysetio.workers.dev/admin/settings \
  -H 'Authorization: Bearer <ADMIN_SECRET>' \
  -H 'Content-Type: application/json' \
  -d '{"max_trial_users": 200}'
```

### Rotate the Anthropic API key

```bash
echo "sk-ant-new-key-here" | npx wrangler secret put ANTHROPIC_API_KEY
```

No redeployment needed — Wrangler secrets update live.

### Rotate the admin token

```bash
echo "new-admin-token" | npx wrangler secret put ADMIN_SECRET
```

Anyone logged into the dashboard will be logged out on next API call.

### View raw database

```bash
npx wrangler d1 execute rdst-keyservice-db --remote \
  --command="SELECT * FROM users ORDER BY created_at DESC"

npx wrangler d1 execute rdst-keyservice-db --remote \
  --command="SELECT * FROM usage_log ORDER BY created_at DESC LIMIT 20"

npx wrangler d1 execute rdst-keyservice-db --remote \
  --command="SELECT * FROM settings"
```
