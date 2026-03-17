"""RDST Key Service - Cloudflare Worker (Python/FastAPI)

Provides free $5 trial credits for RDST by proxying requests to Anthropic.
Users register with email, verify, and get a trial token.

Routes:
  POST /register          - Register for trial (email verification)
  GET  /verify?token=xxx  - Verify email and get trial token
  POST /v1/messages       - Proxy to Anthropic (trial token required)
  GET  /admin             - Admin web dashboard (ADMIN_SECRET required)
  GET  /admin/status      - Admin API: aggregate stats
  GET  /admin/users       - Admin API: list all users
  PUT  /admin/users       - Admin API: update a user (limit, status)
  PUT  /admin/settings    - Admin API: update service settings
"""

from fastapi import FastAPI, Request, Response
from fastapi.responses import HTMLResponse, JSONResponse
from js import fetch, Object
from pyodide.ffi import to_js as _to_js
from workers import WorkerEntrypoint

import hashlib
import hmac
import json
import re
import time
import uuid

app = FastAPI()

# Client attestation secret - must match RDST's key_resolution.py
# Loaded from ATTESTATION_SECRET Wrangler secret; falls back to env var
CLIENT_SECRET = None  # Set at request time from env

# Anthropic pricing ($ per million tokens) - matches RDST's CLAUDE_PRICING
# in lib/functions/llm_analysis.py:32-40
CLAUDE_PRICING = {
    # Sonnet family ($3/$15 per MTok)
    "claude-sonnet-4-6":          {"input": 3.0, "output": 15.0},   # Sonnet 4.6 (latest)
    "claude-sonnet-4-5-20250929": {"input": 3.0, "output": 15.0},
    "claude-sonnet-4-20250514":   {"input": 3.0, "output": 15.0},
    # Opus family ($5/$25 per MTok for 4.5+; $15/$75 for 4.0/4.1)
    "claude-opus-4-6":            {"input": 5.0, "output": 25.0},   # Opus 4.6 (latest)
    "claude-opus-4-5-20251101":   {"input": 5.0, "output": 25.0},   # Opus 4.5
    "claude-opus-4-1-20250805":   {"input": 15.0, "output": 75.0},  # Opus 4.1
    "claude-opus-4-20250514":     {"input": 15.0, "output": 75.0},  # Opus 4.0
    # Haiku family
    "claude-haiku-4-5-20251001":  {"input": 1.0, "output": 5.0},    # Haiku 4.5 (latest)
    "claude-3-5-haiku-20241022":  {"input": 0.80, "output": 4.0},   # Haiku 3.5
    "claude-3-haiku-20240307":    {"input": 0.25, "output": 1.25},  # Haiku 3 (deprecated Apr 2026)
    # Fallback for unknown models (assume Sonnet pricing)
    "default":                    {"input": 3.0, "output": 15.0},
}

EMAIL_RE = re.compile(r"^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$")

# Default maximum trial users (overridable via settings table / admin API)
DEFAULT_MAX_TRIAL_USERS = 100

# --- Email Classification ---

# Disposable/temporary email domains - block these entirely.
# Sourced from https://github.com/disposable-email-domains/disposable-email-domains
DISPOSABLE_DOMAINS = {
    # Major disposable services
    "yopmail.com", "yopmail.fr", "yopmail.net", "yopmail.gq",
    "guerrillamail.com", "guerrillamail.net", "guerrillamail.org",
    "guerrillamail.info", "guerrillamail.de", "guerrillamailblock.com",
    "grr.la", "sharklasers.com", "guerrillamail.biz",
    "mailinator.com", "mailinator.net", "mailinator.org",
    "mailinator2.com", "mailtothis.com",
    "tempmail.com", "temp-mail.org", "temp-mail.io", "temp-mail.de",
    "10minutemail.com", "10minutemail.net", "10minutemail.org",
    "throwaway.email", "throwaway.cc",
    "trashmail.com", "trashmail.net", "trashmail.org", "trashmail.me",
    "dispostable.com", "maildrop.cc", "mailnesia.com",
    "getnada.com", "nada.email", "nada.ltd",
    "tempail.com", "tempr.email", "tempmailo.com",
    "mohmal.com", "emailondeck.com",
    "fakeinbox.com", "fakemail.net",
    "mailcatch.com", "mailexpire.com", "mailforspam.com",
    "mailnull.com", "mailsac.com", "mailslurp.com",
    "mintemail.com", "mytemp.email",
    "harakirimail.com", "spamgourmet.com",
    "burnermail.io", "inboxkitten.com",
    "33mail.com", "anonaddy.me",
    "crazymailing.com", "deadfake.cf",
    "emailfake.com", "emkei.cz",
    "getairmail.com", "getonemail.com",
    "guerrillamail.win", "haltospam.com",
    "instantemailaddress.com", "jetable.org",
    "mailme.lv", "mailnator.com", "mailtemp.info",
    "nomail.xl.cx", "objectmail.com",
    "one-time.email", "otherinbox.com",
    "owlpic.com", "proxymail.eu",
    "rcpt.at", "reallymymail.com",
    "receiveee.com", "rhyta.com",
    "spambox.us",
    "spamfree24.org", "superrito.com",
    "teleworm.us", "tempomail.fr",
    "temporarymail.org", "thankyou2010.com",
    "trash-mail.at", "trash-mail.com",
    "trashymail.com", "trbvm.com",
    "wegwerfmail.de", "wegwerfmail.net",
    "wetrainbayarea.com", "wuzup.net",
    "yomail.info", "zeroe.ml",
    "zoemail.org",
    # ZeroBounce-style validation services (block signup from these)
    "zerobounce.net",
    # Catch-all disposable patterns
    "mailhero.io", "mailseal.de",
    "mytrashmail.com", "nobulk.com",
    "safetymail.info", "sogetthis.com",
    "spamevader.com", "spamspot.com",
    "tempmailaddress.com", "tmpmail.net", "tmpmail.org",
    "trash2009.com", "trashdevil.com",
    "trashdevil.de", "trashemail.de",
    "tmail.ws", "tmails.net",
    "uggsrock.com", "veryreallymymail.com",
    "vpn.st", "wasdag.com",
    "yep.it", "yogamaven.com",
}

# Personal/free email providers - $1.50 limit
PERSONAL_EMAIL_DOMAINS = {
    # Google
    "gmail.com", "googlemail.com",
    # Microsoft
    "outlook.com", "hotmail.com", "live.com", "msn.com",
    "hotmail.co.uk", "hotmail.fr", "hotmail.de", "hotmail.it",
    "hotmail.es", "hotmail.ca", "hotmail.co.jp",
    "outlook.co.uk", "outlook.fr", "outlook.de", "outlook.it",
    "outlook.es", "outlook.com.au", "outlook.co.nz",
    "live.co.uk", "live.fr", "live.de", "live.it", "live.com.au",
    # Yahoo
    "yahoo.com", "yahoo.co.uk", "yahoo.fr", "yahoo.de",
    "yahoo.it", "yahoo.es", "yahoo.ca", "yahoo.co.jp",
    "yahoo.co.in", "yahoo.com.au", "yahoo.com.br",
    "ymail.com", "rocketmail.com",
    # Apple
    "icloud.com", "me.com", "mac.com",
    # AOL
    "aol.com", "aim.com",
    # ProtonMail
    "protonmail.com", "proton.me", "pm.me",
    # Zoho (free tier)
    "zohomail.com",
    # Other major free providers
    "mail.com", "email.com", "usa.com",
    "gmx.com", "gmx.net", "gmx.de", "gmx.at",
    "web.de", "t-online.de", "freenet.de",
    "mail.ru", "yandex.com", "yandex.ru",
    "qq.com", "163.com", "126.com", "sina.com",
    "naver.com", "daum.net",
    "rediffmail.com",
    "tutanota.com", "tutamail.com", "tuta.io",
    "fastmail.com", "fastmail.fm",
    "hushmail.com",
    "inbox.com",
    "mailfence.com",
    "posteo.de", "posteo.net",
    "runbox.com",
    "startmail.com",
    "comcast.net", "verizon.net", "att.net", "sbcglobal.net",
    "bellsouth.net", "cox.net", "charter.net", "earthlink.net",
    "optonline.net", "frontier.com", "windstream.net",
}

# Credit limits by tier (internal: cents; display: ~Sonnet-equivalent tokens)
TIER_LIMIT_PERSONAL = 150   # $1.50 ≈ 275K tokens
TIER_LIMIT_BUSINESS = 500   # $5.00 ≈ 925K tokens


def _classify_email(email: str) -> tuple[str, int]:
    """Classify email and return (tier, limit_cents).

    Uses the hardcoded PERSONAL_EMAIL_DOMAINS set for fast, zero-cost
    classification. Covers ~95% of free email providers.

    Unknown domains default to "business" ($5.00) — the generous tier.

    Returns:
        ("personal", 150) for free/personal email providers
        ("business", 500) for custom/business domains
    """
    domain = email.rsplit("@", 1)[-1].lower()
    if domain in PERSONAL_EMAIL_DOMAINS:
        return "personal", TIER_LIMIT_PERSONAL
    return "business", TIER_LIMIT_BUSINESS


# Domains where dots in the local part are ignored (Gmail, Google Workspace aliases)
_DOT_INSENSITIVE_DOMAINS = {
    "gmail.com", "googlemail.com",
}


def _normalize_email(email: str) -> str:
    """Normalize email to prevent duplicate trial signups.

    Applied to ALL emails:
    - Lowercase (both local and domain)
    - Strip + aliases (user+tag@domain → user@domain)

    Additionally for Gmail/Googlemail:
    - Strip dots from local part (u.s.e.r@gmail.com → user@gmail.com)
    - Normalize googlemail.com → gmail.com
    """
    local, domain = email.rsplit("@", 1)
    local = local.lower()
    domain = domain.lower()

    # Strip + aliases for all providers
    local = local.split("+")[0]

    # Gmail-specific: dots are ignored, googlemail.com is an alias
    if domain in _DOT_INSENSITIVE_DOMAINS:
        local = local.replace(".", "")
        domain = "gmail.com"

    return f"{local}@{domain}"


def _is_disposable(email: str) -> bool:
    """Check if email uses a known disposable/temporary domain."""
    domain = email.rsplit("@", 1)[-1].lower()
    return domain in DISPOSABLE_DOMAINS


async def _check_mx_records(domain: str) -> bool:
    """Check if domain has valid MX records via Cloudflare DNS-over-HTTPS.

    Returns True if MX records exist, False otherwise.
    """
    try:
        url = f"https://cloudflare-dns.com/dns-query?name={domain}&type=MX"
        resp = await fetch(
            url,
            to_js({
                "headers": {"Accept": "application/dns-json"},
            }),
        )
        text = await resp.text()
        data = json.loads(text)
        # Status 0 = NOERROR, type 15 = MX record
        if data.get("Status") == 0:
            answers = data.get("Answer", [])
            return any(a.get("type") == 15 for a in answers)
        return False
    except Exception:
        # On DNS lookup failure, allow through (don't block on infra issues)
        return True


def to_js(obj):
    """Convert Python dict to JavaScript object."""
    return _to_js(obj, dict_converter=Object.fromEntries)


def _d1_row(js_result):
    """Convert a D1 JS proxy result to a Python dict (or None)."""
    if js_result is None or str(type(js_result)) == "<class 'pyodide.ffi.JsNull'>":
        return None
    try:
        return js_result.to_py()
    except AttributeError:
        return None


def calculate_cost_cents(model: str, input_tokens: int, output_tokens: int) -> float:
    """Calculate cost in cents. Same formula as RDST's estimate_cost()."""
    pricing = CLAUDE_PRICING.get(model, CLAUDE_PRICING["default"])
    cost_dollars = (input_tokens / 1_000_000) * pricing["input"] + \
                   (output_tokens / 1_000_000) * pricing["output"]
    return cost_dollars * 100


# Sonnet blended rate at 4:1 input/output ratio (typical for RDST):
# (4 * $3 + 1 * $15) / 5 = $5.40 per MTok
_TOKENS_PER_CENT = 1_000_000 / 540  # ~1,852 tokens per cent


def cents_to_tokens(cents: float) -> int:
    """Convert cents to approximate Sonnet-equivalent tokens."""
    return int(cents * _TOKENS_PER_CENT)


def format_tokens(tokens: int) -> str:
    """Format token count as human-readable string (e.g. '150K', '1.2M')."""
    if tokens >= 1_000_000:
        return f"{tokens / 1_000_000:.1f}M"
    if tokens >= 1_000:
        return f"{tokens // 1_000}K"
    return str(tokens)


def cents_to_token_display(cents: float) -> str:
    """Convert cents to a user-friendly token display string."""
    return format_tokens(cents_to_tokens(cents))


def _validate_attestation(trial_token: str, headers: dict, env) -> str | None:
    """Validate RDST client attestation headers.

    Returns None if valid, error message if invalid.
    """
    secret = getattr(env, "ATTESTATION_SECRET", "")
    if not secret:
        return "Server attestation secret not configured"

    client = headers.get("x-rdst-client")
    if client != "rdst":
        return "Missing or invalid X-RDST-Client header"

    sig_header = headers.get("x-rdst-signature", "")
    parts = sig_header.split(".", 1)
    if len(parts) != 2:
        return "Missing or malformed X-RDST-Signature header"

    timestamp_str, provided_hmac = parts

    try:
        timestamp = int(timestamp_str)
    except ValueError:
        return "Invalid timestamp in signature"

    # Verify timestamp is within 5 minutes
    now = int(time.time())
    if abs(now - timestamp) > 300:
        return "Signature expired"

    # Recompute and compare HMAC
    message = f"{timestamp_str}.{trial_token}"
    expected = hmac.new(
        secret.encode(), message.encode(), hashlib.sha256
    ).hexdigest()[:32]

    if not hmac.compare_digest(provided_hmac, expected):
        return "Invalid signature"

    return None


async def _get_max_trial_users(db) -> int:
    """Read max_trial_users from settings table, falling back to default."""
    try:
        row = _d1_row(await db.prepare(
            "SELECT value FROM settings WHERE key = 'max_trial_users'"
        ).first())
        if row and row["value"]:
            return int(row["value"])
    except Exception:
        pass
    return DEFAULT_MAX_TRIAL_USERS


async def _get_tier_limits(db):
    """Read configurable tier limits from settings table.

    Returns (personal_cents, business_cents), falling back to hardcoded defaults.
    """
    personal = TIER_LIMIT_PERSONAL
    business = TIER_LIMIT_BUSINESS
    try:
        row = _d1_row(await db.prepare(
            "SELECT value FROM settings WHERE key = 'default_limit_personal'"
        ).first())
        if row and row.get("value"):
            personal = int(row["value"])
    except Exception:
        pass
    try:
        row = _d1_row(await db.prepare(
            "SELECT value FROM settings WHERE key = 'default_limit_business'"
        ).first())
        if row and row.get("value"):
            business = int(row["value"])
    except Exception:
        pass
    return personal, business


def _admin_auth(request: Request, env) -> str | None:
    """Check admin authentication. Returns None if valid, error message if not."""
    auth = request.headers.get("authorization", "")
    if not auth.startswith("Bearer "):
        return "Missing Authorization header"
    token = auth[7:]
    secret = getattr(env, "ADMIN_SECRET", "")
    if not secret or not hmac.compare_digest(token.encode(), secret.encode()):
        return "Invalid admin token"
    return None


# --- Routes ---


@app.post("/register")
async def register(request: Request):
    """Register for a trial account. Sends verification email."""
    env = request.scope["env"]
    db = env.DB
    resend_key = getattr(env, "RESEND_API_KEY", "")
    service_url = getattr(env, "SERVICE_URL", "https://rdst-keyservice.readysetio.workers.dev")

    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"detail": "Invalid JSON body"}, status_code=400)

    email = (body.get("email") or "").strip().lower()
    if not email or not EMAIL_RE.match(email):
        return JSONResponse({"detail": "Invalid email address"}, status_code=400)

    # Block disposable/temporary email services
    if _is_disposable(email):
        return JSONResponse({
            "detail": "Disposable or temporary email addresses are not allowed. Please use a permanent email address.",
            "code": "DISPOSABLE_EMAIL",
        }, status_code=400)

    # Verify domain has MX records (real mail server exists)
    domain = email.rsplit("@", 1)[-1].lower()
    has_mx = await _check_mx_records(domain)
    if not has_mx:
        return JSONResponse({
            "detail": "This email domain does not appear to accept mail. Please use a valid email address.",
            "code": "INVALID_DOMAIN",
        }, status_code=400)

    # Normalize email to prevent duplicate signups via dot tricks / plus aliases
    normalized_email = _normalize_email(email)

    # Classify email tier and determine credit limit
    email_tier, limit_cents = _classify_email(normalized_email)

    # Override limit_cents with configurable defaults from settings table
    personal_limit, business_limit = await _get_tier_limits(db)
    limit_cents = personal_limit if email_tier == "personal" else business_limit

    # Rate limit: max 3 registrations per IP per hour
    ip = request.headers.get("cf-connecting-ip", "unknown")
    one_hour_ago = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(time.time() - 3600))

    rate_check = _d1_row(await db.prepare(
        "SELECT COUNT(*) as cnt FROM registration_attempts WHERE ip_address = ? AND attempted_at > ?"
    ).bind(ip, one_hour_ago).first())

    if rate_check and rate_check["cnt"] >= 3:
        return JSONResponse({"detail": "Too many registration attempts. Try again later."}, status_code=429)

    # Check trial user cap
    max_users = await _get_max_trial_users(db)
    user_count = _d1_row(await db.prepare("SELECT COUNT(*) as cnt FROM users").first())
    if user_count and user_count["cnt"] >= max_users:
        return JSONResponse({
            "code": "TRIAL_FULL",
            "detail": "The RDST free trial program is currently full. "
                      "Please email hello@readyset.io for access, or use your own Anthropic API key.",
        }, status_code=503)

    # Check if normalized email already registered (prevents dot/plus alias duplicates)
    existing = _d1_row(await db.prepare("SELECT status FROM users WHERE email = ?").bind(normalized_email).first())
    if existing:
        return JSONResponse({"detail": "This email is already registered."}, status_code=409)

    # Generate tokens
    trial_token = str(uuid.uuid4())
    verification_token = str(uuid.uuid4())
    now = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

    # Insert user with normalized email (prevents duplicate signups)
    # Verification email is sent to the original address so it reaches the user
    await db.prepare(
        "INSERT INTO users (email, token, verification_token, created_at, ip_address, limit_cents, email_tier) VALUES (?, ?, ?, ?, ?, ?, ?)"
    ).bind(normalized_email, trial_token, verification_token, now, ip, limit_cents, email_tier).run()

    # Record registration attempt
    await db.prepare(
        "INSERT INTO registration_attempts (ip_address, attempted_at) VALUES (?, ?)"
    ).bind(ip, now).run()

    # Send verification email via Resend
    verify_url = f"{service_url}/verify?token={verification_token}"

    limit_token_display = cents_to_token_display(limit_cents)
    limit_display = f"{limit_token_display} tokens"

    email_sent = False
    email_error = None

    if resend_key:
        email_body = f"""
<div style="max-width:520px;margin:0 auto;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;color:#1e293b;">
  <div style="text-align:center;padding:32px 0 24px;">
    <img src="https://readyset.io/manifest/apple-touch-icon.png" alt="ReadySet" width="48" height="48" style="border-radius:10px;">
  </div>
  <h2 style="text-align:center;font-size:22px;margin:0 0 8px;">Verify your RDST trial account</h2>
  <p style="text-align:center;color:#64748b;margin:0 0 32px;font-size:15px;">Click below to activate your free {limit_display} trial.</p>
  <div style="text-align:center;margin:0 0 32px;">
    <a href="{verify_url}" style="background:#0f172a;color:#ffffff;padding:14px 32px;text-decoration:none;border-radius:8px;display:inline-block;font-weight:600;font-size:15px;letter-spacing:0.3px;">Verify Email</a>
  </div>
  <p style="text-align:center;color:#94a3b8;font-size:13px;margin:0 0 8px;">Or copy this URL:</p>
  <p style="text-align:center;color:#64748b;font-size:12px;word-break:break-all;margin:0 0 32px;">{verify_url}</p>
  <hr style="border:none;border-top:1px solid #e2e8f0;margin:24px 0;">
  <p style="text-align:center;color:#94a3b8;font-size:12px;">If you didn't request this, you can safely ignore this email.</p>
</div>
"""
        try:
            resend_resp = await fetch(
                "https://api.resend.com/emails",
                to_js({
                    "method": "POST",
                    "headers": {
                        "Authorization": f"Bearer {resend_key}",
                        "Content-Type": "application/json",
                    },
                    "body": json.dumps({
                        "from": "Readyset <noreply@readyset.io>",
                        "to": [email],
                        "subject": "Verify your RDST trial account",
                        "html": email_body,
                    }),
                }),
            )
            if resend_resp.status in (200, 201):
                email_sent = True
            else:
                # Resend returned an error — read the response for details
                try:
                    resend_text = await resend_resp.text()
                    resend_data = json.loads(resend_text)
                    email_error = resend_data.get("message", f"Email service error (HTTP {resend_resp.status})")
                except Exception:
                    email_error = f"Email service error (HTTP {resend_resp.status})"
        except Exception as e:
            email_error = str(e)

    # Build response with delivery feedback
    response_data = {
        "message": "Check your email for verification link",
        "email_sent": email_sent,
        "email_tier": email_tier,
        "limit_display": limit_display,
    }

    if not email_sent and resend_key:
        # Email failed to send — tell the user immediately
        # Don't delete the user record (they can retry verification)
        response_data["message"] = "Registration saved, but the verification email could not be sent."
        response_data["email_error"] = email_error or "Unknown email delivery error"
        response_data["hint"] = (
            "This may mean the email address is invalid or unreachable. "
            "Double-check your email and try 'rdst configure llm' again, or contact hello@readyset.io."
        )
        # Clean up the failed registration so they can retry with correct email
        await db.prepare("DELETE FROM users WHERE email = ?").bind(normalized_email).run()
        return JSONResponse(response_data, status_code=422)

    return JSONResponse(response_data)


@app.get("/verify")
async def verify(request: Request):
    """Verify email and show trial token."""
    env = request.scope["env"]
    db = env.DB
    token = request.query_params.get("token", "")

    if not token:
        return HTMLResponse(_html_page("Invalid Link", "No verification token provided."), status_code=400)

    user = _d1_row(await db.prepare(
        "SELECT email, token, verified, limit_cents FROM users WHERE verification_token = ?"
    ).bind(token).first())

    if not user:
        return HTMLResponse(_html_page("Invalid Link", "This verification link is invalid or has expired."), status_code=404)

    limit_display = cents_to_token_display(user.get('limit_cents') or 500) + " tokens"

    if user["verified"]:
        token_val = user['token']
        return HTMLResponse(_html_page(
            "Already Verified",
            f'<p>Your account is already verified.</p>'
            f'<div style="background:#f3f4f6;padding:16px;border-radius:8px;font-family:monospace;word-break:break-all;position:relative;margin:16px 0;">'
            f'{token_val}'
            f'<button onclick="navigator.clipboard.writeText(\'{token_val}\');this.textContent=\'Copied!\';setTimeout(()=>this.textContent=\'Copy\',2000)" '
            f'style="position:absolute;top:8px;right:8px;background:#0f172a;color:white;border:none;padding:6px 14px;border-radius:6px;cursor:pointer;font-size:13px;font-weight:500;">Copy</button>'
            f'</div>'
        ))

    # Activate the user
    now = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    await db.prepare(
        "UPDATE users SET verified = 1, status = 'active', verified_at = ? WHERE email = ?"
    ).bind(now, user["email"]).run()

    token_val = user["token"]
    return HTMLResponse(_html_page(
        "Email Verified!",
        f'<p>Your RDST trial is now active with <strong>{limit_display}</strong> free.</p>'
        f'<p style="margin:20px 0;color:#64748b;">Your trial token:</p>'
        f'<div style="background:#f3f4f6;padding:16px;border-radius:8px;font-family:monospace;word-break:break-all;position:relative;" id="token-box">'
        f'{token_val}'
        f'<button onclick="navigator.clipboard.writeText(\'{token_val}\');this.textContent=\'Copied!\';setTimeout(()=>this.textContent=\'Copy\',2000)" '
        f'style="position:absolute;top:8px;right:8px;background:#0f172a;color:white;border:none;padding:6px 14px;border-radius:6px;cursor:pointer;font-size:13px;font-weight:500;">Copy</button>'
        f'</div>'
        f'<h3 style="margin-top:28px;">Next Steps</h3>'
        f'<ol style="line-height:1.8;">'
        f'<li>Copy the token above</li>'
        f'<li>Go back to your terminal where <code>rdst init</code> is running</li>'
        f'<li>Paste the token when prompted</li>'
        f'</ol>'
        f'<p style="margin-top:24px;color:#64748b;font-size:14px;">Or export it directly:</p>'
        f'<pre style="background:#1e293b;color:#e2e8f0;padding:12px;border-radius:6px;overflow-x:auto;font-size:13px;">'
        f'export RDST_TRIAL_TOKEN="{token_val}"</pre>'
    ))


@app.post("/v1/messages")
async def proxy_messages(request: Request):
    """Proxy LLM requests to Anthropic with trial token validation."""
    t_start = time.time()
    env = request.scope["env"]
    db = env.DB

    # 1. Extract trial token
    trial_token = request.headers.get("x-api-key", "")
    if not trial_token:
        return JSONResponse({"code": "UNAUTHORIZED", "detail": "Missing x-api-key header"}, status_code=401)

    # 2. Validate client attestation
    headers_dict = {k.lower(): v for k, v in request.headers.items()}
    attestation_error = _validate_attestation(trial_token, headers_dict, env)
    if attestation_error:
        return JSONResponse({"code": "INVALID_CLIENT", "detail": attestation_error}, status_code=403)

    t_attest = time.time()

    # 3. Look up user + count distinct tokens used from this IP in the last hour
    ip = request.headers.get("cf-connecting-ip", "unknown")
    one_hour_ago = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(time.time() - 3600))

    user = _d1_row(await db.prepare(
        "SELECT email, usage_cents, limit_cents, status, "
        "(SELECT COUNT(DISTINCT email) FROM usage_log WHERE ip_address = ? AND created_at > ?) as ip_token_count "
        "FROM users WHERE token = ? AND verified = 1"
    ).bind(ip, one_hour_ago, trial_token).first())

    t_db_lookup = time.time()

    if not user:
        return JSONResponse({"code": "UNAUTHORIZED", "detail": "Invalid trial token"}, status_code=401)

    if (user.get("ip_token_count") or 0) > 3:
        return JSONResponse({
            "code": "IP_RATE_LIMIT",
            "detail": "Too many different trial accounts used from this IP address. Try again later.",
        }, status_code=429)

    if user["status"] == "exhausted" or user["usage_cents"] >= user["limit_cents"]:
        limit_token_display = cents_to_token_display(user['limit_cents'])
        return JSONResponse({
            "code": "TRIAL_EXHAUSTED",
            "detail": f"Trial tokens exhausted ({limit_token_display} tokens used). Get your own key at https://console.anthropic.com/",
        }, status_code=403)

    if user["status"] != "active":
        return JSONResponse({"code": "UNAUTHORIZED", "detail": "Trial account is not active"}, status_code=401)

    # 4. Forward to Anthropic
    real_api_key = getattr(env, "ANTHROPIC_API_KEY", "")
    proxy_target = getattr(env, "PROXY_TARGET", "https://api.anthropic.com")

    # Build forwarded headers (strip attestation headers)
    forward_headers = {}
    skip_headers = {"host", "x-rdst-client", "x-rdst-signature", "content-length", "transfer-encoding"}
    for key, value in request.headers.items():
        if key.lower() not in skip_headers:
            if key.lower() == "x-api-key":
                forward_headers[key] = real_api_key  # Swap trial token for real key
            else:
                forward_headers[key] = value

    body = await request.body()
    body_str = body.decode("utf-8")

    t_pre_forward = time.time()

    try:
        anthropic_resp = await fetch(
            f"{proxy_target}/v1/messages",
            to_js({
                "method": "POST",
                "headers": forward_headers,
                "body": body_str,
            }),
        )
    except Exception as e:
        return JSONResponse({"code": "UPSTREAM_ERROR", "detail": f"Anthropic API error: {e}"}, status_code=502)

    t_anthropic_done = time.time()

    # 5. Read response
    resp_status = anthropic_resp.status
    resp_text = await anthropic_resp.text()

    t_read_body = time.time()

    # 6. Track usage (only on success)
    if 200 <= resp_status < 300:
        try:
            resp_data = json.loads(resp_text)
            usage = resp_data.get("usage", {})
            input_tokens = usage.get("input_tokens", 0)
            output_tokens = usage.get("output_tokens", 0)
            model = resp_data.get("model", "unknown")

            cost = calculate_cost_cents(model, input_tokens, output_tokens)
            cost_rounded = round(cost)
            now = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

            # Atomic usage update — prevents TOCTOU race where concurrent requests
            # all read the same usage_cents, pass the limit check, and the last write wins.
            # This UPDATE increments in SQL so concurrent requests accumulate correctly.
            result = _d1_row(await db.prepare(
                "UPDATE users SET "
                "  usage_cents = usage_cents + ?, "
                "  last_used_at = ?, "
                "  status = CASE WHEN usage_cents + ? >= limit_cents THEN 'exhausted' ELSE 'active' END "
                "WHERE email = ? "
                "RETURNING usage_cents, limit_cents"
            ).bind(cost_rounded, now, cost_rounded, user["email"]).first())

            t_db_update = time.time()

            # Log usage (includes IP for multi-token rate limiting)
            await db.prepare(
                "INSERT INTO usage_log (email, model, input_tokens, output_tokens, cost_cents, created_at, ip_address) VALUES (?, ?, ?, ?, ?, ?, ?)"
            ).bind(user["email"], model, input_tokens, output_tokens, cost, now, ip).run()

            t_db_log = time.time()

            # Add remaining balance header — use actual DB values from atomic update
            new_usage = result["usage_cents"] if result else cost_rounded
            limit = result["limit_cents"] if result else user["limit_cents"]
            remaining = max(0, limit - new_usage)

            # Build timing breakdown (all in ms)
            timing_parts = [
                f'attest;dur={((t_attest - t_start) * 1000):.1f}',
                f'db-lookup;dur={((t_db_lookup - t_attest) * 1000):.1f}',
                f'prep;dur={((t_pre_forward - t_db_lookup) * 1000):.1f}',
                f'anthropic;dur={((t_anthropic_done - t_pre_forward) * 1000):.1f}',
                f'read-body;dur={((t_read_body - t_anthropic_done) * 1000):.1f}',
                f'db-update;dur={((t_db_update - t_read_body) * 1000):.1f}',
                f'db-log;dur={((t_db_log - t_db_update) * 1000):.1f}',
                f'total;dur={((t_db_log - t_start) * 1000):.1f}',
            ]

            resp_headers = {
                "X-RDST-Trial-Remaining-Cents": str(remaining),
                "X-RDST-Trial-Limit-Cents": str(limit),
                "Server-Timing": ", ".join(timing_parts),
            }

            # Forward Anthropic response headers we care about
            for h in ["content-type", "x-request-id"]:
                val = anthropic_resp.headers.get(h)
                if val:
                    resp_headers[h] = val

            return Response(content=resp_text, status_code=resp_status, headers=resp_headers)

        except Exception:
            # If usage tracking fails, still return the response
            pass

    # For error responses or tracking failure, pass through as-is
    return Response(content=resp_text, status_code=resp_status, headers={"content-type": "application/json"})


@app.get("/admin/status")
async def admin_status(request: Request):
    """Admin dashboard - usage stats."""
    env = request.scope["env"]
    auth_error = _admin_auth(request, env)
    if auth_error:
        return JSONResponse({"detail": auth_error}, status_code=401)

    db = env.DB
    now = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    day_ago = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(time.time() - 86400))

    # Aggregate stats
    total = _d1_row(await db.prepare("SELECT COUNT(*) as cnt FROM users").first())
    active = _d1_row(await db.prepare("SELECT COUNT(*) as cnt FROM users WHERE status = 'active'").first())
    exhausted = _d1_row(await db.prepare("SELECT COUNT(*) as cnt FROM users WHERE status = 'exhausted'").first())
    pending = _d1_row(await db.prepare("SELECT COUNT(*) as cnt FROM users WHERE status = 'pending'").first())
    usage = _d1_row(await db.prepare("SELECT SUM(usage_cents) as total FROM users").first())
    recent = _d1_row(await db.prepare(
        "SELECT COUNT(*) as cnt FROM users WHERE created_at > ?"
    ).bind(day_ago).first())

    # Request stats from usage_log (no new writes — just reading what's there)
    req_stats = _d1_row(await db.prepare(
        "SELECT COUNT(*) as total_requests, "
        "COALESCE(SUM(input_tokens), 0) as total_input_tokens, "
        "COALESCE(SUM(output_tokens), 0) as total_output_tokens "
        "FROM usage_log"
    ).first())

    # Model breakdown
    model_result = await db.prepare(
        "SELECT model, COUNT(*) as requests, "
        "SUM(input_tokens) as input_tok, SUM(output_tokens) as output_tok, "
        "SUM(cost_cents) as cost "
        "FROM usage_log GROUP BY model ORDER BY requests DESC"
    ).all()
    models = []
    try:
        for row in model_result.results.to_py():
            models.append({
                "model": row["model"],
                "requests": row["requests"],
                "input_tokens": row["input_tok"] or 0,
                "output_tokens": row["output_tok"] or 0,
                "cost_dollars": round((row["cost"] or 0) / 100, 4),
            })
    except Exception:
        pass

    # Tier breakdown
    personal_count = _d1_row(await db.prepare(
        "SELECT COUNT(*) as cnt FROM users WHERE email_tier = 'personal'"
    ).first())
    business_count = _d1_row(await db.prepare(
        "SELECT COUNT(*) as cnt FROM users WHERE email_tier = 'business' OR email_tier IS NULL"
    ).first())

    total_dollars = (usage["total"] or 0) / 100
    max_users = await _get_max_trial_users(db)
    personal_limit, business_limit = await _get_tier_limits(db)

    # Calculate max exposure based on actual tier mix and configured limits
    personal_n = (personal_count or {}).get("cnt", 0)
    business_n = (business_count or {}).get("cnt", 0)
    max_exposure = (personal_n * personal_limit + business_n * business_limit) / 100

    return JSONResponse({
        "users": {
            "total": total["cnt"],
            "active": active["cnt"],
            "exhausted": exhausted["cnt"],
            "pending": pending["cnt"],
            "personal": personal_n,
            "business": business_n,
            "limit": max_users,
            "remaining_slots": max(0, max_users - total["cnt"]),
        },
        "requests": {
            "total": (req_stats or {}).get("total_requests", 0),
            "total_input_tokens": (req_stats or {}).get("total_input_tokens", 0),
            "total_output_tokens": (req_stats or {}).get("total_output_tokens", 0),
        },
        "defaults": {
            "limit_personal_cents": personal_limit,
            "limit_business_cents": business_limit,
        },
        "models": models,
        "usage_dollars": round(total_dollars, 2),
        "usage_tokens": cents_to_token_display(total_dollars * 100),
        "max_exposure_dollars": round(max_exposure, 2),
        "registrations_24h": recent["cnt"],
        "timestamp": now,
    })


@app.put("/admin/settings")
async def admin_update_settings(request: Request):
    """Update service settings. Requires ADMIN_SECRET Bearer token.

    Supported settings:
      - max_trial_users (int): Maximum number of trial registrations allowed
    """
    env = request.scope["env"]
    auth_error = _admin_auth(request, env)
    if auth_error:
        return JSONResponse({"detail": auth_error}, status_code=401)

    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"detail": "Invalid JSON body"}, status_code=400)

    db = env.DB
    updated = {}

    # max_trial_users
    if "max_trial_users" in body:
        try:
            val = int(body["max_trial_users"])
            if val < 0:
                return JSONResponse({"detail": "max_trial_users must be non-negative"}, status_code=400)
        except (ValueError, TypeError):
            return JSONResponse({"detail": "max_trial_users must be an integer"}, status_code=400)

        await db.prepare(
            "INSERT OR REPLACE INTO settings (key, value) VALUES ('max_trial_users', ?)"
        ).bind(str(val)).run()
        updated["max_trial_users"] = val

    # default_limit_personal (cents)
    if "default_limit_personal" in body:
        try:
            val = int(body["default_limit_personal"])
            if val < 0:
                return JSONResponse({"detail": "default_limit_personal must be non-negative"}, status_code=400)
        except (ValueError, TypeError):
            return JSONResponse({"detail": "default_limit_personal must be an integer (cents)"}, status_code=400)
        await db.prepare(
            "INSERT OR REPLACE INTO settings (key, value) VALUES ('default_limit_personal', ?)"
        ).bind(str(val)).run()
        updated["default_limit_personal"] = val

    # default_limit_business (cents)
    if "default_limit_business" in body:
        try:
            val = int(body["default_limit_business"])
            if val < 0:
                return JSONResponse({"detail": "default_limit_business must be non-negative"}, status_code=400)
        except (ValueError, TypeError):
            return JSONResponse({"detail": "default_limit_business must be an integer (cents)"}, status_code=400)
        await db.prepare(
            "INSERT OR REPLACE INTO settings (key, value) VALUES ('default_limit_business', ?)"
        ).bind(str(val)).run()
        updated["default_limit_business"] = val

    if not updated:
        return JSONResponse({"detail": "No valid settings provided. Supported: max_trial_users, default_limit_personal, default_limit_business"}, status_code=400)

    return JSONResponse({"updated": updated, "message": "Settings updated"})


@app.get("/admin/users")
async def admin_list_users(request: Request):
    """List all trial users with usage details and request counts."""
    env = request.scope["env"]
    auth_error = _admin_auth(request, env)
    if auth_error:
        return JSONResponse({"detail": auth_error}, status_code=401)

    db = env.DB
    result = await db.prepare(
        "SELECT u.email, u.usage_cents, u.limit_cents, u.status, u.verified, "
        "u.created_at, u.verified_at, u.last_used_at, u.ip_address, "
        "u.email_tier, "
        "COALESCE(l.req_count, 0) as request_count, "
        "COALESCE(l.total_in, 0) as total_input_tokens, "
        "COALESCE(l.total_out, 0) as total_output_tokens "
        "FROM users u LEFT JOIN ("
        "  SELECT email, COUNT(*) as req_count, "
        "  SUM(input_tokens) as total_in, SUM(output_tokens) as total_out "
        "  FROM usage_log GROUP BY email"
        ") l ON u.email = l.email "
        "ORDER BY u.created_at DESC"
    ).all()

    users = []
    try:
        rows = result.results.to_py()
    except Exception:
        rows = []

    for row in rows:
        users.append({
            "email": row["email"],
            "usage_cents": row["usage_cents"],
            "limit_cents": row["limit_cents"],
            "usage_dollars": round(row["usage_cents"] / 100, 2),
            "limit_dollars": round(row["limit_cents"] / 100, 2),
            "usage_tokens": cents_to_token_display(row["usage_cents"]),
            "limit_tokens": cents_to_token_display(row["limit_cents"]),
            "status": row["status"],
            "email_tier": row.get("email_tier", "business"),
            "verified": bool(row["verified"]),
            "created_at": row["created_at"],
            "verified_at": row["verified_at"],
            "last_used_at": row["last_used_at"],
            "ip_address": row["ip_address"],
            "request_count": row["request_count"],
            "total_input_tokens": row["total_input_tokens"],
            "total_output_tokens": row["total_output_tokens"],
        })

    return JSONResponse({"users": users})


@app.put("/admin/users")
async def admin_update_user(request: Request):
    """Update a specific user. Requires ADMIN_SECRET Bearer token.

    Body: {"email": "user@example.com", "limit_cents": 1000, "status": "active"}
    - limit_cents: set new credit limit (in cents)
    - status: set status (active, exhausted, pending)
    """
    env = request.scope["env"]
    auth_error = _admin_auth(request, env)
    if auth_error:
        return JSONResponse({"detail": auth_error}, status_code=401)

    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"detail": "Invalid JSON body"}, status_code=400)

    email = body.get("email", "").strip().lower()
    if not email:
        return JSONResponse({"detail": "email is required"}, status_code=400)

    db = env.DB

    # Check user exists
    user = _d1_row(await db.prepare("SELECT email, status, usage_cents FROM users WHERE email = ?").bind(email).first())
    if not user:
        return JSONResponse({"detail": f"User {email} not found"}, status_code=404)

    updated = {}

    if "limit_cents" in body:
        try:
            val = int(body["limit_cents"])
            if val < 0:
                return JSONResponse({"detail": "limit_cents must be non-negative"}, status_code=400)
        except (ValueError, TypeError):
            return JSONResponse({"detail": "limit_cents must be an integer"}, status_code=400)
        await db.prepare("UPDATE users SET limit_cents = ? WHERE email = ?").bind(val, email).run()
        updated["limit_cents"] = val
        # If increasing limit past usage, reactivate
        if val > user["usage_cents"] and user["status"] == "exhausted":
            await db.prepare("UPDATE users SET status = 'active' WHERE email = ?").bind(email).run()
            updated["status"] = "active"

    if "status" in body:
        val = body["status"]
        if val not in ("active", "exhausted", "pending"):
            return JSONResponse({"detail": "status must be: active, exhausted, or pending"}, status_code=400)
        await db.prepare("UPDATE users SET status = ? WHERE email = ?").bind(val, email).run()
        updated["status"] = val

    if not updated:
        return JSONResponse({"detail": "No valid fields. Supported: limit_cents, status"}, status_code=400)

    return JSONResponse({"email": email, "updated": updated})


@app.get("/admin/users/log")
async def admin_user_log(request: Request):
    """Get usage log for a specific user. ?email=user@example.com&limit=50"""
    env = request.scope["env"]
    auth_error = _admin_auth(request, env)
    if auth_error:
        return JSONResponse({"detail": auth_error}, status_code=401)

    email = request.query_params.get("email", "").strip().lower()
    if not email:
        return JSONResponse({"detail": "email query param required"}, status_code=400)

    try:
        limit = min(int(request.query_params.get("limit", "50")), 200)
    except (ValueError, TypeError):
        limit = 50
    db = env.DB

    result = await db.prepare(
        "SELECT model, input_tokens, output_tokens, cost_cents, created_at "
        "FROM usage_log WHERE email = ? ORDER BY created_at DESC LIMIT ?"
    ).bind(email, limit).all()

    logs = []
    try:
        for row in result.results.to_py():
            logs.append({
                "model": row["model"],
                "input_tokens": row["input_tokens"],
                "output_tokens": row["output_tokens"],
                "cost_cents": round(row["cost_cents"], 4) if row["cost_cents"] else 0,
                "created_at": row["created_at"],
            })
    except Exception:
        pass

    return JSONResponse({"email": email, "log": logs})


@app.get("/admin")
async def admin_dashboard(request: Request):
    """Serve the admin web dashboard HTML."""
    return HTMLResponse(_admin_html())


@app.get("/health")
async def health():
    """Health check endpoint."""
    return JSONResponse({"status": "ok"})


# --- Helpers ---


def _html_page(title: str, body: str) -> str:
    """Generate a simple HTML page."""
    return f"""<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>RDST - {title}</title>
    <style>
        body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; max-width: 600px; margin: 40px auto; padding: 0 20px; color: #1e293b; }}
        h1 {{ color: #2563eb; }}
        code {{ background: #f3f4f6; padding: 2px 6px; border-radius: 4px; font-size: 14px; }}
        pre {{ background: #1e293b; color: #e2e8f0; padding: 16px; border-radius: 8px; overflow-x: auto; }}
        a {{ color: #2563eb; }}
    </style>
</head>
<body>
    <div style="text-align:center;padding:16px 0 8px;">
        <img src="https://readyset.io/manifest/apple-touch-icon.png" alt="ReadySet" width="40" height="40" style="border-radius:8px;">
    </div>
    <h1>{title}</h1>
    {body}
    <hr style="margin-top:40px;">
    <p style="color:#94a3b8;font-size:13px;">RDST - Readyset Data and SQL Toolkit</p>
</body>
</html>"""


def _admin_html() -> str:
    """Generate the admin dashboard HTML page."""
    return """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>RDST Admin</title>
<style>
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;background:#f8fafc;color:#1e293b;min-height:100vh}
.login{display:flex;align-items:center;justify-content:center;min-height:100vh;background:#0f172a}
.login-box{background:#fff;padding:40px;border-radius:12px;width:380px;text-align:center}
.login-box h1{font-size:20px;margin-bottom:8px}
.login-box p{color:#64748b;font-size:14px;margin-bottom:24px}
.login-box input{width:100%;padding:12px;border:1px solid #e2e8f0;border-radius:8px;font-size:14px;margin-bottom:16px}
.login-box button{width:100%;padding:12px;background:#0f172a;color:#fff;border:none;border-radius:8px;font-size:14px;font-weight:600;cursor:pointer}
.login-box button:hover{background:#1e293b}
.login-box .err{color:#ef4444;font-size:13px;margin-top:8px;display:none}
header{background:#0f172a;color:#fff;padding:16px 24px;display:flex;align-items:center;justify-content:space-between}
header h1{font-size:18px;font-weight:600}
header button{background:#334155;color:#fff;border:none;padding:8px 16px;border-radius:6px;cursor:pointer;font-size:13px}
header button:hover{background:#475569}
.wrap{max-width:1200px;margin:0 auto;padding:24px}
.cards{display:grid;grid-template-columns:repeat(auto-fit,minmax(160px,1fr));gap:16px;margin-bottom:24px}
.card{background:#fff;border-radius:10px;padding:20px;border:1px solid #e2e8f0}
.card .label{font-size:11px;color:#64748b;text-transform:uppercase;letter-spacing:.5px;margin-bottom:4px}
.card .val{font-size:26px;font-weight:700}
.card .sub{font-size:12px;color:#94a3b8;margin-top:4px}
.section{background:#fff;border-radius:10px;border:1px solid #e2e8f0;margin-bottom:24px;overflow:hidden}
.section-hdr{padding:16px 20px;border-bottom:1px solid #e2e8f0;display:flex;align-items:center;justify-content:space-between}
.section-hdr h2{font-size:15px;font-weight:600}
table{width:100%;border-collapse:collapse;font-size:13px}
th{text-align:left;padding:10px 16px;background:#f8fafc;font-weight:600;color:#64748b;font-size:11px;text-transform:uppercase;letter-spacing:.3px;white-space:nowrap}
td{padding:10px 16px;border-top:1px solid #f1f5f9}
tr:hover td{background:#f8fafc}
.badge{display:inline-block;padding:2px 10px;border-radius:99px;font-size:11px;font-weight:600}
.badge-active{background:#dcfce7;color:#16a34a}
.badge-exhausted{background:#fee2e2;color:#dc2626}
.badge-pending{background:#fef3c7;color:#d97706}
.progress{height:6px;background:#e2e8f0;border-radius:3px;overflow:hidden;width:80px;display:inline-block;vertical-align:middle;margin-left:6px}
.progress-bar{height:100%;border-radius:3px;transition:width .3s}
.actions button{background:none;border:1px solid #e2e8f0;padding:4px 10px;border-radius:4px;cursor:pointer;font-size:12px;margin-right:4px}
.actions button:hover{background:#f1f5f9}
.modal-bg{position:fixed;top:0;left:0;right:0;bottom:0;background:rgba(0,0,0,.5);display:flex;align-items:center;justify-content:center;z-index:100;display:none}
.modal{background:#fff;border-radius:12px;padding:24px;width:480px;max-height:80vh;overflow-y:auto}
.modal h3{margin-bottom:16px;font-size:16px}
.modal label{display:block;font-size:13px;color:#64748b;margin-bottom:4px;margin-top:12px}
.modal input,.modal select{width:100%;padding:10px;border:1px solid #e2e8f0;border-radius:6px;font-size:14px}
.modal .btns{margin-top:20px;display:flex;gap:8px;justify-content:flex-end}
.modal .btns button{padding:8px 20px;border-radius:6px;font-size:13px;font-weight:600;cursor:pointer}
.modal .btn-cancel{background:#f1f5f9;border:1px solid #e2e8f0;color:#64748b}
.modal .btn-save{background:#0f172a;border:none;color:#fff}
.settings-row{padding:16px 20px;display:flex;align-items:center;gap:16px;border-bottom:1px solid #f1f5f9}
.settings-row:last-child{border-bottom:none}
.settings-row label{font-size:13px;font-weight:600;min-width:160px}
.settings-row input{width:120px;padding:8px;border:1px solid #e2e8f0;border-radius:6px;font-size:14px}
.settings-row button{padding:6px 14px;background:#0f172a;color:#fff;border:none;border-radius:6px;font-size:12px;cursor:pointer}
.toast{position:fixed;bottom:24px;right:24px;background:#0f172a;color:#fff;padding:12px 20px;border-radius:8px;font-size:13px;z-index:200;display:none}
.num{font-variant-numeric:tabular-nums}
.muted{color:#94a3b8;font-size:12px}
.log-row td{font-size:12px;padding:6px 12px}
.log-row:hover td{background:#f0f9ff}
.expand-row td{padding:0;border-top:none}
.expand-inner{background:#f8fafc;padding:12px 16px;border-top:1px solid #e2e8f0}
</style>
</head>
<body>

<!-- Login Screen -->
<div id="loginScreen" class="login">
<div class="login-box">
  <img src="https://readyset.io/manifest/apple-touch-icon.png" alt="ReadySet" width="40" height="40" style="border-radius:8px;margin-bottom:16px;">
  <h1>RDST Admin</h1>
  <p>Enter your admin token to continue</p>
  <input type="password" id="tokenInput" placeholder="Admin token" onkeydown="if(event.key==='Enter')doLogin()">
  <button onclick="doLogin()">Sign In</button>
  <div class="err" id="loginErr">Invalid token</div>
</div>
</div>

<!-- Dashboard -->
<div id="dashboard" style="display:none">
<header>
  <div style="display:flex;align-items:center;gap:12px">
    <img src="https://readyset.io/manifest/apple-touch-icon.png" alt="" width="28" height="28" style="border-radius:6px;">
    <h1>RDST Trial Admin</h1>
  </div>
  <div style="display:flex;gap:8px;align-items:center">
    <button onclick="loadAll()">Refresh</button>
    <button onclick="doLogout()">Logout</button>
  </div>
</header>

<div class="wrap">
  <!-- Stats Cards -->
  <div class="cards">
    <div class="card"><div class="label">Users</div><div class="val" id="statTotal">-</div><div class="sub" id="statSlots">-</div></div>
    <div class="card"><div class="label">Active</div><div class="val" id="statActive">-</div></div>
    <div class="card"><div class="label">Exhausted</div><div class="val" id="statExhausted">-</div></div>
    <div class="card"><div class="label">Total Spent</div><div class="val" id="statSpent">-</div><div class="sub" id="statExposure">-</div></div>
    <div class="card"><div class="label">API Requests</div><div class="val" id="statRequests">-</div><div class="sub" id="statTokens">-</div></div>
    <div class="card"><div class="label">24h Signups</div><div class="val" id="statRecent">-</div></div>
  </div>

  <!-- Model Breakdown -->
  <div class="section" id="modelSection" style="display:none">
    <div class="section-hdr"><h2>Model Usage Breakdown</h2></div>
    <div style="overflow-x:auto">
      <table>
        <thead><tr><th>Model</th><th>Requests</th><th>Input Tokens</th><th>Output Tokens</th><th>Cost</th></tr></thead>
        <tbody id="modelBody"></tbody>
      </table>
    </div>
  </div>

  <!-- Settings -->
  <div class="section">
    <div class="section-hdr"><h2>Settings</h2></div>
    <div class="settings-row">
      <label>Max Trial Users</label>
      <input type="number" id="setMaxUsers" min="0" style="width:100px">
      <button onclick="saveMaxUsers()">Save</button>
    </div>
    <div class="settings-row">
      <label>Personal Email Limit</label>
      <span style="color:#94a3b8;font-size:11px;margin-right:4px">$</span>
      <input type="number" id="setLimitPersonal" min="0" step="0.50" style="width:100px">
      <span style="color:#94a3b8;font-size:11px;margin-left:2px;margin-right:8px">(gmail, yahoo, etc.)</span>
      <button onclick="saveDefaultLimits()">Save</button>
    </div>
    <div class="settings-row">
      <label>Business Email Limit</label>
      <span style="color:#94a3b8;font-size:11px;margin-right:4px">$</span>
      <input type="number" id="setLimitBusiness" min="0" step="0.50" style="width:100px">
      <span style="color:#94a3b8;font-size:11px;margin-left:2px">(company domains)</span>
    </div>
    <div class="settings-row" style="border-bottom:none">
      <label></label>
      <span style="color:#94a3b8;font-size:11px">Changes apply to new registrations only. Edit existing users in the table below.</span>
    </div>
  </div>

  <!-- Users Table -->
  <div class="section">
    <div class="section-hdr">
      <h2>Users</h2>
      <div style="display:flex;align-items:center;gap:12px">
        <input type="text" id="searchInput" placeholder="Search by email..." oninput="renderUsers()" style="padding:6px 12px;border:1px solid #e2e8f0;border-radius:6px;font-size:13px;width:220px">
        <span style="color:#64748b;font-size:12px" id="userCount"></span>
      </div>
    </div>
    <div style="padding:0 20px 12px;display:flex;align-items:center;justify-content:space-between;border-bottom:1px solid #e2e8f0" id="paginationBar">
      <div style="display:flex;align-items:center;gap:6px">
        <button onclick="changePage(-1)" id="prevBtn" style="background:none;border:1px solid #e2e8f0;padding:4px 10px;border-radius:4px;cursor:pointer;font-size:12px">&laquo; Prev</button>
        <span id="pageInfo" style="font-size:12px;color:#64748b;min-width:100px;text-align:center"></span>
        <button onclick="changePage(1)" id="nextBtn" style="background:none;border:1px solid #e2e8f0;padding:4px 10px;border-radius:4px;cursor:pointer;font-size:12px">Next &raquo;</button>
      </div>
      <select id="pageSizeSelect" onchange="pageSize=parseInt(this.value);currentPage=0;renderUsers()" style="padding:4px 8px;border:1px solid #e2e8f0;border-radius:4px;font-size:12px">
        <option value="15">15 per page</option>
        <option value="25" selected>25 per page</option>
        <option value="50">50 per page</option>
        <option value="100">100 per page</option>
      </select>
    </div>
    <div style="overflow-x:auto">
      <table>
        <thead>
          <tr>
            <th></th>
            <th>Email</th>
            <th>Tier</th>
            <th>Status</th>
            <th>Requests</th>
            <th>Usage</th>
            <th>Limit</th>
            <th>Tokens (In/Out)</th>
            <th>Last Used</th>
            <th>Registered</th>
            <th>Actions</th>
          </tr>
        </thead>
        <tbody id="usersBody"></tbody>
      </table>
    </div>
  </div>
</div>
</div>

<!-- Edit User Modal -->
<div class="modal-bg" id="editModal">
<div class="modal">
  <h3>Edit User</h3>
  <div style="font-size:14px;color:#0f172a;font-weight:600" id="editEmail"></div>
  <label>Credit Limit ($)</label>
  <input type="number" id="editLimit" step="0.5" min="0">
  <label>Status</label>
  <select id="editStatus">
    <option value="active">Active</option>
    <option value="exhausted">Exhausted</option>
    <option value="pending">Pending</option>
  </select>
  <div class="btns">
    <button class="btn-cancel" onclick="closeModal()">Cancel</button>
    <button class="btn-save" onclick="saveUser()">Save</button>
  </div>
</div>
</div>

<!-- Log Modal (per-user request history) -->
<div class="modal-bg" id="logModal">
<div class="modal" style="width:640px">
  <h3>Request Log</h3>
  <div style="font-size:14px;color:#0f172a;font-weight:600;margin-bottom:12px" id="logEmail"></div>
  <div style="overflow-x:auto;max-height:400px;overflow-y:auto">
    <table>
      <thead><tr><th>Time</th><th>Model</th><th>Input Tok</th><th>Output Tok</th><th>Cost</th></tr></thead>
      <tbody id="logBody"></tbody>
    </table>
  </div>
  <div class="btns" style="margin-top:12px"><button class="btn-cancel" onclick="closeLogModal()">Close</button></div>
</div>
</div>

<!-- Toast -->
<div class="toast" id="toast"></div>

<script>
const BASE = location.origin;
let TOKEN = sessionStorage.getItem('rdst_admin') || '';

function headers() {
  return {'Authorization': 'Bearer ' + TOKEN, 'Content-Type': 'application/json'};
}

function fmt(n) { return n != null ? n.toLocaleString() : '-'; }
function fmtK(n) { return n >= 1000000 ? (n/1000000).toFixed(1)+'M' : n >= 1000 ? (n/1000).toFixed(1)+'K' : String(n); }

async function doLogin() {
  TOKEN = document.getElementById('tokenInput').value.trim();
  try {
    const r = await fetch(BASE + '/admin/status', {headers: headers()});
    if (r.ok) {
      sessionStorage.setItem('rdst_admin', TOKEN);
      document.getElementById('loginScreen').style.display = 'none';
      document.getElementById('dashboard').style.display = 'block';
      loadAll();
    } else {
      document.getElementById('loginErr').style.display = 'block';
    }
  } catch(e) {
    document.getElementById('loginErr').style.display = 'block';
  }
}

function doLogout() {
  sessionStorage.removeItem('rdst_admin');
  TOKEN = '';
  document.getElementById('dashboard').style.display = 'none';
  document.getElementById('loginScreen').style.display = 'flex';
  document.getElementById('tokenInput').value = '';
  document.getElementById('loginErr').style.display = 'none';
}

function toast(msg) {
  const t = document.getElementById('toast');
  t.textContent = msg;
  t.style.display = 'block';
  setTimeout(() => t.style.display = 'none', 3000);
}

async function loadAll() {
  await Promise.all([loadStatus(), loadUsers()]);
}

async function loadStatus() {
  try {
    const r = await fetch(BASE + '/admin/status', {headers: headers()});
    if (!r.ok) { doLogout(); return; }
    const d = await r.json();
    document.getElementById('statTotal').textContent = d.users.total;
    document.getElementById('statSlots').textContent = d.users.remaining_slots + ' of ' + d.users.limit + ' slots left'
      + ' (' + (d.users.personal||0) + ' personal, ' + (d.users.business||0) + ' business)';
    document.getElementById('statActive').textContent = d.users.active;
    document.getElementById('statExhausted').textContent = d.users.exhausted;
    document.getElementById('statSpent').textContent = (d.usage_tokens || '0') + ' tokens ($' + d.usage_dollars.toFixed(2) + ')';
    document.getElementById('statExposure').textContent = 'Max exposure: $' + d.max_exposure_dollars.toFixed(2);
    document.getElementById('statRequests').textContent = fmt(d.requests.total);
    const totalTok = d.requests.total_input_tokens + d.requests.total_output_tokens;
    document.getElementById('statTokens').textContent = fmtK(totalTok) + ' tokens total';
    document.getElementById('statRecent').textContent = d.registrations_24h;
    document.getElementById('setMaxUsers').value = d.users.limit;

    // Default tier limits
    if (d.defaults) {
      document.getElementById('setLimitPersonal').value = ((d.defaults.limit_personal_cents || 150) / 100).toFixed(2);
      document.getElementById('setLimitBusiness').value = ((d.defaults.limit_business_cents || 500) / 100).toFixed(2);
    }

    // Model breakdown
    const ms = document.getElementById('modelSection');
    const mb = document.getElementById('modelBody');
    if (d.models && d.models.length > 0) {
      ms.style.display = 'block';
      mb.innerHTML = d.models.map(m =>
        '<tr>' +
        '<td style="font-weight:500;font-size:12px">' + esc(m.model) + '</td>' +
        '<td class="num">' + fmt(m.requests) + '</td>' +
        '<td class="num">' + fmtK(m.input_tokens) + '</td>' +
        '<td class="num">' + fmtK(m.output_tokens) + '</td>' +
        '<td class="num">$' + m.cost_dollars.toFixed(4) + '</td>' +
        '</tr>'
      ).join('');
    } else {
      ms.style.display = 'none';
    }
  } catch(e) { console.error(e); }
}

let allUsers = [];
let currentPage = 0;
let pageSize = 25;

async function loadUsers() {
  try {
    const r = await fetch(BASE + '/admin/users', {headers: headers()});
    if (!r.ok) return;
    const d = await r.json();
    allUsers = d.users;
    currentPage = 0;
    renderUsers();
  } catch(e) { console.error(e); }
}

function renderUsers() {
  const query = (document.getElementById('searchInput').value || '').toLowerCase().trim();
  const filtered = query ? allUsers.filter(u => u.email.toLowerCase().includes(query)) : allUsers;
  const totalPages = Math.max(1, Math.ceil(filtered.length / pageSize));
  if (currentPage >= totalPages) currentPage = totalPages - 1;
  if (currentPage < 0) currentPage = 0;
  const start = currentPage * pageSize;
  const page = filtered.slice(start, start + pageSize);

  document.getElementById('userCount').textContent = filtered.length + (query ? ' matched' : ' users');
  document.getElementById('pageInfo').textContent = 'Page ' + (currentPage+1) + ' of ' + totalPages;
  document.getElementById('prevBtn').disabled = currentPage === 0;
  document.getElementById('nextBtn').disabled = currentPage >= totalPages - 1;
  document.getElementById('paginationBar').style.display = filtered.length > pageSize ? 'flex' : 'none';

  const tbody = document.getElementById('usersBody');
  tbody.innerHTML = page.map(u => {
    const pct = u.limit_cents > 0 ? Math.min(100, (u.usage_cents / u.limit_cents) * 100) : 0;
    const barColor = pct >= 100 ? '#ef4444' : pct >= 75 ? '#f59e0b' : '#22c55e';
    const badge = u.status === 'active' ? 'badge-active' : u.status === 'exhausted' ? 'badge-exhausted' : 'badge-pending';
    const tierBadge = (u.email_tier || 'business') === 'personal' ? 'badge-pending' : 'badge-active';
    const tierLabel = (u.email_tier || 'business') === 'personal' ? 'personal' : 'business';
    const lastUsed = u.last_used_at ? u.last_used_at.replace('T',' ').replace('Z','') : '-';
    const created = u.created_at ? u.created_at.split('T')[0] : '-';
    const hasLog = u.request_count > 0;
    return '<tr>' +
      '<td style="width:30px;text-align:center">' +
        (hasLog ? '<button onclick="toggleLog(this,\\''+esc(u.email)+'\\')" style="background:none;border:none;cursor:pointer;font-size:14px" title="View request log">&#9654;</button>' : '') +
      '</td>' +
      '<td style="font-weight:500">' + esc(u.email) + '</td>' +
      '<td><span class="badge ' + tierBadge + '" style="font-size:10px">' + tierLabel + '</span></td>' +
      '<td><span class="badge ' + badge + '">' + u.status + '</span></td>' +
      '<td class="num" style="text-align:center">' + u.request_count + '</td>' +
      '<td>' + (u.usage_tokens||'0') + '<br><span class="muted" style="font-size:10px">$' + u.usage_dollars.toFixed(2) + '</span>' +
        '<div class="progress"><div class="progress-bar" style="width:' + pct + '%;background:' + barColor + '"></div></div></td>' +
      '<td>' + (u.limit_tokens||'0') + '<br><span class="muted" style="font-size:10px">$' + u.limit_dollars.toFixed(2) + '</span></td>' +
      '<td class="muted num">' + fmtK(u.total_input_tokens) + ' / ' + fmtK(u.total_output_tokens) + '</td>' +
      '<td class="muted">' + lastUsed + '</td>' +
      '<td class="muted">' + created + '</td>' +
      '<td class="actions">' +
        '<button onclick="openEdit(\\''+esc(u.email)+'\\','+u.limit_cents+',\\''+u.status+'\\')">Edit</button>' +
        (hasLog ? '<button onclick="openLog(\\''+esc(u.email)+'\\')">Log</button>' : '') +
      '</td>' +
      '</tr>';
  }).join('');
  if (!page.length) {
    tbody.innerHTML = '<tr><td colspan="11" style="text-align:center;color:#94a3b8;padding:24px">No users found</td></tr>';
  }
}

function changePage(delta) {
  currentPage += delta;
  renderUsers();
}

function esc(s) { const d = document.createElement('div'); d.textContent = s; return d.innerHTML; }

async function toggleLog(btn, email) {
  const tr = btn.closest('tr');
  const next = tr.nextElementSibling;
  // If already expanded, collapse
  if (next && next.classList.contains('expand-row')) {
    next.remove();
    btn.innerHTML = '&#9654;';
    return;
  }
  btn.innerHTML = '&#9660;';
  const expandTr = document.createElement('tr');
  expandTr.className = 'expand-row';
  expandTr.innerHTML = '<td colspan="11"><div class="expand-inner">Loading...</div></td>';
  tr.after(expandTr);
  try {
    const r = await fetch(BASE + '/admin/users/log?email=' + encodeURIComponent(email) + '&limit=20', {headers: headers()});
    if (!r.ok) { expandTr.querySelector('.expand-inner').textContent = 'Failed to load'; return; }
    const d = await r.json();
    if (!d.log.length) { expandTr.querySelector('.expand-inner').textContent = 'No requests yet'; return; }
    let html = '<table style="width:100%;font-size:12px"><thead><tr><th>Time</th><th>Model</th><th>In Tokens</th><th>Out Tokens</th><th>Cost</th></tr></thead><tbody>';
    d.log.forEach(l => {
      html += '<tr class="log-row"><td>' + (l.created_at||'').replace('T',' ').replace('Z','') + '</td>'
        + '<td>' + esc(l.model||'') + '</td>'
        + '<td class="num">' + fmt(l.input_tokens) + '</td>'
        + '<td class="num">' + fmt(l.output_tokens) + '</td>'
        + '<td class="num">$' + (l.cost_cents/100).toFixed(4) + '</td></tr>';
    });
    html += '</tbody></table>';
    if (d.log.length >= 20) html += '<div style="text-align:center;padding:8px;color:#64748b;font-size:11px">Showing last 20 requests. <a href="#" onclick="openLog(\\''+esc(email)+'\\');return false">View all</a></div>';
    expandTr.querySelector('.expand-inner').innerHTML = html;
  } catch(e) { expandTr.querySelector('.expand-inner').textContent = 'Error: ' + e.message; }
}

async function openLog(email) {
  document.getElementById('logEmail').textContent = email;
  document.getElementById('logBody').innerHTML = '<tr><td colspan="5" style="text-align:center;color:#64748b">Loading...</td></tr>';
  document.getElementById('logModal').style.display = 'flex';
  try {
    const r = await fetch(BASE + '/admin/users/log?email=' + encodeURIComponent(email) + '&limit=100', {headers: headers()});
    if (!r.ok) { document.getElementById('logBody').innerHTML = '<tr><td colspan="5">Failed</td></tr>'; return; }
    const d = await r.json();
    if (!d.log.length) { document.getElementById('logBody').innerHTML = '<tr><td colspan="5" style="text-align:center;color:#64748b">No requests</td></tr>'; return; }
    document.getElementById('logBody').innerHTML = d.log.map(l =>
      '<tr class="log-row"><td>' + (l.created_at||'').replace('T',' ').replace('Z','') + '</td>'
      + '<td style="font-size:11px">' + esc(l.model||'') + '</td>'
      + '<td class="num">' + fmt(l.input_tokens) + '</td>'
      + '<td class="num">' + fmt(l.output_tokens) + '</td>'
      + '<td class="num">$' + (l.cost_cents/100).toFixed(4) + '</td></tr>'
    ).join('');
  } catch(e) { document.getElementById('logBody').innerHTML = '<tr><td colspan="5">Error</td></tr>'; }
}
function closeLogModal() { document.getElementById('logModal').style.display = 'none'; }

async function saveMaxUsers() {
  const val = parseInt(document.getElementById('setMaxUsers').value);
  if (isNaN(val) || val < 0) { toast('Invalid value'); return; }
  try {
    const r = await fetch(BASE + '/admin/settings', {method:'PUT', headers: headers(), body: JSON.stringify({max_trial_users: val})});
    if (r.ok) { toast('Max users updated to ' + val); loadStatus(); }
    else { const d = await r.json(); toast('Error: ' + d.detail); }
  } catch(e) { toast('Error: ' + e.message); }
}

async function saveDefaultLimits() {
  const personal = parseFloat(document.getElementById('setLimitPersonal').value);
  const business = parseFloat(document.getElementById('setLimitBusiness').value);
  if (isNaN(personal) || personal < 0 || isNaN(business) || business < 0) { toast('Invalid limit value'); return; }
  const body = {
    default_limit_personal: Math.round(personal * 100),
    default_limit_business: Math.round(business * 100),
  };
  try {
    const r = await fetch(BASE + '/admin/settings', {method:'PUT', headers: headers(), body: JSON.stringify(body)});
    if (r.ok) { toast('Default limits updated: personal $' + personal.toFixed(2) + ', business $' + business.toFixed(2)); loadStatus(); }
    else { const d = await r.json(); toast('Error: ' + d.detail); }
  } catch(e) { toast('Error: ' + e.message); }
}

let editingEmail = '';
function openEdit(email, limitCents, status) {
  editingEmail = email;
  document.getElementById('editEmail').textContent = email;
  document.getElementById('editLimit').value = (limitCents / 100).toFixed(2);
  document.getElementById('editStatus').value = status;
  document.getElementById('editModal').style.display = 'flex';
}
function closeModal() { document.getElementById('editModal').style.display = 'none'; }

async function saveUser() {
  const limitDollars = parseFloat(document.getElementById('editLimit').value);
  const status = document.getElementById('editStatus').value;
  if (isNaN(limitDollars) || limitDollars < 0) { toast('Invalid limit'); return; }
  const body = {email: editingEmail, limit_cents: Math.round(limitDollars * 100), status: status};
  try {
    const r = await fetch(BASE + '/admin/users', {method:'PUT', headers: headers(), body: JSON.stringify(body)});
    if (r.ok) { toast('Updated ' + editingEmail); closeModal(); loadAll(); }
    else { const d = await r.json(); toast('Error: ' + d.detail); }
  } catch(e) { toast('Error: ' + e.message); }
}

// Auto-login if token in session
if (TOKEN) {
  fetch(BASE + '/admin/status', {headers: headers()}).then(r => {
    if (r.ok) {
      document.getElementById('loginScreen').style.display = 'none';
      document.getElementById('dashboard').style.display = 'block';
      loadAll();
    } else {
      sessionStorage.removeItem('rdst_admin');
      TOKEN = '';
    }
  });
}
</script>
</body>
</html>"""


# --- Cloudflare Worker Entry Point ---


class Default(WorkerEntrypoint):
    """Cloudflare Worker entry point - bridges incoming requests to FastAPI via ASGI."""

    async def fetch(self, request):
        import asgi_bridge

        return await asgi_bridge.fetch(app, request.js_object, self.env)
